import io
import json
import zipfile
from pathlib import Path

import pytest

from ray_de.documents import extract_document, read_document, ReadError, MAX_BYTES


def docx_bytes(text="Hello Word"):
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        archive.writestr("word/document.xml", '<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"><w:body><w:p><w:r><w:t>' + text + '</w:t></w:r></w:p><w:tbl><w:tr><w:tc><w:p><w:r><w:t>0012</w:t></w:r></w:p></w:tc></w:tr></w:tbl></w:body></w:document>')
    return stream.getvalue()


def test_markdown_preserves_content_but_redacts_credentials():
    result = extract_document("notes.MD", b"# Plans\nHello\nclient_secret=private-value")
    assert "# Plans" in json.dumps(result)
    assert "private-value" not in json.dumps(result)
    assert result["sha256"] and not result["truncated"]


def test_word_paragraphs_and_table_cells_are_read_in_order():
    result = extract_document("notes.docx", docx_bytes())
    text = json.dumps(result)
    assert text.index("Hello Word") < text.index("0012")
    assert "table" in text.lower()


def test_excel_keeps_sheet_cell_identifiers_formulas_and_hidden_sheet_warning():
    from openpyxl import Workbook
    wb = Workbook()
    wb.active.title = "Customers"
    wb.active.append(["0012", 25, "=B1*2"])
    wb.create_sheet("Hidden").sheet_state = "hidden"
    wb["Hidden"]["A1"] = "Also read"
    stream = io.BytesIO()
    wb.save(stream)
    result = extract_document("data.xlsx", stream.getvalue())
    text = json.dumps(result)
    assert all(value in text for value in ["0012", "B1", "=B1*2", "Customers", "Hidden", "Also read"])
    assert "not calculated" in text


def test_pdf_text_and_scanned_pdf_limit():
    from pypdf import PdfWriter
    from pypdf.generic import DictionaryObject, NameObject, DecodedStreamObject
    writer = PdfWriter()
    page = writer.add_blank_page(300, 300)
    font = DictionaryObject({NameObject("/Type"): NameObject("/Font"), NameObject("/Subtype"): NameObject("/Type1"), NameObject("/BaseFont"): NameObject("/Helvetica")})
    page[NameObject("/Resources")] = DictionaryObject({NameObject("/Font"): DictionaryObject({NameObject("/F1"): font})})
    content = DecodedStreamObject()
    content.set_data(b"BT /F1 12 Tf 30 200 Td (Quarterly revenue) Tj ET")
    page[NameObject("/Contents")] = writer._add_object(content)
    writer.add_blank_page(300, 300)
    stream = io.BytesIO()
    writer.write(stream)
    result = extract_document("report.pdf", stream.getvalue())
    assert result["sections"][0]["location"] == "Page 1"
    assert "Quarterly revenue" in json.dumps(result)
    assert "OCR" in " ".join(result["warnings"])


def test_real_legacy_excel_values_keep_string_identifiers():
    result = extract_document("reading_sample.xls", (Path(__file__).parent / "fixtures" / "reading_sample.xls").read_bytes())
    text = json.dumps(result)
    assert all(value in text for value in ["0012", "25", "Customers", "A1", "Hai"])
    assert "saved cell values" in text


def test_encrypted_pdf_has_a_clear_error_without_requesting_password():
    from pypdf import PdfWriter
    writer = PdfWriter()
    writer.add_blank_page(100, 100)
    writer.encrypt("synthetic-password")
    stream = io.BytesIO()
    writer.write(stream)
    with pytest.raises(ReadError, match="unlocked copy"):
        extract_document("locked.pdf", stream.getvalue())


@pytest.mark.parametrize("name,data", [("bad.docx", b"broken"), ("bad.pdf", b"broken"), ("bad.xlsx", b"broken"), ("bad.xls", b"broken"), ("run.exe", b"MZ"), ("big.md", b"a" * (MAX_BYTES + 1))], ids=["docx", "pdf", "xlsx", "xls", "unsupported", "large"])
def test_invalid_and_large_inputs_have_fixed_safe_errors(name, data):
    with pytest.raises(ReadError) as error:
        extract_document(name, data)
    assert "Traceback" not in str(error.value) and "broken" not in str(error.value)


def test_zip_bomb_and_entity_expansion_are_rejected():
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("word/document.xml", "a" * 2_000_000)
    with pytest.raises(ReadError):
        extract_document("bomb.docx", stream.getvalue())
    with pytest.raises(ReadError):
        extract_document("entity.docx", docx_bytes('&bad;'))


def test_truncation_is_explicit_and_worker_reads_real_docx():
    result = extract_document("large.md", b"a" * 100_000)
    assert result["truncated"] and result["warnings"]
    assert len(json.dumps(result)) < 90_000
    assert "Hello Word" in json.dumps(read_document("read.docx", docx_bytes()))


def test_document_worker_cannot_import_code_from_the_inspected_directory(tmp_path, monkeypatch):
    package = tmp_path / "ray_de"
    package.mkdir()
    (package / "__init__.py").write_text("from pathlib import Path\nPath('unexpected_execution').write_text('ran')\n")
    (package / "documents.py").write_text("print('{}')\n")
    monkeypatch.chdir(tmp_path)
    result = read_document("notes.md", b"Read this safely")
    assert "Read this safely" in json.dumps(result)
    assert not (tmp_path / "unexpected_execution").exists()
