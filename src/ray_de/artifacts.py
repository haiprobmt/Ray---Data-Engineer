"""Host materialization and validation of model-authored project source files."""
import argparse
import ast
import base64
import json
from pathlib import Path, PurePosixPath

from .memory import safe_text


def materialize(project, artifacts):
    pending = []
    seen = set()
    for artifact in artifacts:
        rel = PurePosixPath(artifact.path)
        from .tenant_repository import is_native_path, validate_native, TEXT_SUFFIXES
        native = bool(project.config.fabric.tenant and is_native_path(artifact.path))
        if (rel.is_absolute() or ".." in rel.parts or "\\" in artifact.path or ":" in artifact.path
                or any(p.startswith(".") and not (native and p == ".platform" and p == rel.name) for p in rel.parts)
                or rel.name.lower() in {"agents.md", "skill.md", "config.yaml"}
                or not (native and (rel.name == ".platform" or rel.suffix.lower() in TEXT_SUFFIXES)) and rel.suffix.lower() not in {".py", ".json", ".ipynb", ".md", ".csv", ".sql", ".txt", ".xml"}):
            raise ValueError("Artifact must be an ordinary source file inside the project repository")
        path = (project.repo / str(rel)).resolve()
        if not path.is_relative_to(project.repo) or path in seen:
            raise ValueError("Artifact escapes the repository or duplicates a path")
        safe_text(artifact.content, limit=6_000_000)
        if native:
            validate_native(artifact.path, artifact.content.encode())
        elif path.suffix in {".json", ".ipynb"}:
            json.loads(artifact.content)
        elif path.suffix == ".py":
            ast.parse(artifact.content)
        seen.add(path)
        pending.append((path, artifact.content))
    for path, content in pending:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")


def validate_sources(root):
    from .context import IGNORE
    root = Path(root).resolve()
    count = 0
    for path in Path(root).rglob("*"):
        from .tenant_repository import is_native_path, validate_native
        rel = path.relative_to(root)
        native = is_native_path(rel.as_posix())
        if not path.is_file() or any((part.startswith(".") and not (native and part == ".platform" and part == path.name)) or part in IGNORE for part in rel.parts):
            continue
        if native:
            validate_native(rel.as_posix(), path.read_bytes())
            count += 1
            continue
        if path.suffix.lower() in {".json", ".ipynb"}:
            value = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(value, dict) and "definition" in value:
                from .cloud import definition_parts
                _resolve_definition(root, value)
                definition_parts({"definition": value["definition"]})
            count += 1
        elif path.suffix == ".py":
            ast.parse(path.read_text(encoding="utf-8"))
            count += 1
    if not count:
        raise ValueError("No authored JSON/notebook/Python source was available for validation")
    return count


def notebook_line_sources(content):
    """Fabric requires line arrays even though nbformat permits source strings."""
    value = json.loads(content.decode("utf-8-sig"))
    if not isinstance(value, dict) or not isinstance(value.get("cells"), list):
        raise ValueError("Notebook must contain a cells array")
    changed = False
    for cell in value["cells"]:
        if not isinstance(cell, dict):
            raise ValueError("Invalid notebook cell")
        source = cell.get("source")
        if isinstance(source, str):
            cell["source"] = source.splitlines(keepends=True)
            changed = True
        elif not isinstance(source, list) or any(not isinstance(line, str) for line in source):
            raise ValueError("Notebook cell source must be text or an array of text lines")
    return json.dumps(value, ensure_ascii=False, indent=2).encode("utf-8") if changed else content


def _resolve_definition(root, value):
    """Compile references in memory; validation never needs a writable temp folder."""
    sources = {}
    compiled = False
    if not isinstance(value.get("definition"), dict):
        return compiled, sources
    for part in value["definition"].get("parts", []):
        if not isinstance(part, dict) or part.get("payloadType") != "SourceFile":
            continue
        if set(part) != {"path", "source", "payloadType"} or not isinstance(part["source"], str):
            raise ValueError("SourceFile parts require path, source and payloadType")
        rel = PurePosixPath(part["source"])
        if rel.is_absolute() or ".." in rel.parts or ":" in part["source"] or "\\" in part["source"]:
            raise ValueError("Definition source must stay inside the repository")
        source = (root / str(rel)).resolve(strict=True)
        if not source.is_relative_to(root) or not source.is_file() or source.stat().st_size > 4_000_000:
            raise ValueError("Definition source escapes repository or exceeds size limit")
        content = source.read_bytes()
        safe_text(content.decode("utf-8"), limit=4_000_000)
        if source.suffix.lower() == ".ipynb" or str(part.get("path", "")).lower().endswith(".ipynb"):
            content = notebook_line_sources(content)
            if len(content) > 4_000_000:
                raise ValueError("Normalized notebook exceeds source size limit")
            sources[source] = content
        part.pop("source")
        part.update(payloadType="InlineBase64", payload=base64.b64encode(content).decode("ascii"))
        compiled = True
    return compiled, sources


def compile_definitions(project):
    """Encode repository source references before validation and independent review."""
    from .context import IGNORE
    pending = []
    pending_sources = {}
    for path in sorted(project.repo.rglob("*.json")):
        if any(p.startswith(".") or p in IGNORE for p in path.relative_to(project.repo).parts):
            continue
        if not path.resolve().is_relative_to(project.repo) or path.stat().st_size > 6_000_000:
            raise ValueError("Definition file escapes repository or exceeds size limit")
        value = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(value, dict) or not isinstance(value.get("definition"), dict):
            continue
        compiled, sources = _resolve_definition(project.repo, value)
        pending_sources.update(sources)
        if compiled:
            from .cloud import definition_parts
            definition_parts({"definition": value["definition"]})
            pending.append((path, json.dumps(value, indent=2)))
    for source, content in pending_sources.items():
        source.write_bytes(content)
    for path, content in pending:
        path.write_text(content, encoding="utf-8")


def source_evidence(project, limit=180000, *, priority_paths=(), required_paths=()):
    """Supply review inputs before background files; never silently omit required source."""
    from .documents import read_document, ReadError
    binary = {".docx", ".pdf", ".xlsx", ".xls"}
    required = set(required_paths)
    priority = set(priority_paths) | required
    candidates = []
    for path in project.repo.rglob("*"):
        rel = path.relative_to(project.repo)
        from .tenant_repository import is_native_path, TEXT_SUFFIXES
        native = bool(project.config.fabric.tenant and is_native_path(rel.as_posix()))
        if not path.is_file() or any((p.startswith(".") and not (native and p == ".platform" and p == path.name)) or p in {"__pycache__", "node_modules", "venv"} for p in rel.parts):
            continue
        if not path.resolve().is_relative_to(project.repo):
            raise ValueError("Source evidence escapes repository")
        if native and (path.name == ".platform" or path.suffix.lower() in TEXT_SUFFIXES) or path.suffix.lower() in {".json", ".ipynb", ".py", ".sql", ".csv", ".md", ".txt", ".xml"} | binary:
            candidates.append((rel.as_posix(), path))
    available = {rel for rel, _ in candidates}
    if required - available:
        raise ValueError("Required review source is missing or unsupported")
    candidates.sort(key=lambda entry: (entry[0] not in required, entry[0] not in priority, entry[0]))
    files = []
    used = 0
    document_count = 0
    for rel, path in candidates:
        if path.stat().st_size > (20_000_000 if path.suffix.lower() in binary else 6_000_000):
            raise ValueError("Source evidence file exceeds size limit")
        if path.suffix.lower() in binary:
            if document_count >= 6 or used >= limit:
                if rel in required:
                    raise ValueError("Required document exceeds the evidence budget; split the source stage")
                files.append({"path": rel, "omitted": "Document reading budget exceeded"})
                continue
            document_count += 1
            try:
                document = read_document(path.name, path.read_bytes())
                if rel in required and (document["truncated"] or not document["sections"]):
                    raise ValueError("Required document could not be read completely")
                text = json.dumps(document, ensure_ascii=False)
            except ReadError as exc:
                if rel in required:
                    raise ValueError("Required document could not be read") from None
                files.append({"path": rel, "omitted": str(exc)})
                continue
        else:
            text = path.read_text(encoding="utf-8")
        safe_text(text, limit=6_000_000)
        if used + len(text) > limit:
            if rel in required:
                raise ValueError("Required review source exceeds the evidence budget; split the source stage")
            files.append({"path": rel, "omitted": "Source evidence budget exceeded; inspect this file if relevant to review"})
            continue
        files.append({"path": rel, "content": text})
        used += len(text)
    return json.dumps(files, ensure_ascii=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("root", nargs="?", type=Path, default=Path.cwd())
    args = parser.parse_args()
    print(f"Validated {validate_sources(args.root)} source files")
