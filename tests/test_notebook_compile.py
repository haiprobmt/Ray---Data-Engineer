import base64
import json
import pytest

from ray_de.artifacts import compile_definitions


def test_fabric_notebook_cell_sources_are_line_arrays_before_review(project):
    notebook = {"nbformat": 4, "nbformat_minor": 5, "metadata": {}, "cells": [
        {"cell_type": "markdown", "metadata": {}, "source": "# Overview\nTwo lines 😀"},
        {"cell_type": "code", "metadata": {}, "source": "print('hello')\n", "outputs": [], "execution_count": None},
        {"cell_type": "code", "metadata": {}, "source": [], "outputs": [], "execution_count": None}]}
    source = project.repo / "notebook.ipynb"
    source.write_text(json.dumps(notebook), encoding="utf-8")
    definition = project.repo / "notebook.json"
    definition.write_text(json.dumps({"definition": {"parts": [{"path": "notebook-content.ipynb",
        "source": "notebook.ipynb", "payloadType": "SourceFile"}]}}))
    compile_definitions(project)
    payload = json.loads(definition.read_text())["definition"]["parts"][0]["payload"]
    compiled = base64.b64decode(payload)
    result = json.loads(compiled)
    assert all(isinstance(cell["source"], list) for cell in result["cells"])
    assert "".join(result["cells"][0]["source"]) == notebook["cells"][0]["source"]
    assert "".join(result["cells"][1]["source"]) == notebook["cells"][1]["source"]
    assert compiled == source.read_bytes()
    before = definition.read_bytes(), source.read_bytes()
    compile_definitions(project)
    assert (definition.read_bytes(), source.read_bytes()) == before


@pytest.mark.parametrize("source", [None, 123, ["valid", 5]])
def test_invalid_notebook_sources_fail_before_upload(source):
    from ray_de.artifacts import notebook_line_sources
    with pytest.raises(ValueError):
        notebook_line_sources(json.dumps({"cells": [{"source": source}]}).encode())
