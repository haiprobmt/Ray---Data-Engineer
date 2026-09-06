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
        if (rel.is_absolute() or ".." in rel.parts or "\\" in artifact.path or ":" in artifact.path
                or any(p.startswith(".") for p in rel.parts)
                or rel.name.lower() in {"agents.md", "skill.md", "config.yaml"}
                or rel.suffix.lower() not in {".py", ".json", ".ipynb", ".md", ".csv", ".sql", ".txt", ".xml"}):
            raise ValueError("Artifact must be an ordinary source file inside the project repository")
        path = (project.repo / str(rel)).resolve()
        if not path.is_relative_to(project.repo) or path in seen:
            raise ValueError("Artifact escapes the repository or duplicates a path")
        safe_text(artifact.content, limit=6_000_000)
        if path.suffix in {".json", ".ipynb"}:
            json.loads(artifact.content)
        elif path.suffix == ".py":
            ast.parse(artifact.content)
        seen.add(path)
        pending.append((path, artifact.content))
    for path, content in pending:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")


def validate_sources(root):
    count = 0
    for path in Path(root).rglob("*"):
        if not path.is_file() or any(part.startswith(".") or part == "__pycache__" for part in path.relative_to(root).parts):
            continue
        if path.suffix.lower() in {".json", ".ipynb"}:
            value = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(value, dict) and "definition" in value:
                from .cloud import definition_parts
                definition_parts({"definition": value["definition"]})
            count += 1
        elif path.suffix == ".py":
            ast.parse(path.read_text(encoding="utf-8"))
            count += 1
    if not count:
        raise ValueError("No authored JSON/notebook/Python source was available for validation")
    return count


def compile_definitions(project):
    """Encode repository source references before validation and independent review."""
    pending = []
    for path in sorted(project.repo.rglob("*.json")):
        if any(p.startswith(".") for p in path.relative_to(project.repo).parts):
            continue
        if not path.resolve().is_relative_to(project.repo) or path.stat().st_size > 6_000_000:
            raise ValueError("Definition file escapes repository or exceeds size limit")
        value = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(value, dict) or not isinstance(value.get("definition"), dict):
            continue
        compiled = False
        for part in value["definition"].get("parts", []):
            if not isinstance(part, dict) or part.get("payloadType") != "SourceFile":
                continue
            if set(part) != {"path", "source", "payloadType"}:
                raise ValueError("SourceFile parts require path, source and payloadType")
            rel = PurePosixPath(part["source"])
            if rel.is_absolute() or ".." in rel.parts or ":" in part["source"] or "\\" in part["source"]:
                raise ValueError("Definition source must stay inside the repository")
            source = (project.repo / str(rel)).resolve(strict=True)
            if not source.is_relative_to(project.repo) or source.stat().st_size > 4_000_000:
                raise ValueError("Definition source escapes repository or exceeds size limit")
            content = source.read_bytes()
            safe_text(content.decode("utf-8"), limit=4_000_000)
            part.pop("source")
            part.update(payloadType="InlineBase64", payload=base64.b64encode(content).decode("ascii"))
            compiled = True
        if compiled:
            from .cloud import definition_parts
            definition_parts({"definition": value["definition"]})
            pending.append((path, json.dumps(value, indent=2)))
    for path, content in pending:
        path.write_text(content, encoding="utf-8")


def source_evidence(project, limit=180000):
    """Bounded source for reviewers when the model runtime cannot open files."""
    files = []
    used = 0
    for path in sorted(project.repo.rglob("*")):
        rel = path.relative_to(project.repo)
        if not path.is_file() or any(p.startswith(".") or p == "__pycache__" for p in rel.parts):
            continue
        if not path.resolve().is_relative_to(project.repo):
            raise ValueError("Source evidence escapes repository")
        if path.suffix.lower() not in {".json", ".ipynb", ".py", ".sql", ".csv", ".md", ".txt", ".xml"}:
            continue
        text = path.read_text(encoding="utf-8")
        safe_text(text, limit=6_000_000)
        if used + len(text) > limit:
            files.append({"path": rel.as_posix(), "omitted": "Source evidence budget exceeded; inspect this file before passing review"})
            continue
        files.append({"path": rel.as_posix(), "content": text})
        used += len(text)
    return json.dumps(files, ensure_ascii=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("root", nargs="?", type=Path, default=Path.cwd())
    args = parser.parse_args()
    print(f"Validated {validate_sources(args.root)} source files")
