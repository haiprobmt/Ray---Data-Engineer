from __future__ import annotations
import hashlib, json, re, secrets, sqlite3, zipfile
from pathlib import Path
from contextlib import closing
from datetime import datetime, timezone
from .state import now

SECRET_KEY = r"(?:password|passwd|pwd|client[_-]?secret|api[_-]?key|access[_-]?token|refresh[_-]?token|authorization|accountkey|sharedaccesssignature)"
SECRET = re.compile(
    r'(?i)(?:bearer\s+[A-Za-z0-9._-]{15,}|sk-[A-Za-z0-9_-]{15,}|\d{6,}:[A-Za-z0-9_-]{25,}|\b'
    + SECRET_KEY + r'''["']?\s*[:=]\s*(?:"[^"\r\n]{6,}"|'[^'\r\n]{6,}'|[^\s"',;}{]{6,}))'''
)


def safe_text(text, limit=32000):
    if not isinstance(text, str) or len(text) > limit:
        raise ValueError("Text exceeds the memory limit")
    if SECRET.search(text):
        raise ValueError("Possible secret detected; do not save it in memory")
    return text


def redact(text):
    return SECRET.sub("[REDACTED]", str(text))


def redact_data(value):
    """Keep field context when removing credentials from structured observations."""
    if isinstance(value, str):
        return redact(value)
    if isinstance(value, (list, tuple)):
        return [redact_data(v) for v in value]
    if isinstance(value, dict):
        result = {
            redact(k): "[REDACTED]" if re.fullmatch(SECRET_KEY, str(k), re.I) and v
            else redact_data(v)
            for k, v in value.items()
        }
        columns, rows = value.get("columns"), result.get("rows")
        if isinstance(columns, list) and isinstance(rows, list):
            sensitive = {i for i, name in enumerate(columns) if re.fullmatch(SECRET_KEY, str(name), re.I)}
            result["rows"] = [
                ["[REDACTED]" if i in sensitive and cell is not None else cell for i, cell in enumerate(row)]
                if isinstance(row, list) else row for row in rows
            ]
        return result
    return value


def save_decision(project, title, context, decision, consequences, actor):
    for text in (title, context, decision, consequences, actor):
        safe_text(text)
    if not title.strip() or "\n" in title:
        raise ValueError("A one-line title is required")
    directory = project.directory / "decisions"
    directory.mkdir(exist_ok=True)
    if not directory.resolve().is_relative_to(project.directory):
        raise ValueError("Decision directory escapes project")
    id = (
        "DEC-"
        + datetime.now(timezone.utc).strftime("%Y%m%d")
        + "-"
        + secrets.token_hex(4)
    )
    text = f"# {title}\n\nStatus: Accepted\nDate: {now()}\nDecided by: {actor}\n\n## Context\n\n{context}\n\n## Decision\n\n{decision}\n\n## Consequences\n\n{consequences}\n"
    path = directory / (id + ".md")
    path.write_text(text, encoding="utf-8")
    return {"id": id, "path": str(path)}


def recall(project, query, limit=4):
    words = set(re.findall(r"[a-z0-9_]{3,}", query.lower()))
    ranked = []
    for p in (project.directory / "decisions").glob("*.md"):
        if (
            not p.resolve().is_relative_to(project.directory)
            or p.stat().st_size > 32000
        ):
            continue
        text = p.read_text(encoding="utf-8")
        score = len(words & set(re.findall(r"[a-z0-9_]{3,}", text.lower())))
        if score or not words:
            ranked.append((score, p.name, redact(text)))
    return [
        {"file": name, "text": text}
        for _, name, text in sorted(ranked, key=lambda r: (-r[0], r[1]))[:limit]
    ]


def backup(store, projects, output):
    output = Path(output).resolve()
    if output.exists():
        raise ValueError("Backup output already exists")
    output.parent.mkdir(parents=True, exist_ok=True)
    import tempfile

    with tempfile.TemporaryDirectory(prefix="backup-", dir=output.parent) as tmp:
        dbpath = Path(tmp) / "ray.db"
        with store.connect() as src:
            with closing(sqlite3.connect(dbpath)) as target:
                src.backup(target)
        entries = {"ray.db": dbpath.read_bytes()}
        for project in projects:
            entries[f"projects/{project.id}/config.yaml"] = (
                project.config_path.read_bytes()
            )
            for p in [
                project.directory / "CONTEXT.md",
                *(project.directory / "decisions").glob("*.md"),
            ]:
                if p.is_file() and p.resolve().is_relative_to(project.directory):
                    entries[
                        f"projects/{project.id}/"
                        + p.relative_to(project.directory).as_posix()
                    ] = p.read_bytes()
        manifest = {
            name: hashlib.sha256(body).hexdigest() for name, body in entries.items()
        }
        temporary = Path(tmp) / "backup.zip"
        with zipfile.ZipFile(temporary, "w", zipfile.ZIP_DEFLATED) as z:
            for name, body in entries.items():
                z.writestr(name, body)
            z.writestr("manifest.json", json.dumps(manifest, indent=2))
        temporary.replace(output)
    return {
        "output": str(output),
        "files": len(entries),
        "credentials_included": False,
        "conversation_rollouts_included": False,
    }


def verify_backup(path):
    with zipfile.ZipFile(path) as z:
        if z.testzip():
            raise ValueError("Corrupt backup")
        hashes = json.loads(z.read("manifest.json"))
        for name, digest in hashes.items():
            if hashlib.sha256(z.read(name)).hexdigest() != digest:
                raise ValueError("Backup integrity mismatch")
    return {"verified_files": len(hashes)}
