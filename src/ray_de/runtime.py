from __future__ import annotations

import importlib.metadata
import os
import shutil
import subprocess
import sys
from pathlib import Path


def resolve_binary(kind: str) -> str:
    key = "SENIOR_DE_CODEX_BIN" if kind == "codex" else "RAY_FAB_BIN"
    explicit = os.environ.get(key)
    if explicit:
        path = Path(explicit).expanduser().resolve(strict=True)
        if not path.is_file() or path.suffix.lower() in {".cmd", ".bat", ".ps1"}:
            raise ValueError(f"{key} must point to a native executable")
        return str(path)
    found = shutil.which(kind + (".exe" if os.name == "nt" else ""))
    if found:
        return str(Path(found).resolve())
    if kind == "fab":
        found = Path(sys.executable).parent / ("fab.exe" if os.name == "nt" else "fab")
        if found.is_file():
            return str(found)
    if kind == "codex":
        # Explicitly mark fallback in doctor; no opaque SDK runtime selection.
        import openai_codex_cli_bin

        directory = Path(openai_codex_cli_bin.__file__).parent
        candidates = list(directory.rglob("codex.exe" if os.name == "nt" else "codex"))
        if len(candidates) == 1:
            return str(candidates[0].resolve())
    raise FileNotFoundError(f"{kind} is unavailable; set {key}")


def clean_env() -> dict[str, str]:
    # No inherited Azure, Fabric, Telegram, proxy, or API credentials.
    allowed = {
        "SYSTEMROOT",
        "WINDIR",
        "COMSPEC",
        "PATH",
        "PATHEXT",
        "TEMP",
        "TMP",
        "USERPROFILE",
        "APPDATA",
        "LOCALAPPDATA",
        "LANG",
        "LC_ALL",
        "HOME",
    }
    result = {k: v for k, v in os.environ.items() if k.upper() in allowed}
    result["PYTHONIOENCODING"] = "utf-8"
    return result


def probe(binary: str, args: list[str], *, env=None) -> dict:
    try:
        result = subprocess.run(
            [binary, *args],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=20,
            env=env or clean_env(),
            shell=False,
        )
        return {
            "ok": result.returncode == 0,
            "exit_code": result.returncode,
            "output": (result.stdout or result.stderr).strip()[:300],
        }
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {"ok": False, "error": type(exc).__name__}


def versions() -> dict:
    result = {"python": sys.version.split()[0]}
    for name in ("ray-fabric-engineer", "openai-codex", "ms-fabric-cli"):
        try:
            result[name] = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            result[name] = "not installed"
    return result
