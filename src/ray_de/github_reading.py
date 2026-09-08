"""Public GitHub source inspection using fixed, read-only API endpoints."""
from __future__ import annotations

import base64
from datetime import datetime, timezone
import json
from pathlib import PurePosixPath
import re
import urllib.error
import urllib.parse
import urllib.request

from .documents import ReadError, read_document, EXTENSIONS
from .memory import redact, redact_data

SHA = re.compile(r"[0-9a-f]{40}")
TEXT = {".py", ".js", ".jsx", ".ts", ".tsx", ".json", ".yaml", ".yml", ".toml", ".ini", ".cfg", ".sql", ".xml", ".cs", ".go", ".rs", ".java", ".sh", ".ps1", ".ipynb", ".html", ".css", ".rb", ".r", ".scala", ".tf", ".md", ".txt", ".csv"}


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def parse_url(url):
    parsed = urllib.parse.urlsplit(url)
    if (parsed.scheme != "https" or parsed.netloc.lower() != "github.com" or parsed.query
            or "%" in parsed.netloc):
        raise ReadError("Use an HTTPS GitHub repository, file or folder link, without sign-in details or query parameters.")
    parts = urllib.parse.unquote(parsed.path).strip("/").split("/")
    if (len(parts) < 2 or any(p in {"", ".", ".."} or "\\" in p or any(ord(c) < 32 for c in p) for p in parts)
            or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9-]{0,38}", parts[0])
            or not re.fullmatch(r"[A-Za-z0-9_.-]{1,100}", parts[1])):
        raise ReadError("That GitHub repository link is not valid.")
    repo = parts[1].removesuffix(".git")
    if repo in {"", ".", ".."}:
        raise ReadError("That GitHub repository link is not valid.")
    if len(parts) > 2 and (parts[2] not in {"tree", "blob"} or len(parts) < 4):
        raise ReadError("Send a GitHub repository, file or folder link. Issue and pull-request pages are not source files.")
    return parts[0], repo, parts[2:]


def github_urls(text):
    return list(dict.fromkeys(u.rstrip(".,;!?)>]}") for u in re.findall(r"https://github\.com/[^\s<>\"`]+", text)))


def _safe_path(path):
    rel = PurePosixPath(path)
    return bool(path) and not rel.is_absolute() and ".." not in rel.parts and "\\" not in path and not any(ord(c) < 32 for c in path)


def _readable(path):
    rel = PurePosixPath(path)
    if (not _safe_path(path) or any(p in {"node_modules", "vendor", "dist", "build", ".git"} for p in rel.parts)
            or any(p.lower() == ".env" or p.lower().startswith(".env.") for p in rel.parts)):
        return False
    return rel.suffix.lower() in TEXT | EXTENSIONS or rel.name.lower() in {"dockerfile", "makefile", "license", "readme"}


class GitHubReader:
    def __init__(self, request=None, *, cancel=None):
        self.request = request or self._request
        self.cancel = cancel

    def _request(self, path):
        if self.cancel:
            self.cancel.check()
        request = urllib.request.Request("https://api.github.com" + path, headers={
            "Accept": "application/vnd.github+json", "User-Agent": "Ray-source-reader",
            "X-GitHub-Api-Version": "2022-11-28"})
        try:
            with urllib.request.build_opener(NoRedirect()).open(request, timeout=15) as response:
                body = response.read(4_000_001)
            if len(body) > 4_000_000:
                raise ReadError("This GitHub response is too large. Send a link to a smaller repository or file.")
            return json.loads(body)
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                raise ReadError("GitHub could not find this repository or file. It may be private; this reader uses public access.") from None
            if exc.code in {403, 429}:
                raise ReadError("GitHub denied this read or its public request limit was reached. Try again later.") from None
            raise ReadError("GitHub could not return the source. Check the link and try again.") from None
        except ReadError:
            raise
        except Exception:
            raise ReadError("I couldn't reach GitHub. Check the connection and try again.") from None

    def _get(self, path):
        if self.cancel:
            self.cancel.check()
        return self.request(path)

    def read(self, url):
        owner, repo, tail = parse_url(url)
        base = f"/repos/{owner}/{repo}"
        metadata = self._get(base)
        ref = metadata.get("default_branch")
        scope = ""
        if tail:
            # GitHub links don't separate slashes in refs from slashes in paths.
            # Resolve the longest existing commit ref, with a bounded search.
            pieces = tail[1:]
            commit = None
            for split in range(min(len(pieces), 8), 0, -1):
                ref = "/".join(pieces[:split])
                try:
                    commit = self._get(base + "/commits/" + urllib.parse.quote(ref, safe=""))
                    scope = "/".join(pieces[split:])
                    break
                except ReadError as exc:
                    if "could not find" not in str(exc):
                        raise
            if commit is None:
                raise ReadError("The GitHub branch or file could not be found. Try the repository's main link.")
        else:
            if not isinstance(ref, str) or not ref:
                raise ReadError("This repository has no readable default branch.")
            commit = self._get(base + "/commits/" + urllib.parse.quote(ref, safe=""))
        sha = commit.get("sha", "")
        tree_sha = commit.get("commit", {}).get("tree", {}).get("sha", "")
        if not SHA.fullmatch(sha) or not SHA.fullmatch(tree_sha):
            raise ReadError("GitHub did not return a valid source version.")
        tree = self._get(base + "/git/trees/" + tree_sha + "?recursive=1")
        blobs = [dict(path=e["path"], sha=e["sha"], size=e.get("size", 0)) for e in tree.get("tree", [])
                 if e.get("type") == "blob" and e.get("mode") in {"100644", "100755"}
                 and isinstance(e.get("path"), str) and _safe_path(e["path"]) and SHA.fullmatch(e.get("sha", ""))]
        result = {"source": "public_github", "repository": f"{owner}/{repo}", "url": f"https://github.com/{owner}/{repo}",
                  "commit": sha, "captured_at": datetime.now(timezone.utc).isoformat(),
                  "description": redact(str(metadata.get("description") or ""))[:1000],
                  "tree": blobs[:3000], "tree_truncated": bool(tree.get("truncated")) or len(blobs) > 3000,
                  "files": [], "warnings": [], "executed_code": False}
        candidates = [e for e in result["tree"] if _readable(e["path"]) and (not scope or e["path"] == scope or (tail[0] == "tree" and e["path"].startswith(scope + "/")))]
        if tail and tail[0] == "blob" and not candidates:
            raise ReadError("This file was not available in the source listing or its format is unsupported.")
        def rank(entry):
            name = PurePosixPath(entry["path"]).name.lower()
            return (0 if name.startswith("readme") else 1 if name in {"pyproject.toml", "package.json", "requirements.txt", "cargo.toml", "go.mod"} else 2 if entry["path"].startswith(("src/", "app/", "lib/")) else 3, entry["path"].count("/"), entry["path"])
        candidates.sort(key=rank)
        result["files"] = self.read_paths(result, [e["path"] for e in candidates[:10]])
        result["warnings"].append("Source inspection only. Repository code and tests were not executed. Only the files listed as read were inspected.")
        if len(candidates) > 10:
            result["warnings"].append("This is a sample of the repository. Ask about specific files for a closer check.")
        if result["tree_truncated"]:
            result["warnings"].append("The file listing is incomplete because the repository is large.")
        return result

    def read_paths(self, repository, paths):
        owner, repo, _ = parse_url(repository["url"])
        entries = {e["path"]: e for e in repository["tree"]}
        if len(paths) > 10 or any(p not in entries or not _readable(p) for p in paths):
            raise ReadError("Choose up to ten readable files from this repository's file listing.")
        files = []
        for path in dict.fromkeys(paths):
            entry = entries[path]
            if not SHA.fullmatch(entry["sha"]):
                raise ReadError("GitHub did not return a valid file version.")
            if type(entry.get("size")) is not int or not 0 <= entry["size"] <= 500_000:
                files.append({"path": path, "omitted": "File exceeds the 500 KB source reading limit."})
                continue
            blob = self._get(f"/repos/{owner}/{repo}/git/blobs/{entry['sha']}")
            try:
                if blob.get("encoding") != "base64":
                    raise ValueError()
                data = base64.b64decode("".join(blob["content"].split()), validate=True)
                if len(data) > 500_000:
                    raise ValueError()
                if PurePosixPath(path).suffix.lower() in EXTENSIONS - {".md", ".txt", ".csv"}:
                    document = read_document(path, data, cancel=self.cancel)
                    content = "\n".join(s["location"] + "\n" + s["text"] for s in document["sections"])
                    warnings = document["warnings"]
                else:
                    content, warnings = data.decode("utf-8-sig"), []
                files.append({"path": path, "blob": entry["sha"], "content": redact(content)[:12000],
                              "truncated": len(content) > 12000, "warnings": warnings})
            except (ValueError, KeyError):
                files.append({"path": path, "omitted": "This file could not be decoded as readable source."})
        return redact_data(files)
