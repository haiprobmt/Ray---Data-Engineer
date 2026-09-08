"""Native Fabric Git source, tenant scaffolding and identity-preserving allocation."""
from __future__ import annotations
import argparse
import ast
import base64
import hashlib
import json
import re
import shutil
import subprocess
from pathlib import Path, PurePosixPath

from .tenant_config import identifier, relative_path
from .memory import safe_text

TEXT_SUFFIXES = {".py", ".json", ".ipynb", ".sql", ".yml", ".yaml", ".xml", ".txt", ".md",
                 ".tmdl", ".pbir", ".pbism", ".rdl", ".sqlproj", ".csv", ".r", ".scala"}


def is_native_path(relative):
    path = PurePosixPath(relative)
    return any(re.fullmatch(r".+\.[A-Z][A-Za-z0-9]+", p) for p in path.parts[:-1])


def validate_native(relative, content):
    relative_path(relative)
    path = PurePosixPath(relative)
    if any(p.startswith(".") for p in path.parts if p != ".platform") or (path.name == ".platform" and not is_native_path(relative)):
        raise ValueError("Hidden Git source must be item .platform metadata")
    if path.name != ".platform" and path.suffix.lower() not in TEXT_SUFFIXES:
        raise ValueError("Unsupported native source format; add a reviewed format adapter")
    if len(content) > 4_000_000:
        raise ValueError("Native source file exceeds four megabytes")
    text = content.decode("utf-8-sig")
    safe_text(text, limit=4_000_000)
    if path.name == ".platform" or path.suffix.lower() in {".json", ".ipynb", ".pbir", ".pbism"}:
        value = json.loads(text)
        if path.name == ".platform":
            identifier(value["config"]["logicalId"])
            if value.get("version") != "2.0" or not re.fullmatch(r"[A-Za-z][A-Za-z0-9]+", value["metadata"]["type"]):
                raise ValueError("Unsupported Fabric platform metadata")
    elif path.suffix == ".py":
        if text.startswith("# Fabric notebook source") and is_native_path(relative):
            # Fabric Git source encodes magic and non-Python cells as comments.
            # Raw magic cells are accepted only at cell boundaries, never stripped
            # from ordinary Python. Remaining Python cells must parse independently.
            cells = re.split(r"(?m)^# CELL \*+.*$", text)
            for cell in cells:
                lines = cell.splitlines()
                first = next((line.strip() for line in lines if line.strip() and not line.lstrip().startswith("#")), "")
                if first.startswith("%%"):
                    if not re.fullmatch(r"%%(?:sql|pyspark|spark|sparkR|configure|sh|bash|capture)(?:\s.*)?", first):
                        raise ValueError("Unsupported Fabric notebook cell magic")
                    index = next(i for i, line in enumerate(lines) if line.strip() == first)
                    body = "\n".join(lines[index+1:])
                    if first.split()[0] in {"%%pyspark", "%%capture"}:
                        ast.parse(body)
                    elif first.split()[0] == "%%configure":
                        json.loads("\n".join(line for line in lines[index+1:] if not line.lstrip().startswith("#")))
                    continue
                ast.parse(cell)
        else:
            ast.parse(text)
    return text


def item_inventory(files):
    result = {}
    for path, content in files.items():
        if PurePosixPath(path).name != ".platform":
            continue
        value = json.loads(content.decode("utf-8-sig"))
        logical = identifier(value["config"]["logicalId"])
        if logical in result:
            raise ValueError("Duplicate logical item ID within a workspace")
        result[logical] = {"directory": str(PurePosixPath(path).parent), "type": value["metadata"]["type"],
                           "name": value["metadata"]["displayName"]}
    return result


def git_blob_sha(content):
    return hashlib.sha1(b"blob " + str(len(content)).encode() + b"\0" + content).hexdigest()


def remote_tree(gateway, github, commit):
    if not re.fullmatch(r"[0-9a-f]{40}", commit or ""):
        raise ValueError("A full reviewed Git commit is required")
    base = f"repos/{github.owner}/{github.repository}"
    details = gateway.raw("github", base + "/git/commits/" + commit)
    tree_sha = details.get("tree", {}).get("sha")
    if not re.fullmatch(r"[0-9a-f]{40}", tree_sha or ""):
        raise ValueError("Git commit has no tree")
    tree = gateway.raw("github", base + "/git/trees/" + tree_sha + "?recursive=1")
    if tree.get("truncated") or not isinstance(tree.get("tree"), list):
        raise ValueError("Git tree is incomplete")
    files = {}
    for entry in tree["tree"]:
        relative_path(entry["path"])
        if entry["type"] == "blob":
            if entry["path"].casefold() in {p.casefold() for p in files}:
                raise ValueError("Git tree contains duplicate paths")
            files[entry["path"]] = entry
    return tree_sha, files


def remote_blob(gateway, github, entry):
    if entry.get("mode") != "100644":
        raise ValueError("Only ordinary source blobs are supported")
    sha = entry["sha"]
    if not re.fullmatch(r"[0-9a-f]{40}", sha):
        raise ValueError("Invalid Git blob identity")
    value = gateway.raw("github", f"repos/{github.owner}/{github.repository}/git/blobs/{sha}")
    if value.get("encoding") != "base64" or value.get("size", 0) > 4_000_000:
        raise ValueError("Unsupported Git blob encoding or size")
    content = base64.b64decode("".join(value["content"].split()), validate=True)
    if git_blob_sha(content) != sha:
        raise ValueError("Git blob did not match its content identity")
    return content


def publication(project, gateway, grant, arguments):
    from .tenant import github_settings
    github = github_settings(project)
    files, removed = arguments["files"], arguments.get("remove", [])
    if (not isinstance(files, list) or not files or len(files) > 500 or not isinstance(removed, list)
            or any(not isinstance(p, str) for p in files + removed) or len({p.casefold() for p in files + removed}) != len(files + removed)):
        raise ValueError("Publication requires unique source paths and at most 500 files")
    prefixes = [w.directory + "/" for w in github.workspaces if w.environment == grant.environment]
    for path in files + removed:
        relative_path(path)
        if not any(path.startswith(prefix) for prefix in prefixes):
            raise ValueError("Publication crosses its enrolled workspace/environment directories")
    tree_sha, tree = remote_tree(gateway, github, arguments["expected_head"])
    local, size = {}, 0
    for relative in files:
        path = (project.repo / relative).resolve(strict=True)
        if not path.is_relative_to(project.repo) or not path.is_file():
            raise ValueError("Published source escapes the reviewed checkout")
        content = path.read_bytes()
        validate_native(relative, content)
        size += len(content)
        if size > 180000:
            raise ValueError("Split publication into source stages within the independent review budget")
        local[relative] = content
    for path in removed:
        if path not in tree:
            raise ValueError("Removal does not match the reviewed remote tree")
    # Metadata is read for every item in each affected workspace. Logical identity
    # is checked per workspace so moving an item into another workspace cannot
    # disguise a delete/recreate operation.
    affected = [prefix for prefix in prefixes if any(p.startswith(prefix) for p in files + removed)]
    for prefix in affected:
        before = {p: remote_blob(gateway, github, entry) for p, entry in tree.items()
                  if p.startswith(prefix) and PurePosixPath(p).name == ".platform"}
        after = {p: data for p, data in before.items() if p not in removed}
        after.update({p: data for p, data in local.items() if p.startswith(prefix) and PurePosixPath(p).name == ".platform"})
        old_items, new_items = item_inventory(before), item_inventory(after)
        if old_items.keys() - new_items.keys() or any(old_items[k]["type"] != new_items[k]["type"] for k in old_items):
            raise ValueError("Git publication would delete or recreate an existing item")
        for logical, info in old_items.items():
            replacement_directory = new_items[logical]["directory"]
            if replacement_directory != info["directory"]:
                old_paths = {path for path in tree if path.startswith(info["directory"] + "/")}
                if not old_paths <= set(removed) or any(replacement_directory + path[len(info["directory"]):] not in local for path in old_paths):
                    raise ValueError("Move must include every existing item part and remove all previous paths")
        for path in removed:
            if not path.startswith(prefix):
                continue
            owner = next((key for key, info in old_items.items() if path.startswith(info["directory"] + "/")), None)
            if owner is None:
                raise ValueError("Only complete identity-preserving item moves can remove old Git paths")
            suffix = path[len(old_items[owner]["directory"]):]
            replacement = new_items[owner]["directory"] + suffix
            if replacement == path or replacement not in local:
                raise ValueError("Removed item file has no reviewed replacement at the new location")
        remaining_paths = (set(tree) - set(removed)) | set(local)
        for logical, info in new_items.items():
            owned = [p for p in remaining_paths if p.startswith(info["directory"] + "/")]
            if len(owned) < 2:
                raise ValueError("Native item needs definition files as well as .platform metadata")
    return {"base_tree": tree_sha, "files": {p: base64.b64encode(c).decode() for p, c in local.items()},
            "remove": removed, "expected_head": arguments["expected_head"], "message": arguments["message"]}


def scaffold(root, tenant_id, workspaces):
    """Local-only creation; never runs scripts, hooks or code from exported items."""
    identifier(tenant_id)
    root = Path(root).resolve()
    if root.exists() and any(root.iterdir()):
        raise ValueError("Tenant scaffold requires a new or empty directory")
    if not workspaces or len(set(w.casefold() for w in workspaces)) != len(workspaces):
        raise ValueError("Specify unique workspace directory names")
    for workspace in workspaces:
        if not re.fullmatch(r"[a-z][a-z0-9_-]{0,63}", workspace):
            raise ValueError("Workspace directory must be a short lowercase slug")
    git = shutil.which("git")
    if not git:
        raise RuntimeError("Install Git before initializing a tenant repository")
    root.mkdir(parents=True, exist_ok=True)
    (root / "tenant.json").write_text(json.dumps({"tenant_id": tenant_id, "workspaces": workspaces}, indent=2), encoding="utf-8")
    (root / "README.md").write_text("# Fabric tenant source\n\nOne native Fabric Git directory per workspace. Credentials stay outside this repository.\n", encoding="utf-8")
    (root / ".gitignore").write_text(".ray/\n.env\n*.dpapi\n__pycache__/\n", encoding="utf-8")
    (root / "deployment").mkdir()
    for workspace in workspaces:
        path = root / "workspaces" / workspace
        path.mkdir(parents=True)
        (path / "README.md").write_text("# Workspace source\n\nImport the existing Fabric baseline before organizing item folders.\n", encoding="utf-8")
    from .runtime import clean_env
    result = subprocess.run([git, "-c", "init.templateDir=", "init", "--initial-branch=main", str(root)],
                            shell=False, capture_output=True, timeout=30, env=clean_env())
    if result.returncode:
        raise RuntimeError("Git initialization failed; local scaffold is preserved for inspection")
    return {"root": str(root), "tenant_id": tenant_id, "workspaces": workspaces, "remote_created": False}


def import_workspace(project, gateway, workspace, commit):
    from .tenant import github_settings
    from .context import manifest
    github = github_settings(project)
    binding = next((w for w in github.workspaces if w.workspace == workspace), None)
    if binding is None:
        raise ValueError("Workspace has no enrolled Git directory")
    manifest(project.repo)
    _, tree = remote_tree(gateway, github, commit)
    selected = {p: entry for p, entry in tree.items() if p.startswith(binding.directory + "/")}
    if not selected:
        raise ValueError("No workspace files exist at the requested commit")
    pending, total = {}, 0
    for relative, entry in selected.items():
        content = remote_blob(gateway, github, entry)
        validate_native(relative, content)
        path = (project.repo / relative).resolve()
        if not path.is_relative_to(project.repo):
            raise ValueError("Imported source escapes the checkout")
        if path.exists() and path.read_bytes() != content:
            raise ValueError("Import would overwrite local edits; use a fresh checkout or reconcile first")
        pending[path] = content
        total += len(content)
        if total > 20_000_000 or len(pending) > 1000:
            raise ValueError("Workspace import exceeds bounds")
    item_inventory({p.relative_to(project.repo).as_posix(): data for p, data in pending.items()})
    for path, content in pending.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
    return {"commit": commit, "directory": binding.directory, "files": len(pending), "source": "live_tenant_api" if gateway.live else "provided_executor"}


def render_definition(project, workspace, logical_id, source):
    """Render a reviewed/FMD REST definition into its existing native Git item.

    Metadata identity is preserved; this is local preparation, never deployment.
    """
    from .tenant import github_settings, source_json
    from .context import manifest
    from .artifacts import _resolve_definition
    logical_id = identifier(logical_id)
    binding = next((w for w in github_settings(project).workspaces if w.workspace == workspace), None)
    if binding is None:
        raise ValueError("Workspace has no enrolled Git directory")
    workspace_root = (project.repo / binding.directory).resolve(strict=True)
    if not workspace_root.is_relative_to(project.repo):
        raise ValueError("Workspace source escapes the tenant checkout")
    manifest(workspace_root)
    platforms = {p.relative_to(workspace_root).as_posix(): p.read_bytes() for p in workspace_root.rglob(".platform")}
    item = item_inventory(platforms).get(logical_id)
    if not item:
        raise ValueError("Import the native item baseline before rendering its definition")
    item_root = (workspace_root / item["directory"]).resolve(strict=True)
    value = source_json(project, source)
    _resolve_definition(project.repo, value)
    parts = value.get("definition", {}).get("parts")
    if not isinstance(parts, list) or not 1 <= len(parts) <= 100:
        raise ValueError("Expected a complete Fabric item definition")
    pending = {}
    for part in parts:
        if set(part) != {"path", "payload", "payloadType"} or part["payloadType"] != "InlineBase64" or part["path"] == ".platform":
            raise ValueError("Definition rendering requires inline parts and preserves existing .platform metadata")
        relative_path(part["path"])
        target = (item_root / part["path"]).resolve()
        if not target.is_relative_to(item_root) or target in pending:
            raise ValueError("Definition part escapes or duplicates an item path")
        content = base64.b64decode(part["payload"], validate=True)
        validate_native(target.relative_to(project.repo).as_posix(), content)
        pending[target] = content
    existing = {p.resolve() for p in item_root.rglob("*") if p.is_file() and p.name != ".platform"}
    if existing - pending.keys():
        raise ValueError("Rendering would remove existing definition parts; reconcile the format explicitly")
    for path, content in pending.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
    return {"source_prepared": True, "logical_id": logical_id, "files": [p.relative_to(project.repo).as_posix() for p in pending]}


def allocate(root, workspace_directory, allocation, *, apply=False):
    """Move local item directories only. A cloud sync still requires reviewed actions."""
    relative_path(workspace_directory)
    root = Path(root).resolve(strict=True)
    workspace = (root / workspace_directory).resolve(strict=True)
    if not workspace.is_relative_to(root):
        raise ValueError("Workspace path escapes the tenant checkout")
    from .context import manifest
    manifest(workspace)  # reject junctions/symlinks before any move
    files = {p.relative_to(workspace).as_posix(): p.read_bytes() for p in workspace.rglob(".platform")}
    inventory = item_inventory(files)
    moves = []
    for logical, folder in allocation.items():
        identifier(logical); relative_path(folder)
        if logical not in inventory or any(p.startswith(".") for p in PurePosixPath(folder).parts):
            raise ValueError("Unknown logical ID or unsafe allocation folder")
        source = (workspace / inventory[logical]["directory"]).resolve(strict=True)
        target = (workspace / folder / source.name).resolve()
        if not target.is_relative_to(workspace) or not source.is_relative_to(workspace):
            raise ValueError("Allocation must stay inside the workspace")
        if source == target:
            continue
        if target.is_relative_to(source):
            raise ValueError("Allocation cannot nest an item inside itself")
        if target.exists() or any(target == previous[1] for previous in moves):
            raise ValueError("Allocation would overwrite another item")
        moves.append((source, target))
    if apply:
        for source, target in moves:
            target.parent.mkdir(parents=True, exist_ok=True)
            source.rename(target)
    return {"applied": apply, "moves": [{"from": s.relative_to(root).as_posix(), "to": t.relative_to(root).as_posix()} for s, t in moves]}


def validate_checkout(root):
    """Validate native source without executing notebooks, SQL or repository scripts."""
    from .context import manifest
    root = Path(root).resolve(strict=True)
    paths = manifest(root)
    native, count = {}, 0
    for relative in paths:
        path = root / relative
        if path.name == ".platform" or is_native_path(relative):
            content = path.read_bytes()
            validate_native(relative, content)
            count += 1
            parts = PurePosixPath(relative).parts
            if parts[0] != "workspaces" or len(parts) < 4:
                raise ValueError("Native items must live in a tenant workspace directory")
            native.setdefault(parts[1], {})[relative] = content
    items = sum(len(item_inventory(files)) for files in native.values())
    return {"validated_files": count, "items": items, "cloud_operations": 0}


def main():
    parser = argparse.ArgumentParser(description="Create a local Fabric tenant Git checkout")
    parser.add_argument("root", type=Path)
    parser.add_argument("--tenant")
    parser.add_argument("--workspace", action="append")
    parser.add_argument("--validate", action="store_true", help="Check native source without creating a repository")
    args = parser.parse_args()
    if not args.validate and (not args.tenant or not args.workspace):
        parser.error("scaffolding requires --tenant and --workspace")
    print(json.dumps(validate_checkout(args.root) if args.validate else scaffold(args.root, args.tenant, args.workspace), indent=2))


if __name__ == "__main__":
    main()
