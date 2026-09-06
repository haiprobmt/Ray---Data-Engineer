"""Host-owned, read-only workspace enrollment from an authenticated chat command."""

import hashlib
from pathlib import Path
from urllib.parse import urlsplit

import yaml

from .config import ProjectConfig, load_project
from .fabric import canonical_id


def workspace_input(text):
    parts = text.split()
    if len(parts) != 2 or parts[0].upper() not in {"DEV", "TEST", "PROD"}:
        raise ValueError("Use /connect DEV|TEST|PROD <workspace URL or UUID>. Send no passwords or tokens.")
    environment, value = parts[0].upper(), parts[1]
    if value.startswith("https://"):
        url = urlsplit(value)
        segments = url.path.strip("/").split("/")
        if (url.netloc not in {"app.fabric.microsoft.com", "app.powerbi.com"}
                or len(segments) < 2 or segments[0] != "groups"):
            raise ValueError("Use a Fabric workspace URL containing /groups/<workspace UUID>.")
        value = segments[1]
    return environment, canonical_id(value)


class WorkspaceRegistry:
    def __init__(self, store, root):
        self.store = store
        self.root = Path(root).resolve()
        with store.connect() as db:
            db.execute("CREATE TABLE IF NOT EXISTS telegram_workspaces (project_id TEXT PRIMARY KEY, actor TEXT NOT NULL)")

    def owned(self, actor):
        with self.store.connect() as db:
            return [r[0] for r in db.execute(
                "SELECT project_id FROM telegram_workspaces WHERE actor=? ORDER BY project_id", (actor,)
            )]

    def load(self, actor):
        result = []
        for id in self.owned(actor):
            directory = (self.root / id).resolve()
            if directory.parent != self.root:
                raise ValueError("Invalid enrolled project path")
            project = load_project(directory / "config.yaml")
            if project.id != id or project.repo != directory / "repo":
                raise ValueError("Enrolled project target changed")
            self.store.bind(project)
            result.append(project)
        return result

    def enroll(self, actor, text):
        environment, workspace = workspace_input(text)
        for existing in self.load(actor):
            if existing.config.fabric.workspace_write and any(w.id == workspace and w.environment == environment for w in existing.config.fabric.workspaces):
                return existing
        suffix = hashlib.sha256(f"{actor}:{environment}:{workspace}".encode()).hexdigest()[:24]
        id = "fabric-" + environment.lower() + "-" + suffix
        owned = self.owned(actor)
        if id not in owned and len(owned) >= 100:
            raise ValueError("Workspace enrollment limit reached")
        directory = (self.root / id).resolve()
        if directory.parent != self.root:
            raise ValueError("Invalid enrolled project path")
        config = ProjectConfig.model_validate({
            "project_id": id, "name": f"Fabric {environment} {workspace}",
            "repo_path": "repo",
            "fabric": {"workspaces": [{"id": workspace, "environment": environment}]},
        })
        path = directory / "config.yaml"
        if not path.exists():
            (directory / "repo").mkdir(parents=True, exist_ok=True)
            # Exclusive creation never overwrites an existing policy or target.
            with path.open("x", encoding="utf-8") as output:
                output.write(yaml.safe_dump(config.model_dump(), sort_keys=False))
        project = load_project(path)
        if project.config != config or project.repo != directory / "repo":
            raise ValueError("Existing workspace configuration differs; review locally")
        self.store.bind(project)
        with self.store.connect() as db:
            db.execute("INSERT OR IGNORE INTO telegram_workspaces VALUES (?,?)", (id, actor))
        return project

    def enable_authoring(self, actor, project, data_dir):
        """Called only by an explicit authenticated host command, never model output."""
        if project.id not in self.owned(actor):
            raise ValueError("Only the owner can enable an enrolled workspace")
        workspaces = project.config.fabric.workspaces
        if len(workspaces) != 1 or workspaces[0].environment == "PROD":
            raise ValueError("Authoring requires a single DEV or TEST workspace")
        if project.config.fabric.workspace_write and project.config.fabric.create_items:
            return project
        suffix = hashlib.sha256((project.id + ":authoring-v1").encode()).hexdigest()[:24]
        id = "fabric-author-" + suffix
        directory = self.root / id
        cfg = project.config.model_dump()
        cfg.update(project_id=id, name="Ray authoring " + workspaces[0].id, repo_path="repo",
                   validation_commands=[["{python}", "-m", "ray_de.artifacts"]], post_validation_commands=[])
        cfg["policy"].update(local_write=True, fabric_dev_write=workspaces[0].environment == "DEV",
                             fabric_test_write="approval" if workspaces[0].environment == "TEST" else False)
        cfg["fabric"].update(workspace_write=True, create_items=True, allow_definition_export=True)
        config = ProjectConfig.model_validate(cfg)
        path = directory / "config.yaml"
        if not path.exists():
            (directory / "repo").mkdir(parents=True, exist_ok=True)
            with path.open("x", encoding="utf-8") as output:
                output.write(yaml.safe_dump(config.model_dump(), sort_keys=False))
        enabled = load_project(path)
        if enabled.config != config or enabled.repo != directory / "repo":
            raise ValueError("Existing authoring project differs; review locally")
        self.store.bind(enabled)
        # Reuse only this owner's same-workspace authentication, never runtime
        # config, logs, model memories or sessions. Credentials stay in state.
        import shutil
        data_dir = Path(data_dir).resolve()
        for relative in (Path("codex") / project.id / "auth.json",
                         Path("fabric") / project.id / ".config" / "fab" / "auth.json",
                         Path("fabric") / project.id / ".config" / "fab" / "cache.bin"):
            source = data_dir / relative
            destination = data_dir / Path(*[id if part == project.id else part for part in relative.parts])
            if source.is_file() and not destination.exists():
                if not source.resolve().is_relative_to(data_dir) or not destination.resolve().is_relative_to(data_dir):
                    raise ValueError("Authentication profile escapes state directory")
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(source, destination)
        with self.store.connect() as db:
            db.execute("INSERT OR IGNORE INTO telegram_workspaces VALUES (?,?)", (id, actor))
        return enabled
