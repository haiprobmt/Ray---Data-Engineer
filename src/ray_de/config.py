from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Literal
from uuid import UUID

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator
from .tenant_config import TenantConfig


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)


class Workspace(StrictModel):
    id: str
    environment: Literal["DEV", "TEST", "PROD"]

    @model_validator(mode="after")
    def valid_id(self):
        if str(UUID(self.id)) != self.id.lower():
            raise ValueError("Workspace ID must be a canonical UUID")
        return self


class Policy(StrictModel):
    local_write: bool = False
    fabric_dev_write: bool = False
    fabric_test_write: Literal[False, "approval"] = False
    fabric_prod_write: Literal[False] = False


class WriteTarget(StrictModel):
    workspace_id: str
    item_id: str
    item_type: str = Field(pattern=r"^[A-Za-z][A-Za-z0-9]{0,79}$")
    job_type: str | None = Field(default=None, pattern=r"^[A-Za-z][A-Za-z0-9]{0,79}$")

    @model_validator(mode="after")
    def validate_target(self):
        if any(
            str(UUID(value)) != value.lower()
            for value in (self.workspace_id, self.item_id)
        ):
            raise ValueError("Write target IDs must be canonical UUIDs")
        if (
            self.job_type
            and self.item_type in {"Notebook", "DataPipeline"}
            and self.job_type != {"Notebook": "RunNotebook", "DataPipeline": "Pipeline"}[self.item_type]
        ):
            raise ValueError("Job type does not match item type")
        return self


class FabricConfig(StrictModel):
    tenant: TenantConfig | None = None
    workspaces: list[Workspace] = Field(default_factory=list)
    write_targets: list[WriteTarget] = Field(default_factory=list)
    allow_definition_export: bool = False
    workspace_write: bool = False
    create_items: bool = False

    @model_validator(mode="after")
    def unique(self):
        ids = [w.id.lower() for w in self.workspaces]
        if len(ids) != len(set(ids)):
            raise ValueError("A workspace may appear only once, in one environment")
        targets = [
            (t.workspace_id.lower(), t.item_id.lower()) for t in self.write_targets
        ]
        if len(targets) != len(set(targets)) or any(
            ws not in ids for ws, item in targets
        ):
            raise ValueError(
                "Write targets must be unique and belong to configured workspaces"
            )
        if self.tenant:
            for entry in [*self.tenant.grants, *(self.tenant.github.workspaces if self.tenant.github else [])]:
                if entry.workspace and not entry.workspace.startswith("@"):
                    match = next((w for w in self.workspaces if w.id.lower() == entry.workspace.lower()), None)
                    if match is None or match.environment != entry.environment:
                        raise ValueError("Tenant capability workspace must match its enrolled environment")
        return self


class ExecutionConfig(StrictModel):
    repair_attempts: int = Field(default=3, ge=0, le=3)
    max_stages: int = Field(default=24, ge=1, le=48)
    max_read_rounds: int = Field(default=12, ge=1, le=24)
    max_model_calls: int = Field(default=64, ge=1, le=128)
    max_seconds: int = Field(default=3600, ge=30, le=7200)


class ProjectConfig(StrictModel):
    project_id: str = Field(pattern=r"^[a-z][a-z0-9_-]{0,63}$")
    name: str
    repo_path: str
    policy: Policy = Field(default_factory=Policy)
    fabric: FabricConfig = Field(default_factory=FabricConfig)
    validation_commands: list[list[str]] = Field(default_factory=list)
    post_validation_commands: list[list[str]] = Field(default_factory=list)
    model: str | None = None
    reasoning_effort: Literal["minimal", "low", "medium", "high", "xhigh"] | None = None
    execution: ExecutionConfig = Field(default_factory=ExecutionConfig)
    timeout_seconds: int = Field(default=600, ge=10, le=7200)

    @model_validator(mode="after")
    def valid_commands(self):
        if any(
            not c or any(not a or "\x00" in a for a in c)
            for c in self.validation_commands + self.post_validation_commands
        ):
            raise ValueError("Validation commands must be nonempty argument arrays")
        return self


class UniqueLoader(yaml.SafeLoader):
    pass


def unique_mapping(loader, node, deep=False):
    result = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in result:
            raise ValueError(f"Duplicate configuration key: {key}")
        result[key] = loader.construct_object(value_node, deep=deep)
    return result


UniqueLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, unique_mapping
)


class Project:
    def __init__(self, config: ProjectConfig, config_path: Path):
        self.config = config
        self.config_path = config_path.resolve()
        self.directory = self.config_path.parent
        candidate = Path(config.repo_path).expanduser()
        self.repo = (
            candidate if candidate.is_absolute() else self.directory / candidate
        ).resolve(strict=True)
        if not self.repo.is_dir():
            raise ValueError("repo_path must be an existing directory")
        self.id = config.project_id

    @property
    def workspaces(self):
        result = list(self.config.fabric.workspaces)
        tenant = self.config.fabric.tenant
        store = getattr(self, "state_store", None)
        if tenant and store:
            resources = store.tenant_resources(self)
            for grant in tenant.grants:
                resource = resources.get(grant.key, {})
                if grant.operation == "create_workspace" and resource.get("kind") == "workspace":
                    entry = Workspace(id=resource["id"], environment=grant.environment)
                    if all(w.id.lower() != entry.id.lower() for w in result):
                        result.append(entry)
        return result

    @property
    def binding(self) -> str:
        # Bind tasks to both their repository and policy, so configuration changes
        # cannot silently resume an old thread with a different target or privilege.
        config = self.config.model_dump()
        # Preserve bindings created before these optional settings existed.
        if config["execution"] == ExecutionConfig().model_dump():
            config.pop("execution")
        if config["reasoning_effort"] is None:
            config.pop("reasoning_effort")
        if config["fabric"]["tenant"] is None:
            config["fabric"].pop("tenant")
        if not config["post_validation_commands"]:
            config.pop("post_validation_commands")
        if not config["fabric"]["write_targets"]:
            config["fabric"].pop("write_targets")
        if not config["fabric"]["allow_definition_export"]:
            config["fabric"].pop("allow_definition_export")
        for key in ("workspace_write", "create_items"):
            if not config["fabric"][key]:
                config["fabric"].pop(key)
        payload = {
            "config": config,
            "repo": str(self.repo),
            "directory": str(self.directory),
        }
        return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()


def load_project(path: Path) -> Project:
    path = path.resolve(strict=True)
    if path.stat().st_size > 128_000:
        raise ValueError("Project configuration is too large")
    config = ProjectConfig.model_validate(
        yaml.load(path.read_text(encoding="utf-8"), Loader=UniqueLoader)
    )
    return Project(config, path)
