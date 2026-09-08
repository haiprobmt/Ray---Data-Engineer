"""Explicit, immutable capability enrollment. No credentials or arbitrary HTTP routes."""
from __future__ import annotations

import re
from pathlib import PurePosixPath
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

Operation = Literal[
    "create_workspace", "assign_capacity", "provision_identity", "assign_workspace_role",
    "create_folder", "move_item", "create_connection", "update_connection", "assign_connection_role",
    "create_application", "create_service_principal", "create_application_credential", "create_group", "add_group_member",
    "git_connect", "git_initialize", "git_commit", "git_update", "github_create_repository", "github_publish",
]
OPERATIONS = set(Operation.__args__)
KEY = r"^[a-z][a-z0-9_-]{0,63}$"


def identifier(value):
    if not isinstance(value, str) or str(UUID(value)) != value.lower() or UUID(value).int == 0:
        raise ValueError("Expected a nonzero canonical UUID")
    return value.lower()


def relative_path(value):
    path = PurePosixPath(value)
    if (not value or path.is_absolute() or path.as_posix() != value or "\\" in value
            or any(p in {"..", "."} or p.endswith((".", " ")) or any(c in p for c in ':*?<>|"')
                   or re.fullmatch(r"(?:CON|PRN|AUX|NUL|COM[1-9]|LPT[1-9])(?:\..*)?", p, flags=re.I) for p in path.parts)
            or any(ord(c) < 32 for c in value) or len(value) > 240):
        raise ValueError("Expected a safe relative POSIX path")
    return value


def secret_references(value):
    """Secret-shaped fields must contain an opaque local secret reference."""
    if isinstance(value, dict):
        for key, child in value.items():
            if key.lower() in {"key", "password", "serviceprincipalsecret", "accesstoken", "client_secret"}:
                if not isinstance(child, dict) or set(child) != {"secretRef"} or not re.fullmatch(KEY, child["secretRef"]):
                    raise ValueError("Credentials must use a local secretRef")
            else:
                secret_references(child)
    elif isinstance(value, list):
        for child in value:
            secret_references(child)


def principal_references(value):
    """Yield named, non-secret principals referenced by an enrolled grant."""
    if isinstance(value, dict):
        if "principalRef" in value:
            if set(value) != {"principalRef"} or not isinstance(value["principalRef"], str):
                raise ValueError("A principalRef must be the only field in its object")
            yield value["principalRef"]
        else:
            for child in value.values():
                yield from principal_references(child)
    elif isinstance(value, list):
        for child in value:
            yield from principal_references(child)


class Model(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)


class TenantGrant(Model):
    key: str = Field(pattern=KEY)
    operation: Operation
    environment: Literal["DEV", "TEST"] = "DEV"
    workspace: str = ""
    parameters: dict = Field(default_factory=dict)

    @model_validator(mode="after")
    def valid(self):
        if self.workspace and not re.fullmatch(r"@[a-z][a-z0-9_-]{0,63}", self.workspace):
            identifier(self.workspace)
        secret_references(self.parameters)
        return self


class GitWorkspace(Model):
    workspace: str
    directory: str
    environment: Literal["DEV", "TEST"] = "DEV"

    @model_validator(mode="after")
    def valid(self):
        if not re.fullmatch(r"@[a-z][a-z0-9_-]{0,63}", self.workspace):
            identifier(self.workspace)
        relative_path(self.directory)
        if not self.directory.startswith("workspaces/") or any(p.startswith(".") for p in PurePosixPath(self.directory).parts):
            raise ValueError("Git workspace directories must be beneath workspaces/")
        return self


class WorkspaceAdminUser(Model):
    id: str
    type: Literal["User"] = "User"

    @model_validator(mode="after")
    def valid(self):
        identifier(self.id)
        return self


class GitHubConfig(Model):
    owner: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9-]{0,38}$")
    repository: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,99}$")
    branch: str = Field(default="main", pattern=r"^[A-Za-z0-9][A-Za-z0-9_/-]{0,99}$")
    credential_ref: str = Field(pattern=KEY)
    workspaces: list[GitWorkspace] = Field(default_factory=list, max_length=100)

    @model_validator(mode="after")
    def valid(self):
        relative_path(self.branch)
        if "//" in self.branch or self.branch.endswith("/"):
            raise ValueError("Invalid Git branch")
        paths = [w.directory.casefold() for w in self.workspaces]
        if len(set(w.workspace for w in self.workspaces)) != len(self.workspaces):
            raise ValueError("A workspace can have only one Git directory")
        if any(a == b or a.startswith(b + "/") or b.startswith(a + "/")
               for i, a in enumerate(paths) for b in paths[i+1:]):
            raise ValueError("Git workspace directories must not overlap")
        return self


class TenantConfig(Model):
    tenant_id: str
    client_id: str
    workspace_admins: dict[str, WorkspaceAdminUser] = Field(default_factory=dict, max_length=20)
    grants: list[TenantGrant] = Field(default_factory=list, max_length=200)
    github: GitHubConfig | None = None

    @model_validator(mode="after")
    def valid(self):
        identifier(self.tenant_id)
        identifier(self.client_id)
        names = [g.key for g in self.grants]
        if len(names) != len(set(names)):
            raise ValueError("Capability keys must be unique")
        if any(not re.fullmatch(KEY, name) for name in self.workspace_admins):
            raise ValueError("Workspace administrator keys must be safe identifiers")
        references = {reference for grant in self.grants for reference in principal_references(grant.parameters)}
        if references - self.workspace_admins.keys():
            raise ValueError("Principal references must name an enrolled workspace administrator")
        creates = {"@" + g.key: g for g in self.grants if g.operation == "create_workspace"}
        for entry in [*self.grants, *(self.github.workspaces if self.github else [])]:
            if entry.workspace.startswith("@"):
                parent = creates.get(entry.workspace)
                if parent is None or parent.environment != entry.environment:
                    raise ValueError("Workspace reference must name a creation grant in the same environment")
        return self
