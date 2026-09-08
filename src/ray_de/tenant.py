"""Pure capability resolution and request construction; no cloud I/O."""
from __future__ import annotations
import json
import re
from pathlib import Path

from .tenant_config import identifier, relative_path, secret_references


def settings(project):
    tenant = project.config.fabric.tenant
    if tenant is None:
        raise ValueError("Enroll a tenant and explicit capabilities before using tenant operations")
    return tenant


def grant_for(project, key, workspace=None):
    tenant = settings(project)
    grant = next((g for g in tenant.grants if g.key == key), None)
    if grant is None or workspace is not None and workspace != grant.workspace:
        raise ValueError("Tenant capability or workspace is outside the configured grant")
    policy = project.config.policy
    if not policy.local_write or not (policy.fabric_dev_write if grant.environment == "DEV" else policy.fabric_test_write == "approval"):
        raise ValueError("Tenant capability writes are disabled for this environment")
    return grant


def resources(project, store):
    return store.tenant_resources(project)


def resolve_resources(value, catalog, principals=None):
    principals = principals or {}
    if isinstance(value, dict):
        if "principalRef" in value:
            if set(value) != {"principalRef"} or value["principalRef"] not in principals:
                raise ValueError("Unknown or invalid workspace administrator reference")
            return principals[value["principalRef"]].model_dump()
        if "resourceRef" in value:
            if set(value) - {"resourceRef", "field"}:
                raise ValueError("Invalid resource reference")
            field = value.get("field", "id")
            if field not in {"id", "appId", "applicationId", "servicePrincipalId"}:
                raise ValueError("Unsupported resource field")
            resource = catalog.get(value["resourceRef"], {})
            return identifier(resource.get(field))
        return {k: resolve_resources(v, catalog, principals) for k, v in value.items()}
    if isinstance(value, list):
        return [resolve_resources(v, catalog, principals) for v in value]
    return value


def workspace_id(project, store, reference, environment=None):
    if reference.startswith("@"):
        resource = resources(project, store).get(reference[1:], {})
        if resource.get("kind") != "workspace":
            raise ValueError("Workspace creation has no verified successful receipt")
        reference = resource.get("id")
    value = identifier(reference)
    match = next((w for w in project.workspaces if w.id.lower() == value), None)
    if match is None or environment and match.environment != environment or match.environment == "PROD":
        raise ValueError("Workspace is outside the tenant's managed DEV/TEST scope")
    return value


def github_settings(project):
    github = settings(project).github
    if github is None:
        raise ValueError("Personal GitHub repository is not enrolled")
    return github


def git_binding(project, workspace):
    github = github_settings(project)
    bound = next((w for w in github.workspaces if w.workspace == workspace), None)
    if bound is None:
        raise ValueError("Workspace has no enrolled Git directory")
    return {"gitProviderType": "GitHub", "ownerName": github.owner,
            "repositoryName": github.repository, "branchName": github.branch, "directoryName": bound.directory}


def source_json(project, relative):
    relative_path(relative)
    path = (project.repo / relative).resolve(strict=True)
    if not path.is_relative_to(project.repo) or not path.is_file() or path.stat().st_size > 6_000_000:
        raise ValueError("Action source must be a bounded file in the reviewed repository")
    from .memory import safe_text
    text = path.read_text(encoding="utf-8")
    safe_text(text, limit=6_000_000)
    value = json.loads(text)
    if not isinstance(value, dict):
        raise ValueError("Action source must be a JSON object")
    return value


def fields(value, required=(), optional=()):
    if not isinstance(value, dict) or set(required) - value.keys() or value.keys() - set(required) - set(optional):
        raise ValueError("Capability parameters contain missing or unsupported fields")


def label(value):
    if not isinstance(value, str) or not value.strip() or len(value) > 200 or any(ord(c) < 32 for c in value):
        raise ValueError("Invalid resource name")
    return value


def build_request(project, store, grant, arguments):
    """Only enumerated operations construct endpoints; source cannot choose a URL."""
    p = resolve_resources(grant.parameters, resources(project, store), settings(project).workspace_admins)
    secret_references(p)
    op = grant.operation
    dynamic = {"create_folder", "move_item", "git_commit", "git_update", "github_publish"}
    if op not in dynamic:
        fields(arguments)
    ws = workspace_id(project, store, grant.workspace, grant.environment) if grant.workspace else ""
    if op in {"assign_capacity", "provision_identity", "assign_workspace_role", "create_folder", "move_item",
              "git_connect", "git_initialize", "git_commit", "git_update"} and not ws:
        raise ValueError("Capability requires a managed workspace")
    provider, method, endpoint, payload = "fabric", "post", "", p
    if op == "create_workspace":
        fields(p, ("displayName", "capacityId"))
        label(p["displayName"]); identifier(p["capacityId"])
        endpoint = "workspaces"
    elif op == "assign_capacity":
        fields(p, ("capacityId",)); identifier(p["capacityId"])
        endpoint = f"workspaces/{ws}/assignToCapacity"
    elif op == "provision_identity":
        fields(p); endpoint, payload = f"workspaces/{ws}/provisionIdentity", None
    elif op in {"assign_workspace_role", "assign_connection_role"}:
        fields(p, ("principal", "role"), ("connectionId", "assignmentId"))
        fields(p["principal"], ("id", "type"))
        identifier(p["principal"]["id"])
        if p["principal"]["type"] not in {"User", "Group", "ServicePrincipal"}:
            raise ValueError("Only a specific enrolled principal can receive a role")
        roles = {"Admin", "Member", "Contributor", "Viewer"} if op == "assign_workspace_role" else {"Owner", "User", "UserWithReshare"}
        if p["role"] not in roles:
            raise ValueError("Unsupported role")
        if op == "assign_workspace_role":
            endpoint = f"workspaces/{ws}/roleAssignments"
            if "assignmentId" in p:
                method, endpoint = "patch", endpoint + "/" + identifier(p["assignmentId"])
                payload = {"role": p["role"]}
            else:
                payload = {"principal": p["principal"], "role": p["role"]}
        else:
            endpoint = f"connections/{identifier(p.get('connectionId'))}/roleAssignments"
            payload = {"principal": p["principal"], "role": p["role"]}
    elif op == "create_folder":
        fields(p, ("allowed_names",)); fields(arguments, ("displayName",), ("parentFolderId",))
        if arguments["displayName"] not in p["allowed_names"]:
            raise ValueError("Folder name is outside the enrolled allocation")
        label(arguments["displayName"])
        if arguments.get("parentFolderId"):
            identifier(arguments["parentFolderId"])
        endpoint, payload = f"workspaces/{ws}/folders", arguments
    elif op == "move_item":
        fields(p); fields(arguments, ("itemId", "targetFolderId"))
        endpoint = f"workspaces/{ws}/items/{identifier(arguments['itemId'])}/move"
        payload = {"targetFolderId": identifier(arguments["targetFolderId"])}
    elif op in {"create_connection", "update_connection"}:
        fields(p, ("displayName", "connectionDetails", "credentialDetails"), ("connectivityType", "privacyLevel", "connectionId"))
        label(p["displayName"])
        if p.get("connectivityType", "ShareableCloud") != "ShareableCloud":
            raise ValueError("Only explicitly configured shareable cloud connections are supported")
        fields(p["credentialDetails"], ("credentials",), ("singleSignOnType", "connectionEncryption"))
        # The host never skips connection authentication tests.
        fields(p["connectionDetails"], ("type", "creationMethod", "parameters"))
        if not isinstance(p["connectionDetails"]["parameters"], list):
            raise ValueError("Connection parameters must be a list")
        names = []
        for parameter in p["connectionDetails"]["parameters"]:
            fields(parameter, ("name", "dataType", "value"))
            if not isinstance(parameter["name"], str) or parameter["name"] in names:
                raise ValueError("Connection parameters must have unique names")
            names.append(parameter["name"])
        credentials = p["credentialDetails"]["credentials"]
        credential_fields = {"Key": ("key",), "Basic": ("username", "password"),
                             "ServicePrincipal": ("tenantId", "servicePrincipalClientId", "servicePrincipalSecret"),
                             "WorkspaceIdentity": (), "Anonymous": ()}
        kind = credentials.get("credentialType")
        if kind not in credential_fields:
            raise ValueError("Connector authentication needs a supported unattended credential type")
        fields(credentials, ("credentialType", *credential_fields[kind]))
        if kind == "ServicePrincipal":
            identifier(credentials["tenantId"]); identifier(credentials["servicePrincipalClientId"])
        payload = {k: v for k, v in p.items() if k != "connectionId"}
        if op == "create_connection":
            payload["connectivityType"] = "ShareableCloud"; endpoint = "connections"
        else:
            method, endpoint = "patch", "connections/" + identifier(p.get("connectionId"))
            payload.pop("connectivityType", None)
    elif op == "create_application":
        fields(p, ("displayName",)); label(p["displayName"])
        provider, endpoint, payload = "graph", "applications", {**p, "signInAudience": "AzureADMyOrg"}
    elif op == "create_service_principal":
        fields(p, ("appId",)); identifier(p["appId"])
        # Only applications created by this project's verified capability can be instantiated.
        if not any(r.get("kind") == "application" and r.get("appId") == p["appId"] for r in resources(project, store).values()):
            raise ValueError("Service principal requires an application created by this project")
        provider, endpoint = "graph", "servicePrincipals"
    elif op == "create_application_credential":
        fields(p, ("applicationId", "credential_ref", "displayName", "endDateTime"))
        from datetime import datetime, timezone, timedelta
        expiry = datetime.fromisoformat(p["endDateTime"].replace("Z", "+00:00"))
        if expiry.tzinfo is None or not datetime.now(timezone.utc) < expiry <= datetime.now(timezone.utc) + timedelta(days=366):
            raise ValueError("Credential expiry must be explicit and within one year")
        if not re.fullmatch(r"[a-z][a-z0-9_-]{0,63}", p["credential_ref"]):
            raise ValueError("Invalid protected credential reference")
        if not any(r.get("kind") == "application" and r.get("id") == p["applicationId"] for r in resources(project, store).values()):
            raise ValueError("Credentials can only be issued for an application created by this project")
        label(p["displayName"])
        provider, endpoint = "graph", f"applications/{identifier(p['applicationId'])}/addPassword"
        payload = {"passwordCredential": {"displayName": p["displayName"], "endDateTime": p["endDateTime"]}, "storeSecretRef": p["credential_ref"]}
    elif op == "create_group":
        fields(p, ("displayName", "mailNickname")); label(p["displayName"])
        if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_-]{0,63}", p["mailNickname"]):
            raise ValueError("Invalid group alias")
        provider, endpoint, payload = "graph", "groups", {**p, "mailEnabled": False, "securityEnabled": True, "groupTypes": [], "isAssignableToRole": False}
    elif op == "add_group_member":
        fields(p, ("groupId", "memberId")); identifier(p["memberId"])
        if not any(r.get("kind") == "group" and r.get("id") == p["groupId"] for r in resources(project, store).values()):
            raise ValueError("Membership changes are limited to groups created by this project")
        provider, endpoint = "graph", f"groups/{identifier(p['groupId'])}/members/$ref"
        payload = {"@odata.id": "https://graph.microsoft.com/v1.0/directoryObjects/" + p["memberId"]}
    elif op == "git_connect":
        fields(p, ("connectionId",))
        endpoint, payload = f"workspaces/{ws}/git/connect", {"gitProviderDetails": git_binding(project, grant.workspace),
            "myGitCredentials": {"source": "ConfiguredConnection", "connectionId": identifier(p["connectionId"])}}
    elif op == "git_initialize":
        fields(p, ("initializationStrategy",))
        if p["initializationStrategy"] not in {"PreferWorkspace", "PreferRemote"}:
            raise ValueError("Unsupported initialization strategy")
        endpoint = f"workspaces/{ws}/git/initializeConnection"
    elif op == "git_commit":
        fields(p); fields(arguments, ("workspaceHead", "snapshot", "message"))
        label(arguments["message"])
        endpoint, payload = f"workspaces/{ws}/git/commitToGit", {"mode": "All", "workspaceHead": arguments["workspaceHead"], "comment": arguments["message"]}
    elif op == "git_update":
        fields(p); fields(arguments, ("workspaceHead", "remoteCommitHash"))
        endpoint, payload = f"workspaces/{ws}/git/updateFromGit", arguments
    elif op == "github_create_repository":
        fields(p); github = github_settings(project)
        provider, endpoint, payload = "github", "user/repos", {"name": github.repository, "private": True, "auto_init": True}
    elif op == "github_publish":
        fields(p); fields(arguments, ("expected_head", "message", "files"), ("remove",))
        label(arguments["message"])
        provider, endpoint, payload = "github", "", arguments
    else:
        raise ValueError("Unimplemented capability")
    if op in {"git_commit", "git_update"}:
        for key in ("workspaceHead", "remoteCommitHash"):
            if key in payload and payload[key] is not None and not re.fullmatch(r"[0-9a-f]{40}", payload[key]):
                raise ValueError("Git heads must be full SHA-1 hashes")
        if op == "git_update" and payload["remoteCommitHash"] is None:
            raise ValueError("A reviewed remote commit is required")
    return dict(provider=provider, method=method, endpoint=endpoint, payload=payload, workspace_id=ws)


def review_paths(project, proposal):
    if proposal.operation != "tenant_action":
        return []
    grant = grant_for(project, proposal.item_id, proposal.workspace_id)
    arguments = source_json(project, proposal.definition_path)
    paths = []
    if grant.operation == "github_publish":
        paths = arguments.get("files", [])
    elif grant.operation == "git_commit":
        paths = [arguments.get("snapshot", "")]
    if not isinstance(paths, list) or any(not isinstance(p, str) for p in paths):
        raise ValueError("Invalid referenced source paths")
    for path in paths:
        relative_path(path)
        if not (project.repo / path).resolve(strict=True).is_relative_to(project.repo):
            raise ValueError("Referenced review source escapes repository")
    return paths
