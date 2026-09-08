"""Offline contract tests; these do not claim live Fabric/Graph/GitHub acceptance."""
import base64
import copy
import hashlib
import io
import json
from email.message import Message
from pathlib import Path

import pytest

from ray_de.cloud import CloudActions, TenantActions, digest
from ray_de.config import Project, ProjectConfig
from ray_de.fabric import PolicyError, TenantGateway
from ray_de.orchestrator import repo_digest
from ray_de.state import StateStore
from ray_de.tenant import build_request, grant_for
from ray_de.tenant_config import TenantConfig
from ray_de.tenant_repository import git_blob_sha, scaffold, allocate, publication, import_workspace

TENANT, CLIENT, DEV, TEST, CAP, NEW, ITEM, FOLDER, APP, APPID, SP, GROUP, CONN, PRINCIPAL, OP = [f"{i:08d}-1111-1111-1111-{i:012d}" for i in range(1, 16)]
PREFIX = "workspaces/dev"
HEAD = "a" * 40


def platform(logical=ITEM, name="Load", kind="Notebook"):
    return json.dumps({"version": "2.0", "config": {"logicalId": logical}, "metadata": {"type": kind, "displayName": name}}).encode()


class Managed:
    def __init__(self):
        self.calls, self.lost, self.fail_verify = [], None, False
        self.workspaces = {w: {"id": w, "displayName": "DEV" if w == DEV else "TEST", "capacityId": CAP, "capacityRegion": "West US"} for w in (DEV, TEST)}
        self.items = {w: {ITEM: {"id": ITEM, "displayName": "Load", "type": "Notebook"}} for w in (DEV, TEST)}
        self.folders, self.roles, self.connections, self.apps, self.sps, self.groups, self.members = {}, {}, {}, {}, {}, {}, {}
        self.git = {w: {"gitConnectionState": "NotConnected"} for w in (DEV, TEST)}
        self.credentials = {}
        self.status = {w: {"workspaceHead": HEAD, "remoteCommitHash": HEAD, "changes": []} for w in (DEV, TEST)}
        self.repo_exists = True
        self.repo = {"id": 1234, "private": True, "owner": {"login": "hai", "type": "User"}, "default_branch": "main"}
        self.head = HEAD
        self.blobs, self.trees, self.commits = {}, {}, {}
        self.seed({PREFIX + "/README.md": b"# Workspace source\n"})

    def seed(self, files):
        tree = {}
        for path, data in files.items():
            sha = git_blob_sha(data)
            self.blobs[sha] = data
            tree[path] = {"path": path, "mode": "100644", "type": "blob", "sha": sha}
        tree_sha = digest(tree)[:40]
        self.trees[tree_sha] = tree
        self.commits[self.head] = {"sha": self.head, "tree": {"sha": tree_sha}}

    def managed_request(self, provider, method, endpoint, payload=None):
        self.calls.append((provider, method, endpoint, copy.deepcopy(payload)))
        code, data = 200, {}
        path = endpoint.split("?")[0]
        parts = path.split("/")
        if provider == "github":
            if path == "user":
                data = {"login": "hai"}
            elif path == "user/repos":
                assert payload["private"] is True
                self.repo_exists = True; data = self.repo; code = 201
            elif len(parts) == 3:
                code, data = (200, self.repo) if self.repo_exists else (404, {})
            elif "/git/ref/" in path:
                data = {"object": {"sha": self.head}}
            elif "/git/refs/" in path:
                assert payload["force"] is False
                self.head = payload["sha"]
                for status in self.status.values():
                    status["remoteCommitHash"] = self.head
                    status["changes"] = [{"remoteChange": "Modified", "conflictType": "None", "itemMetadata": {"itemIdentifier": {"objectId": ITEM, "logicalId": ITEM}}}]
            elif path.endswith("/git/blobs"):
                raw = base64.b64decode(payload["content"])
                sha = git_blob_sha(raw); self.blobs[sha] = raw
                data, code = {"sha": sha}, 201
            elif "/git/blobs/" in path:
                raw = self.blobs[parts[-1]]
                data = {"encoding": "base64", "content": base64.b64encode(raw).decode(), "size": len(raw)}
            elif path.endswith("/git/trees"):
                tree = copy.deepcopy(self.trees[payload["base_tree"]])
                for entry in payload["tree"]:
                    if entry["sha"] is None:
                        tree.pop(entry["path"], None)
                    else:
                        tree[entry["path"]] = entry
                sha = digest(tree)[:40]; self.trees[sha] = tree; data = {"sha": sha}; code = 201
            elif "/git/trees/" in path:
                data = {"tree": list(self.trees[parts[-1]].values()), "truncated": False}
            elif path.endswith("/git/commits"):
                sha = digest(payload)[:40]
                self.commits[sha] = {"sha": sha, "tree": {"sha": payload["tree"]}, "parents": payload["parents"]}
                data, code = self.commits[sha], 201
            elif "/git/commits/" in path:
                data = self.commits[parts[-1]]
            else:
                raise AssertionError((provider, method, endpoint))
        elif provider == "graph":
            table = {"applications": self.apps, "servicePrincipals": self.sps, "groups": self.groups}[parts[0]]
            if path.endswith("/addPassword"):
                data = {"keyId": OP, "displayName": payload["passwordCredential"]["displayName"], "endDateTime": payload["passwordCredential"]["endDateTime"], "credential_stored": True, "credential_ref": payload["storeSecretRef"]}
                self.apps[parts[1]]["passwordCredentials"] = [data]
            elif len(parts) == 1:
                id = {"applications": APP, "servicePrincipals": SP, "groups": GROUP}[parts[0]]
                data = {"id": id, **payload}
                if parts[0] == "applications": data["appId"] = APPID
                table[id] = data; code = 201
            elif path.endswith("/members/$ref"):
                self.members.setdefault(parts[1], []).append({"id": payload["@odata.id"].rsplit("/", 1)[1]})
                code = 204
            elif path.endswith("/members"):
                data = {"value": self.members.get(parts[1], [])}
            else:
                data = table[parts[1]]
        elif path == "capacities":
            data = {"value": [{"id": CAP, "state": "Active", "region": "West US"}]}
        elif path == "connections/supportedConnectionTypes":
            data = {"value": [{"type": kind, "creationMethods": [{"name": "SQL" if kind == "SQL" else "GitHubSourceControl.Contents"}], "supportedCredentialTypes": ["Key", "ServicePrincipal", "WorkspaceIdentity"]} for kind in ("SQL", "GitHubSourceControl")]}
        elif parts[0] == "connections":
            if path.endswith("/roleAssignments"):
                if method == "post":
                    self.roles.setdefault(path, []).append({"id": PRINCIPAL, **payload}); code = 201
                data = {"value": self.roles.get(path, [])}
            elif method in {"post", "patch"}:
                id = CONN if len(parts) == 1 else parts[1]
                details = payload.get("connectionDetails", self.connections.get(id, {}).get("connectionDetails", {}))
                params = {p["name"]: p["value"] for p in details.get("parameters", [])}
                details = dict(details, path=params.get("url") or params.get("server", "") + ";" + params.get("database", ""))
                data = {"id": id, **payload, "connectionDetails": details,
                        "credentialDetails": {"credentialType": payload["credentialDetails"]["credentials"]["credentialType"], "skipTestConnection": False}}
                self.connections[id] = data; code = 201 if method == "post" else 200
            else:
                data = {"value": list(self.connections.values())} if len(parts) == 1 else self.connections[parts[1]]
        elif path == "workspaces":
            if method == "post":
                data = {"id": NEW, **payload, "capacityRegion": "West US"}
                self.workspaces[NEW] = data; self.items[NEW] = {}; code = 201
            else:
                data = {"value": list(self.workspaces.values())}
        elif parts[0] == "workspaces":
            ws = parts[1]
            if len(parts) == 2:
                if self.fail_verify and ws == NEW: raise TimeoutError("simulated read failure")
                data = self.workspaces[ws]
            elif parts[2] == "assignToCapacity":
                self.workspaces[ws]["capacityId"] = payload["capacityId"]
            elif parts[2] == "provisionIdentity":
                data = {"applicationId": APPID, "servicePrincipalId": SP}
                self.workspaces[ws]["workspaceIdentity"] = data
            elif parts[2] == "roleAssignments":
                key = "/".join(parts[:3])
                if method == "post": self.roles.setdefault(key, []).append({"id": PRINCIPAL, **payload}); code = 201
                if method == "patch": self.roles[key][0]["role"] = payload["role"]
                data = {"value": self.roles.get(key, [])}
            elif parts[2] == "folders":
                if method == "post":
                    data = {"id": FOLDER, **payload}; self.folders.setdefault(ws, {})[FOLDER] = data; code = 201
                else:
                    data = {"value": list(self.folders.get(ws, {}).values())} if len(parts) == 3 else self.folders[ws][parts[3]]
            elif parts[2] == "items":
                if path.endswith("/move"):
                    self.items[ws][parts[3]]["folderId"] = payload["targetFolderId"]
                elif path.endswith("/getDefinition"):
                    data = {"definition": {"parts": [{"path": "notebook-content.py", "payloadType": "InlineBase64", "payload": base64.b64encode(b"# Fabric notebook source\nprint(1)\n").decode()}]}}
                else:
                    data = {"value": list(self.items[ws].values())} if len(parts) == 3 else self.items[ws][parts[3]]
            elif parts[2] == "sqlDatabases":
                data = {"id": parts[3], "workspaceId": ws, "type": "SQLDatabase", "properties": {
                    "serverFqdn": "example.database.fabric.microsoft.com,1433", "databaseName": "FMD"}}
            elif parts[2] == "git":
                operation = parts[3]
                if operation == "connection": data = self.git[ws]
                elif operation == "myGitCredentials": data = self.credentials[ws]
                elif operation == "status": data = self.status[ws]
                elif operation == "connect":
                    self.git[ws] = {"gitConnectionState": "Connected", "gitProviderDetails": payload["gitProviderDetails"]}
                    self.credentials[ws] = payload["myGitCredentials"]
                elif operation == "initializeConnection":
                    self.git[ws]["gitConnectionState"] = "ConnectedAndInitialized"
                    data = {"requiredAction": "CommitToGit" if payload["initializationStrategy"] == "PreferWorkspace" else "UpdateFromGit", "workspaceHead": self.status[ws]["workspaceHead"], "remoteCommitHash": self.head}
                elif operation in {"commitToGit", "updateFromGit"}:
                    self.status[ws] = {"workspaceHead": self.head, "remoteCommitHash": self.head, "changes": []}
                else: raise AssertionError(endpoint)
            else:
                raise AssertionError(endpoint)
        else:
            raise AssertionError((provider, method, endpoint))
        if self.lost == (method, path):
            self.lost = None
            raise TimeoutError("simulated lost response after mutation")
        return {"status_code": code, "text": copy.deepcopy(data), "headers": {}}


def grants():
    from datetime import datetime, timedelta, timezone
    values = [
        {"key": "new", "operation": "create_workspace", "parameters": {"displayName": "FMD Code", "capacityId": CAP}},
        {"key": "capacity", "operation": "assign_capacity", "workspace": DEV, "parameters": {"capacityId": CAP}},
        {"key": "identity", "operation": "provision_identity", "workspace": "@new"},
        {"key": "role", "operation": "assign_workspace_role", "workspace": DEV, "parameters": {"principal": {"id": PRINCIPAL, "type": "ServicePrincipal"}, "role": "Contributor"}},
        {"key": "user-admin", "operation": "assign_workspace_role", "workspace": DEV, "parameters": {"principal": {"principalRef": "primary"}, "role": "Admin"}},
        {"key": "folder", "operation": "create_folder", "workspace": DEV, "parameters": {"allowed_names": ["Bronze", "Silver"]}},
        {"key": "move", "operation": "move_item", "workspace": DEV},
        {"key": "app", "operation": "create_application", "parameters": {"displayName": "FMD execution"}},
        {"key": "sp", "operation": "create_service_principal", "parameters": {"appId": {"resourceRef": "app", "field": "appId"}}},
        {"key": "group", "operation": "create_group", "parameters": {"displayName": "FMD readers", "mailNickname": "fmd-readers"}},
        {"key": "member", "operation": "add_group_member", "parameters": {"groupId": {"resourceRef": "group"}, "memberId": PRINCIPAL}},
        {"key": "connection", "operation": "create_connection", "parameters": {
            "displayName": "GitHub", "connectionDetails": {"type": "GitHubSourceControl", "creationMethod": "GitHubSourceControl.Contents", "parameters": [{"name": "url", "dataType": "Text", "value": "https://github.com/hai/fabric"}]},
            "credentialDetails": {"credentials": {"credentialType": "Key", "key": {"secretRef": "github"}}}}},
        {"key": "connect", "operation": "git_connect", "workspace": DEV, "parameters": {"connectionId": {"resourceRef": "connection"}}},
        {"key": "initialize", "operation": "git_initialize", "workspace": DEV, "parameters": {"initializationStrategy": "PreferWorkspace"}},
        {"key": "baseline", "operation": "git_commit", "workspace": DEV},
        {"key": "publish", "operation": "github_publish"},
        {"key": "update", "operation": "git_update", "workspace": DEV},
        {"key": "repository", "operation": "github_create_repository"},
        {"key": "test-folder", "operation": "create_folder", "workspace": TEST, "environment": "TEST", "parameters": {"allowed_names": ["Reviewed"]}},
    ]
    values.extend([
        {"key": "app-credential", "operation": "create_application_credential", "parameters": {"applicationId": {"resourceRef": "app"},
            "credential_ref": "execution", "displayName": "FMD execution credential", "endDateTime": (datetime.now(timezone.utc) + timedelta(days=30)).isoformat()}},
        {"key": "connection-update", "operation": "update_connection", "parameters": {**copy.deepcopy(values[11]["parameters"]), "connectionId": {"resourceRef": "connection"}}},
        {"key": "connection-role", "operation": "assign_connection_role", "parameters": {"connectionId": {"resourceRef": "connection"}, "principal": {"id": PRINCIPAL, "type": "ServicePrincipal"}, "role": "User"}},
        {"key": "sql-connection", "operation": "create_connection", "parameters": {"displayName": "SQL FMD",
            "connectionDetails": {"type": "SQL", "creationMethod": "SQL", "parameters": []}, "credentialDetails": {"credentials": {"credentialType": "WorkspaceIdentity"}},
            "sqlDatabase": {"workspace": DEV, "itemId": ITEM}}},
    ])
    return values


@pytest.fixture
def setup(tmp_path):
    repo = tmp_path / "repo"; repo.mkdir()
    (repo / "main.py").write_text("value = 1\n")
    config = ProjectConfig.model_validate({"project_id": "tenant-test", "name": "Offline tenant fixture", "repo_path": str(repo),
        "policy": {"local_write": True, "fabric_dev_write": True, "fabric_test_write": "approval"},
        "fabric": {"workspaces": [{"id": DEV, "environment": "DEV"}, {"id": TEST, "environment": "TEST"}],
            "tenant": {"tenant_id": TENANT, "client_id": CLIENT, "workspace_admins": {"primary": {"id": OP, "type": "User"}}, "grants": grants(), "github": {
                "owner": "hai", "repository": "fabric", "credential_ref": "github", "workspaces": [
                    {"workspace": DEV, "directory": PREFIX}, {"workspace": TEST, "directory": "workspaces/test", "environment": "TEST"}]}}},
        "validation_commands": [["{python}", "-c", "pass"]], "post_validation_commands": [["{python}", "-c", "pass"]]})
    project = Project(config, tmp_path / "config.yaml")
    store = StateStore(tmp_path / "state" / "ray.db"); store.bind(project)
    task = store.create(project.id, "Offline tenant capability test", "write")
    fake = Managed(); cloud = CloudActions(project, store, tmp_path / "state", transport=fake)
    def plan(key, arguments=None, *, review_extra=()):
        (repo / "action.json").write_text(json.dumps(arguments or {}))
        store.update(project.id, task["id"], "COMPLETED", result={"cloud_eligible": True, "host_validation": ["synthetic source check"],
            "review": {"verdict": "PASS"}, "repo_digest": repo_digest(project), "review_paths": ["action.json", *review_extra]})
        grant = next(g for g in config.fabric.tenant.grants if g.key == key)
        return cloud.prepare(task["id"], "tenant_action", grant.workspace, key, "action.json", "local:test")
    return project, store, task, fake, cloud, plan


def execute(setup, key, arguments=None, *, review_extra=()):
    plan = setup[5](key, arguments, review_extra=review_extra)
    return setup[4].execute(plan["id"], "local:test")


def test_creation_receipt_enrolls_workspace_and_dependent_identity(setup):
    p, store, task, fake, cloud, plan = setup
    before = p.binding
    result = execute(setup, "new")
    assert result["state"] == "SUCCEEDED" and p.binding == before
    assert any(w.id == NEW and w.environment == "DEV" for w in p.workspaces)
    assert execute(setup, "identity")["state"] == "SUCCEEDED"
    assert store.tenant_resources(p)["identity"]["servicePrincipalId"] == SP
    with pytest.raises(ValueError): plan("new")
    with pytest.raises(ValueError): cloud.execute(result["id"], "local:test")


def test_missing_creation_receipt_cannot_authorize_reference(setup):
    with pytest.raises(ValueError, match="receipt"): setup[5]("identity")


def test_named_workspace_administrator_resolves_to_user_admin(setup):
    request = build_request(setup[0], setup[1], grant_for(setup[0], "user-admin", DEV), {})
    assert request["payload"] == {"principal": {"id": OP, "type": "User"}, "role": "Admin"}
    assert execute(setup, "user-admin")["state"] == "SUCCEEDED"
    assert any(call[2] == f"workspaces/{DEV}/roleAssignments" and call[3] == request["payload"] for call in setup[3].calls)


def test_lost_creation_response_not_adopted_by_name_or_replayed(setup):
    p, store, task, fake, cloud, plan = setup
    pending = plan("new"); fake.lost = ("post", "workspaces")
    with pytest.raises(TimeoutError): cloud.execute(pending["id"], "local:test")
    assert cloud.get(pending["id"])["state"] == "UNCERTAIN"
    assert not any(w.id == NEW for w in p.workspaces)
    with pytest.raises(ValueError): cloud.reconcile(pending["id"], "local:test")
    assert sum(c[1:3] == ("post", "workspaces") for c in fake.calls) == 1


def test_creation_read_failure_reconciles_without_resubmission(setup):
    p, store, task, fake, cloud, plan = setup
    pending = plan("new"); fake.fail_verify = True
    with pytest.raises(TimeoutError): cloud.execute(pending["id"], "local:test")
    fake.fail_verify = False
    assert cloud.reconcile(pending["id"], "local:test")["state"] == "SUCCEEDED"
    assert sum(c[1:3] == ("post", "workspaces") for c in fake.calls) == 1


def test_test_capability_requires_exact_approval(setup):
    pending = setup[5]("test-folder", {"displayName": "Reviewed"})
    with pytest.raises(ValueError): setup[4].execute(pending["id"], "local:test")
    with pytest.raises(ValueError): setup[4].approve(pending["id"], "local:test", "wrong")
    setup[4].approve(pending["id"], "local:test", pending["digest"])
    assert setup[4].execute(pending["id"], "local:test")["state"] == "SUCCEEDED"


def test_folder_allocation_keeps_item_identity(setup):
    assert execute(setup, "folder", {"displayName": "Bronze"})["state"] == "SUCCEEDED"
    before = copy.deepcopy(setup[3].items[DEV][ITEM])
    assert execute(setup, "move", {"itemId": ITEM, "targetFolderId": FOLDER})["state"] == "SUCCEEDED"
    assert setup[3].items[DEV][ITEM] == dict(before, folderId=FOLDER)
    with pytest.raises(ValueError): setup[5]("folder", {"displayName": "Arbitrary"})


def test_capacity_and_workspace_role_contracts(setup):
    assert execute(setup, "capacity")["state"] == "SUCCEEDED"
    assert execute(setup, "role")["state"] == "SUCCEEDED"
    with pytest.raises(ValueError): setup[5]("role", {"role": "Admin"})


def test_graph_app_sp_and_group_membership_use_recorded_ids(setup):
    for key in ("app", "sp", "group", "member"):
        assert execute(setup, key)["state"] == "SUCCEEDED"
    assert setup[3].sps[SP]["appId"] == APPID
    assert setup[3].members[GROUP] == [{"id": PRINCIPAL}]
    assert not setup[3].groups[GROUP]["isAssignableToRole"]


def test_application_credential_receipt_contains_only_protected_reference(setup):
    execute(setup, "app")
    result = execute(setup, "app-credential")
    assert result["state"] == "SUCCEEDED"
    receipt = json.loads(result["result"])
    assert receipt["credential_ref"] == "execution" and receipt["key_id"] == OP


def test_connection_update_and_sharing_keep_source_target(setup):
    execute(setup, "connection")
    assert execute(setup, "connection-update")["state"] == "SUCCEEDED"
    request = next(c for c in setup[3].calls if c[1] == "patch" and c[2] == "connections/" + CONN)
    assert "connectionDetails" not in request[3]
    assert request[3]["connectivityType"] == "ShareableCloud"
    assert execute(setup, "connection-role")["state"] == "SUCCEEDED"


def test_sql_connection_binds_to_discovered_database_properties(setup):
    result = execute(setup, "sql-connection")
    assert result["state"] == "SUCCEEDED"
    call = next(c for c in setup[3].calls if c[1:3] == ("post", "connections"))
    params = {p["name"]: p["value"] for p in call[3]["connectionDetails"]["parameters"]}
    assert params == {"server": "example.database.fabric.microsoft.com,1433", "database": "FMD"}


def test_directory_role_group_change_is_rejected(setup):
    execute(setup, "group")
    setup[3].groups[GROUP]["isAssignableToRole"] = True
    with pytest.raises(ValueError): setup[5]("member")


def test_connection_uses_reference_and_git_initialization_returns_next_step(setup):
    execute(setup, "connection")
    call = next(c for c in setup[3].calls if c[1:3] == ("post", "connections"))
    assert call[3]["credentialDetails"]["credentials"]["key"] == {"secretRef": "github"}
    execute(setup, "connect")
    result = execute(setup, "initialize")
    assert result["state"] == "SUCCEEDED"
    assert json.loads(result["result"])["required_action"] == "CommitToGit"
    assert json.loads(result["result"])["full_sync"] is False


def test_private_repository_creation_checks_personal_owner(setup):
    setup[3].repo_exists = False
    assert execute(setup, "repository")["state"] == "SUCCEEDED"
    assert setup[3].repo["private"] is True


def test_changed_source_or_remote_state_blocks_prepared_action(setup):
    pending = setup[5]("new")
    (setup[0].repo / "changed.py").write_text("changed = True")
    with pytest.raises(ValueError): setup[4].execute(pending["id"], "local:test")
    assert not any(c[1] == "post" for c in setup[3].calls)


def test_tenant_resources_need_success_and_matching_plan_digest(setup):
    pending = execute(setup, "new")
    with setup[1].connect() as db:
        db.execute("UPDATE plans SET state='UNCERTAIN' WHERE id=?", (pending["id"],))
    assert setup[1].tenant_resources(setup[0]) == {}


def write_native(project, prefix=PREFIX + "/Load.Notebook"):
    data = {prefix + "/.platform": platform(), prefix + "/notebook-content.py": b"# Fabric notebook source\nprint(1)\n"}
    for relative, raw in data.items():
        path = project.repo / relative; path.parent.mkdir(parents=True, exist_ok=True); path.write_bytes(raw)
    return list(data)


def test_git_publish_and_update_are_bound_to_reviewed_commit(setup):
    p, store, task, fake, cloud, plan = setup
    execute(setup, "connection"); execute(setup, "connect")
    files = write_native(p)
    result = execute(setup, "publish", {"expected_head": HEAD, "message": "Add notebook", "files": files}, review_extra=files)
    commit = json.loads(result["result"])["commit"]
    assert commit == fake.head and commit != HEAD
    assert execute(setup, "update", {"workspaceHead": HEAD, "remoteCommitHash": commit})["state"] == "SUCCEEDED"
    assert all(not (call[1] == "patch" and call[3].get("force")) for call in fake.calls)


def test_git_update_rejects_external_unreviewed_commit(setup):
    execute(setup, "connection"); execute(setup, "connect")
    with pytest.raises(ValueError, match="reviewed publication"):
        setup[5]("update", {"workspaceHead": HEAD, "remoteCommitHash": HEAD})


def test_clean_git_status_does_not_hide_wrong_deployed_definition(setup):
    p, _, _, fake, _, _ = setup
    execute(setup, "connection"); execute(setup, "connect")
    files = write_native(p)
    result = execute(setup, "publish", {"expected_head": HEAD, "message": "Add", "files": files}, review_extra=files)
    commit = json.loads(result["result"])["commit"]
    original = fake.managed_request
    def wrong(provider, method, endpoint, payload=None):
        response = original(provider, method, endpoint, payload)
        if "/getDefinition" in endpoint:
            response["text"]["definition"]["parts"][0]["payload"] = base64.b64encode(b"print(999)").decode()
        return response
    fake.managed_request = wrong
    with pytest.raises(ValueError, match="differs from the reviewed Git"):
        execute(setup, "update", {"workspaceHead": HEAD, "remoteCommitHash": commit})


def test_git_sync_cannot_smuggle_unreviewed_remote_edits_with_a_readme_publish(setup):
    p, _, _, fake, _, plan = setup
    execute(setup, "connection"); execute(setup, "connect")
    files = write_native(p)
    fake.seed({f: (p.repo / f).read_bytes() for f in files})
    external = "f" * 40
    fake.head = external
    fake.seed({files[0]: platform(), files[1]: b"print('external unreviewed code')"})
    marker = PREFIX + "/README.md"
    (p.repo / marker).write_text("Reviewed documentation change")
    result = execute(setup, "publish", {"expected_head": external, "message": "Docs", "files": [marker]}, review_extra=[marker])
    commit = json.loads(result["result"])["commit"]
    with pytest.raises(PolicyError, match="unreviewed source"):
        plan("update", {"workspaceHead": HEAD, "remoteCommitHash": commit})
    assert not any(c[2].endswith("/git/updateFromGit") for c in fake.calls)


def test_git_publication_requires_every_source_file_in_review(setup):
    files = write_native(setup[0])
    with pytest.raises(ValueError, match="independent source review"):
        setup[5]("publish", {"expected_head": HEAD, "message": "Add", "files": files})


def test_publication_rejects_cross_environment_paths(setup):
    files = write_native(setup[0], "workspaces/test/Load.Notebook")
    with pytest.raises(ValueError, match="environment"):
        setup[5]("publish", {"expected_head": HEAD, "message": "Add", "files": files}, review_extra=files)


def test_publication_rejects_replaced_logical_identity(setup):
    p, _, _, fake, _, plan = setup
    files = write_native(p)
    fake.seed({f: (p.repo / f).read_bytes() for f in files})
    (p.repo / files[0]).write_bytes(platform(NEW))
    with pytest.raises(ValueError, match="delete or recreate"):
        plan("publish", {"expected_head": HEAD, "message": "Replace", "files": files}, review_extra=files)


def test_publication_allows_complete_folder_move_preserving_logical_id(setup):
    p, _, _, fake, _, _ = setup
    old = write_native(p)
    fake.seed({f: (p.repo / f).read_bytes() for f in old})
    new = write_native(p, PREFIX + "/Bronze/Load.Notebook")
    result = execute(setup, "publish", {"expected_head": HEAD, "message": "Allocate Bronze", "files": new, "remove": old}, review_extra=new)
    assert result["state"] == "SUCCEEDED"


def test_publication_rejects_partial_folder_move(setup):
    p, _, _, fake, _, plan = setup
    old = write_native(p)
    fake.seed({f: (p.repo / f).read_bytes() for f in old})
    new = write_native(p, PREFIX + "/Bronze/Load.Notebook")
    with pytest.raises(ValueError, match="every existing item part"):
        plan("publish", {"expected_head": HEAD, "message": "Incomplete move", "files": new, "remove": [old[0]]}, review_extra=new)


def test_lost_git_ref_response_reconciles_without_second_patch(setup):
    p, _, _, fake, cloud, plan = setup
    files = write_native(p)
    pending = plan("publish", {"expected_head": HEAD, "message": "Add", "files": files}, review_extra=files)
    fake.lost = ("patch", "repos/hai/fabric/git/refs/heads/main")
    with pytest.raises(TimeoutError): cloud.execute(pending["id"], "local:test")
    assert cloud.reconcile(pending["id"], "local:test")["state"] == "SUCCEEDED"
    assert sum(c[1] == "patch" for c in fake.calls) == 1


def test_initial_baseline_requires_reviewed_export_and_is_single_use(setup):
    p, store, task, fake, _, _ = setup
    execute(setup, "connection"); execute(setup, "connect"); execute(setup, "initialize")
    gateway = TenantGateway(p, store, p.repo.parent / "state", executor=fake.managed_request)
    (p.repo / "baseline.json").write_text(json.dumps(gateway.export(DEV)))
    args = {"workspaceHead": HEAD, "snapshot": "baseline.json", "message": "Initial baseline"}
    assert execute(setup, "baseline", args, review_extra=["baseline.json"])["state"] == "SUCCEEDED"
    with pytest.raises(ValueError): setup[5]("baseline", args, review_extra=["baseline.json"])


def test_local_scaffold_and_allocation_never_recreate_item_identity(tmp_path):
    root = tmp_path / "tenant"
    result = scaffold(root, TENANT, ["dev"])
    assert (root / ".git").is_dir() and not result["remote_created"]
    item = root / PREFIX / "Load.Notebook"; item.mkdir()
    (item / ".platform").write_bytes(platform())
    (item / "notebook-content.py").write_bytes(b"print(1)")
    moves = allocate(root, PREFIX, {ITEM: "Bronze"}, apply=True)
    assert moves["applied"] and not item.exists()
    assert (root / PREFIX / "Bronze/Load.Notebook/.platform").read_bytes() == platform()
    assert allocate(root, PREFIX, {ITEM: "Bronze"}, apply=True)["moves"] == []
    with pytest.raises(ValueError): allocate(root, PREFIX, {ITEM: "../../escape"}, apply=True)


def test_native_import_refuses_overwrite(setup):
    p, store, task, fake, _, _ = setup
    files = {PREFIX + "/Load.Notebook/.platform": platform(), PREFIX + "/Load.Notebook/notebook-content.py": b"print(1)"}
    fake.seed(files)
    gateway = TenantGateway(p, store, p.repo.parent / "state", executor=fake.managed_request)
    assert import_workspace(p, gateway, DEV, HEAD)["files"] == 2
    (p.repo / PREFIX / "Load.Notebook/notebook-content.py").write_text("print(2)")
    with pytest.raises(ValueError, match="overwrite"): import_workspace(p, gateway, DEV, HEAD)


def test_fmd_rest_definition_renders_into_existing_native_item(setup):
    from ray_de.tenant_repository import render_definition
    p = setup[0]
    files = write_native(p)
    (p.repo / "new-source.txt").write_text("# Fabric notebook source\nprint(2)\n")
    (p.repo / "definition.json").write_text(json.dumps({"definition": {"format": "fabricGitSource", "parts": [
        {"path": "notebook-content.py", "payloadType": "SourceFile", "source": "new-source.txt"}]}}))
    result = render_definition(p, DEV, ITEM, "definition.json")
    assert result["source_prepared"] and (p.repo / files[0]).read_bytes() == platform()
    assert "print(2)" in (p.repo / files[1]).read_text()


def test_model_tenant_proposal_gets_real_host_validation_and_review(setup):
    from ray_de.orchestrator import Orchestrator
    p, store, task, _, cloud, _ = setup
    class Runner:
        def run(self, project, prompt, schema, **kwargs):
            if "verdict" in schema["properties"]:
                kwargs["on_thread"]("reviewer")
                assert "create_workspace" in prompt and "capacityId" in prompt and "action.json" in prompt
                return {"verdict": "PASS", "summary": "Synthetic reviewer", "findings": [], "evidence": ["Reviewed explicit capability and arguments"]}
            kwargs["on_thread"]("primary")
            return {"status": "completed", "message": "Workspace source prepared", "recommendation": None, "question": None,
                "options": [], "evidence": ["Prepared capability arguments"], "skills_used": [], "artifacts": [{"path": "action.json", "content": "{}"}],
                "cloud_actions": [{"operation": "tenant_action", "workspace_id": "", "item_id": "new", "definition_path": "action.json"}]}
    report = Orchestrator(store, Runner()).run(p, task["id"], "Create the enrolled workspace")
    assert report["cloud_eligible"] and report["host_validation"]
    plan = cloud.prepare(task["id"], "tenant_action", "", "new", "action.json", "local:test")
    assert cloud.execute(plan["id"], "local:test")["state"] == "SUCCEEDED"


def test_model_can_allocate_local_source_with_host_scope(setup):
    p, store, task, fake, _, _ = setup
    write_native(p)
    (p.repo / "allocation.json").write_text(json.dumps({ITEM: "Bronze"}))
    gateway = TenantGateway(p, store, p.repo.parent / "state", executor=fake.managed_request)
    result = gateway.read("allocate", DEV, {"id": "allocation.json"}, task_id=task["id"])
    assert result["data"]["applied"] and not fake.calls
    assert (p.repo / PREFIX / "Bronze/Load.Notebook/.platform").read_bytes() == platform()
    with pytest.raises(PolicyError): gateway.read("allocate", DEV, {"id": "allocation.json"})


def test_native_checkout_validation_keeps_logical_ids_unique_per_workspace(setup):
    from ray_de.tenant_repository import validate_checkout
    p = setup[0]
    write_native(p)
    write_native(p, "workspaces/test/Load.Notebook")
    assert validate_checkout(p.repo)["items"] == 2
    write_native(p, PREFIX + "/Duplicate.Notebook")
    with pytest.raises(ValueError, match="Duplicate logical"):
        validate_checkout(p.repo)


def test_example_enrollment_is_local_only_and_has_source_validation():
    import yaml
    config = ProjectConfig.model_validate(yaml.safe_load(Path("docs/examples/tenant-config.yaml").read_text()))
    assert not config.policy.local_write and not config.policy.fabric_dev_write
    assert config.validation_commands and len(config.fabric.tenant.github.workspaces) == 3


def test_git_sync_checks_folder_placement_even_with_clean_status(setup):
    p, _, _, fake, _, _ = setup
    execute(setup, "connection"); execute(setup, "connect")
    files = write_native(p, PREFIX + "/Bronze/Load.Notebook")
    result = execute(setup, "publish", {"expected_head": HEAD, "message": "Allocate", "files": files}, review_extra=files)
    commit = json.loads(result["result"])["commit"]
    with pytest.raises(PolicyError, match="folder differs"):
        execute(setup, "update", {"workspaceHead": HEAD, "remoteCommitHash": commit})
    fake.folders[DEV] = {FOLDER: {"id": FOLDER, "displayName": "Bronze"}}
    fake.items[DEV][ITEM]["folderId"] = FOLDER
    with setup[1].connect() as db:
        pending = db.execute("SELECT id FROM plans WHERE state='UNCERTAIN'").fetchone()["id"]
    assert setup[4].reconcile(pending, "local:test")["state"] == "SUCCEEDED"


def test_creation_lro_receipt_survives_poll_failure_without_replay(setup):
    p, store, _, fake, cloud, plan = setup
    action = plan("new")
    original = fake.managed_request
    unavailable = True
    def asynchronous(provider, method, endpoint, payload=None):
        if endpoint == "operations/" + OP:
            if unavailable: raise TimeoutError("lost polling response")
            return {"status_code": 200, "text": {"status": "Succeeded"}, "headers": {"retry-after": "0"}}
        if endpoint == "operations/" + OP + "/result":
            return {"status_code": 200, "text": fake.workspaces[NEW]}
        response = original(provider, method, endpoint, payload)
        if method == "post" and endpoint == "workspaces":
            return {"status_code": 202, "headers": {"x-ms-operation-id": OP, "retry-after": "0"}}
        return response
    fake.managed_request = asynchronous
    with pytest.raises(TimeoutError): cloud.execute(action["id"], "local:test")
    assert json.loads(cloud.get(action["id"])["remote"]) == {"kind": "operation", "id": OP}
    unavailable = False
    assert cloud.reconcile(action["id"], "local:test")["state"] == "SUCCEEDED"
    assert sum(c[1:3] == ("post", "workspaces") for c in fake.calls) == 1
    assert any(w.id == NEW for w in p.workspaces)


def test_git_status_read_handles_fabric_async_result(setup):
    p, store, _, fake, _, _ = setup
    def asynchronous(provider, method, endpoint, payload=None):
        if endpoint.endswith("/git/status"):
            return {"status_code": 202, "headers": {"location": "https://api.fabric.microsoft.com/v1/operations/" + OP}}
        if endpoint == "operations/" + OP:
            return {"status_code": 200, "text": {"status": "Succeeded"}}
        if endpoint == "operations/" + OP + "/result":
            return {"status_code": 200, "text": fake.status[DEV]}
        raise AssertionError(endpoint)
    gateway = TenantGateway(p, store, p.repo.parent / "state", executor=asynchronous)
    assert gateway.read("git_status", DEV)["data"]["workspaceHead"] == HEAD


def test_sql_discovery_never_adopts_by_name_or_arbitrary_host(setup):
    p, store, _, fake, _, _ = setup
    gateway = TenantGateway(p, store, p.repo.parent / "state", executor=fake.managed_request)
    with pytest.raises(PolicyError, match="creation receipt"):
        gateway.sql_connection_target({"workspace": DEV, "createdItemName": "FMD"}, "DEV")
    original = fake.managed_request
    def wrong_host(provider, method, endpoint, payload=None):
        response = original(provider, method, endpoint, payload)
        response["text"]["properties"]["serverFqdn"] = "attacker.example"
        return response
    gateway.executor = wrong_host
    with pytest.raises(PolicyError, match="supported Fabric endpoint"):
        gateway.sql_connection_target({"workspace": DEV, "itemId": ITEM}, "DEV")


@pytest.mark.parametrize("change", ["prod", "raw_secret", "overlap", "unknown_reference", "unknown_principal", "mixed_principal"])
def test_bad_tenant_enrollment_is_rejected(setup, change):
    config = setup[0].config.fabric.tenant.model_dump()
    if change == "prod": config["grants"][0]["environment"] = "PROD"
    elif change == "raw_secret": config["grants"][11]["parameters"]["credentialDetails"]["credentials"]["key"] = "do-not-store"
    elif change == "overlap": config["github"]["workspaces"][1]["directory"] = PREFIX + "/nested"
    elif change == "unknown_reference": config["grants"][2]["workspace"] = "@unknown"
    elif change == "unknown_principal": config["grants"][4]["parameters"]["principal"] = {"principalRef": "unknown"}
    else: config["grants"][4]["parameters"]["principal"] = {"principalRef": "primary", "type": "User"}
    with pytest.raises(ValueError): TenantConfig.model_validate(config)


def test_workspace_administrator_changes_project_binding(setup, tmp_path):
    raw = setup[0].config.model_dump()
    raw["fabric"]["tenant"]["workspace_admins"]["primary"]["id"] = PRINCIPAL
    changed = Project(ProjectConfig.model_validate(raw), tmp_path / "changed.yaml")
    assert changed.binding != setup[0].binding
