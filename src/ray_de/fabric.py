from __future__ import annotations

import getpass
import json
import re
import subprocess
import sys
from pathlib import Path
from urllib.parse import quote
from uuid import UUID

from .runtime import clean_env
from .state import now
from .errors import RayError
from .memory import redact_data


class PolicyError(ValueError):
    pass


class TenantGateway:
    """Tenant-scoped cloud reads; mutation executors remain in cloud.py."""
    def __init__(self, project, store, data_dir, *, actor="host", executor=None):
        from .tenant import settings
        self.tenant = settings(project)
        self.project, self.store, self.data_dir, self.actor = project, store, Path(data_dir), actor
        self.executor = executor or self._execute
        self.live = executor is None

    def _execute(self, provider, method, endpoint, payload=None):
        envelope = dict(provider=provider, method=method, endpoint=endpoint, payload=payload,
                        tenant_id=self.tenant.tenant_id, client_id=self.tenant.client_id)
        if self.tenant.github:
            envelope["github"] = self.tenant.github.model_dump(exclude={"workspaces"})
        proc = subprocess.run([sys.executable, "-m", "ray_de.tenant_worker"],
            input=json.dumps(envelope), shell=False, capture_output=True, text=True, encoding="utf-8",
            timeout=90, env=fabric_env(self.data_dir, self.project.id))
        if proc.returncode:
            raise RuntimeError("Managed read unavailable; verify local enrollment and resource access")
        return json.loads(proc.stdout)

    def raw(self, provider, endpoint, *, task_id=None, method="get", payload=None, missing=False, response_envelope=False):
        from .tenant_worker import endpoint_path
        if method != "get" and not (provider == "fabric" and method == "post" and endpoint.split("?")[0].endswith("/getDefinition")):
            raise PolicyError("Tenant read gateway cannot mutate cloud resources")
        endpoint_path(provider, method, endpoint, self.tenant.github.model_dump(exclude={"workspaces"}) if self.tenant.github else None)
        action = self.store.audit_start(self.project.id, task_id, self.actor, "tenant_read", provider + ":" + endpoint, None)
        try:
            response = self.executor(provider, method, endpoint, payload)
            if response.get("status_code") == 404 and missing:
                self.store.audit_finish(action, "SUCCEEDED")
                return None
            if response.get("status_code") == 202:
                if provider != "fabric":
                    raise RuntimeError("Unexpected asynchronous read provider")
                from .control import Control
                from .tenant_config import identifier
                from urllib.parse import urlsplit
                import time
                headers = {k.lower(): v for k, v in response.get("headers", {}).items()}
                op = headers.get("x-ms-operation-id")
                if not op:
                    uri = urlsplit(headers.get("location", ""))
                    if uri.scheme != "https" or uri.netloc != "api.fabric.microsoft.com" or not re.fullmatch(r"/v1/operations/[0-9a-f-]{36}", uri.path):
                        raise RuntimeError("Unrecognized asynchronous read location")
                    op = uri.path.rsplit("/", 1)[1]
                op = identifier(op)
                token = Control(self.store).token(self.project.id)
                deadline = time.monotonic() + self.project.config.timeout_seconds
                while time.monotonic() < deadline:
                    token.check()
                    state = self.raw("fabric", "operations/" + op, task_id=task_id)
                    if state.get("status") == "Succeeded":
                        data = self.raw("fabric", "operations/" + op + "/result", task_id=task_id)
                        self.store.audit_finish(action, "SUCCEEDED")
                        return data
                    if state.get("status") in {"Failed", "Cancelled"}:
                        raise RuntimeError("Managed read operation failed")
                    token.wait(min(30, max(1, int(headers.get("retry-after", "2")))))
                raise TimeoutError("Managed read operation is still unresolved")
            if response.get("status_code") not in {200, 201, 204}:
                raise RuntimeError("Managed read was rejected by the provider")
            self.store.audit_finish(action, "SUCCEEDED")
            return response if response_envelope else response.get("text", {})
        except Exception as exc:
            self.store.audit_finish(action, "FAILED", type(exc).__name__)
            raise

    def pages(self, provider, endpoint, *, task_id=None):
        from urllib.parse import urlsplit, parse_qs, urlencode
        values, seen, current = [], set(), endpoint
        for _ in range(100):
            data = self.raw(provider, current, task_id=task_id)
            if not isinstance(data.get("value"), list):
                raise RuntimeError("Invalid managed list response")
            values.extend(data["value"])
            continuation = data.get("continuationToken")
            if continuation:
                current = endpoint + ("&" if "?" in endpoint else "?") + "continuationToken=" + quote(continuation, safe="")
            elif data.get("@odata.nextLink"):
                uri = urlsplit(data["@odata.nextLink"])
                if provider != "graph" or uri.scheme != "https" or uri.netloc != "graph.microsoft.com" or uri.path != "/v1.0/" + endpoint.split("?")[0]:
                    raise RuntimeError("Managed pagination changed its target")
                current = uri.path.removeprefix("/v1.0/") + "?" + uri.query
            elif data.get("continuationUri"):
                raise RuntimeError("Unsupported list continuation")
            else:
                return {"value": values}
            if current in seen or len(values) > 10000:
                raise RuntimeError("Managed pagination exceeded bounds")
            seen.add(current)
        raise RuntimeError("Managed list remains incomplete")

    def export(self, workspace, *, task_id=None):
        """Complete review snapshot. Unsupported exports are explicitly reported."""
        from .tenant import workspace_id
        ws = workspace_id(self.project, self.store, workspace)
        items = self.pages("fabric", f"workspaces/{ws}/items", task_id=task_id)["value"]
        result = {"workspace_id": ws, "items": [], "unsupported": []}
        for item in items:
            item_id = canonical_id(item["id"])
            endpoint = f"workspaces/{ws}/items/{item_id}/getDefinition"
            if item["type"] == "Notebook":
                endpoint += "?format=fabricGitSource"
            try:
                definition = self.raw("fabric", endpoint, method="post", task_id=task_id)
            except RuntimeError:
                result["unsupported"].append({"id": item_id, "type": item["type"], "reason": "definition export unavailable"})
                continue
            result["items"].append({"metadata": item, "definition": definition})
        if len(json.dumps(result).encode()) > 6_000_000:
            raise RuntimeError("Workspace export exceeds review snapshot limit")
        return result

    def sql_connection_target(self, descriptor, environment):
        from .tenant import fields, workspace_id
        from .cloud import digest
        fields(descriptor, ("workspace",), ("itemId", "createdItemName"))
        if bool(descriptor.get("itemId")) == bool(descriptor.get("createdItemName")):
            raise ValueError("SQL binding needs an exact itemId or a receipt-backed createdItemName")
        ws = workspace_id(self.project, self.store, descriptor["workspace"], environment)
        item = descriptor.get("itemId")
        if not item:
            with self.store.connect() as db:
                rows = db.execute("SELECT body,digest,remote FROM plans WHERE project_id=? AND state='SUCCEEDED'", (self.project.id,)).fetchall()
            matches = []
            for row in rows:
                body, remote = json.loads(row["body"]), json.loads(row["remote"] or "{}")
                if (body.get("operation") == "create_item" and body.get("binding") == self.project.binding
                        and digest(body) == row["digest"] and body.get("workspace_id") == ws
                        and body.get("item_type") == "SQLDatabase" and body.get("payload", {}).get("displayName") == descriptor["createdItemName"]
                        and remote.get("kind") == "created_item"):
                    matches.append(canonical_id(remote["id"]))
            if len(set(matches)) != 1:
                raise PolicyError("SQL database has no unique verified creation receipt")
            item = matches[0]
        item = canonical_id(item)
        actual = self.raw("fabric", f"workspaces/{ws}/sqlDatabases/{item}")
        if actual.get("id") != item or actual.get("workspaceId") != ws or actual.get("type") != "SQLDatabase":
            raise PolicyError("SQL database metadata does not match the enrolled item")
        properties = actual.get("properties", {})
        server, database = properties.get("serverFqdn"), properties.get("databaseName")
        if not re.fullmatch(r"[A-Za-z0-9-]+\.database\.fabric\.microsoft\.com(?:,1433)?", server or "") or not re.fullmatch(r"[A-Za-z0-9_ -]{1,128}", database or ""):
            raise PolicyError("SQL database did not expose a supported Fabric endpoint")
        return [{"name": "server", "dataType": "Text", "value": server}, {"name": "database", "dataType": "Text", "value": database}]

    def read(self, kind, workspace="", arguments=None, *, task_id=None):
        from .tenant import fields, workspace_id, github_settings, resources
        arguments = arguments or {}
        fields(arguments, (), ("id", "output"))
        if task_id:
            self.store.task(self.project.id, task_id)
        if kind in {"scaffold", "export_to_source", "import", "render", "allocate"}:
            if not task_id or self.store.task(self.project.id, task_id)["mode"] != "write" or not self.project.config.policy.local_write:
                raise PolicyError("Local tenant source creation requires a write task and local-write policy")
            if kind == "scaffold":
                from .tenant_repository import scaffold
                github = github_settings(self.project)
                data = scaffold(self.project.repo, self.tenant.tenant_id, [w.directory.removeprefix("workspaces/") for w in github.workspaces])
            elif kind == "allocate":
                from .tenant import git_binding, source_json
                from .tenant_repository import allocate
                workspace_id(self.project, self.store, workspace)
                data = allocate(self.project.repo, git_binding(self.project, workspace)["directoryName"],
                                source_json(self.project, arguments.get("id", "")), apply=True)
            elif kind == "render":
                from .tenant_repository import render_definition
                data = render_definition(self.project, workspace, arguments.get("id", ""), arguments.get("output", ""))
            elif kind == "import":
                from .tenant_repository import import_workspace
                data = import_workspace(self.project, self, workspace, arguments.get("id", ""))
            else:
                from .tenant_config import relative_path
                output = arguments.get("output", "")
                relative_path(output)
                path = (self.project.repo / output).resolve()
                if not path.is_relative_to(self.project.repo) or path.exists() or path.suffix != ".json" or any(p.startswith(".") for p in Path(output).parts):
                    raise PolicyError("Export requires a new ordinary JSON source file in the checkout")
                exported = self.export(workspace, task_id=task_id)
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(json.dumps(exported, indent=2), encoding="utf-8")
                data = {"path": output, "items": len(exported["items"]), "unsupported": exported["unsupported"]}
        elif kind == "resources":
            data = resources(self.project, self.store)
        elif kind in {"capacities", "connections", "connection_types"}:
            endpoint = {"capacities": "capacities", "connections": "connections", "connection_types": "connections/supportedConnectionTypes"}[kind]
            data = self.pages("fabric", endpoint, task_id=task_id)
        elif kind in {"github_repository", "github_branch", "github_tree", "github_blob"}:
            github = github_settings(self.project)
            endpoint = f"repos/{github.owner}/{github.repository}"
            if kind == "github_branch":
                endpoint += "/git/ref/heads/" + github.branch
            elif kind in {"github_tree", "github_blob"}:
                sha = arguments.get("id", "")
                if not re.fullmatch(r"[0-9a-f]{40}", sha):
                    raise ValueError("A full Git object SHA is required")
                endpoint += "/git/" + ("trees/" if kind == "github_tree" else "blobs/") + sha
                if kind == "github_tree":
                    endpoint += "?recursive=1"
            data = self.raw("github", endpoint, task_id=task_id)
            if data.get("truncated"):
                raise RuntimeError("Git tree was truncated; narrow the enrolled workspace")
        elif kind == "export":
            data = self.export(workspace, task_id=task_id)
        else:
            ws = workspace_id(self.project, self.store, workspace)
            routes = {"workspace": "", "items": "/items", "folders": "/folders", "roles": "/roleAssignments",
                      "git_connection": "/git/connection", "git_status": "/git/status", "git_credentials": "/git/myGitCredentials"}
            if kind not in routes:
                raise ValueError("Unsupported tenant read")
            if kind == "git_status":
                with self.store.connect() as db:
                    pending = db.execute("SELECT body FROM plans WHERE state IN ('EXECUTING','UNCERTAIN','VALIDATION_FAILED')").fetchall()
                if any(json.loads(row["body"]).get("workspace_id") == ws for row in pending):
                    raise PolicyError("Reconcile the workspace's pending action before requesting Git status")
            endpoint = f"workspaces/{ws}" + routes[kind]
            data = self.pages("fabric", endpoint, task_id=task_id) if kind in {"items", "folders", "roles"} else self.raw("fabric", endpoint, task_id=task_id)
        return {"source": "live_tenant_api" if self.live else "provided_executor", "captured_at": now(), "kind": kind, "data": data}


def canonical_id(value):
    try:
        parsed = str(UUID(value))
        if value.lower() != parsed:
            raise ValueError()
        return parsed
    except (ValueError, TypeError, AttributeError) as exc:
        raise PolicyError("Target must be a canonical UUID") from exc


def authorize(project, operation, workspace_id, item_id=None):
    if operation not in {
        "get_workspace",
        "list_items",
        "get_item",
        "get_lakehouse",
        "get_sql_database",
        "get_environment",
        "list_lakehouse_tables",
    }:
        raise PolicyError("Operation is unavailable through the read-only gateway")
    ws_id = canonical_id(workspace_id)
    workspace = next(
        (w for w in project.workspaces if w.id.lower() == ws_id), None
    )
    if workspace is None:
        raise PolicyError("Workspace is outside the project allow-list")
    base = f"workspaces/{ws_id}"
    if operation == "get_workspace":
        endpoint = base
    elif operation == "list_items":
        endpoint = base + "/items"
    elif operation == "get_item":
        endpoint = base + "/items/" + canonical_id(item_id)
    elif operation == "get_lakehouse":
        endpoint = base + "/lakehouses/" + canonical_id(item_id)
    elif operation in {"get_sql_database", "get_environment"}:
        collection = {"get_sql_database": "sqlDatabases", "get_environment": "environments"}[operation]
        endpoint = base + "/" + collection + "/" + canonical_id(item_id)
    else:
        endpoint = base + "/lakehouses/" + canonical_id(item_id) + "/tables"
    return workspace.environment, endpoint


def fabric_env(data_dir: Path, project_id: str):
    profile = data_dir / "fabric" / project_id
    config_dir = profile / ".config" / "fab"
    config_dir.mkdir(parents=True, exist_ok=True)
    config_file = config_dir / "config.json"
    if not config_file.exists():
        config_file.write_text(
            json.dumps(
                {
                    "output_format": "json",
                    "debug_enabled": "false",
                    "check_cli_version_updates": "false",
                    "encryption_fallback_enabled": "false",
                }
            ),
            encoding="utf-8",
        )
    env = clean_env()
    # Fabric 1.7.0 uses expanduser; isolate its config/token cache per project.
    env.update(USERPROFILE=str(profile), HOME=str(profile))
    return env


class FabricGateway:
    def __init__(self, project, store, data_dir, *, executor=None, actor=None, sql_executor=None):
        self.project, self.store, self.data_dir = project, store, data_dir
        self.executor = executor or self._execute
        self.live_transport = executor is None
        self.actor = actor or getpass.getuser()
        self.sql_executor = sql_executor or self._execute_sql
        self.live_sql = executor is None and sql_executor is None

    def _execute_sql(self, request):
        try:
            result = subprocess.run(
                [sys.executable, "-m", "ray_de.sql_worker"],
                input=json.dumps(request), shell=False, capture_output=True, text=True,
                encoding="utf-8", errors="replace", timeout=75,
                env=fabric_env(self.data_dir, self.project.id), cwd=self.data_dir,
            )
        except subprocess.TimeoutExpired:
            raise RayError("TIMED_OUT") from None
        except OSError:
            raise RayError("LOCAL_RESOURCE") from None
        try:
            envelope = json.loads(result.stdout)
        except (ValueError, TypeError):
            raise RayError("FABRIC_SQL_UNAVAILABLE") from None
        if result.returncode or not isinstance(envelope, dict) or "error_code" in envelope:
            code = envelope.get("error_code") if isinstance(envelope, dict) else None
            raise RayError(code if code in {"FABRIC_SQL_SETUP", "FABRIC_SQL_SIGNIN_REQUIRED", "FABRIC_SQL_TABLE_UNAVAILABLE", "FABRIC_READ_TOO_LARGE"} else "FABRIC_SQL_UNAVAILABLE")
        return envelope

    def notebook_job(self, workspace_id, item_id, job_id, *, task_id=None):
        """Host diagnostics for a bound Notebook job, including its exit value."""
        environment, _ = authorize(self.project, "get_item", workspace_id, item_id)
        workspace_id, item_id, job_id = map(canonical_id, (workspace_id, item_id, job_id))
        endpoint = f"workspaces/{workspace_id}/notebooks/{item_id}/jobs/execute/instances/{job_id}?beta=true"
        if task_id:
            self.store.task(self.project.id, task_id)
        action = self.store.audit_start(self.project.id, task_id, self.actor, "get_notebook_job", endpoint, environment)
        try:
            data = self.executor(endpoint)
            if data.get("id") != job_id or data.get("itemId") not in {None, item_id}:
                raise PolicyError("Notebook job receipt did not match the requested target")
            if len(json.dumps(data).encode()) > 16000:
                raise RayError("FABRIC_READ_TOO_LARGE")
            self.store.audit_finish(action, "SUCCEEDED")
            return redact_data(data)
        except Exception as exc:
            self.store.audit_finish(action, "FAILED", type(exc).__name__)
            raise

    def read(self, request, *, task_id=None):
        from .schemas import ReadRequest
        if task_id:
            self.store.task(self.project.id, task_id)
        try:
            req = ReadRequest.model_validate(request)
            operation = req.operation
            if operation == "tenant_read":
                return TenantGateway(self.project, self.store, self.data_dir, actor=self.actor).read(
                    req.item_id, req.workspace_id, req.tenant_arguments.model_dump(exclude_defaults=True), task_id=task_id)
            base_operation = "get_item" if operation == "get_notebook_job" else operation if operation in {"get_item", "list_items", "get_sql_database", "get_environment"} else "get_lakehouse"
            environment, endpoint = authorize(self.project, base_operation, req.workspace_id, req.item_id)
        except ValueError:
            action = self.store.audit_start(self.project.id, task_id, self.actor, "policy_check", "rejected-target", None)
            self.store.audit_finish(action, "DENIED", "PolicyError")
            raise
        action = self.store.audit_start(self.project.id, task_id, self.actor, operation, endpoint, environment)
        try:
            if operation == "get_notebook_job":
                data = self.notebook_job(req.workspace_id, req.item_id, req.job_id, task_id=task_id)
                source = "live_fabric_api" if self.live_transport else "provided_executor"
            elif operation in {"get_item", "list_items", "get_lakehouse", "get_sql_database", "get_environment", "list_lakehouse_tables"}:
                data = self.call(operation, req.workspace_id, req.item_id, task_id=task_id)
                source = "live_fabric_api" if self.live_transport else "provided_executor"
            else:
                # Discover credentials-free connection metadata through the authorized
                # control plane. Never accept a model-supplied server or database.
                details = self.call("get_lakehouse", req.workspace_id, req.item_id, task_id=task_id)
                if canonical_id(details.get("id")) != canonical_id(req.item_id):
                    raise PolicyError("Lakehouse metadata did not match the requested item")
                if details.get("workspaceId") and canonical_id(details["workspaceId"]) != canonical_id(req.workspace_id):
                    raise PolicyError("Lakehouse metadata did not match the workspace")
                sql = details.get("properties", {}).get("sqlEndpointProperties", {})
                if sql.get("provisioningStatus") != "Success":
                    raise RayError("FABRIC_SQL_NOT_READY")
                server = sql.get("connectionString")
                if not isinstance(server, str) or not re.fullmatch(r"[a-zA-Z0-9-]+\.datawarehouse\.fabric\.microsoft\.com", server):
                    raise PolicyError("Lakehouse SQL hostname is missing or unsupported")
                endpoint_id = canonical_id(sql.get("id"))
                endpoint_info = self.call("get_item", req.workspace_id, endpoint_id, task_id=task_id)
                if canonical_id(endpoint_info.get("id")) != endpoint_id or endpoint_info.get("type") != "SQLEndpoint":
                    raise PolicyError("SQL endpoint metadata did not match the Lakehouse")
                database = endpoint_info.get("displayName")
                if not isinstance(database, str) or not re.fullmatch(r"[A-Za-z0-9_ -]{1,128}", database):
                    raise PolicyError("SQL database name is missing or unsupported")
                data = self.sql_executor({"server": server, "database": database,
                    "operation": operation, "schema_name": req.schema_name, "table_name": req.table_name})
                source = "live_fabric_sql" if self.live_sql else "provided_executor"
            encoded = json.dumps(data, ensure_ascii=False)
            if len(encoded.encode("utf-8")) > 16000:
                raise RayError("FABRIC_READ_TOO_LARGE")
            data = redact_data(data)
            self.store.audit_finish(action, "SUCCEEDED")
            return {"request": req.model_dump(), "source": source, "captured_at": now(), "data": data}
        except Exception as exc:
            self.store.audit_finish(action, "FAILED", type(exc).__name__)
            raise

    def _execute(self, endpoint):
        argv = [
            sys.executable,
            str(Path(__file__).with_name("fabric_worker.py")),
            "get",
            endpoint,
        ]
        try:
            result = subprocess.run(
                argv,
                shell=False,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=60,
                env=fabric_env(self.data_dir, self.project.id),
                cwd=self.data_dir,
            )
        except subprocess.TimeoutExpired:
            raise RayError("TIMED_OUT") from None
        except OSError:
            raise RayError("LOCAL_RESOURCE") from None
        if result.returncode:
            # CLI stderr can contain secrets or user data. Retain only error category.
            try:
                if json.loads(result.stdout).get("error_code") == "FABRIC_SIGNIN_REQUIRED":
                    raise RayError("FABRIC_SIGNIN_REQUIRED")
            except (ValueError, AttributeError):
                pass
            raise RayError("FABRIC_UNAVAILABLE")
        try:
            envelope = json.loads(result.stdout)
        except json.JSONDecodeError:
            raise RayError("FABRIC_UNAVAILABLE") from None
        if not isinstance(envelope, dict) or envelope.get("status_code") != 200:
            status = envelope.get("status_code") if isinstance(envelope, dict) else None
            raise RayError({401: "FABRIC_AUTH_REQUIRED", 403: "FABRIC_FORBIDDEN", 404: "FABRIC_NOT_FOUND", 429: "FABRIC_LIMIT"}.get(status, "FABRIC_UNAVAILABLE"))
        payload = envelope.get("text")
        if not isinstance(payload, dict):
            raise RayError("FABRIC_UNAVAILABLE")
        return payload

    def call(self, operation, workspace_id, item_id=None, *, task_id=None):
        if task_id:
            self.store.task(self.project.id, task_id)
        try:
            environment, endpoint = authorize(
                self.project, operation, workspace_id, item_id
            )
        except PolicyError:
            action = self.store.audit_start(
                self.project.id,
                task_id,
                self.actor,
                "policy_check",
                "rejected-target",
                None,
            )
            self.store.audit_finish(action, "DENIED", "PolicyError")
            raise
        action = self.store.audit_start(
            self.project.id, task_id, self.actor, operation, endpoint, environment
        )
        try:
            if operation in {"list_items", "list_lakehouse_tables"}:
                payload = self._pages(endpoint, task_id, environment)
            else:
                payload = self.executor(endpoint)
            self.store.audit_finish(action, "SUCCEEDED")
            return redact_data(payload)
        except Exception as exc:
            self.store.audit_finish(action, "FAILED", type(exc).__name__)
            raise

    def _pages(self, endpoint, task_id, environment):
        records, seen = [], set()
        current = endpoint
        for _ in range(100):
            action = self.store.audit_start(
                self.project.id, task_id, self.actor, "read_page", endpoint, environment
            )
            try:
                payload = self.executor(current)
                values = payload.get("value", payload.get("data"))
                if not isinstance(values, list) or any(
                    not isinstance(v, dict) for v in values
                ):
                    raise RuntimeError("Fabric list response has an unexpected shape")
                records.extend(values)
                self.store.audit_finish(action, "SUCCEEDED")
            except Exception as exc:
                self.store.audit_finish(action, "FAILED", type(exc).__name__)
                raise
            token = payload.get("continuationToken")
            if not token:
                if payload.get("continuationUri"):
                    raise RuntimeError("Continuation URI without token is unsupported")
                return {"value": records}
            if not isinstance(token, str) or len(token) > 16000 or token in seen:
                raise RuntimeError("Invalid or repeated continuation token")
            seen.add(token)
            # Never follow response-provided URLs; preserve the authorized endpoint.
            current = endpoint + "?continuationToken=" + quote(token, safe="")
        raise RuntimeError("Pagination exceeded 100 pages; snapshot is incomplete")

    def list_workspaces(self):
        # Fetch only configured IDs; never enumerate the user's whole tenant.
        return [
            self.call("get_workspace", w.id)
            for w in self.project.workspaces
        ]

    def snapshot(self):
        if not self.project.workspaces:
            raise PolicyError(
                "Configure an allow-listed workspace before taking a snapshot"
            )
        workspaces = []
        for ws in self.project.workspaces:
            info = self.call("get_workspace", ws.id)
            items = self.call("list_items", ws.id)["value"]
            candidates = [item for item in items if item.get("type") == "Lakehouse"]
            lakehouses, read_errors = [], []
            for item in candidates[:20]:
                try:
                    lakehouses.append(self.call("get_lakehouse", ws.id, item.get("id")))
                except RayError as exc:
                    # A restricted/unready item must not block the entire inventory.
                    read_errors.append({"item_id": canonical_id(item.get("id")), "error_code": exc.code})
            workspaces.append(
                {
                    "id": ws.id,
                    "environment": ws.environment,
                    "metadata": info,
                    "items": items,
                    "lakehouses": lakehouses,
                    "lakehouse_read_errors": read_errors,
                    "lakehouse_details_truncated": len(candidates) > 20,
                }
            )
        snapshot = {
            "project_id": self.project.id,
            "binding": self.project.binding,
            "captured_at": now(),
            "workspaces": workspaces,
            "read_only": True,
            "source": "live_fabric_api" if self.live_transport else "provided_executor",
        }
        from .control import Control
        Control(self.store).set_setting("fabric_observation:" + self.project.id, {
            "binding": self.project.binding,
            "captured_at": snapshot["captured_at"],
            "source": snapshot["source"],
            "completed_reads": ["workspace metadata", "item inventory", "lakehouse metadata"],
        })
        return snapshot
