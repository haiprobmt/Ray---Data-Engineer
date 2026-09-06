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


class PolicyError(ValueError):
    pass


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
        "list_lakehouse_tables",
    }:
        raise PolicyError("Operation is unavailable through the read-only gateway")
    ws_id = canonical_id(workspace_id)
    workspace = next(
        (w for w in project.config.fabric.workspaces if w.id.lower() == ws_id), None
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
        result = subprocess.run(
            [sys.executable, "-m", "ray_de.sql_worker"],
            input=json.dumps(request), shell=False, capture_output=True, text=True,
            encoding="utf-8", errors="replace", timeout=75,
            env=fabric_env(self.data_dir, self.project.id), cwd=self.data_dir,
        )
        try:
            envelope = json.loads(result.stdout)
        except (ValueError, TypeError):
            raise RayError("FABRIC_SQL_UNAVAILABLE") from None
        if result.returncode or not isinstance(envelope, dict) or "error_code" in envelope:
            code = envelope.get("error_code") if isinstance(envelope, dict) else None
            raise RayError(code if code in {"FABRIC_SQL_SETUP", "FABRIC_SQL_SIGNIN_REQUIRED"} else "FABRIC_SQL_UNAVAILABLE")
        return envelope

    def read(self, request, *, task_id=None):
        from .schemas import ReadRequest
        if task_id:
            self.store.task(self.project.id, task_id)
        try:
            req = ReadRequest.model_validate(request)
            operation = req.operation
            environment, endpoint = authorize(self.project, "get_lakehouse", req.workspace_id, req.item_id)
        except ValueError:
            action = self.store.audit_start(self.project.id, task_id, self.actor, "policy_check", "rejected-target", None)
            self.store.audit_finish(action, "DENIED", "PolicyError")
            raise
        action = self.store.audit_start(self.project.id, task_id, self.actor, operation, endpoint, environment)
        try:
            if operation in {"get_lakehouse", "list_lakehouse_tables"}:
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
            from .memory import redact
            def redacted(value):
                if isinstance(value, str):
                    return redact(value)
                if isinstance(value, list):
                    return [redacted(v) for v in value]
                if isinstance(value, dict):
                    return {redact(k): redacted(v) for k, v in value.items()}
                return value
            data = redacted(data)
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
        except json.JSONDecodeError as exc:
            raise RuntimeError("Fabric CLI returned invalid JSON") from exc
        if not isinstance(envelope, dict) or envelope.get("status_code") != 200:
            status = envelope.get("status_code") if isinstance(envelope, dict) else None
            raise RayError({401: "FABRIC_AUTH_REQUIRED", 403: "FABRIC_FORBIDDEN", 404: "FABRIC_NOT_FOUND", 429: "FABRIC_LIMIT"}.get(status, "FABRIC_UNAVAILABLE"))
        payload = envelope.get("text")
        if not isinstance(payload, dict):
            raise RuntimeError("Fabric returned an unexpected response shape")
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
            return payload
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
            for w in self.project.config.fabric.workspaces
        ]

    def snapshot(self):
        if not self.project.config.fabric.workspaces:
            raise PolicyError(
                "Configure an allow-listed workspace before taking a snapshot"
            )
        workspaces = []
        for ws in self.project.config.fabric.workspaces:
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
