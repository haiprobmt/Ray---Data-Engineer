"""Source-bound Fabric actions. This is the only write executor in Ray."""

from __future__ import annotations
import base64, hashlib, json, secrets, subprocess, tempfile, time, sys, re
from pathlib import Path, PurePosixPath
from urllib.parse import urlparse
from .control import Control
from .fabric import PolicyError, canonical_id, fabric_env, FabricGateway
from .config import WriteTarget
from .orchestrator import repo_digest, validate
from .memory import safe_text
from .errors import RayError


def digest(value):
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def definition_parts(value):
    if not isinstance(value, dict) or set(value) != {"definition"}:
        raise ValueError("Expected only a definition object")
    definition = value["definition"]
    if not isinstance(definition, dict) or set(definition) - {"parts", "format"}:
        raise ValueError("Unsupported definition fields")
    if definition.get("format") is not None and not re.fullmatch(r"[A-Za-z][A-Za-z0-9_-]{0,79}", definition["format"]):
        raise ValueError("Unsupported definition format")
    parts = definition.get("parts")
    if not isinstance(parts, list) or not 1 <= len(parts) <= 100:
        raise ValueError("Definition must contain 1-100 parts")
    result = {}
    total = 0
    for part in parts:
        if not isinstance(part, dict) or set(part) != {
            "path",
            "payload",
            "payloadType",
        }:
            raise ValueError("Invalid definition part")
        path = part["path"]
        if (
            not isinstance(path, str)
            or not path
            or path in {".", "/"}
            or "\\" in path
            or ":" in path
            or PurePosixPath(path).is_absolute()
            or PurePosixPath(path).as_posix() != path
            or ".." in PurePosixPath(path).parts
            or path in result
        ):
            raise ValueError("Unsafe or duplicate definition path")
        if path == ".platform":
            raise ValueError(
                "Metadata changes are not supported by definition-only promotion"
            )
        if part["payloadType"] != "InlineBase64":
            raise ValueError("Only inline payloads are supported")
        data = base64.b64decode(part["payload"], validate=True)
        total += len(data)
        if total > 4_000_000:
            raise ValueError("Definition exceeds four megabytes")
        safe_text(data.decode("utf-8"), limit=4_000_000)
        if PurePosixPath(path).suffix.lower() in {".json", ".ipynb"}:
            # Fabric reformats exported JSON. Compare its complete value, keeping
            # every field and embedded code string; whitespace/key order is immaterial.
            data = json.dumps(json.loads(data), sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")
        result[path] = hashlib.sha256(data).hexdigest()
    return result


def response_body(response):
    value = response.get("text")
    return value if isinstance(value, dict) else {}


class RemoteFailed(RayError):
    """The service explicitly reported a terminal failure."""
    def __init__(self, message="", *, code="FABRIC_ACTION_FAILED"):
        super().__init__(code)


class FabricTransport:
    def __init__(self, project, store, data_dir, actor, task_id):
        self.project, self.store, self.data_dir, self.actor, self.task_id = (
            project,
            store,
            data_dir,
            actor,
            task_id,
        )

    def request(self, method, endpoint, payload=None):
        # Only CloudActions constructs these paths. No user-supplied URL is accepted.
        if method not in {"get", "post", "patch"} or endpoint.startswith(("http", "/")):
            raise PolicyError("Unsupported transport request")
        mutating = method == "patch" or (
            method == "post" and not endpoint.split("?", 1)[0].endswith("/getDefinition")
        )
        def rejection(code):
            # Failure to observe an already-submitted operation does not establish
            # its outcome. Preserve its receipt for read-only reconciliation.
            return RemoteFailed(code=code) if mutating else RayError(code)
        parts = endpoint.split("/")
        workspace_id = parts[1] if parts[0] == "workspaces" and len(parts) > 1 else None
        environment = next(
            (
                w.environment
                for w in self.project.workspaces
                if w.id.lower() == workspace_id
            ),
            None,
        )
        action = self.store.audit_start(
            self.project.id,
            self.task_id,
            self.actor,
            "fabric_" + method,
            endpoint,
            environment,
        )
        try:
            with tempfile.TemporaryDirectory(
                prefix="fabric-", dir=self.data_dir
            ) as directory:
                args = [
                    sys.executable,
                    str(Path(__file__).with_name("fabric_worker.py")),
                    method,
                    endpoint,
                ]
                if payload is not None:
                    body = Path(directory) / "payload.json"
                    body.write_text(json.dumps(payload), encoding="utf-8")
                    args += [str(body)]
                proc = subprocess.run(
                    args,
                    shell=False,
                    capture_output=True,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    timeout=90,
                    env=fabric_env(self.data_dir, self.project.id),
                )
                if proc.returncode:
                    try:
                        if json.loads(proc.stdout).get("error_code") == "FABRIC_SIGNIN_REQUIRED":
                            raise rejection("FABRIC_SIGNIN_REQUIRED")
                    except (ValueError, AttributeError):
                        pass
                    raise RuntimeError(
                        "Fabric request failed; inspect the platform before retrying a write"
                    )
                result = json.loads(proc.stdout)
                if isinstance(result, dict) and result.get("status_code") in {400, 401, 403, 404, 409, 429}:
                    raise rejection({400: "FABRIC_REQUEST_REJECTED", 401: "FABRIC_AUTH_REQUIRED", 403: "FABRIC_FORBIDDEN", 404: "FABRIC_NOT_FOUND", 409: "FABRIC_CONFLICT", 429: "FABRIC_LIMIT"}[result["status_code"]])
                if not isinstance(result, dict) or result.get("status_code") not in {
                    200,
                    201,
                    202,
                    204,
                }:
                    raise RuntimeError("Fabric did not confirm request success")
                self.store.audit_finish(action, "SUCCEEDED")
                return result
        except RayError as exc:
            self.store.audit_finish(action, "FAILED", exc.code)
            raise
        except Exception as exc:
            self.store.audit_finish(
                action,
                "UNCERTAIN" if method in {"post", "patch"} else "FAILED",
                type(exc).__name__,
            )
            raise RuntimeError(
                "Fabric request outcome unavailable; no automatic mutation retry"
            ) from None


class TenantTransport:
    """Host-only mutations to fixed Fabric, Graph and personal GitHub endpoints."""
    def __init__(self, project, store, data_dir, actor, task_id):
        self.project, self.store, self.data_dir, self.actor, self.task_id = project, store, data_dir, actor, task_id

    def managed_request(self, provider, method, endpoint, payload=None):
        from .tenant import settings
        from .tenant_worker import endpoint_path
        tenant = settings(self.project)
        github = tenant.github.model_dump(exclude={"workspaces"}) if tenant.github else None
        endpoint_path(provider, method, endpoint, github)
        envelope = dict(provider=provider, method=method, endpoint=endpoint, payload=payload,
                        tenant_id=tenant.tenant_id, client_id=tenant.client_id, github=github)
        action = self.store.audit_start(self.project.id, self.task_id, self.actor, "tenant_" + method, provider + ":" + endpoint, None)
        try:
            proc = subprocess.run([sys.executable, "-m", "ray_de.tenant_worker"], input=json.dumps(envelope),
                shell=False, capture_output=True, text=True, encoding="utf-8", timeout=90,
                env=fabric_env(self.data_dir, self.project.id))
            if proc.returncode:
                raise RuntimeError("Managed request unavailable; inspect before retrying")
            response = json.loads(proc.stdout)
            status = response.get("status_code")
            # Reads return 404 to the scoped gateway when it explicitly expects absence.
            if status == 404 and method == "get":
                self.store.audit_finish(action, "SUCCEEDED")
                return response
            if status in {400, 401, 403, 404, 409, 412, 422, 429}:
                raise RemoteFailed(code="FABRIC_REQUEST_REJECTED")
            if status not in {200, 201, 202, 204}:
                raise RuntimeError("Managed provider did not confirm success")
            self.store.audit_finish(action, "SUCCEEDED")
            return response
        except Exception as exc:
            self.store.audit_finish(action, "FAILED" if isinstance(exc, RemoteFailed) or method == "get" else "UNCERTAIN", type(exc).__name__)
            raise


class CloudActions:
    def __init__(self, project, store, data_dir, *, transport=None):
        self.project, self.store, self.data_dir = project, store, Path(data_dir)
        self.control = Control(store)
        self.transport = transport

    def _read_item(self, actor, task_id, ws, item=None):
        transport = self._transport(actor, task_id)
        gateway = FabricGateway(self.project, self.store, self.data_dir, actor=actor,
                                executor=lambda endpoint: response_body(transport.request("get", endpoint)))
        return gateway.call("get_item" if item else "list_items", ws, item, task_id=task_id)

    def _item_payload(self, operation, definition_path):
        path = (self.project.repo / definition_path).resolve(strict=True)
        if not path.is_relative_to(self.project.repo) or path.stat().st_size > 6_000_000:
            raise PolicyError("Item payload must be inside the reviewed repository")
        text = path.read_text(encoding="utf-8")
        safe_text(text, limit=6_000_000)
        payload = json.loads(text)
        allowed = {"displayName", "description"}
        if operation == "create_item":
            allowed |= {"type", "definition", "creationPayload", "folderId"}
        if not isinstance(payload, dict) or not payload or set(payload) - allowed:
            raise ValueError("Unsupported item payload fields")
        if "displayName" in payload and (not isinstance(payload["displayName"], str) or not payload["displayName"].strip() or len(payload["displayName"]) > 256):
            raise ValueError("An item needs a nonempty displayName up to 256 characters")
        if "description" in payload and (not isinstance(payload["description"], str) or len(payload["description"]) > 256):
            raise ValueError("Item description must be at most 256 characters")
        if operation == "create_item":
            if not payload.get("displayName") or not re.fullmatch(r"[A-Za-z][A-Za-z0-9]{0,79}", payload.get("type", "")):
                raise ValueError("Creation requires displayName and a Fabric item type")
            if "definition" in payload and "creationPayload" in payload:
                raise ValueError("Use definition or creationPayload, not both")
            if "definition" in payload:
                definition_parts({"definition": payload["definition"]})
            if payload.get("folderId"):
                canonical_id(payload["folderId"])
        return path, payload

    def _check_item_name(self, actor, task_id, ws, payload):
        items = self._read_item(actor, task_id, ws)["value"]
        if any(i.get("displayName", "").casefold() == payload["displayName"].casefold() for i in items):
            raise PolicyError("An item with this name already exists. Inspect it; creation never overwrites or adopts it")

    def _prepare_item(self, task_id, operation, workspace_id, item_id, definition_path, actor):
        token = self.control.token(self.project.id)
        report = self._review(task_id)
        ws = canonical_id(workspace_id)
        env = self._workspace_policy(ws)
        path, payload = self._item_payload(operation, definition_path)
        if operation == "create_item":
            if not self.project.config.fabric.create_items or item_id:
                raise PolicyError("Creation requires enabled create_items and an empty item_id")
            self._check_item_name(actor, task_id, ws, payload)
            before, item, item_type = {}, "", payload["type"]
        else:
            env, target = self._target(operation, ws, item_id)
            item, item_type = canonical_id(item_id), target.item_type
            metadata = self._read_item(actor, task_id, ws, item)
            before = {k: metadata.get(k, "") for k in payload}
        with self.store.connect() as db:
            pending = db.execute("SELECT body FROM plans WHERE project_id=? AND state IN ('EXECUTING','UNCERTAIN','VALIDATION_FAILED')", (self.project.id,)).fetchall()
        for row in pending:
            other = json.loads(row[0])
            if other.get("workspace_id") == ws and (item and other.get("item_id") == item or not item and other.get("payload", {}).get("displayName", "").casefold() == payload["displayName"].casefold()):
                raise PolicyError("This target has an unresolved action; reconcile before proceeding")
        token.check()
        self._review(task_id, report["repo_digest"])
        body = {"operation": operation, "workspace_id": ws, "item_id": item, "environment": env,
                "item_type": item_type, "job_type": None, "binding": self.project.binding,
                "repo_digest": report["repo_digest"], "payload": payload, "rollback": before,
                "definition_path": path.relative_to(self.project.repo).as_posix(),
                "summary": f"{operation} {item_type} {payload.get('displayName', item)} in {env} workspace {ws}"}
        id = secrets.token_urlsafe(16)
        with self.store.connect() as db:
            db.execute("INSERT INTO plans VALUES (?,?,?,?,?,?,?,?,?,?,?)", (id, self.project.id, task_id, actor,
                digest(body), json.dumps(body), "PENDING_APPROVAL" if env == "TEST" else "READY", time.time()+3600, None, None, None))
        self._sync_task(task_id, new_plan_id=id)
        return self.get(id)

    def _verify_item(self, plan, actor, item):
        body = plan["body"]
        metadata = self._read_item(actor, plan["task_id"], body["workspace_id"], item)
        if metadata.get("type") != body["item_type"] or any(metadata.get(k, "") != body["payload"][k] for k in ("displayName", "description") if k in body["payload"]):
            raise RayError("FABRIC_VERIFICATION_FAILED")
        if "definition" in body["payload"]:
            current = self._definition(self._transport(actor, plan["task_id"]), body["workspace_id"], item,
                                       self.control.token(self.project.id), body["payload"]["definition"].get("format"))
            if definition_parts(current) != definition_parts({"definition": body["payload"]["definition"]}):
                raise RayError("FABRIC_VERIFICATION_FAILED")
        return metadata

    def _execute_item(self, plan, actor):
        body, id = plan["body"], plan["id"]
        token = self.control.token(self.project.id)
        if plan["actor"] != actor or plan["expires"] < time.time() or body["binding"] != self.project.binding:
            raise PolicyError("Action actor, expiry, or policy changed")
        self._review(plan["task_id"], body["repo_digest"])
        env = self._workspace_policy(body["workspace_id"])
        expected = "APPROVED" if env == "TEST" else "READY"
        if plan["state"] != expected:
            raise PolicyError("Action is not executable; it cannot be replayed")
        if body["operation"] == "create_item":
            if not self.project.config.fabric.create_items:
                raise PolicyError("Item creation is disabled")
            self._check_item_name(actor, plan["task_id"], body["workspace_id"], body["payload"])
        else:
            self._target("update_item", body["workspace_id"], body["item_id"])
            current = self._read_item(actor, plan["task_id"], body["workspace_id"], body["item_id"])
            if any(current.get(k, "") != v for k, v in body["rollback"].items()):
                raise PolicyError("Item metadata changed since review")
        token.check()
        self._review(plan["task_id"], body["repo_digest"])
        if body["binding"] != self.project.binding:
            raise PolicyError("Policy changed")
        with self.store.connect() as db:
            if db.execute("UPDATE plans SET state='EXECUTING' WHERE id=? AND state=? AND expires>?", (id, expected, time.time())).rowcount != 1:
                raise PolicyError("Action expired or cannot execute twice")
        try:
            self._sync_task(plan["task_id"])
            transport = self._transport(actor, plan["task_id"])
            self._no_managed_conflict(body["workspace_id"])
            endpoint = f"workspaces/{body['workspace_id']}/items"
            creating = body["operation"] == "create_item"
            response = transport.request("post" if creating else "patch", endpoint if creating else endpoint + "/" + body["item_id"], body["payload"])
            if response["status_code"] == 202:
                response = self._wait_operation(transport, response, token, id)
            item = canonical_id(response_body(response).get("id")) if creating else body["item_id"]
            self._remote(id, {"kind": "created_item" if creating else "updated_item", "id": item, "workspace_id": body["workspace_id"], "item_type": body["item_type"]})
            self._verify_item(plan, actor, item)
            token.check()
            passed, evidence = validate(self.project, token, commands=self.project.config.post_validation_commands)
            self._finish(id, "SUCCEEDED" if passed else "VALIDATION_FAILED", {"item_id": item, "workspace_id": body["workspace_id"], "evidence": ["Remote item metadata matches the reviewed payload", "Definition verified when supplied"] + evidence})
        except BaseException as exc:
            self._finish(id, "FAILED" if isinstance(exc, RemoteFailed) else "UNCERTAIN", {"error": type(exc).__name__, "instruction": "Reconcile the recorded receipt; do not resubmit creation"})
            raise
        finally:
            self._sync_task(plan["task_id"])
        return self.get(id)

    def _workspace_policy(self, ws):
        ws = canonical_id(ws)
        self._no_managed_conflict(ws)
        environment = next((w.environment for w in self.project.workspaces if w.id.lower() == ws), None)
        policy = self.project.config.policy
        if environment == "DEV" and policy.fabric_dev_write:
            return environment
        if environment == "TEST" and policy.fabric_test_write == "approval":
            return environment
        raise PolicyError("Writes are not enabled for this workspace/environment")

    def authorize_proposal(self, proposal):
        if proposal["operation"] == "tenant_action":
            from .tenant import grant_for
            grant_for(self.project, proposal["item_id"], proposal["workspace_id"])
            return
        if proposal["operation"] == "create_item":
            self._workspace_policy(proposal["workspace_id"])
            if not self.project.config.fabric.create_items:
                raise PolicyError("Item creation is not enabled for this workspace")
        else:
            self._target(proposal["operation"], proposal["workspace_id"], proposal["item_id"])

    def _target(self, operation, ws, item):
        ws, item = canonical_id(ws), canonical_id(item)
        self._no_managed_conflict(ws)
        if operation not in {"update_item", "update_definition", "run_job", "deploy_to_test", "publish_environment"}:
            raise PolicyError("Unsupported cloud operation")
        environment = next(
            (
                w.environment
                for w in self.project.workspaces
                if w.id.lower() == ws
            ),
            None,
        )
        policy = self.project.config.policy
        if environment == "PROD":
            raise PolicyError("PROD mutations are disabled in this POC")
        if operation == "deploy_to_test":
            if environment != "TEST" or policy.fabric_test_write != "approval":
                raise PolicyError("TEST deployment is not enabled")
        elif environment == "TEST" and self.project.config.fabric.workspace_write and policy.fabric_test_write == "approval":
            pass
        elif environment != "DEV" or not policy.fabric_dev_write:
            raise PolicyError("DEV writes are not enabled for this target")
        target = next(
            (
                t
                for t in self.project.config.fabric.write_targets
                if t.workspace_id.lower() == ws and t.item_id.lower() == item
            ),
            None,
        )
        if not target and self.project.config.fabric.workspace_write:
            metadata = self._read_item("host-policy", None, ws, item)
            item_type = metadata.get("type")
            target = WriteTarget(workspace_id=ws, item_id=item, item_type=item_type,
                                 job_type={"Notebook": "RunNotebook", "DataPipeline": "Pipeline", "SparkJobDefinition": "sparkjob"}.get(item_type))
        if not target:
            raise PolicyError("Item is outside the explicit write-target allow-list")
        if operation == "run_job" and not target.job_type:
            raise PolicyError("Job execution is disabled for this item")
        if operation == "publish_environment" and target.item_type != "Environment":
            raise PolicyError("Only an Environment can be published")
        if not self.project.config.fabric.allow_definition_export:
            raise PolicyError(
                "Definition export must be enabled for preflight and rollback capture"
            )
        if not self.project.config.post_validation_commands and not self.project.config.fabric.workspace_write:
            raise PolicyError(
                "Configure post-deployment validation before enabling writes"
            )
        return environment, target

    def _no_managed_conflict(self, ws):
        with self.store.connect() as db:
            rows = db.execute("SELECT body FROM plans WHERE state IN ('EXECUTING','UNCERTAIN','VALIDATION_FAILED')").fetchall()
        if any((body := json.loads(row["body"])).get("operation") == "tenant_action" and body.get("workspace_id") == ws for row in rows):
            raise PolicyError("A tenant/Git operation for this workspace is unresolved")

    def _review(self, task_id, expected_digest=None):
        task = self.store.task(self.project.id, task_id)
        report = json.loads(task["result"] or "{}")
        if (
            task["mode"] != "write"
            or not report.get("cloud_eligible")
            or not report.get("host_validation")
            or (report.get("review") or {}).get("verdict")
            not in {"PASS", "PASS_WITH_COMMENTS"}
        ):
            raise PolicyError("A validated and reviewed write task is required")
        if not report.get("repo_digest") or report["repo_digest"] != repo_digest(
            self.project
        ):
            raise PolicyError(
                "Source changed since validation/review; run a new reviewed task"
            )
        if expected_digest is not None and report["repo_digest"] != expected_digest:
            raise PolicyError(
                "This plan belongs to an earlier source revision; create a new plan"
            )
        return report

    def _transport(self, actor, task_id):
        return self.transport or FabricTransport(
            self.project, self.store, self.data_dir, actor, task_id
        )

    def _definition(self, transport, ws, item, token, format=None):
        endpoint = f"workspaces/{ws}/items/{item}/getDefinition"
        if format:
            endpoint += "?format=" + format
        response = transport.request("post", endpoint)
        if response["status_code"] == 202:
            response = self._wait_operation(transport, response, token, None)
        body = response_body(response)
        if "definition" not in body:
            raise RuntimeError("Definition response is missing")
        # Service .platform metadata does not participate in definition-only changes.
        body = {"definition": dict(body["definition"])}
        body["definition"]["parts"] = [
            p
            for p in body["definition"].get("parts", [])
            if p.get("path") != ".platform"
        ]
        definition_parts(body)
        return body

    def _headers(self, response):
        return {k.lower(): str(v) for k, v in response.get("headers", {}).items()}

    def _remote(self, id, remote):
        if id:
            with self.store.connect() as db:
                db.execute(
                    "UPDATE plans SET remote=? WHERE id=?", (json.dumps(remote), id)
                )

    def _wait_operation(self, transport, response, token, id):
        headers = self._headers(response)
        operation = headers.get("x-ms-operation-id")
        if not operation:
            uri = urlparse(headers.get("location", ""))
            if (
                uri.scheme != "https"
                or uri.hostname != "api.fabric.microsoft.com"
                or not uri.path.startswith("/v1/operations/")
            ):
                raise RuntimeError("Invalid operation location")
            operation = uri.path.split("/")[3]
        operation = canonical_id(operation)
        endpoint = "operations/" + operation
        self._remote(id, {"kind": "operation", "id": operation})
        deadline = time.monotonic() + self.project.config.timeout_seconds
        while time.monotonic() < deadline:
            delay = float(headers.get("retry-after", "2"))
            if delay < 0 or delay > self.project.config.timeout_seconds:
                raise RuntimeError("Retry delay exceeds polling budget")
            token.wait(delay)
            response = transport.request("get", endpoint)
            headers = self._headers(response)
            status = response_body(response).get("status")
            if status == "Succeeded":
                return transport.request("get", endpoint + "/result")
            if status in {"Failed", "Cancelled"}:
                raise RemoteFailed("Fabric operation failed")
            if status not in {"Running", "NotStarted"}:
                raise RuntimeError("Unknown Fabric operation status")
        raise TimeoutError("Operation is still pending; reconcile without resubmitting")

    def prepare(
        self, task_id, operation, workspace_id, item_id, definition_path, actor
    ):
        if operation == "tenant_action":
            return TenantActions(self.project, self.store, self.data_dir, transport=self.transport).prepare_tenant(
                task_id, workspace_id, item_id, definition_path, actor)
        if operation in {"create_item", "update_item"}:
            return self._prepare_item(task_id, operation, workspace_id, item_id, definition_path, actor)
        token = self.control.token(self.project.id)
        report = self._review(task_id)
        env, target = self._target(operation, workspace_id, item_id)
        ws, item = canonical_id(workspace_id), canonical_id(item_id)
        with self.store.connect() as db:
            unresolved = db.execute(
                "SELECT body FROM plans WHERE project_id=? AND state IN ('EXECUTING','UNCERTAIN','VALIDATION_FAILED')",
                (self.project.id,),
            ).fetchall()
        if any(
            (
                json.loads(row["body"]).get("workspace_id"),
                json.loads(row["body"]).get("item_id"),
            )
            == (ws, item)
            for row in unresolved
        ):
            raise PolicyError(
                "Target has an unresolved action; reconcile it before planning another mutation"
            )
        path = (self.project.repo / definition_path).resolve(strict=True)
        if (
            not path.is_relative_to(self.project.repo)
            or path.stat().st_size > 6_000_000
        ):
            raise PolicyError("Definition must be inside the reviewed repository")
        payload = json.loads(path.read_text(encoding="utf-8"))
        expected = definition_parts(payload)
        required = (
            "pipeline-content.json" if target.item_type == "DataPipeline" else None
        )
        if required and required not in expected:
            raise ValueError("DataPipeline requires pipeline-content.json")
        if (
            target.item_type == "Notebook"
            and not {"notebook-content.py", "notebook-content.ipynb"} & expected.keys()
        ):
            raise ValueError("Notebook content part is required")
        if target.item_type == "Notebook":
            expected_path = (
                "notebook-content.py"
                if payload["definition"].get("format") == "fabricGitSource"
                else "notebook-content.ipynb"
            )
            if expected_path not in expected:
                raise ValueError(
                    "Notebook content path does not match definition format"
                )
        transport = self._transport(actor, task_id)
        metadata = response_body(
            transport.request("get", f"workspaces/{ws}/items/{item}")
        )
        if metadata.get("type") != target.item_type:
            raise PolicyError("Remote item type does not match the allow-list")
        before = self._definition(
            transport, ws, item, token, payload["definition"].get("format")
        )
        if operation in {"run_job", "publish_environment"} and definition_parts(before) != expected:
            raise RayError("FABRIC_DEFINITION_CHANGED")
        if operation == "publish_environment":
            self._environment_idle(actor, task_id, ws, item)
        token.check()
        self._review(task_id, report["repo_digest"])
        id = secrets.token_urlsafe(16)
        body = {
            "operation": operation,
            "workspace_id": ws,
            "item_id": item,
            "environment": env,
            "item_type": target.item_type,
            "job_type": target.job_type,
            "binding": self.project.binding,
            "repo_digest": report["repo_digest"],
            "payload": payload,
            "rollback": before,
            "before_digest": digest(definition_parts(before)),
            "definition_path": path.relative_to(self.project.repo).as_posix(),
            "summary": f"{operation} {target.item_type} {item} in {env} workspace {ws}",
        }
        state = "PENDING_APPROVAL" if env == "TEST" else "READY"
        with self.store.connect() as db:
            db.execute(
                "INSERT INTO plans VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (
                    id,
                    self.project.id,
                    task_id,
                    actor,
                    digest(body),
                    json.dumps(body),
                    state,
                    time.time() + 3600,
                    None,
                    None,
                    None,
                ),
            )
        self._sync_task(task_id, new_plan_id=id)
        return self.get(id)

    def get(self, id):
        with self.store.connect() as db:
            row = db.execute(
                "SELECT * FROM plans WHERE id=? AND project_id=?", (id, self.project.id)
            ).fetchone()
        if not row:
            raise ValueError("Plan does not belong to this project")
        result = dict(row)
        result["body"] = json.loads(result["body"])
        if digest(result["body"]) != result["digest"]:
            raise PolicyError("Plan integrity check failed")
        return result

    def approve(self, id, actor, expected_digest):
        plan = self.get(id)
        self.control.token(self.project.id).check()
        if plan["actor"] != actor or plan["digest"] != expected_digest:
            raise PolicyError("Approval is bound to a different actor or action digest")
        if plan["expires"] < time.time():
            raise PolicyError("Approval expired")
        self._review(plan["task_id"], plan["body"]["repo_digest"])
        if plan["body"]["binding"] != self.project.binding:
            raise PolicyError("Project policy changed")
        with self.store.connect() as db:
            if (
                db.execute(
                    "UPDATE plans SET state='APPROVED',approved_by=? WHERE id=? AND state='PENDING_APPROVAL' AND expires>?",
                    (actor, id, time.time()),
                ).rowcount
                != 1
            ):
                raise PolicyError("Approval already consumed or invalid")
        return self.get(id)

    def receipts(self, task_id):
        self.store.task(self.project.id, task_id)
        with self.store.connect() as db:
            plans = [
                dict(r)
                for r in db.execute(
                    "SELECT id,state,digest,remote,result,body FROM plans WHERE project_id=? AND task_id=? ORDER BY rowid",
                    (self.project.id, task_id),
                )
            ]
        for plan in plans:
            body = json.loads(plan.pop("body"))
            plan.update(operation=body.get("operation"), item_type=body.get("item_type"))
            if body.get("operation") == "tenant_action":
                plan["tenant_operation"] = body.get("tenant_operation")
            for key in ("remote", "result"):
                plan[key] = json.loads(plan[key]) if plan[key] else None
        return plans

    def _sync_task(self, task_id, *, new_plan_id=None):
        task = self.store.task(self.project.id, task_id)
        report = json.loads(task["result"] or "{}")
        plans = self.receipts(task_id)
        current_ids = report.get("stage_plan_ids")
        if new_plan_id:
            if current_ids is None:
                current_ids = [p["id"] for p in plans]
            current_ids = list(dict.fromkeys([*current_ids, new_plan_id]))
            report["stage_plan_ids"] = current_ids
        current = plans if current_ids is None else [p for p in plans if p["id"] in current_ids]
        bad = {"UNCERTAIN", "FAILED", "VALIDATION_FAILED", "CANCELLED", "SUPERSEDED"}
        unresolved = any(p["state"] in {"UNCERTAIN", "VALIDATION_FAILED"} or
                         (p["state"] == "EXECUTING" and p not in current) for p in plans)
        if unresolved or any(p["state"] in bad for p in current):
            status = "BLOCKED"
        elif any(p["state"] != "SUCCEEDED" for p in current):
            status = (
                "APPROVAL_REQUIRED"
                if any(p["state"] == "PENDING_APPROVAL" for p in current)
                else "WAITING"
            )
        else:
            unprepared = len(report.get("cloud_actions", [])) > len(current)
            status = "WAITING" if report.get("continue_work") or unprepared else "COMPLETED"
        executing = next((p for p in current if p["state"] == "EXECUTING"), None)
        if status == "BLOCKED":
            phase = "needs_attention"
        elif executing:
            phase = "running_job" if executing.get("operation") == "run_job" else "applying_changes"
        elif status == "APPROVAL_REQUIRED":
            phase = "awaiting_approval"
        elif status == "COMPLETED":
            phase = "finished"
        else:
            phase = "preparing_next_step" if current and all(p["state"] == "SUCCEEDED" for p in current) else "preparing_fabric"
        report.setdefault("model_message", report.get("message", ""))
        summaries = []
        for plan in plans:
            remote = plan.get("remote") or {}
            target = remote.get("item_type") or remote.get("kind") or "action"
            summaries.append(f"{plan['state']}: {target} {remote.get('id', plan['id'])}")
        if summaries:
            report["message"] = report["model_message"] + "\nFabric action receipts:\n" + "\n".join(summaries)
        report.update(cloud_plans=plans, status=status.lower(), phase=phase)
        self.store.update(self.project.id, task_id, status, result=report)

    def execute(self, id, actor):
        plan = self.get(id)
        if plan["body"]["operation"] == "tenant_action":
            return TenantActions(self.project, self.store, self.data_dir, transport=self.transport).execute_tenant(plan, actor)
        if plan["body"]["operation"] in {"create_item", "update_item"}:
            return self._execute_item(plan, actor)
        body = plan["body"]
        token = self.control.token(self.project.id)
        if plan["actor"] != actor or plan["expires"] < time.time():
            raise PolicyError("Plan actor or expiry check failed")
        self._review(plan["task_id"], plan["body"]["repo_digest"])
        self._target(body["operation"], body["workspace_id"], body["item_id"])
        if body["binding"] != self.project.binding:
            raise PolicyError("Policy changed")
        expected_state = "APPROVED" if body["environment"] == "TEST" else "READY"
        if plan["state"] != expected_state:
            raise PolicyError("Plan is not executable; actions cannot be replayed")
        transport = self._transport(actor, plan["task_id"])
        ws, item = body["workspace_id"], body["item_id"]
        before = self._definition(
            transport, ws, item, token, body["payload"]["definition"].get("format")
        )
        if digest(definition_parts(before)) != body["before_digest"]:
            raise PolicyError("Target changed since planning; create a new plan")
        if body["operation"] == "publish_environment":
            self._environment_idle(actor, plan["task_id"], ws, item)
        token.check()
        self._review(plan["task_id"], plan["body"]["repo_digest"])
        with self.store.connect() as db:
            if (
                db.execute(
                    "UPDATE plans SET state='EXECUTING' WHERE id=? AND state=? AND expires>?",
                    (id, expected_state, time.time()),
                ).rowcount
                != 1
            ):
                raise PolicyError("Plan cannot be executed twice")
        try:
            self._sync_task(plan["task_id"])
            self._no_managed_conflict(ws)
            base = f"workspaces/{ws}/items/{item}"
            if body["operation"] == "publish_environment":
                response = transport.request("post", f"workspaces/{ws}/environments/{item}/staging/publish?beta=false")
                if response["status_code"] == 202:
                    self._wait_operation(transport, response, token, id)
                    self._verify_environment(plan, actor, token)
                else:
                    version = canonical_id(response_body(response).get("publishDetails", {}).get("targetVersion"))
                    self._remote(id, {"kind": "environment_publish", "id": version})
                    self._verify_environment(plan, actor, token, version=version)
            elif body["operation"] == "run_job":
                response = transport.request(
                    "post", base + "/jobs/" + body["job_type"] + "/instances", {}
                )
                headers = self._headers(response)
                uri = urlparse(headers.get("location", ""))
                prefix = "/v1/" + base + "/jobs/instances/"
                if (
                    response["status_code"] != 202
                    or uri.scheme != "https"
                    or uri.hostname != "api.fabric.microsoft.com"
                    or not uri.path.startswith(prefix)
                ):
                    raise RuntimeError("Unexpected job location")
                job = canonical_id(uri.path[len(prefix) :])
                self._remote(
                    id, {"kind": "job", "id": job, "workspace_id": ws, "item_id": item}
                )
                self._wait_job(
                    transport, base + "/jobs/instances/" + job, headers, token
                )
            else:
                response = transport.request(
                    "post", base + "/updateDefinition", body["payload"]
                )
                if response["status_code"] == 202:
                    self._wait_operation(transport, response, token, id)
                after = self._definition(
                    transport,
                    ws,
                    item,
                    token,
                    body["payload"]["definition"].get("format"),
                )
                if definition_parts(after) != definition_parts(body["payload"]):
                    raise RuntimeError(
                        "Remote definition did not match the reviewed payload"
                    )
            token.check()
            passed, evidence = validate(
                self.project,
                token,
                commands=self.project.config.post_validation_commands,
            )
            if not passed:
                self._finish(id, "VALIDATION_FAILED", {"evidence": evidence})
                return self.get(id)
            self._finish(
                id,
                "SUCCEEDED",
                {"evidence": evidence, "remote_validation": "completed"},
            )
        except BaseException as exc:
            self._finish(
                id,
                "FAILED" if isinstance(exc, RemoteFailed) else "UNCERTAIN",
                {
                    "error": type(exc).__name__,
                    "instruction": "Inspect/reconcile the target before creating another action. Do not replay this plan.",
                },
            )
            raise
        finally:
            self._sync_task(plan["task_id"])
        return self.get(id)

    def _environment(self, actor, task_id, ws, item):
        gateway = FabricGateway(self.project, self.store, self.data_dir, actor=actor,
            executor=lambda endpoint: response_body(self._transport(actor, task_id).request("get", endpoint)))
        details = gateway.call("get_environment", ws, item, task_id=task_id)
        if canonical_id(details.get("id")) != item or details.get("type") != "Environment":
            raise PolicyError("Environment read did not match the target")
        if details.get("workspaceId") and canonical_id(details["workspaceId"]) != ws:
            raise PolicyError("Environment read did not match the workspace")
        return details.get("properties", {}).get("publishDetails", {})

    def _environment_idle(self, actor, task_id, ws, item):
        state = self._environment(actor, task_id, ws, item).get("state")
        if state not in {None, "Success", "Failed", "Cancelled"}:
            raise PolicyError("Environment has an ongoing or unknown publish state")

    def _verify_environment(self, plan, actor, token, *, version=None):
        body = plan["body"]
        deadline = time.monotonic() + self.project.config.timeout_seconds
        while time.monotonic() < deadline:
            token.check()
            details = self._environment(actor, plan["task_id"], body["workspace_id"], body["item_id"])
            if version and details.get("targetVersion") != version:
                raise PolicyError("Environment publish version changed; inspect the receipt")
            state = details.get("state")
            if state in {"Failed", "Cancelled"}:
                raise RemoteFailed("Environment publish failed")
            if state == "Success":
                after = self._definition(self._transport(actor, plan["task_id"]), body["workspace_id"],
                    body["item_id"], token, body["payload"]["definition"].get("format"))
                if definition_parts(after) != definition_parts(body["payload"]):
                    raise PolicyError("Environment definition changed during publication")
                return
            if state not in {"Waiting", "Running", "Cancelling"}:
                raise RuntimeError("Unknown environment publish state")
            token.wait(2)
        raise TimeoutError("Environment still publishing; reconcile without resubmitting")

    def _wait_job(self, transport, endpoint, headers, token):
        deadline = time.monotonic() + self.project.config.timeout_seconds
        while time.monotonic() < deadline:
            delay = float(headers.get("retry-after", "2"))
            if delay < 0 or delay > self.project.config.timeout_seconds:
                raise RuntimeError("Invalid job polling delay")
            token.wait(delay)
            response = transport.request("get", endpoint)
            headers = self._headers(response)
            status = response_body(response).get("status")
            if status == "Completed":
                return
            if status in {"Failed", "Cancelled", "Deduped"}:
                raise RemoteFailed("Job did not complete successfully")
            if status not in {"NotStarted", "InProgress"}:
                raise RuntimeError("Unknown job status")
        raise TimeoutError("Job still running; reconcile without resubmitting")

    def _finish(self, id, state, result):
        with self.store.connect() as db:
            db.execute(
                "UPDATE plans SET state=?,result=? WHERE id=?",
                (state, json.dumps(result), id),
            )

    def reconcile(self, id, actor):
        try:
            return self._reconcile(id, actor)
        except RemoteFailed as exc:
            # A terminal service outcome resolves uncertainty, without resubmission.
            self._finish(id, "FAILED", {"reconciled": True, "error_code": exc.code,
                                        "instruction": "Inspect the failed job before preparing new work. This plan cannot be replayed."})
            self._sync_task(self.get(id)["task_id"])
            raise

    def _reconcile(self, id, actor):
        plan = self.get(id)
        if plan["body"]["operation"] == "tenant_action":
            return TenantActions(self.project, self.store, self.data_dir, transport=self.transport).execute_tenant(plan, actor, reconcile=True)
        if actor != plan["actor"]:
            raise PolicyError("Plan belongs to another actor")
        if plan["state"] not in {"UNCERTAIN", "VALIDATION_FAILED"}:
            raise PolicyError(
                "Only uncertain/failed-validation actions need reconciliation"
            )
        body = plan["body"]
        token = self.control.token(self.project.id)
        transport = self._transport(actor, plan["task_id"])
        self._review(plan["task_id"], plan["body"]["repo_digest"])
        if body["binding"] != self.project.binding:
            raise PolicyError("Policy changed")
        if body["operation"] in {"create_item", "update_item"}:
            self._workspace_policy(body["workspace_id"])
            remote = json.loads(plan["remote"] or "{}")
            item = body["item_id"]
            if body["operation"] == "create_item":
                if remote.get("kind") == "operation":
                    response = self._wait_operation(transport, {"headers": {"x-ms-operation-id": canonical_id(remote["id"]), "retry-after": "0"}}, token, id)
                    item = canonical_id(response_body(response).get("id"))
                    self._remote(id, {"kind": "created_item", "id": item, "workspace_id": body["workspace_id"], "item_type": body["item_type"]})
                elif remote.get("kind") == "created_item":
                    item = canonical_id(remote["id"])
                else:
                    raise RuntimeError("No creation receipt was recorded; inspect Fabric manually. Do not recreate or adopt by name")
            self._verify_item(plan, actor, item)
            token.check()
            passed, evidence = validate(self.project, token, commands=self.project.config.post_validation_commands)
            self._finish(id, "SUCCEEDED" if passed else "VALIDATION_FAILED", {"reconciled": True, "item_id": item, "workspace_id": body["workspace_id"], "evidence": ["Remote item verified against reviewed payload"] + evidence})
            self._sync_task(plan["task_id"])
            return self.get(id)
        self._target(body["operation"], body["workspace_id"], body["item_id"])
        if body["operation"] == "publish_environment":
            remote = json.loads(plan["remote"] or "{}")
            if remote.get("kind") == "operation":
                self._wait_operation(transport, {"headers": {"x-ms-operation-id": canonical_id(remote["id"]), "retry-after": "0"}}, token, id)
                self._verify_environment(plan, actor, token)
            elif remote.get("kind") == "environment_publish":
                self._verify_environment(plan, actor, token, version=canonical_id(remote["id"]))
            else:
                raise RuntimeError("No publish receipt recorded; inspect Fabric manually")
        elif body["operation"] == "run_job":
            remote = json.loads(plan["remote"] or "{}")
            if remote.get("kind") != "job":
                raise RuntimeError("No job ID was recorded; inspect Fabric manually")
            self._wait_job(
                transport,
                f"workspaces/{body['workspace_id']}/items/{body['item_id']}/jobs/instances/{canonical_id(remote['id'])}",
                {"retry-after": "0"},
                token,
            )
        else:
            after = self._definition(
                transport,
                body["workspace_id"],
                body["item_id"],
                token,
                body["payload"]["definition"].get("format"),
            )
            if definition_parts(after) != definition_parts(body["payload"]):
                raise RuntimeError(
                    "Target differs from proposed content; manual inspection required"
                )
        passed, evidence = validate(
            self.project, token, commands=self.project.config.post_validation_commands
        )
        self._finish(
            id,
            "SUCCEEDED" if passed else "VALIDATION_FAILED",
            {"reconciled": True, "evidence": evidence},
        )
        self._sync_task(plan["task_id"])
        return self.get(id)


class TenantActions(CloudActions):
    """Reviewed, receipt-backed tenant capabilities. All mutations stay in this module."""
    def gateway(self, actor, task_id):
        from .fabric import TenantGateway
        transport = self.transport or TenantTransport(self.project, self.store, self.data_dir, actor, task_id)
        gateway = TenantGateway(self.project, self.store, self.data_dir, actor=actor, executor=transport.managed_request)
        gateway.live = self.transport is None
        return gateway, transport

    def _tenant_request(self, gateway, grant, arguments):
        from .tenant import build_request
        if grant.operation in {"create_connection", "update_connection"} and "sqlDatabase" in grant.parameters:
            parameters = dict(grant.parameters)
            descriptor = parameters.pop("sqlDatabase")
            details = parameters.get("connectionDetails", {})
            if details != {"type": "SQL", "creationMethod": "SQL", "parameters": []}:
                raise PolicyError("SQL discovery requires the SQL connector with empty discovery parameters")
            parameters["connectionDetails"] = dict(details, parameters=gateway.sql_connection_target(descriptor, grant.environment))
            grant = grant.model_copy(update={"parameters": parameters})
        return build_request(self.project, self.store, grant, arguments)

    def _unresolved_tenant(self, except_id=None):
        from .tenant import settings
        tenant = settings(self.project)
        workspaces = {w.id.lower() for w in self.project.workspaces}
        with self.store.connect() as db:
            rows = db.execute("SELECT id, project_id, body FROM plans WHERE state IN ('EXECUTING','UNCERTAIN','VALIDATION_FAILED')").fetchall()
        for row in rows:
            body = json.loads(row["body"])
            if row["id"] != except_id and (row["project_id"] == self.project.id or body.get("tenant_id") == tenant.tenant_id or body.get("workspace_id") in workspaces):
                raise PolicyError("Reconcile the unresolved tenant/workspace action before another mutation")

    def _repository(self, gateway):
        from .tenant import github_settings
        github = github_settings(self.project)
        repo = gateway.raw("github", f"repos/{github.owner}/{github.repository}")
        if repo.get("private") is not True or repo.get("owner", {}).get("login", "").casefold() != github.owner.casefold() or repo.get("owner", {}).get("type") != "User":
            raise PolicyError("The enrolled repository must be private and owned by the configured personal account")
        return repo

    def _branch(self, gateway):
        from .tenant import github_settings
        github = github_settings(self.project)
        self._repository(gateway)
        ref = gateway.raw("github", f"repos/{github.owner}/{github.repository}/git/ref/heads/{github.branch}")
        sha = ref.get("object", {}).get("sha", "")
        if not re.fullmatch(r"[0-9a-f]{40}", sha):
            raise PolicyError("Enrolled Git branch has no valid head")
        return sha

    def _git_state(self, gateway, grant, ws):
        from .tenant import git_binding
        connection = gateway.raw("fabric", f"workspaces/{ws}/git/connection")
        expected = git_binding(self.project, grant.workspace)
        provider = connection.get("gitProviderDetails", {})
        if any(provider.get(k) != v for k, v in expected.items()):
            raise PolicyError("Fabric Git connection differs from the enrolled repository/branch/directory")
        return gateway.raw("fabric", f"workspaces/{ws}/git/status")

    def _preflight_tenant(self, gateway, grant, request, arguments):
        from .tenant import resources, source_json, github_settings, git_binding, settings
        from .tenant_repository import publication, remote_tree
        op, ws, p = grant.operation, request["workspace_id"], request["payload"]
        base = f"workspaces/{ws}"
        if op in {"create_workspace", "create_application", "create_service_principal", "create_application_credential", "create_group", "create_connection"} and grant.key in resources(self.project, self.store):
            raise PolicyError("Creation already has a verified resource receipt; use the existing resource")
        if op == "create_workspace":
            workspaces = gateway.pages("fabric", "workspaces")["value"]
            if any(w.get("displayName", "").casefold() == p["displayName"].casefold() for w in workspaces):
                raise PolicyError("Workspace name already exists; enroll its exact ID instead of adopting by name")
            capacity = next((c for c in gateway.pages("fabric", "capacities")["value"] if c.get("id") == p["capacityId"]), None)
            if not capacity or capacity.get("state") != "Active":
                raise PolicyError("Configured capacity is unavailable or inactive")
            return {"capacity": capacity, "name_available": True}
        if op == "assign_capacity":
            current = gateway.raw("fabric", base)
            capacity = next((c for c in gateway.pages("fabric", "capacities")["value"] if c.get("id") == p["capacityId"]), None)
            if not capacity or capacity.get("state") != "Active":
                raise PolicyError("Configured capacity is unavailable or inactive")
            if current.get("capacityId") and current.get("capacityId") != p["capacityId"]:
                if not current.get("capacityRegion") or current.get("capacityRegion") != capacity.get("region"):
                    raise PolicyError("Capacity reassignment must stay in the verified current region")
            return {"workspace": current, "capacity": capacity}
        if op == "provision_identity":
            current = gateway.raw("fabric", base)
            if current.get("workspaceIdentity"):
                raise PolicyError("Workspace already has an execution identity")
            return current
        if op in {"assign_workspace_role", "assign_connection_role"}:
            endpoint = request["endpoint"].split("/roleAssignments")[0] + "/roleAssignments"
            roles = gateway.pages("fabric", endpoint)["value"]
            parameters = grant.parameters
            from .tenant import resolve_resources
            parameters = resolve_resources(parameters, resources(self.project, self.store), settings(self.project).workspace_admins)
            principal = parameters["principal"]["id"]
            current = next((r for r in roles if r.get("principal", {}).get("id") == principal), None)
            if current and request["method"] != "patch":
                raise PolicyError("Principal already has a role; enroll its exact assignment for a role update")
            if request["method"] == "patch":
                ranks = {"Viewer": 0, "Contributor": 1, "Member": 2, "Admin": 3}
                if not current or current.get("id") != parameters["assignmentId"] or ranks[current["role"]] > ranks[p["role"]]:
                    raise PolicyError("Role update cannot target a different assignment or remove existing authority")
            return {"assignment": current}
        if op == "create_folder":
            folders = gateway.pages("fabric", base + "/folders")["value"]
            if p.get("parentFolderId") and not any(f["id"] == p["parentFolderId"] for f in folders):
                raise PolicyError("Parent folder is outside the workspace")
            if any(f.get("displayName") == p["displayName"] and f.get("parentFolderId") == p.get("parentFolderId") for f in folders):
                raise PolicyError("Folder already exists; use its existing ID")
            return {"folders": folders}
        if op == "move_item":
            item = gateway.raw("fabric", base + "/items/" + arguments["itemId"])
            folder = gateway.raw("fabric", base + "/folders/" + p["targetFolderId"])
            if item.get("id") != arguments["itemId"] or folder.get("id") != p["targetFolderId"]:
                raise PolicyError("Move targets did not match the workspace inventory")
            return {"item": item, "folder": folder}
        if op in {"create_connection", "update_connection"}:
            types = gateway.pages("fabric", "connections/supportedConnectionTypes")["value"]
            kind = p["connectionDetails"]["type"]
            supported = next((t for t in types if t.get("type") == kind), None)
            if not supported or p["credentialDetails"]["credentials"]["credentialType"] not in supported.get("supportedCredentialTypes", []):
                raise PolicyError("Connector does not support the enrolled unattended credentials")
            if not any(m.get("name") == p["connectionDetails"]["creationMethod"] for m in supported.get("creationMethods", [])):
                raise PolicyError("Connector creation method is unavailable")
            if op == "create_connection":
                if any(c.get("displayName") == p["displayName"] for c in gateway.pages("fabric", "connections")["value"]):
                    raise PolicyError("Connection name already exists; enroll its exact ID")
                return {"connector": supported}
            current = gateway.raw("fabric", request["endpoint"])
            if current.get("connectivityType") != "ShareableCloud":
                raise PolicyError("Connection update requires an existing shareable cloud connection")
            if current.get("connectionDetails", {}).get("type") != kind:
                raise PolicyError("Connection updates cannot change the connector type")
            self._verify_connection_path(current, p)
            return {"connection": current, "connector": supported}
        if op == "create_service_principal":
            app = next(r for r in resources(self.project, self.store).values() if r.get("kind") == "application" and r.get("appId") == p["appId"])
            current = gateway.raw("graph", "applications/" + app["id"])
            if current.get("appId") != p["appId"] or current.get("signInAudience") != "AzureADMyOrg":
                raise PolicyError("Owned application changed before service principal creation")
            return current
        if op == "create_application_credential":
            current = gateway.raw("graph", request["endpoint"].removesuffix("/addPassword") + "?$select=id,appId,displayName,signInAudience,passwordCredentials")
            if current.get("signInAudience") != "AzureADMyOrg":
                raise PolicyError("Application is no longer single-tenant")
            return current
        if op == "add_group_member":
            endpoint = request["endpoint"].removesuffix("/members/$ref")
            group = gateway.raw("graph", endpoint + "?$select=id,displayName,groupTypes,mailEnabled,securityEnabled,isAssignableToRole")
            if group.get("isAssignableToRole") is not False or group.get("groupTypes") or group.get("securityEnabled") is not True:
                raise PolicyError("Membership changes require a non-role-assignable static security group")
            # Groups granting the executor workspace/capacity authority cannot be
            # used as a path to expanding its own enrolled scope.
            from .tenant import resolve_resources
            member = resolve_resources(grant.parameters, resources(self.project, self.store), settings(self.project).workspace_admins)["memberId"]
            members = gateway.pages("graph", endpoint + "/members")["value"]
            if any(m.get("id") == member for m in members):
                raise PolicyError("Group membership already exists")
            return {"group": group, "members": sorted(m["id"] for m in members)}
        if op in {"create_application", "create_group"}:
            return {"new_resource": grant.key}
        if op == "github_create_repository":
            github = github_settings(self.project)
            user = gateway.raw("github", "user")
            if user.get("login", "").casefold() != github.owner.casefold():
                raise PolicyError("GitHub credential must belong to the enrolled personal owner")
            if gateway.raw("github", f"repos/{github.owner}/{github.repository}", missing=True) is not None:
                raise PolicyError("Repository already exists; use the enrolled existing repository")
            return {"owner": github.owner, "repository_absent": True}
        if op == "github_publish":
            head = self._branch(gateway)
            if head != arguments["expected_head"]:
                raise PolicyError("GitHub branch changed before publication")
            return publication(self.project, gateway, grant, arguments)
        if op == "git_connect":
            connection = gateway.raw("fabric", base + "/git/connection")
            if connection.get("gitConnectionState") != "NotConnected":
                raise PolicyError("Workspace is already connected; do not silently replace its Git binding")
            head = self._branch(gateway)
            _, tree = remote_tree(gateway, github_settings(self.project), head)
            prefix = git_binding(self.project, grant.workspace)["directoryName"] + "/"
            if not any(path.startswith(prefix) for path in tree):
                raise PolicyError("Publish the enrolled directory before connecting Fabric")
            credential = gateway.raw("fabric", "connections/" + p["myGitCredentials"]["connectionId"])
            if credential.get("connectionDetails", {}).get("type") != "GitHubSourceControl":
                raise PolicyError("Git binding requires a GitHub source-control connection")
            github = github_settings(self.project)
            if credential.get("connectionDetails", {}).get("path", "").rstrip("/") != f"https://github.com/{github.owner}/{github.repository}":
                raise PolicyError("Git credential connection must be scoped to the enrolled repository URL")
            return {"connection": connection, "remote_head": head, "credential": credential}
        if op == "git_initialize":
            connection = gateway.raw("fabric", base + "/git/connection")
            if any(connection.get("gitProviderDetails", {}).get(k) != v for k, v in git_binding(self.project, grant.workspace).items()):
                raise PolicyError("Git initialization target changed")
            if connection.get("gitConnectionState") not in {"Connected", "ConnectedAndNotInitialized"}:
                raise PolicyError("Git initialization requires a newly connected workspace")
            items = gateway.pages("fabric", base + "/items")["value"]
            if p["initializationStrategy"] == "PreferRemote" and items:
                raise PolicyError("Remote initialization may only target an empty workspace")
            if p["initializationStrategy"] == "PreferWorkspace":
                _, tree = remote_tree(gateway, github_settings(self.project), self._branch(gateway))
                prefix = git_binding(self.project, grant.workspace)["directoryName"] + "/"
                if any(path.startswith(prefix) and path.endswith("/.platform") for path in tree):
                    raise PolicyError("Baseline initialization requires a Git directory without existing items")
            return {"connection": connection, "items": items, "head": self._branch(gateway)}
        if op in {"git_commit", "git_update"}:
            status = self._git_state(gateway, grant, ws)
            if status.get("workspaceHead") != p["workspaceHead"]:
                raise PolicyError("Fabric workspace head changed before Git operation")
            head = self._branch(gateway)
            if status.get("remoteCommitHash") != head:
                raise PolicyError("GitHub and Fabric remote heads do not match")
            changes = status.get("changes")
            if not isinstance(changes, list) or any(c.get("conflictType") not in {None, "None"} or c.get("remoteChange") == "Deleted" or c.get("workspaceChange") == "Deleted" for c in changes):
                raise PolicyError("Git sync contains a conflict or item deletion")
            if op == "git_commit":
                if grant.key in resources(self.project, self.store) or any(c.get("remoteChange") not in {None, "None"} for c in changes):
                    raise PolicyError("Workspace-to-Git commit is limited to the initial clean baseline")
                snapshot = source_json(self.project, arguments["snapshot"])
                current = gateway.export(grant.workspace)
                if digest(current) != digest(snapshot):
                    raise PolicyError("Workspace export changed since its source review")
                changed_ids = {c.get("itemMetadata", {}).get("itemIdentifier", {}).get("objectId") for c in changes}
                if any(i["id"] in changed_ids for i in current["unsupported"]):
                    raise PolicyError("An item to be committed has no reviewed export")
                return {"status": status, "snapshot_digest": digest(snapshot), "head": head}
            if p["remoteCommitHash"] != head or any(c.get("workspaceChange") not in {None, "None"} for c in changes):
                raise PolicyError("Workspace drift or an unexpected commit blocks Git deployment")
            published = [r for r in resources(self.project, self.store).values() if r.get("kind") == "git_commit" and r.get("commit") == head]
            if not published or not any(git_binding(self.project, grant.workspace)["directoryName"] in r.get("directories", []) for r in published):
                raise PolicyError("Deploy only a successful reviewed publication for this workspace directory")
            changed = self._git_review_coverage(gateway, grant, p["workspaceHead"], head)
            return {"status": status, "head": head, "reviewed_changed_paths": changed,
                    "items": gateway.pages("fabric", base + "/items")["value"]}
        raise PolicyError("Unimplemented tenant preflight")

    def _git_review_coverage(self, gateway, grant, workspace_head, remote_head):
        """Do not carry unrelated, unreviewed remote edits into a workspace sync."""
        from .tenant import github_settings, git_binding
        from .tenant_repository import remote_tree, git_blob_sha
        github = github_settings(self.project)
        prefix = git_binding(self.project, grant.workspace)["directoryName"] + "/"
        _, after = remote_tree(gateway, github, remote_head)
        before = remote_tree(gateway, github, workspace_head)[1] if workspace_head else {}
        changed = sorted(path for path in before.keys() | after.keys() if path.startswith(prefix)
                         and (before.get(path, {}).get("sha"), before.get(path, {}).get("mode"))
                         != (after.get(path, {}).get("sha"), after.get(path, {}).get("mode")))
        allowed, removed = {}, set()
        with self.store.connect() as db:
            rows = db.execute("SELECT body,digest,result FROM plans WHERE project_id=? AND state='SUCCEEDED'", (self.project.id,)).fetchall()
        for row in rows:
            body, result = json.loads(row["body"]), json.loads(row["result"] or "{}")
            if (body.get("binding") != self.project.binding or digest(body) != row["digest"]
                    or body.get("tenant_operation") != "github_publish" or body.get("environment") != grant.environment
                    or result.get("kind") != "git_commit"):
                continue
            publication = body["before"]
            for path, content in publication["files"].items():
                allowed.setdefault(path, set()).add(git_blob_sha(base64.b64decode(content, validate=True)))
            removed.update(publication["remove"])
        for path in changed:
            if path in after:
                if after[path].get("mode") != "100644" or after[path]["sha"] not in allowed.get(path, set()):
                    raise PolicyError("Git sync includes unreviewed source from an external commit; publish every changed file through review")
            elif path not in removed:
                raise PolicyError("Git sync includes a path removal without a reviewed move receipt")
        return changed

    def prepare_tenant(self, task_id, workspace, key, definition_path, actor):
        from .tenant import grant_for, build_request, source_json, review_paths
        from .schemas import CloudProposal
        grant = grant_for(self.project, key, workspace)
        self._unresolved_tenant()
        token = self.control.token(self.project.id)
        report = self._review(task_id)
        proposal = CloudProposal(operation="tenant_action", workspace_id=workspace, item_id=key, definition_path=definition_path)
        required = {definition_path, *review_paths(self.project, proposal)}
        if not required <= set(report.get("review_paths", [])):
            raise PolicyError("All tenant action inputs must be included in the independent source review")
        arguments = source_json(self.project, definition_path)
        gateway, _ = self.gateway(actor, task_id)
        request = self._tenant_request(gateway, grant, arguments)
        before = self._preflight_tenant(gateway, grant, request, arguments)
        token.check()
        self._review(task_id, report["repo_digest"])
        body = dict(operation="tenant_action", tenant_operation=grant.operation, tenant_grant=key,
            tenant_id=self.project.config.fabric.tenant.tenant_id,
            workspace_reference=workspace, workspace_id=request["workspace_id"], item_id=key, item_type="TenantCapability",
            environment=grant.environment, binding=self.project.binding, repo_digest=report["repo_digest"],
            arguments=arguments, request=request, before=before, definition_path=definition_path,
            summary=f"{grant.operation} using enrolled capability {key} in {grant.environment}")
        id = secrets.token_urlsafe(16)
        with self.store.connect() as db:
            db.execute("INSERT INTO plans VALUES (?,?,?,?,?,?,?,?,?,?,?)", (id, self.project.id, task_id, actor,
                digest(body), json.dumps(body), "PENDING_APPROVAL" if grant.environment == "TEST" else "READY", time.time()+3600, None, None, None))
        self._sync_task(task_id, new_plan_id=id)
        return self.get(id)

    def _tenant_lro(self, gateway, response, token, id):
        class ReadTransport:
            def request(self, method, endpoint, payload=None):
                if method != "get":
                    raise PolicyError("Operation polling is read-only")
                return gateway.raw("fabric", endpoint, response_envelope=True)
        return self._wait_operation(ReadTransport(), response, token, id)

    def _publish_github(self, plan, gateway, transport, token):
        from .tenant import github_settings
        from .tenant_repository import git_blob_sha
        github = github_settings(self.project)
        base = f"repos/{github.owner}/{github.repository}"
        publication = plan["body"]["before"]
        entries = []
        for path, encoded in publication["files"].items():
            token.check()
            response = transport.managed_request("github", "post", base + "/git/blobs", {"content": encoded, "encoding": "base64"})
            sha = response_body(response).get("sha")
            if sha != git_blob_sha(base64.b64decode(encoded, validate=True)):
                raise RuntimeError("GitHub blob did not match reviewed source")
            entries.append({"path": path, "mode": "100644", "type": "blob", "sha": sha})
        entries.extend({"path": path, "mode": "100644", "type": "blob", "sha": None} for path in publication["remove"])
        token.check()
        tree = response_body(transport.managed_request("github", "post", base + "/git/trees", {"base_tree": publication["base_tree"], "tree": entries})).get("sha")
        if not re.fullmatch(r"[0-9a-f]{40}", tree or ""):
            raise RuntimeError("GitHub returned no valid tree receipt")
        token.check()
        commit = response_body(transport.managed_request("github", "post", base + "/git/commits", {
            "message": publication["message"], "tree": tree, "parents": [publication["expected_head"]]})).get("sha")
        if not re.fullmatch(r"[0-9a-f]{40}", commit or ""):
            raise RuntimeError("GitHub returned no valid commit receipt")
        receipt = {"kind": "git_publish", "commit": commit, "tree": tree, "expected_head": publication["expected_head"]}
        self._remote(plan["id"], receipt)
        if self._branch(gateway) != publication["expected_head"]:
            raise PolicyError("Git branch moved while creating the reviewed commit; no force push")
        token.check()
        self._review(plan["task_id"], plan["body"]["repo_digest"])
        transport.managed_request("github", "patch", base + "/git/refs/heads/" + github.branch, {"sha": commit, "force": False})
        return receipt

    def _verify_tenant(self, gateway, grant, request, plan, remote):
        from .tenant import git_binding, github_settings, settings
        from .tenant_config import identifier
        from .tenant_repository import remote_tree, git_blob_sha
        op, ws, p = grant.operation, request["workspace_id"], request["payload"]
        response = remote.get("response", {})
        result = {"verified": True, "operation": op}
        creates = {"create_workspace": ("fabric", "workspaces", "workspace"), "create_folder": ("fabric", f"workspaces/{ws}/folders", "folder"),
                   "create_application": ("graph", "applications", "application"), "create_service_principal": ("graph", "servicePrincipals", "service_principal"),
                   "create_group": ("graph", "groups", "group"), "create_connection": ("fabric", "connections", "connection")}
        if op in creates:
            provider, collection, kind = creates[op]
            id = identifier(response.get("id"))
            endpoint = collection + "/" + id
            if op == "create_group":
                endpoint += "?$select=id,displayName,groupTypes,mailEnabled,securityEnabled,isAssignableToRole"
            actual = gateway.raw(provider, endpoint)
            if actual.get("id") != id or p.get("displayName") and actual.get("displayName") != p["displayName"]:
                raise PolicyError("Created resource did not match its receipt and reviewed name")
            if op == "create_workspace" and actual.get("capacityId") != p["capacityId"]:
                raise PolicyError("Created workspace capacity did not match enrollment")
            if op == "create_application" and actual.get("signInAudience") != "AzureADMyOrg":
                raise PolicyError("Application audience differs from the enrolled single tenant")
            if op == "create_service_principal" and actual.get("appId") != p["appId"]:
                raise PolicyError("Service principal is attached to a different application")
            if op == "create_group" and (actual.get("isAssignableToRole") is not False or actual.get("securityEnabled") is not True or actual.get("mailEnabled") is not False):
                raise PolicyError("Created group is not the requested static security group")
            if op == "create_folder" and actual.get("parentFolderId") != p.get("parentFolderId"):
                raise PolicyError("Folder allocation differs from the reviewed parent")
            if op == "create_connection":
                self._verify_connection(actual, p)
            result.update(kind=kind, id=id)
            if actual.get("appId"):
                result["appId"] = identifier(actual["appId"])
        elif op == "assign_capacity":
            actual = gateway.raw("fabric", f"workspaces/{ws}")
            if actual.get("capacityId") != p["capacityId"]:
                raise PolicyError("Workspace capacity was not assigned")
            result.update(workspace_id=ws, capacity_id=p["capacityId"])
        elif op == "provision_identity":
            actual = gateway.raw("fabric", f"workspaces/{ws}").get("workspaceIdentity", {})
            for field in ("applicationId", "servicePrincipalId"):
                if identifier(actual.get(field)) != identifier(response.get(field, actual.get(field))):
                    raise PolicyError("Workspace identity differs from its creation receipt")
            result.update(kind="workspace_identity", **{k: actual[k] for k in ("applicationId", "servicePrincipalId")})
        elif op in {"assign_workspace_role", "assign_connection_role"}:
            from .tenant import resolve_resources, resources
            parameters = resolve_resources(grant.parameters, resources(self.project, self.store), settings(self.project).workspace_admins)
            endpoint = request["endpoint"].split("/roleAssignments")[0] + "/roleAssignments"
            roles = gateway.pages("fabric", endpoint)["value"]
            if not any(r.get("principal", {}).get("id") == parameters["principal"]["id"] and r.get("role") == p["role"] for r in roles):
                raise PolicyError("Requested principal role was not observed")
        elif op == "move_item":
            before = plan["body"]["before"]["item"]
            actual = gateway.raw("fabric", f"workspaces/{ws}/items/{before['id']}")
            if any(actual.get(k) != before.get(k) for k in ("id", "type", "displayName")) or actual.get("folderId") != p["targetFolderId"]:
                raise PolicyError("Item move changed identity/name or did not reach its target folder")
        elif op == "update_connection":
            self._verify_connection(gateway.raw("fabric", request["endpoint"]), p)
        elif op == "add_group_member":
            members = gateway.pages("graph", request["endpoint"].removesuffix("/$ref"))["value"]
            if not any(m.get("id") == p["@odata.id"].rsplit("/", 1)[1] for m in members):
                raise PolicyError("Group membership has not been observed")
        elif op == "create_application_credential":
            if response.get("credential_stored") is not True or response.get("credential_ref") != p["storeSecretRef"]:
                raise PolicyError("Application credential was not secured in protected local storage")
            actual = gateway.raw("graph", request["endpoint"].removesuffix("/addPassword") + "?$select=id,appId,displayName,signInAudience,passwordCredentials")
            key_id = identifier(response.get("keyId"))
            if not any(c.get("keyId") == key_id for c in actual.get("passwordCredentials", [])):
                raise PolicyError("Application credential key was not observed")
            result.update(kind="application_credential", key_id=key_id, credential_ref=p["storeSecretRef"])
        elif op == "github_create_repository":
            actual = self._repository(gateway)
            if response.get("id") != actual.get("id"):
                raise PolicyError("Repository creation receipt does not match the target")
            result.update(kind="github_repository", repository_id=actual["id"], default_branch=actual["default_branch"])
        elif op == "github_publish":
            github = github_settings(self.project)
            commit = remote.get("commit")
            if self._branch(gateway) != commit:
                raise PolicyError("Git branch does not match the recorded publication commit")
            tree_sha, tree = remote_tree(gateway, github, commit)
            publication = plan["body"]["before"]
            if tree_sha != remote.get("tree") or any(tree.get(path, {}).get("sha") != git_blob_sha(base64.b64decode(encoded, validate=True))
                    for path, encoded in publication["files"].items()) or any(path in tree for path in publication["remove"]):
                raise PolicyError("Published Git tree differs from reviewed source")
            result.update(kind="git_commit", commit=commit, directories=[w.directory for w in github.workspaces
                if any(path.startswith(w.directory + "/") for path in publication["files"])])
        elif op in {"git_connect", "git_initialize"}:
            connection = gateway.raw("fabric", f"workspaces/{ws}/git/connection")
            expected = git_binding(self.project, grant.workspace)
            if any(connection.get("gitProviderDetails", {}).get(k) != v for k, v in expected.items()):
                raise PolicyError("Git connection was not established to the enrolled target")
            if op == "git_connect":
                credentials = gateway.raw("fabric", f"workspaces/{ws}/git/myGitCredentials")
                if credentials.get("connectionId") != p["myGitCredentials"]["connectionId"] or credentials.get("source") != "ConfiguredConnection":
                    raise PolicyError("Calling identity's Git credentials were not configured")
            else:
                if response.get("requiredAction") not in {"CommitToGit", "UpdateFromGit", "None"}:
                    raise PolicyError("Git initialization has no valid next-action receipt")
                result.update(required_action=response["requiredAction"], workspace_head=response.get("workspaceHead"), remote_commit=response.get("remoteCommitHash"), full_sync=False)
        elif op in {"git_commit", "git_update"}:
            status = self._git_state(gateway, grant, ws)
            head = self._branch(gateway)
            if status.get("workspaceHead") != head or status.get("remoteCommitHash") != head or status.get("changes") != []:
                raise PolicyError("Git operation completed but workspace and branch are not clean and synchronized")
            if op == "git_update" and head != p["remoteCommitHash"]:
                raise PolicyError("Deployed commit differs from the reviewed publication")
            if op == "git_update":
                self._verify_git_items(gateway, grant, ws, head, plan["body"]["before"]["items"])
            result.update(kind="git_baseline" if op == "git_commit" else "git_deployment", commit=head, workspace_id=ws)
        else:
            raise PolicyError("No verifier exists for this capability")
        return result

    def _verify_git_items(self, gateway, grant, ws, commit, before):
        from .tenant import github_settings, git_binding
        from .tenant_repository import remote_tree, remote_blob, item_inventory
        github = github_settings(self.project)
        prefix = git_binding(self.project, grant.workspace)["directoryName"] + "/"
        _, tree = remote_tree(gateway, github, commit)
        metadata = {path: remote_blob(gateway, github, entry) for path, entry in tree.items()
                    if path.startswith(prefix) and path.endswith("/.platform")}
        expected = item_inventory(metadata)
        actual = gateway.pages("fabric", f"workspaces/{ws}/items")["value"]
        folders = {f["id"]: f for f in gateway.pages("fabric", f"workspaces/{ws}/folders")["value"]}
        def folder_path(item):
            names, seen, current = [], set(), item.get("folderId")
            while current:
                if current in seen or current not in folders:
                    raise PolicyError("Deployed folder hierarchy is missing or cyclic")
                seen.add(current)
                folder = folders[current]
                names.insert(0, folder["displayName"])
                current = folder.get("parentFolderId")
            return "/".join(names)
        by_id = {item["id"]: item for item in actual}
        if any(item["id"] not in by_id or by_id[item["id"]].get("type") != item.get("type") for item in before):
            raise PolicyError("Git deployment removed or recreated an existing physical item")
        def canonical(path, raw):
            text = raw.decode("utf-8-sig").replace("\r\n", "\n").rstrip("\n")
            if Path(path).suffix.lower() in {".json", ".ipynb", ".pbir", ".pbism"}:
                return json.dumps(json.loads(text), sort_keys=True, separators=(",", ":"))
            return text
        for logical, info in expected.items():
            matches = [item for item in actual if item.get("type") == info["type"] and item.get("displayName") == info["name"]]
            if len(matches) != 1:
                raise PolicyError("A native Git item is missing or ambiguous in Fabric")
            desired_folder = info["directory"].removeprefix(prefix).rsplit("/", 1)[0] if "/" in info["directory"].removeprefix(prefix) else ""
            if folder_path(matches[0]) != desired_folder:
                raise PolicyError("Deployed item folder differs from the reviewed Git allocation")
            endpoint = f"workspaces/{ws}/items/{canonical_id(matches[0]['id'])}/getDefinition"
            if info["type"] == "Notebook":
                endpoint += "?format=fabricGitSource"
            definition = gateway.raw("fabric", endpoint, method="post").get("definition", {})
            parts = definition.get("parts", [])
            observed = {}
            for part in parts:
                if part.get("payloadType") != "InlineBase64":
                    raise PolicyError("Deployed item export has an unsupported part format")
                content = base64.b64decode(part["payload"], validate=True)
                if part["path"] == ".platform":
                    if json.loads(content).get("config", {}).get("logicalId") != logical:
                        raise PolicyError("Deployed logical item identity does not match Git")
                    continue
                if part["path"] in observed:
                    raise PolicyError("Deployed definition contains duplicate parts")
                observed[part["path"]] = canonical(part["path"], content)
            desired = {path[len(info["directory"])+1:]: canonical(path, remote_blob(gateway, github, entry))
                for path, entry in tree.items() if path.startswith(info["directory"] + "/") and not path.endswith("/.platform")}
            if desired != observed:
                raise PolicyError("Deployed item definition differs from the reviewed Git commit")

    @staticmethod
    def _verify_connection(actual, requested):
        if actual.get("displayName") != requested["displayName"] or actual.get("connectionDetails", {}).get("type") != requested["connectionDetails"]["type"]:
            raise PolicyError("Connection metadata differs from its reviewed template")
        if actual.get("credentialDetails", {}).get("credentialType") != requested["credentialDetails"]["credentials"]["credentialType"]:
            raise PolicyError("Connection credential type was not configured")
        if actual.get("credentialDetails", {}).get("skipTestConnection") is True:
            raise PolicyError("Connection authentication was skipped")
        TenantActions._verify_connection_path(actual, requested)

    @staticmethod
    def _verify_connection_path(actual, requested):
        details = requested["connectionDetails"]
        parameters = {p["name"]: p["value"] for p in details["parameters"]}
        if details["type"] == "SQL":
            expected = parameters.get("server", "") + ";" + parameters.get("database", "")
        elif details["type"] == "GitHubSourceControl":
            expected = parameters.get("url", "")
        else:
            return  # Connector-specific binding is retained in the reviewed template.
        if not expected or actual.get("connectionDetails", {}).get("path", "").rstrip("/") != expected.rstrip("/"):
            raise PolicyError("Connection endpoint differs from the enrolled source")

    def execute_tenant(self, plan, actor, *, reconcile=False):
        from .tenant import grant_for, build_request
        body, id = plan["body"], plan["id"]
        grant = grant_for(self.project, body["tenant_grant"], body["workspace_reference"])
        token = self.control.token(self.project.id)
        if plan["actor"] != actor or body["binding"] != self.project.binding:
            raise PolicyError("Tenant action actor or project binding changed")
        self._unresolved_tenant(id)
        self._review(plan["task_id"], body["repo_digest"])
        gateway, transport = self.gateway(actor, plan["task_id"])
        request = self._tenant_request(gateway, grant, body["arguments"])
        if request != body["request"]:
            raise PolicyError("Resolved tenant resources changed since planning")
        remote = json.loads(plan["remote"] or "{}")
        if reconcile:
            if plan["state"] not in {"UNCERTAIN", "VALIDATION_FAILED"} or not remote:
                raise PolicyError("Reconciliation requires an unresolved action with a recorded receipt")
        else:
            expected = "APPROVED" if grant.environment == "TEST" else "READY"
            if plan["state"] != expected or plan["expires"] < time.time():
                raise PolicyError("Tenant action is expired, not approved, or already consumed")
            before = self._preflight_tenant(gateway, grant, request, body["arguments"])
            if digest(before) != digest(body["before"]):
                raise PolicyError("Tenant target changed after planning; obtain a fresh review and plan")
            token.check()
            self._review(plan["task_id"], body["repo_digest"])
            with self.store.connect() as db:
                # Global serialization also protects workspace Git operations from
                # competing capabilities across tasks in this project.
                rows = db.execute("SELECT id,project_id,body FROM plans WHERE id<>? AND state IN ('EXECUTING','UNCERTAIN','VALIDATION_FAILED')", (id,)).fetchall()
                targets = {w.id.lower() for w in self.project.workspaces}
                for row in rows:
                    other = json.loads(row["body"])
                    if row["project_id"] == self.project.id or other.get("tenant_id") == body["tenant_id"] or other.get("workspace_id") in targets:
                        raise PolicyError("Another tenant or workspace action is unresolved")
                if db.execute("UPDATE plans SET state='EXECUTING' WHERE id=? AND state=? AND expires>?", (id, expected, time.time())).rowcount != 1:
                    raise PolicyError("Tenant action cannot execute twice")
        try:
            if not reconcile:
                self._sync_task(plan["task_id"])
                if grant.operation == "github_publish":
                    remote = self._publish_github(plan, gateway, transport, token)
                else:
                    token.check()
                    payload = request["payload"]
                    if grant.operation == "update_connection":
                        # UpdateConnection cannot change connectionDetails. Keep it
                        # as a reviewed pre/postcondition, not an unsupported API field.
                        payload = {k: v for k, v in payload.items() if k != "connectionDetails"}
                        payload["connectivityType"] = "ShareableCloud"
                    response = transport.managed_request(request["provider"], request["method"], request["endpoint"], payload)
                    if response.get("status_code") == 202:
                        if request["provider"] != "fabric":
                            raise RuntimeError("Unexpected asynchronous response from managed provider")
                        response = self._tenant_lro(gateway, response, token, id)
                    remote = {"kind": "managed_response", "response": response_body(response)}
                    self._remote(id, remote)
            elif remote.get("kind") == "operation":
                response = self._tenant_lro(gateway, {"headers": {"x-ms-operation-id": remote["id"], "retry-after": "0"}}, token, id)
                remote = {"kind": "managed_response", "response": response_body(response)}
                self._remote(id, remote)
            token.check()
            result = self._verify_tenant(gateway, grant, request, plan, remote)
            result.update(reconciled=reconcile, source="live_tenant_api" if self.transport is None else "provided_executor")
            passed, evidence = validate(self.project, token, commands=self.project.config.post_validation_commands)
            result["evidence"] = ["Remote state verified against the enrolled capability and reviewed inputs", *evidence]
            with self.store.connect() as db:
                db.execute("UPDATE plans SET state=?,result=? WHERE id=?", ("SUCCEEDED" if passed else "VALIDATION_FAILED", json.dumps(result), id))
                if passed and result.get("kind"):
                    db.execute("INSERT OR REPLACE INTO tenant_resources VALUES (?,?,?,?,?)", (self.project.id, grant.key, result["kind"], json.dumps(result), id))
            return self.get(id)
        except RemoteFailed as exc:
            self._finish(id, "FAILED", {"error_code": exc.code})
            raise
        except Exception:
            self._finish(id, "UNCERTAIN", {"instruction": "Read-only reconciliation is required; never replay this action"})
            raise
        finally:
            self._sync_task(plan["task_id"])
