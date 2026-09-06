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
        parts = endpoint.split("/")
        workspace_id = parts[1] if parts[0] == "workspaces" and len(parts) > 1 else None
        environment = next(
            (
                w.environment
                for w in self.project.config.fabric.workspaces
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
                            raise RemoteFailed(code="FABRIC_SIGNIN_REQUIRED")
                    except (ValueError, AttributeError):
                        pass
                    raise RuntimeError(
                        "Fabric request failed; inspect the platform before retrying a write"
                    )
                result = json.loads(proc.stdout)
                if isinstance(result, dict) and result.get("status_code") in {400, 401, 403, 404, 409, 429}:
                    raise RemoteFailed(code={400: "FABRIC_REQUEST_REJECTED", 401: "FABRIC_AUTH_REQUIRED", 403: "FABRIC_FORBIDDEN", 404: "FABRIC_NOT_FOUND", 409: "FABRIC_CONFLICT", 429: "FABRIC_LIMIT"}[result["status_code"]])
                if not isinstance(result, dict) or result.get("status_code") not in {
                    200,
                    201,
                    202,
                    204,
                }:
                    raise RuntimeError("Fabric did not confirm request success")
                self.store.audit_finish(action, "SUCCEEDED")
                return result
        except RemoteFailed as exc:
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
        self._sync_task(task_id)
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
        with self.store.connect() as db:
            if db.execute("UPDATE plans SET state='EXECUTING' WHERE id=? AND state=?", (id, expected)).rowcount != 1:
                raise PolicyError("Action cannot execute twice")
        try:
            transport = self._transport(actor, plan["task_id"])
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
        environment = next((w.environment for w in self.project.config.fabric.workspaces if w.id.lower() == ws), None)
        policy = self.project.config.policy
        if environment == "DEV" and policy.fabric_dev_write:
            return environment
        if environment == "TEST" and policy.fabric_test_write == "approval":
            return environment
        raise PolicyError("Writes are not enabled for this workspace/environment")

    def authorize_proposal(self, proposal):
        if proposal["operation"] == "create_item":
            self._workspace_policy(proposal["workspace_id"])
            if not self.project.config.fabric.create_items:
                raise PolicyError("Item creation is not enabled for this workspace")
        else:
            self._target(proposal["operation"], proposal["workspace_id"], proposal["item_id"])

    def _target(self, operation, ws, item):
        ws, item = canonical_id(ws), canonical_id(item)
        if operation not in {"update_item", "update_definition", "run_job", "deploy_to_test"}:
            raise PolicyError("Unsupported cloud operation")
        environment = next(
            (
                w.environment
                for w in self.project.config.fabric.workspaces
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
        if not self.project.config.fabric.allow_definition_export:
            raise PolicyError(
                "Definition export must be enabled for preflight and rollback capture"
            )
        if not self.project.config.post_validation_commands and not self.project.config.fabric.workspace_write:
            raise PolicyError(
                "Configure post-deployment validation before enabling writes"
            )
        return environment, target

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
        if operation == "run_job" and definition_parts(before) != expected:
            raise PolicyError("Remote job definition differs from the reviewed source")
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
        self._sync_task(task_id)
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

    def _sync_task(self, task_id):
        task = self.store.task(self.project.id, task_id)
        report = json.loads(task["result"] or "{}")
        with self.store.connect() as db:
            plans = [
                dict(r)
                for r in db.execute(
                    "SELECT id,state,digest,remote,result FROM plans WHERE project_id=? AND task_id=? ORDER BY rowid",
                    (self.project.id, task_id),
                )
            ]
        bad = {"UNCERTAIN", "FAILED", "VALIDATION_FAILED", "CANCELLED"}
        for plan in plans:
            for key in ("remote", "result"):
                plan[key] = json.loads(plan[key]) if plan[key] else None
        if any(p["state"] in bad for p in plans):
            status = "BLOCKED"
        elif any(p["state"] != "SUCCEEDED" for p in plans):
            status = (
                "APPROVAL_REQUIRED"
                if any(p["state"] == "PENDING_APPROVAL" for p in plans)
                else "WAITING"
            )
        else:
            status = "COMPLETED"
        report.setdefault("model_message", report.get("message", ""))
        summaries = []
        for plan in plans:
            remote = plan.get("remote") or {}
            target = remote.get("item_type") or remote.get("kind") or "action"
            summaries.append(f"{plan['state']}: {target} {remote.get('id', plan['id'])}")
        if summaries:
            report["message"] = report["model_message"] + "\nFabric action receipts:\n" + "\n".join(summaries)
        report.update(cloud_plans=plans, status=status.lower())
        self.store.update(self.project.id, task_id, status, result=report)

    def execute(self, id, actor):
        plan = self.get(id)
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
            base = f"workspaces/{ws}/items/{item}"
            if body["operation"] == "run_job":
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
        plan = self.get(id)
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
        if body["operation"] == "run_job":
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
