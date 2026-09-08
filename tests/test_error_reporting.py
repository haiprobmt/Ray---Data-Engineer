import asyncio
import json
from types import SimpleNamespace

import pytest

from ray_de.errors import RayError, describe_error, error_text, model_error_code, record_failure
from ray_de.schemas import TurnResult, ReviewResult
from ray_de.conversation import ConversationResult
from ray_de.sdk_worker import strict_output_schema
from ray_de.telegram import Gateway
from ray_de.service import TaskService
from test_telegram import setup, update


@pytest.mark.parametrize("model", [TurnResult, ReviewResult, ConversationResult])
def test_model_schemas_require_every_property_recursively(model):
    original = model.model_json_schema()
    converted = strict_output_schema(original)
    def inspect(node):
        if isinstance(node, dict):
            if "properties" in node:
                assert set(node["required"]) == set(node["properties"])
                assert node["additionalProperties"] is False
            assert "default" not in node
            for value in node.values():
                inspect(value)
        elif isinstance(node, list):
            for value in node:
                inspect(value)
    inspect(converted)
    assert original == model.model_json_schema()
    if model is TurnResult:
        assert "cloud_actions" in converted["required"]


def test_failed_task_explained_immediately_and_after_restart(tmp_path):
    async def run():
        g, api, svc, p, store = setup(tmp_path)
        def fail(*args, **kwargs):
            raise RayError("MODEL_SCHEMA_REJECTED")
        svc.run = fail
        await g.handle(update(1, "/work Inspect the project"))
        await asyncio.gather(*g.jobs.values())
        immediate = api.calls[-1][1]["text"]
        assert "could not read the reply" in immediate and "needs a fix" in immediate
        assert "response schema" not in immediate and "/details technical" in immediate
        task_id = g.control.session("telegram:123:123")["task_id"]
        task = store.task(p.id, task_id)
        assert task["status"] == "ERROR"
        assert json.loads(task["result"])["error"]["code"] == "MODEL_SCHEMA_REJECTED"
        reopened = Gateway(g.config, tmp_path / "gateway.yaml", store, store.path.parent, api, service=svc)
        await reopened.handle(update(2, "/status"))
        assert "could not read the reply" in api.calls[-1][1]["text"]
        await reopened.handle(update(3, "/details technical"))
        assert "response schema" in api.calls[-1][1]["text"]
        assert "Next step:" in api.calls[-1][1]["text"]
    asyncio.run(run())


def test_unknown_exception_does_not_leak_credentials(tmp_path):
    async def run():
        g, api, svc, p, store = setup(tmp_path)
        secret = "credential-in-an-unrecognized-format"
        def fail(*args, **kwargs):
            raise RuntimeError("https://private.example/?token=" + secret)
        svc.run = fail
        await g.handle(update(1, "/work Inspect the project"))
        await asyncio.gather(*g.jobs.values())
        task_id = g.control.session("telegram:123:123")["task_id"]
        assert secret not in store.task(p.id, task_id)["result"]
        assert secret not in json.dumps(api.calls)
        assert "couldn't finish this step" in api.calls[-1][1]["text"]
    asyncio.run(run())


def test_fabric_failure_before_model_is_saved_and_replaces_previous_failure(tmp_path, monkeypatch):
    g, api, svc, p, store = setup(tmp_path)
    task = store.create(p.id, "Inspect", "read")
    record_failure(store, p.id, task["id"], RayError("MODEL_SCHEMA_REJECTED"))
    def fail(gateway):
        raise RayError("FABRIC_FORBIDDEN")
    monkeypatch.setattr("ray_de.service.FabricGateway.snapshot", fail)
    runner = SimpleNamespace(run=lambda *args, **kwargs: pytest.fail("model must not run"))
    with pytest.raises(RayError):
        TaskService(store, runner, store.path.parent).run(p, task["id"], "Inspect", "actor")
    saved = store.task(p.id, task["id"])
    assert saved["status"] == "ERROR"
    assert json.loads(saved["result"])["error"]["code"] == "FABRIC_FORBIDDEN"


@pytest.mark.parametrize("status,code", [(401,"FABRIC_AUTH_REQUIRED"),(403,"FABRIC_FORBIDDEN"),(404,"FABRIC_NOT_FOUND"),(429,"FABRIC_LIMIT"),(500,"FABRIC_UNAVAILABLE")])
def test_fabric_http_failure_keeps_status_category_only(tmp_path, monkeypatch, status, code):
    from ray_de.fabric import FabricGateway
    g, api, svc, p, store = setup(tmp_path)
    monkeypatch.setattr("ray_de.fabric.subprocess.run", lambda *a, **kw: SimpleNamespace(returncode=0, stdout=json.dumps({"status_code":status,"text":{"token":"private-detail"}})))
    with pytest.raises(RayError) as raised:
        FabricGateway(p, store, store.path.parent)._execute("workspaces/test")
    assert raised.value.code == code
    assert "private-detail" not in str(raised.value)


def test_sdk_failure_classification_never_copies_service_message():
    code = model_error_code({"message": "Invalid schema: Missing cloud_actions. token=PRIVATE"})
    report = describe_error(RayError(code), "model")
    assert code == "MODEL_SCHEMA_REJECTED" and "PRIVATE" not in json.dumps(report)
    assert "PRIVATE" not in error_text({"code":code, "message":"PRIVATE"})
    assert "unexpected error" in error_text({"code":{"bad":"PRIVATE"}})


def test_timeout_is_paused_with_an_explanation(tmp_path):
    async def run():
        g, api, svc, p, store = setup(tmp_path)
        def fail(*args, **kwargs):
            raise TimeoutError("raw secret")
        svc.run = fail
        await g.handle(update(1, "/work Inspect"))
        await asyncio.gather(*g.jobs.values())
        task = store.task(p.id, g.control.session("telegram:123:123")["task_id"])
        assert task["status"] == "PAUSED"
        assert "took too long" in api.calls[-1][1]["text"]
    asyncio.run(run())


def test_model_worker_error_survives_subprocess_boundary(tmp_path, monkeypatch):
    from pathlib import Path
    from ray_de.codex_client import CodexRunner
    g, api, svc, p, store = setup(tmp_path)
    class Process:
        returncode = 1
        def __init__(self, args, **kwargs):
            Path(args[4]).write_text(json.dumps({"_ray_error":{"code":"MODEL_SCHEMA_REJECTED"}}))
        def poll(self):
            return 1
    monkeypatch.setattr("ray_de.codex_client.resolve_binary", lambda kind: "codex.exe")
    monkeypatch.setattr("ray_de.codex_client.subprocess.Popen", Process)
    with pytest.raises(RayError) as raised:
        CodexRunner(store.path.parent).run(p, "test", TurnResult.model_json_schema())
    assert raised.value.code == "MODEL_SCHEMA_REJECTED"


def test_invalid_reply_reports_the_failed_check_without_private_values():
    from pydantic import ValidationError
    from test_recovery import output
    payload = output()
    payload.update(status="working", continue_work=True, message="private reply text")
    with pytest.raises(ValidationError) as raised:
        TurnResult.model_validate(payload)
    report = describe_error(raised.value, "model")
    assert report["validation_issues"] == [{"field": "response", "code": "CONTINUATION_STATUS"}]
    assert "CONTINUATION_STATUS" in error_text(report)
    assert "private reply text" not in json.dumps(report)


def test_invalid_reply_fields_and_stored_details_are_allowlisted():
    from pydantic import ValidationError
    from test_recovery import output
    payload = output()
    payload.update(status="secret-status", **{"private-field-name": "private-field-value"})
    with pytest.raises(ValidationError) as raised:
        TurnResult.model_validate(payload)
    report = describe_error(raised.value, "model")
    assert {"field": "status", "code": "INVALID_CHOICE"} in report["validation_issues"]
    assert "private" not in json.dumps(report) and "secret-status" not in json.dumps(report)
    report["validation_issues"].append({"field": "private-field-name", "code": "private-field-value"})
    assert "private" not in error_text(report)


def test_invalid_chat_reply_does_not_resume_an_unrelated_task():
    from ray_de.telegram_format import friendly_error
    text = friendly_error({"code": "MODEL_OUTPUT_INVALID", "stage": "conversation"})
    assert "/resume" not in text and "Send your message again" in text
