import copy
import json

import pytest

from ray_de.artifacts import materialize, validate_sources, compile_definitions
from ray_de.cloud import CloudActions
from ray_de.config import ProjectConfig
from ray_de.demo import fixture, DEV, TEST, ITEM, OP, ACTOR
from ray_de.fabric import PolicyError
from ray_de.orchestrator import repo_digest
from ray_de.schemas import LocalArtifact


class ItemFabric:
    def __init__(self):
        self.items = {}
        self.writes = []
        self.lro = False
        self.lost = False
        self.fail_read = False

    def request(self, method, endpoint, payload=None):
        status, headers, body = 200, {}, {}
        if method == "get" and endpoint.endswith("/items"):
            body = {"value": list(self.items.values())}
        elif method == "post" and endpoint.endswith("/items"):
            self.writes.append((method, endpoint, payload))
            self.items[ITEM] = {"id": ITEM, **copy.deepcopy(payload)}
            if self.lost:
                raise TimeoutError("Lost creation response")
            status, body = 201, self.items[ITEM]
            if self.lro:
                status, body, headers = 202, {}, {"x-ms-operation-id": OP, "retry-after": "0"}
        elif endpoint.startswith("operations/"):
            body = self.items[ITEM] if endpoint.endswith("/result") else {"status": "Succeeded"}
        elif method == "patch":
            self.writes.append((method, endpoint, payload))
            self.items[ITEM].update(payload)
            if self.lost:
                raise TimeoutError("Lost update response")
            body = self.items[ITEM]
        elif method == "get":
            if self.fail_read:
                raise TimeoutError("Metadata unavailable")
            body = self.items[ITEM]
        else:
            raise AssertionError((method, endpoint))
        return {"status_code": status, "headers": headers, "text": copy.deepcopy(body)}


@pytest.fixture
def authoring(tmp_path):
    p, s, t, _, _ = fixture(tmp_path)
    cfg = p.config.model_dump()
    cfg["fabric"].update(workspace_write=True, create_items=True, write_targets=[])
    cfg["post_validation_commands"] = []
    p.config = ProjectConfig.model_validate(cfg)
    f = ItemFabric()
    return p, s, t, f, CloudActions(p, s, tmp_path / "state", transport=f)


def plan(a, op="create_item", ws=DEV, payload=None):
    p, s, t, f, c = a
    (p.repo / "item.json").write_text(json.dumps(payload or {"displayName": "ray_demo", "type": "Lakehouse"}))
    report = json.loads(s.task(p.id, t["id"])["result"])
    report["repo_digest"] = repo_digest(p)
    s.update(p.id, t["id"], "COMPLETED", result=report)
    return c.prepare(t["id"], op, ws, "" if op == "create_item" else ITEM, "item.json", ACTOR)


@pytest.mark.parametrize("lro", [False, True])
def test_create_receipt_and_replay(authoring, lro):
    p, s, t, f, c = authoring
    f.lro = lro
    action = plan(authoring)
    result = c.execute(action["id"], ACTOR)
    assert result["state"] == "SUCCEEDED"
    receipt = json.loads(s.task(p.id, t["id"])["result"])["cloud_plans"][0]
    assert receipt["remote"]["id"] == ITEM
    assert receipt["result"]["item_id"] == ITEM
    assert "SUCCEEDED: Lakehouse " + ITEM in json.loads(s.task(p.id, t["id"])["result"])["message"]
    with pytest.raises(PolicyError):
        c.execute(action["id"], ACTOR)
    assert len(f.writes) == 1


@pytest.mark.parametrize("late", [False, True])
def test_create_rejects_duplicate_at_plan_and_execution(authoring, late):
    p, s, t, f, c = authoring
    if late:
        action = plan(authoring)
    f.items[ITEM] = {"id": ITEM, "displayName": "RAY_DEMO", "type": "Lakehouse"}
    with pytest.raises(PolicyError, match="already exists"):
        c.execute(action["id"], ACTOR) if late else plan(authoring)
    assert not f.writes


def test_create_lost_response_never_adopts_by_name(authoring):
    p, s, t, f, c = authoring
    action = plan(authoring)
    f.lost = True
    with pytest.raises(TimeoutError):
        c.execute(action["id"], ACTOR)
    with pytest.raises(RuntimeError, match="No creation receipt"):
        c.reconcile(action["id"], ACTOR)
    assert c.get(action["id"])["state"] == "UNCERTAIN"
    assert len(f.writes) == 1


def test_create_receipt_reconciles_failed_readback(authoring):
    p, s, t, f, c = authoring
    action = plan(authoring)
    f.fail_read = True
    with pytest.raises(TimeoutError):
        c.execute(action["id"], ACTOR)
    f.fail_read = False
    assert c.reconcile(action["id"], ACTOR)["state"] == "SUCCEEDED"
    assert len(f.writes) == 1


def test_test_create_requires_exact_approval(authoring):
    p, s, t, f, c = authoring
    action = plan(authoring, ws=TEST)
    with pytest.raises(PolicyError):
        c.execute(action["id"], ACTOR)
    with pytest.raises(PolicyError):
        c.approve(action["id"], ACTOR, "wrong")
    c.approve(action["id"], ACTOR, action["digest"])
    assert c.execute(action["id"], ACTOR)["state"] == "SUCCEEDED"


def test_workspace_metadata_update_checks_freshness(authoring):
    p, s, t, f, c = authoring
    f.items[ITEM] = {"id": ITEM, "displayName": "before", "type": "Lakehouse"}
    action = plan(authoring, "update_item", payload={"displayName": "after"})
    f.items[ITEM]["displayName"] = "someone_else_changed_it"
    with pytest.raises(PolicyError, match="changed"):
        c.execute(action["id"], ACTOR)
    assert not f.writes
    f.items[ITEM]["displayName"] = "before"
    assert c.execute(action["id"], ACTOR)["state"] == "SUCCEEDED"


@pytest.mark.parametrize("reason", ["policy", "source", "actor"])
def test_create_rechecks_authorization(authoring, reason):
    p, s, t, f, c = authoring
    action = plan(authoring)
    if reason == "policy":
        cfg = p.config.model_dump()
        cfg["fabric"]["create_items"] = False
        p.config = ProjectConfig.model_validate(cfg)
    elif reason == "source":
        (p.repo / "changed.py").write_text("x=1")
    with pytest.raises(PolicyError):
        c.execute(action["id"], "intruder" if reason == "actor" else ACTOR)
    assert not f.writes


@pytest.mark.parametrize("path", ["../outside.py", "C:/outside.py", ".codex/config.json", "AGENTS.md", "nested/SKILL.md"])
def test_artifacts_reject_escape_and_instruction_files(authoring, path):
    p = authoring[0]
    with pytest.raises(ValueError):
        materialize(p, [LocalArtifact(path="first.py", content="x=1"), LocalArtifact(path=path, content="x=1")])
    assert not (p.repo / "first.py").exists()


def test_artifacts_validate_all_before_writing(authoring):
    p = authoring[0]
    with pytest.raises(SyntaxError):
        materialize(p, [LocalArtifact(path="first.py", content="x=1"), LocalArtifact(path="bad.py", content="invalid (")])
    assert not (p.repo / "first.py").exists()
    materialize(p, [LocalArtifact(path="item.json", content='{"displayName":"sample","type":"Lakehouse"}')])
    assert validate_sources(p.repo) >= 1


def test_source_parts_compile_before_review_without_model_base64(authoring):
    import base64
    p = authoring[0]
    source = '{"properties":{"activities":[]}}'
    (p.repo / "pipeline.json").write_text(source)
    payload = {"definition": {"parts": [{"path": "pipeline-content.json", "source": "pipeline.json", "payloadType": "SourceFile"}]}}
    (p.repo / "create.json").write_text(json.dumps(payload))
    compile_definitions(p)
    part = json.loads((p.repo / "create.json").read_text())["definition"]["parts"][0]
    assert part["payloadType"] == "InlineBase64" and "source" not in part
    assert base64.b64decode(part["payload"]).decode() == source
    payload["definition"]["parts"][0]["source"] = "../outside.py"
    (p.repo / "create.json").write_text(json.dumps(payload))
    with pytest.raises(ValueError, match="inside"):
        compile_definitions(p)


def test_definition_json_reformatting_preserves_complete_semantics():
    import base64
    from ray_de.cloud import definition_parts
    def parts(text):
        return definition_parts({"definition": {"parts": [{"path": "pipeline-content.json", "payloadType": "InlineBase64", "payload": base64.b64encode(text.encode()).decode()}]}})
    assert parts('{"a":1,"b":{"c":2}}') == parts('{ "b": { "c": 2 }, "a": 1 }')
    assert parts('{"a":1}') != parts('{"a":2}')
    assert parts('{"code":"x = 1"}') != parts('{"code":"x=1"}')


def test_fabric_http_rejection_has_safe_action_error(authoring, monkeypatch, tmp_path):
    from types import SimpleNamespace
    from ray_de.cloud import FabricTransport, RemoteFailed
    from ray_de.errors import describe_error
    p, s, t, f, c = authoring
    monkeypatch.setattr("ray_de.cloud.subprocess.run", lambda *a, **k: SimpleNamespace(returncode=0, stdout=json.dumps({"status_code": 403, "text": {}})))
    transport = FabricTransport(p, s, tmp_path, ACTOR, t["id"])
    with pytest.raises(RemoteFailed) as error:
        transport.request("post", f"workspaces/{DEV}/items", {"displayName": "demo", "type": "Lakehouse"})
    assert describe_error(error.value, "cloud_action")["code"] == "FABRIC_FORBIDDEN"
    with s.connect() as db:
        latest = db.execute("SELECT status,error_code FROM actions ORDER BY rowid DESC LIMIT 1").fetchone()
    assert tuple(latest) == ("FAILED", "FABRIC_FORBIDDEN")
