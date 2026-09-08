import copy
import base64
import json

import pytest

from ray_de.cloud import RemoteFailed
from ray_de.config import ProjectConfig
from ray_de.demo import ACTOR, DEV, ITEM, OP, TEST, fixture
from ray_de.fabric import FabricGateway, PolicyError, authorize
from ray_de.orchestrator import repo_digest


@pytest.fixture
def setup(tmp_path):
    project, store, task, transport, cloud = fixture(tmp_path)
    config = project.config.model_dump()
    for target in config["fabric"]["write_targets"]:
        target.update(item_type="Environment", job_type=None)
    config["fabric"]["workspace_write"] = True
    project.config = ProjectConfig.model_validate(config)
    source = {"definition": {"parts": [{"path": "Setting/Sparkcompute.yml", "payloadType": "InlineBase64",
        "payload": base64.b64encode(b"runtime_version: 2.0").decode()}]}}
    (project.repo / "definition.json").write_text(json.dumps(source))
    report = json.loads(store.task(project.id, task["id"])["result"])
    report["repo_digest"] = repo_digest(project)
    store.update(project.id, task["id"], "COMPLETED", result=report)
    class EnvironmentTransport:
        def __init__(self):
            self.calls = []
            self.state = "Success"
            self.version = OP
            self.lro = False
            self.lost = False
            self.fail_read = False
            self.published = False
        def request(self, method, endpoint, payload=None):
            self.calls.append((method, endpoint))
            if endpoint.endswith("/getDefinition"):
                data = source
            elif endpoint.endswith("/staging/publish?beta=false"):
                self.published = True
                if self.lost:
                    raise TimeoutError("response lost")
                if self.lro:
                    return {"status_code": 202, "headers": {"x-ms-operation-id": OP, "retry-after": "0"}, "text": {}}
                data = {"publishDetails": {"state": "Running", "targetVersion": OP}}
            elif endpoint.startswith("operations/"):
                data = {} if endpoint.endswith("/result") else {"status": "Succeeded"}
            elif "/environments/" in endpoint:
                if self.fail_read and self.published:
                    raise TimeoutError("read unavailable")
                data = {"id": ITEM, "type": "Environment", "properties": {"publishDetails": {"state": self.state, "targetVersion": self.version}}}
            else:
                data = {"id": ITEM, "type": "Environment"}
            return {"status_code": 200, "headers": {}, "text": copy.deepcopy(data)}
    fake = EnvironmentTransport()
    cloud.transport = fake
    return project, store, task, fake, cloud


def prepare(setup, workspace=DEV):
    return setup[4].prepare(setup[2]["id"], "publish_environment", workspace, ITEM, "definition.json", ACTOR)


@pytest.mark.parametrize("lro", [False, True])
def test_publish_receipt_and_no_replay(setup, lro):
    p, s, t, f, c = setup
    f.lro = lro
    plan = prepare(setup)
    assert c.execute(plan["id"], ACTOR)["state"] == "SUCCEEDED"
    with pytest.raises(PolicyError):
        c.execute(plan["id"], ACTOR)
    assert sum(endpoint.endswith("?beta=false") for method, endpoint in f.calls) == 1


def test_test_publish_needs_exact_approval(setup):
    p, s, t, f, c = setup
    plan = prepare(setup, TEST)
    with pytest.raises(PolicyError):
        c.execute(plan["id"], ACTOR)
    c.approve(plan["id"], ACTOR, plan["digest"])
    assert c.execute(plan["id"], ACTOR)["state"] == "SUCCEEDED"


def test_publish_reconciles_read_failure_without_second_write(setup):
    p, s, t, f, c = setup
    plan = prepare(setup)
    f.fail_read = True
    with pytest.raises(TimeoutError):
        c.execute(plan["id"], ACTOR)
    assert c.get(plan["id"])["state"] == "UNCERTAIN"
    f.fail_read = False
    assert c.reconcile(plan["id"], ACTOR)["state"] == "SUCCEEDED"
    assert sum(endpoint.endswith("?beta=false") for method, endpoint in f.calls) == 1


def test_lost_publish_without_receipt_cannot_be_inferred_from_success(setup):
    p, s, t, f, c = setup
    plan = prepare(setup)
    f.lost = True
    with pytest.raises(TimeoutError):
        c.execute(plan["id"], ACTOR)
    with pytest.raises(RuntimeError, match="No publish receipt"):
        c.reconcile(plan["id"], ACTOR)


def test_publish_version_drift_is_rejected(setup):
    p, s, t, f, c = setup
    plan = prepare(setup)
    f.version = ITEM
    with pytest.raises(PolicyError, match="version changed"):
        c.execute(plan["id"], ACTOR)
    assert c.get(plan["id"])["state"] == "UNCERTAIN"


def test_publish_failed_state_never_claims_success(setup):
    p, s, t, f, c = setup
    plan = prepare(setup)
    f.state = "Failed"
    with pytest.raises(RemoteFailed):
        c.execute(plan["id"], ACTOR)
    assert c.get(plan["id"])["state"] == "FAILED"


def test_publish_rejects_changed_review_source(setup):
    p, s, t, f, c = setup
    plan = prepare(setup)
    (p.repo / "changed.txt").write_text("changed after review")
    with pytest.raises(PolicyError):
        c.execute(plan["id"], ACTOR)
    assert not f.published


def test_running_environment_cannot_be_published_again(setup):
    setup[3].state = "Running"
    with pytest.raises(PolicyError, match="ongoing"):
        prepare(setup)
    assert not setup[3].published


@pytest.mark.parametrize("operation,collection", [("get_sql_database", "sqlDatabases"), ("get_environment", "environments")])
def test_supporting_reads_are_workspace_scoped(setup, operation, collection):
    p, s, t, f, c = setup
    assert authorize(p, operation, DEV, ITEM)[1] == f"workspaces/{DEV}/{collection}/{ITEM}"
    with pytest.raises(PolicyError):
        authorize(p, operation, OP, ITEM)
    gateway = FabricGateway(p, s, c.data_dir, executor=lambda endpoint: {"id": ITEM})
    result = gateway.read({"operation": operation, "workspace_id": DEV, "item_id": ITEM})
    assert result["source"] == "provided_executor"
