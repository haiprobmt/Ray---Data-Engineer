import json
import pytest
from ray_de.demo import fixture, DEV, TEST, ITEM, ACTOR, definition, run_demo
from ray_de.cloud import definition_parts
from ray_de.control import Control, TaskStopped
from ray_de.config import ProjectConfig
from ray_de.fabric import PolicyError


@pytest.fixture
def setup(tmp_path):
    return fixture(tmp_path / "fixture")


def prepare(s, operation="update_definition", ws=DEV):
    p, store, task, transport, cloud = s
    return cloud.prepare(task["id"], operation, ws, ITEM, "definition.json", ACTOR)


def test_dev_update_replay_and_receipt(setup):
    p, s, t, f, c = setup
    plan = prepare(setup)
    assert c.execute(plan["id"], ACTOR)["state"] == "SUCCEEDED"
    with pytest.raises(PolicyError):
        c.execute(plan["id"], ACTOR)
    assert len(f.mutations) == 1
    assert (
        json.loads(s.task(p.id, t["id"])["result"])["cloud_plans"][0]["state"]
        == "SUCCEEDED"
    )


@pytest.mark.parametrize("reason", ["missing", "actor", "digest", "expired", "source"])
def test_test_gate_rejects_invalid_approval(setup, reason):
    p, s, t, f, c = setup
    plan = prepare(setup, "deploy_to_test", TEST)
    with pytest.raises(PolicyError):
        if reason == "missing":
            c.execute(plan["id"], ACTOR)
        elif reason == "actor":
            c.approve(plan["id"], "intruder", plan["digest"])
        elif reason == "digest":
            c.approve(plan["id"], ACTOR, "wrong")
        elif reason == "expired":
            with s.connect() as db:
                db.execute("UPDATE plans SET expires=0 WHERE id=?", (plan["id"],))
            c.approve(plan["id"], ACTOR, plan["digest"])
        else:
            (p.repo / "changed.py").write_text("changed")
            c.approve(plan["id"], ACTOR, plan["digest"])
    assert not f.mutations


def test_test_approval_and_lro(setup):
    p, s, t, f, c = setup
    plan = prepare(setup, "deploy_to_test", TEST)
    f.async_update = True
    c.approve(plan["id"], ACTOR, plan["digest"])
    with pytest.raises(PolicyError):
        c.approve(plan["id"], ACTOR, plan["digest"])
    assert c.execute(plan["id"], ACTOR)["state"] == "SUCCEEDED"
    assert json.loads(c.get(plan["id"])["remote"])["kind"] == "operation"


@pytest.mark.parametrize("reason", ["target", "source", "stop", "tamper", "policy"])
def test_preflight_blocks_mutation(setup, reason):
    p, s, t, f, c = setup
    plan = prepare(setup)
    if reason == "target":
        f.definitions[DEV] = definition("changed remote")
    if reason == "source":
        (p.repo / "main.py").write_text("changed local")
    if reason == "stop":
        Control(s).stop(p.id)
    if reason == "tamper":
        with s.connect() as db:
            db.execute("UPDATE plans SET body='{}' WHERE id=?", (plan["id"],))
    if reason == "policy":
        cfg = p.config.model_dump()
        cfg["policy"]["fabric_dev_write"] = False
        p.config = ProjectConfig.model_validate(cfg)
    with pytest.raises((PolicyError, TaskStopped)):
        c.execute(plan["id"], ACTOR)
    assert not f.mutations


def test_lost_write_response_reconciles_without_resubmit(setup):
    p, s, t, f, c = setup
    plan = prepare(setup)
    f.lose_response = True
    with pytest.raises(TimeoutError):
        c.execute(plan["id"], ACTOR)
    assert c.get(plan["id"])["state"] == "UNCERTAIN"
    with pytest.raises(PolicyError):
        c.execute(plan["id"], ACTOR)
    assert c.reconcile(plan["id"], ACTOR)["state"] == "SUCCEEDED"
    assert len(f.mutations) == 1


def test_stale_reconciliation_rejected(setup):
    p, s, t, f, c = setup
    plan = prepare(setup)
    f.lose_response = True
    with pytest.raises(TimeoutError):
        c.execute(plan["id"], ACTOR)
    (p.repo / "main.py").write_text("modified")
    with pytest.raises(PolicyError):
        c.reconcile(plan["id"], ACTOR)
    assert len(f.mutations) == 1


def test_job_requires_matching_reviewed_definition(setup):
    with pytest.raises(PolicyError, match="differs"):
        prepare(setup, "run_job")


def test_job_failure_never_claims_success_or_retries(setup):
    p, s, t, f, c = setup
    f.definitions[DEV] = json.loads((p.repo / "definition.json").read_text())
    f.job_status = "Failed"
    plan = prepare(setup, "run_job")
    with pytest.raises(RuntimeError):
        c.execute(plan["id"], ACTOR)
    assert c.get(plan["id"])["state"] == "FAILED" and len(f.mutations) == 1


def test_post_validation_failure_is_recorded(setup, monkeypatch):
    p, s, t, f, c = setup
    plan = prepare(setup)
    monkeypatch.setattr(
        "ray_de.cloud.validate", lambda *a, **k: (False, ["Row count failed"])
    )
    assert c.execute(plan["id"], ACTOR)["state"] == "VALIDATION_FAILED"
    assert s.task(p.id, t["id"])["status"] == "BLOCKED"


@pytest.mark.parametrize(
    "reason", ["prod", "unlisted", "noexport", "novalidation", "noeligibility"]
)
def test_target_policy_cannot_be_bypassed(setup, reason):
    p, s, t, f, c = setup
    cfg = p.config.model_dump()
    if reason == "prod":
        cfg["fabric"]["workspaces"][0]["environment"] = "PROD"
    if reason == "unlisted":
        cfg["fabric"]["write_targets"] = []
    if reason == "noexport":
        cfg["fabric"]["allow_definition_export"] = False
    if reason == "novalidation":
        cfg["post_validation_commands"] = []
    if reason == "noeligibility":
        report = json.loads(s.task(p.id, t["id"])["result"])
        report["cloud_eligible"] = False
        s.update(p.id, t["id"], "BLOCKED", result=report)
    p.config = ProjectConfig.model_validate(cfg)
    with pytest.raises(PolicyError):
        prepare(setup)
    assert not f.calls


@pytest.mark.parametrize("path", ["../bad", "/bad", "a\\bad", ".platform", ""])
def test_definition_path_rejected(path):
    data = definition("hello")
    data["definition"]["parts"][0]["path"] = path
    with pytest.raises(ValueError):
        definition_parts(data)


def test_secret_definition_rejected():
    with pytest.raises(ValueError):
        definition_parts(definition("api_key = sk-" + "a" * 30))


def test_recovery_never_replays_executing_plan(setup):
    p, s, t, f, c = setup
    plan = prepare(setup)
    with s.connect() as db:
        db.execute("UPDATE plans SET state='EXECUTING' WHERE id=?", (plan["id"],))
    Control(s).recover(p.id)
    assert c.get(plan["id"])["state"] == "UNCERTAIN" and not f.mutations


def test_offline_end_to_end(tmp_path):
    report = run_demo(tmp_path / "demo")
    assert not report["live_services_contacted"] and all(
        c["passed"] for c in report["checks"]
    )
    assert report["synthetic_mutations"] == 4


def test_unresolved_target_cannot_get_a_new_plan(setup):
    p, s, t, f, c = setup
    plan = prepare(setup)
    f.lose_response = True
    with pytest.raises(TimeoutError):
        c.execute(plan["id"], ACTOR)
    with pytest.raises(PolicyError, match="unresolved"):
        prepare(setup)
    assert len(f.mutations) == 1


def test_new_review_does_not_revalidate_old_plan(setup):
    from ray_de.demo import SyntheticRunner
    from ray_de.orchestrator import Orchestrator

    p, s, t, f, c = setup
    plan = prepare(setup)
    (p.repo / "new.py").write_text("new_revision = True")
    Orchestrator(s, SyntheticRunner()).run(p, t["id"], "Review a new source revision")
    with pytest.raises(PolicyError, match="earlier source"):
        c.execute(plan["id"], ACTOR)
    assert not f.mutations


def test_shared_service_updates_then_runs_job(setup, monkeypatch):
    from ray_de.demo import SyntheticRunner
    from ray_de.service import TaskService

    p, s, t, f, c = setup

    class Runner(SyntheticRunner):
        def run(self, *args, **kwargs):
            result = super().run(*args, **kwargs)
            if "status" in result:
                result["cloud_actions"] = [
                    {
                        "operation": op,
                        "workspace_id": DEV,
                        "item_id": ITEM,
                        "definition_path": "definition.json",
                    }
                    for op in ("update_definition", "run_job")
                ]
            return result

    monkeypatch.setattr("ray_de.service.CloudActions", lambda *a, **k: c)
    result = TaskService(s, Runner(), s.path.parent).run(
        p,
        t["id"],
        "Update then validate",
        ACTOR,
        snapshot={"project_id": p.id, "binding": p.binding, "workspaces": []},
    )
    assert result["status"] == "completed" and len(f.mutations) == 2


def test_transport_audits_mutation_and_sanitizes_errors(setup, monkeypatch):
    import subprocess
    from ray_de.cloud import FabricTransport

    p, s, t, f, c = setup
    calls = []

    def run(argv, **kwargs):
        calls.append(argv)
        return subprocess.CompletedProcess(argv, 1, "secret-response", "secret-error")

    monkeypatch.setattr("ray_de.cloud.subprocess.run", run)
    with pytest.raises(RuntimeError) as error:
        FabricTransport(p, s, s.path.parent, ACTOR, t["id"]).request(
            "post",
            f"workspaces/{DEV}/items/{ITEM}/updateDefinition",
            definition("safe"),
        )
    assert len(calls) == 1 and "secret" not in str(error.value)
    with s.connect() as db:
        rows = [dict(r) for r in db.execute("SELECT * FROM actions")]
    assert rows[-1]["status"] == "UNCERTAIN" and rows[-1]["task_id"] == t["id"]
    assert "secret" not in json.dumps(rows)
