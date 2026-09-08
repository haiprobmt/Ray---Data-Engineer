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


def test_lost_auth_while_polling_keeps_job_uncertain_and_reconciles(setup, monkeypatch):
    from types import SimpleNamespace
    from ray_de.cloud import FabricTransport
    from ray_de.errors import RayError
    p, s, t, f, c = setup
    f.definitions[DEV] = json.loads((p.repo / "definition.json").read_text())
    plan = prepare(setup, "run_job")
    original = f.request
    real = FabricTransport(p, s, c.data_dir, ACTOR, t["id"])
    def request(method, endpoint, payload=None):
        if method == "get" and "/jobs/instances/" in endpoint:
            return real.request(method, endpoint)
        return original(method, endpoint, payload)
    with monkeypatch.context() as patch:
        patch.setattr(f, "request", request)
        patch.setattr("ray_de.cloud.subprocess.run", lambda *a, **kw: SimpleNamespace(
            returncode=1, stdout='{"error_code":"FABRIC_SIGNIN_REQUIRED"}'))
        with pytest.raises(RayError) as error:
            c.execute(plan["id"], ACTOR)
        assert error.value.code == "FABRIC_SIGNIN_REQUIRED"
    assert c.get(plan["id"])["state"] == "UNCERTAIN"
    assert json.loads(c.get(plan["id"])["remote"])["kind"] == "job"
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
    from ray_de.errors import RayError, describe_error
    with pytest.raises(RayError) as caught:
        prepare(setup, "run_job")
    error = describe_error(caught.value, "cloud_action")
    assert error["code"] == "FABRIC_DEFINITION_CHANGED"
    assert "review" in error["next_step"]
    assert setup[3].mutations == []


def test_job_failure_never_claims_success_or_retries(setup):
    p, s, t, f, c = setup
    f.definitions[DEV] = json.loads((p.repo / "definition.json").read_text())
    f.job_status = "Failed"
    plan = prepare(setup, "run_job")
    with pytest.raises(RuntimeError):
        c.execute(plan["id"], ACTOR)
    assert c.get(plan["id"])["state"] == "FAILED" and len(f.mutations) == 1


def test_reconcile_terminal_failed_job_updates_uncertain_receipt(setup, monkeypatch):
    p, s, t, f, c = setup
    f.definitions[DEV] = json.loads((p.repo / "definition.json").read_text())
    action = prepare(setup, "run_job")
    original = f.request
    def request(method, endpoint, payload=None):
        if method == "get" and "/jobs/instances/" in endpoint:
            raise TimeoutError("polling interrupted")
        return original(method, endpoint, payload)
    with monkeypatch.context() as patch:
        patch.setattr(f, "request", request)
        with pytest.raises(TimeoutError):
            c.execute(action["id"], ACTOR)
    f.job_status = "Failed"
    with pytest.raises(RuntimeError):
        c.reconcile(action["id"], ACTOR)
    assert c.get(action["id"])["state"] == "FAILED"
    assert len(f.mutations) == 1


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
    report = json.loads(s.task(p.id, t["id"])["result"])
    assert report["status"] == "blocked"
    assert report["cloud_plans"][0]["state"] == "UNCERTAIN"


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


def test_new_review_supersedes_old_approval_even_with_unchanged_source(setup):
    from ray_de.demo import SyntheticRunner
    from ray_de.orchestrator import Orchestrator
    p, s, t, f, c = setup
    action = prepare(setup, "deploy_to_test", TEST)
    Orchestrator(s, SyntheticRunner()).run(p, t["id"], "Reassess the task with new requirements")
    with pytest.raises(PolicyError):
        c.approve(action["id"], ACTOR, action["digest"])
    assert c.get(action["id"])["state"] == "SUPERSEDED"
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


def test_task_never_reports_done_while_fabric_work_is_pending(setup, monkeypatch):
    from ray_de.demo import SyntheticRunner
    from ray_de.service import TaskService
    p, s, t, f, c = setup
    class Runner(SyntheticRunner):
        def run(self, *args, **kwargs):
            result = super().run(*args, **kwargs)
            if "status" in result:
                result["cloud_actions"] = [dict(operation=op, workspace_id=DEV, item_id=ITEM,
                    definition_path="definition.json") for op in ("update_definition", "run_job")]
            return result
    original = c.authorize_proposal
    def authorize(proposal):
        assert s.task(p.id, t["id"])["status"] == "WAITING"
        return original(proposal)
    monkeypatch.setattr(c, "authorize_proposal", authorize)
    request = f.request
    def inspect(method, endpoint, payload=None):
        if endpoint.endswith(("updateDefinition", "RunNotebook/instances")):
            saved = json.loads(s.task(p.id, t["id"])["result"])
            assert saved["status"] == "waiting"
            assert saved["phase"] in {"applying_changes", "running_job"}
            assert any(plan["state"] == "EXECUTING" for plan in saved["cloud_plans"])
        return request(method, endpoint, payload)
    monkeypatch.setattr(f, "request", inspect)
    monkeypatch.setattr("ray_de.service.CloudActions", lambda *a, **k: c)
    result = TaskService(s, Runner(), s.path.parent).run(p, t["id"], "Update and run", ACTOR,
        snapshot={"project_id": p.id, "binding": p.binding})
    assert result["status"] == "completed" and len(f.mutations) == 2


def test_test_dependencies_wait_for_preceding_exact_approval(setup, monkeypatch):
    from ray_de.demo import SyntheticRunner
    from ray_de.service import TaskService
    p, s, t, f, c = setup
    class Runner(SyntheticRunner):
        author_turns = 0
        def run(self, *args, **kwargs):
            result = super().run(*args, **kwargs)
            if "status" in result:
                operations = ("deploy_to_test", "run_job") if self.author_turns == 0 else ("run_job",)
                self.author_turns += 1
                result["cloud_actions"] = [dict(operation=op, workspace_id=TEST, item_id=ITEM,
                                                definition_path="definition.json")
                                           for op in operations]
            return result
    cfg = p.config.model_dump()
    cfg["fabric"]["workspace_write"] = True
    p.config = ProjectConfig.model_validate(cfg)
    monkeypatch.setattr("ray_de.service.CloudActions", lambda *a, **k: c)
    service = TaskService(s, Runner(), s.path.parent)
    snapshot = {"project_id": p.id, "binding": p.binding}
    result = service.run(p, t["id"], "Update TEST then run", ACTOR, snapshot=snapshot)
    assert result["status"] == "approval_required"
    assert len(result["cloud_plans"]) == 1 and not f.mutations
    assert result["continue_work"] is True
    action = c.get(result["cloud_plans"][0]["id"])
    c.approve(action["id"], ACTOR, action["digest"])
    c.execute(action["id"], ACTOR)
    assert s.task(p.id, t["id"])["status"] == "WAITING"
    result = service.run(p, t["id"], "Continue from approved update receipt", ACTOR, snapshot=snapshot)
    assert result["status"] == "approval_required" and len(f.mutations) == 1
    job = c.get(result["stage_plan_ids"][0])
    with pytest.raises(PolicyError):
        c.execute(job["id"], ACTOR)
    c.approve(job["id"], ACTOR, job["digest"])
    c.execute(job["id"], ACTOR)
    assert s.task(p.id, t["id"])["status"] == "COMPLETED"
    assert len(f.mutations) == 2


def test_new_review_can_complete_after_a_known_failed_previous_attempt(setup, monkeypatch):
    from ray_de.demo import SyntheticRunner
    from ray_de.service import TaskService
    p, s, t, f, c = setup
    f.definitions[DEV] = json.loads((p.repo / "definition.json").read_text())
    action = prepare(setup, "run_job")
    f.job_status = "Failed"
    with pytest.raises(RuntimeError):
        c.execute(action["id"], ACTOR)
    f.job_status = "Completed"
    class Runner(SyntheticRunner):
        author_turns = 0
        def run(self, *args, **kwargs):
            result = super().run(*args, **kwargs)
            if "status" in result:
                if self.author_turns == 0:
                    result["cloud_actions"] = [dict(operation="run_job", workspace_id=DEV, item_id=ITEM,
                                                    definition_path="definition.json")]
                    result["continue_work"] = True
                self.author_turns += 1
            return result
    monkeypatch.setattr("ray_de.service.CloudActions", lambda *a, **k: c)
    result = TaskService(s, Runner(), s.path.parent).run(p, t["id"], "The cause is fixed; run again", ACTOR,
                snapshot={"project_id": p.id, "binding": p.binding})
    assert result["status"] == "completed"
    assert [receipt["state"] for receipt in result["cloud_plans"]] == ["FAILED", "SUCCEEDED"]
    assert len(f.mutations) == 2


def test_new_model_completion_cannot_hide_unresolved_remote_action(setup, monkeypatch):
    from ray_de.demo import SyntheticRunner
    from ray_de.service import TaskService
    p, s, t, f, c = setup
    action = prepare(setup)
    f.lose_response = True
    with pytest.raises(TimeoutError):
        c.execute(action["id"], ACTOR)
    monkeypatch.setattr("ray_de.service.CloudActions", lambda *a, **k: c)
    result = TaskService(s, SyntheticRunner(), s.path.parent).run(p, t["id"], "Inspect status", ACTOR,
        snapshot={"project_id": p.id, "binding": p.binding})
    assert result["status"] == "blocked"
    assert result["cloud_plans"][0]["state"] == "UNCERTAIN"
    assert len(f.mutations) == 1


@pytest.mark.parametrize("path", ["./.platform", "folder/../.platform", "folder//file.py", "./notebook-content.py"])
def test_definition_part_paths_must_be_canonical(path):
    data = definition("safe")
    data["definition"]["parts"][0]["path"] = path
    with pytest.raises(ValueError):
        definition_parts(data)


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
