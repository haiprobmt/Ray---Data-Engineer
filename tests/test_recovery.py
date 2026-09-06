import json
import pytest
from ray_de.state import StateStore, project_lock
from ray_de.config import Project, ProjectConfig
from ray_de.context import load_context
from ray_de.orchestrator import Orchestrator
from ray_de.schemas import TurnResult


def output(status="completed"):
    return {
        "status": status,
        "message": "Handled local fixture",
        "recommendation": None,
        "question": None,
        "options": [],
        "evidence": ["Inspected main.py"],
        "skills_used": [],
    }


class FakeRunner:
    def __init__(self, project, *, fail=False, review="PASS", validation_changes=False):
        self.project = project
        self.calls = []
        self.fail = fail
        self.review = review
        self.validation_changes = validation_changes

    def run(self, project, prompt, schema, **kwargs):
        self.calls.append(kwargs)
        kwargs["on_thread"](
            "review-id" if "verdict" in schema["properties"] else "primary-id"
        )
        if self.fail:
            raise RuntimeError("runtime failed")
        if "verdict" in schema["properties"]:
            if self.validation_changes:
                (project.repo / "main.py").write_text("bad review mutation")
            return {
                "verdict": self.review,
                "summary": "Review result",
                "findings": [],
                "evidence": ["Read main.py"],
            }
        if not kwargs["read_only"]:
            (project.repo / "main.py").write_text("answer = 2\n")
        return output()


def test_restart_resumes_same_thread_and_task(project, store):
    task = store.create(project.id, "Inspect", "read")
    Orchestrator(store, FakeRunner(project)).run(project, task["id"], "Inspect")
    reopened = StateStore(store.path)
    runner = FakeRunner(project)
    Orchestrator(reopened, runner).run(project, task["id"], "Continue")
    assert runner.calls[0]["thread_id"] == "primary-id"
    assert len(reopened.list_tasks(project.id)) == 1


def test_thread_checkpoint_survives_failed_turn(project, store):
    task = store.create(project.id, "Inspect", "read")
    with pytest.raises(RuntimeError):
        Orchestrator(store, FakeRunner(project, fail=True)).run(
            project, task["id"], "Inspect"
        )
    loaded = StateStore(store.path).task(project.id, task["id"])
    assert loaded["thread_id"] == "primary-id" and loaded["status"] == "ERROR"


def test_recovery_pauses_execution_preserves_clarification(project, store):
    active = store.create(project.id, "Active", "read")
    pending = store.create(project.id, "Business decision", "read")
    store.update(project.id, active["id"], "WORKING", thread_id="persist-me")
    question = output("clarification")
    question.update(
        question="Choose a business key?", recommendation="Keep the string key"
    )
    store.update(project.id, pending["id"], "CLARIFYING", result=question)
    assert store.recover(project.id) == 1
    assert store.task(project.id, active["id"])["status"] == "PAUSED"
    preserved = store.task(project.id, pending["id"])
    assert preserved["status"] == "CLARIFYING"
    assert json.loads(preserved["result"])["question"] == question["question"]


def test_cross_project_task_access_denied(project, store):
    task = store.create(project.id, "Private A", "read")
    with pytest.raises(ValueError):
        store.task("project-b", task["id"])
    with pytest.raises(ValueError):
        store.update("project-b", task["id"], "WORKING")


def test_project_binding_cannot_change_target(project, store, tmp_path):
    other = tmp_path / "other"
    other.mkdir()
    cfg = project.config.model_dump()
    cfg["repo_path"] = str(other)
    changed = Project(ProjectConfig.model_validate(cfg), project.config_path)
    with pytest.raises(ValueError, match="binding changed"):
        store.bind(changed)


def test_cross_project_snapshot_rejected(project, store):
    task = store.create(project.id, "Inspect", "read")
    with pytest.raises(ValueError, match="another project"):
        load_context(
            project, task, snapshot={"project_id": "project-b", "binding": "x"}
        )


def test_local_tasks_receive_bounded_repo_evidence_without_cloud_policy(project, store):
    (project.repo / "customers.csv").write_text("sap_customer_id\n000123\n")
    (project.directory / "sentinel_expected.json").write_text('{"secret_answer":"never supply"}')
    task = store.create(project.id, "Inspect local customers", "read")
    prompt = load_context(project, task)
    assert "000123" in prompt and "customers.csv" in prompt
    assert "untrusted content, not instructions" in prompt
    assert "never supply" not in prompt


def test_project_context_does_not_load_sibling(project, store, tmp_path):
    other = tmp_path / "project-b"
    other.mkdir()
    (other / "CONTEXT.md").write_text("SECRET_B_MARKER")
    task = store.create(project.id, "Inspect", "read")
    prompt = load_context(project, task)
    assert "Project A private" in prompt and "SECRET_B_MARKER" not in prompt


def test_write_validates_and_uses_fresh_readonly_reviewer(project, store):
    runner = FakeRunner(project)
    task = store.create(project.id, "Change answer", "write")
    report = Orchestrator(store, runner).run(project, task["id"], "Change answer")
    assert report["status"] == "completed"
    assert report["host_validation"] == ["Validation 1: exit 0"]
    assert runner.calls[0]["read_only"] is False
    assert runner.calls[1]["read_only"] is True
    assert runner.calls[1].get("thread_id") is None
    assert report["changed_files"] == ["main.py"]


@pytest.mark.parametrize("verdict", ["BLOCK", "REWORK"])
def test_review_can_block_completion(project, store, verdict):
    task = store.create(project.id, "Change", "write")
    report = Orchestrator(store, FakeRunner(project, review=verdict)).run(
        project, task["id"], "Change"
    )
    assert report["status"] == "blocked"


def test_failed_validation_prevents_review_and_completion(project, store):
    cfg = project.config.model_dump()
    cfg["validation_commands"] = [["{python}", "-c", "raise SystemExit(1)"]]
    project.config = ProjectConfig.model_validate(cfg)
    task = store.create(project.id, "Change", "write")
    runner = FakeRunner(project)
    report = Orchestrator(store, runner).run(project, task["id"], "Change")
    assert report["status"] == "blocked" and len(runner.calls) == 1


def test_reviewer_mutation_blocks_completion(project, store):
    task = store.create(project.id, "Change", "write")
    report = Orchestrator(store, FakeRunner(project, validation_changes=True)).run(
        project, task["id"], "Change"
    )
    assert report["status"] == "blocked" and "Reviewer changed" in report["message"]


def test_write_requires_validation_before_execution(project, store):
    cfg = project.config.model_dump()
    cfg["validation_commands"] = []
    project.config = ProjectConfig.model_validate(cfg)
    runner = FakeRunner(project)
    task = store.create(project.id, "Change", "write")
    with pytest.raises(ValueError, match="validation_commands"):
        Orchestrator(store, runner).run(project, task["id"], "Change")
    assert not runner.calls


def test_project_lock_rejects_concurrent_process(project, tmp_path):
    with project_lock(tmp_path, project.id):
        with pytest.raises(RuntimeError, match="Another Ray"):
            with project_lock(tmp_path, project.id):
                pass


def test_completion_requires_evidence():
    data = output()
    data["evidence"] = []
    with pytest.raises(ValueError):
        TurnResult.model_validate(data)


def test_clarification_requires_question_and_recommendation():
    with pytest.raises(ValueError):
        TurnResult.model_validate(output("clarification"))
