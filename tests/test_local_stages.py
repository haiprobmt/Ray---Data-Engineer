import json

import pytest
from pydantic import ValidationError

from ray_de.service import TaskService
from test_recovery import output


class LocalStages:
    def __init__(self, *, review="PASS", noop=False, endless=False):
        self.authors = 0
        self.reviews = 0
        self.review = review
        self.noop = noop
        self.endless = endless
    def run(self, project, prompt, schema, **kwargs):
        if "verdict" in schema["properties"]:
            self.reviews += 1
            return {"verdict": self.review, "summary": "Local source checked", "findings": [], "evidence": ["Read the local source"]}
        self.authors += 1
        result = output()
        if self.authors == 1 or self.endless:
            result.update(continue_work=True, cloud_actions=[], artifacts=[] if self.noop else [
                {"path": f"pipelines/contract_{self.authors}.py", "content": "VERSION = 1\n"},
                {"path": f"tests/test_contract_{self.authors}.py", "content": "def test_version():\n    assert 1 == 1\n"}])
        return result


def run_stages(project, store, tmp_path, runner, mode="write"):
    task = store.create(project.id, "Prepare the contract, then finish the local work", mode)
    report = TaskService(store, runner, tmp_path / "state").run(project, task["id"], task["objective"], "actor",
        snapshot={"project_id": project.id, "binding": project.binding})
    return task, report


def test_local_source_stage_can_continue_without_a_cloud_action(project, store, tmp_path):
    runner = LocalStages()
    task, report = run_stages(project, store, tmp_path, runner)
    assert report["status"] == "completed" and not report["continue_work"]
    assert runner.authors == 2 and runner.reviews == 2
    assert (project.repo / "pipelines/contract_1.py").exists()
    with store.connect() as db:
        assert db.execute("SELECT count(*) FROM plans WHERE task_id=?", (task["id"],)).fetchone()[0] == 0


@pytest.mark.parametrize("verdict", ["REWORK", "BLOCK"])
def test_local_continuation_cannot_skip_review(project, store, tmp_path, verdict):
    runner = LocalStages(review=verdict)
    _, report = run_stages(project, store, tmp_path, runner)
    assert report["status"] == "blocked"
    assert runner.authors == (2 if verdict == "REWORK" else 1)
    assert runner.reviews == runner.authors


def test_local_continuation_requires_real_source_progress(project, store, tmp_path):
    runner = LocalStages(noop=True)
    _, report = run_stages(project, store, tmp_path, runner)
    assert report["status"] == "blocked" and runner.authors == 1
    assert "no verified source changes" in report["user_summary"]


def test_local_read_only_turn_cannot_create_a_write_continuation(project, store, tmp_path):
    runner = LocalStages(noop=True)
    _, report = run_stages(project, store, tmp_path, runner, mode="read")
    assert report["status"] == "blocked" and not report["cloud_eligible"]
    assert runner.authors == 1 and runner.reviews == 0


def test_local_continuation_has_a_bounded_stage_count(project, store, tmp_path, monkeypatch):
    monkeypatch.setattr("ray_de.orchestrator.validate", lambda *a: (True, ["Fixture validation passed"]))
    runner = LocalStages(endless=True)
    task, report = run_stages(project, store, tmp_path, runner)
    assert runner.authors == project.config.execution.max_stages
    assert runner.reviews == runner.authors
    assert report["status"] == "paused" and not report["cloud_eligible"]
    assert store.task(project.id, task["id"])["status"] == "PAUSED"


def test_resume_reviews_preserved_draft_before_continuing(project, store, tmp_path):
    class Interrupted(LocalStages):
        def run(self, project, prompt, schema, **kwargs):
            if "verdict" in schema["properties"]:
                assert "main.py" in prompt
                assert kwargs["read_only"] and not kwargs.get("thread_id")
                return super().run(project, prompt, schema, **kwargs)
            self.authors += 1
            if self.authors == 1:
                (project.repo / "main.py").write_text("answer = 2\n")
                return dict(output("working"), continue_work=True)
            return dict(output(), continue_work=self.authors == 2, artifacts=[])
    runner = Interrupted()
    task = store.create(project.id, "Finish the saved draft and the next local step", "write")
    service = TaskService(store, runner, tmp_path / "state")
    snapshot = {"project_id": project.id, "binding": project.binding}
    with pytest.raises(ValidationError):
        service.run(project, task["id"], task["objective"], "actor", snapshot=snapshot)
    saved = store.task(project.id, task["id"])
    assert saved["status"] == "ERROR" and runner.reviews == 0
    report = json.loads(saved["result"])
    assert report["review_paths"] == ["main.py"] and not report["cloud_eligible"]
    assert report["error"]["validation_issues"] == [{"field": "response", "code": "CONTINUATION_STATUS"}]
    report = service.run(project, task["id"], "Continue the saved task", "actor", snapshot=snapshot)
    assert report["status"] == "completed" and runner.authors == 3 and runner.reviews == 2
    assert (project.repo / "main.py").read_text() == "answer = 2\n"
    assert not store.pending_source(project.id, task["id"])
