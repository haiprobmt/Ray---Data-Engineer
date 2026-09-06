import json
from ray_de.service import TaskService
from ray_de.demo import fixture


def test_continuation_uses_original_objective_and_receipts(tmp_path):
    project, store, task, _, _ = fixture(tmp_path)
    service = TaskService(store, None, tmp_path / "state")
    calls = []
    def stage(p, id, message, actor, **kw):
        calls.append(message)
        report = {"status": "completed", "message": "stage", "continue_work": len(calls) == 1,
                  "cloud_actions": [{"operation": "create_item"}],
                  "cloud_plans": [{"state": "SUCCEEDED", "remote": {"id": "created-id"}}]}
        store.update(p.id, id, "COMPLETED", result=report)
        return report
    service._run_stage = stage
    result = service.run(project, task["id"], "new phase", "actor")
    assert len(calls) == 2 and task["objective"] in calls[1]
    assert "Do not repeat" in calls[1] and not result["continue_work"]


def test_continuation_stops_on_pending_approval(tmp_path):
    project, store, task, _, _ = fixture(tmp_path)
    service = TaskService(store, None, tmp_path / "state")
    calls = []
    def stage(*a, **kw):
        calls.append(1)
        return {"status": "approval_required", "continue_work": True}
    service._run_stage = stage
    assert service.run(project, task["id"], "do work", "actor")["status"] == "approval_required"
    assert len(calls) == 1
