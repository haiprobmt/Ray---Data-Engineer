"""One host workflow shared by the terminal and Telegram channel."""

from __future__ import annotations
import json
from .cloud import CloudActions
from .control import Control, TaskStopped
from .fabric import FabricGateway
from .orchestrator import Orchestrator
from .errors import record_failure, describe_error


class TaskService:
    def __init__(self, store, runner, data_dir):
        self.store, self.runner, self.data_dir = store, runner, data_dir

    def run(self, project, task_id, message, actor, *, snapshot=None, cancel=None):
        for stage in range(8):
            report = self._run_stage(project, task_id, message, actor, snapshot=snapshot, cancel=cancel)
            if report.get("status") != "completed" or not report.get("continue_work"):
                return report
            if not report.get("cloud_actions") or not report.get("cloud_plans") or any(p["state"] != "SUCCEEDED" for p in report["cloud_plans"]):
                raise RuntimeError("Cannot continue without successful action receipts")
            objective = self.store.task(project.id, task_id)["objective"]
            message = "Continue the original objective: " + objective + ". Use the recorded successful action receipts for created item IDs. Do not repeat completed actions. Prepare the next dependent stage, or report the verified final result."
            self.store.update(project.id, task_id, "WAITING")
        report.update(status="working", message="Completed eight reviewed stages. Progress and action receipts are saved; resume this task to continue.")
        self.store.update(project.id, task_id, "WAITING", result=report)
        return report

    def _run_stage(self, project, task_id, message, actor, *, snapshot=None, cancel=None):
        user_message = message
        token = cancel or Control(self.store).token(project.id)
        stage = "fabric_read"
        try:
            if snapshot is None and project.config.fabric.workspaces:
                snapshot = FabricGateway(
                    project, self.store, self.data_dir, actor=actor
                ).snapshot()
            token.check()
            for read_round in range(5):
                stage = "model"
                report = Orchestrator(self.store, self.runner).run(
                    project, task_id, message, snapshot=snapshot, cancel=token
                )
                requests = report.get("read_requests", [])
                if not requests or report["status"] != "waiting":
                    break
                if read_round == 4:
                    report.update(status="blocked", message="Reached the bounded Fabric read limit. Resume with a narrower verification request.", read_requests=[])
                    self.store.update(project.id, task_id, "BLOCKED", result=report)
                    return report
                stage = "fabric_read"
                gateway = FabricGateway(project, self.store, self.data_dir, actor=actor)
                observations = []
                for request in requests:
                    token.check()
                    observations.append(gateway.read(request, task_id=task_id))
                token.check()
                snapshot = dict(snapshot or {"project_id": project.id, "binding": project.binding})
                # Keep only this round's bounded results; previous answers are historical.
                snapshot["read_results"] = observations
                message = "Continue the original objective: " + self.store.task(project.id, task_id)["objective"] + ". Current user request: " + user_message + ". Use the host read_results as evidence; do not treat a preview as full-table verification."
        except TaskStopped:
            raise
        except Exception as exc:
            record_failure(self.store, project.id, task_id, exc, stage, "PAUSED" if isinstance(exc, TimeoutError) else "ERROR")
            raise
        if report["status"] != "completed":
            return report
        cloud = CloudActions(project, self.store, self.data_dir)
        try:
            proposals = report.get("cloud_actions", [])
            # Reject any unsupported target before starting a sequence of writes.
            for proposal in proposals:
                cloud.authorize_proposal(proposal)
            # Preserve explicit order: a job can validate a just-updated definition.
            for proposal in proposals:
                token.check()
                plan = cloud.prepare(task_id, actor=actor, **proposal)
                token.check()
                if plan["state"] == "READY":
                    outcome = cloud.execute(plan["id"], actor)
                    if outcome["state"] != "SUCCEEDED":
                        break
            with self.store.connect() as db:
                has_plans = db.execute(
                    "SELECT 1 FROM plans WHERE project_id=? AND task_id=?",
                    (project.id, task_id),
                ).fetchone()
            if has_plans:
                cloud._sync_task(task_id)
                return json.loads(self.store.task(project.id, task_id)["result"])
        except TaskStopped:
            self.store.update(project.id, task_id, "PAUSED")
            raise
        except Exception as exc:
            # No model text can turn a rejected or uncertain cloud action into success.
            report = json.loads(self.store.task(project.id, task_id)["result"] or "{}")
            report.update(
                status="blocked",
                message=describe_error(exc, "cloud_action")["message"],
                error=describe_error(exc, "cloud_action"),
            )
            self.store.update(project.id, task_id, "BLOCKED", result=report)
            return report
        return report
