"""One host workflow shared by the terminal and Telegram channel."""

from __future__ import annotations
import json
from .cloud import CloudActions
from .control import Control, TaskStopped
from .fabric import FabricGateway
from .orchestrator import Orchestrator
from .errors import RayError, record_failure, describe_error
from .state import now


class TaskService:
    def __init__(self, store, runner, data_dir):
        self.store, self.runner, self.data_dir = store, runner, data_dir

    def run(self, project, task_id, message, actor, *, snapshot=None, cancel=None):
        from .execution import Budget, BudgetRunner, ExecutionLimit
        from .task_context import record_request, merge
        record_request(self.store, project.id, task_id, message)
        budget = Budget(project.config.execution, cancel or Control(self.store).token(project.id))
        runner = BudgetRunner(self.runner, budget, self.store, project.id, task_id)
        try:
            return self._run(project, task_id, message, actor, snapshot=snapshot, cancel=budget, runner=runner)
        except ExecutionLimit:
            report = json.loads(self.store.task(project.id, task_id)["result"] or "{}")
            report.update(status="paused", phase="execution_limit", cloud_eligible=False,
                          user_summary="The execution budget is reached. Progress is saved.",
                          next_step="Use /resume to continue after inspecting the saved result.")
            self.store.update(project.id, task_id, "PAUSED", result=report)
            return report
        finally:
            merge(self.store, project.id, task_id, execution=budget.snapshot())

    def _run(self, project, task_id, message, actor, *, snapshot=None, cancel=None, runner=None):
        from .reading import ReadingStore
        cancel = cancel or Control(self.store).token(project.id)
        cancel.check()
        references = ReadingStore(self.store).read_links(actor, project, message, cancel=cancel)
        for stage in range(project.config.execution.max_stages):
            report = self._run_stage(project, task_id, message, actor, snapshot=snapshot, cancel=cancel, references=references, runner=runner)
            if report.get("status") not in {"completed", "waiting"} or not report.get("continue_work"):
                return report
            receipts = report.get("cloud_plans", [])
            current_ids = report.get("stage_plan_ids")
            current = receipts if current_ids is None else [p for p in receipts if p["id"] in current_ids]
            if report.get("cloud_actions"):
                if not current or any(p["state"] != "SUCCEEDED" for p in current):
                    raise RuntimeError("Cannot continue without successful action receipts")
            elif not (report.get("cloud_eligible") and report.get("review_paths") and report.get("host_validation")
                      and (report.get("review") or {}).get("verdict") in {"PASS", "PASS_WITH_COMMENTS"}):
                report.update(status="blocked", phase="needs_attention", cloud_eligible=False, continue_work=False,
                              user_summary="Ray asked for another local step, but there were no verified source changes to carry forward.",
                              next_step="Finish and check a concrete source change before continuing.")
                self.store.update(project.id, task_id, "BLOCKED", result=report)
                return report
            objective = self.store.task(project.id, task_id)["objective"]
            message = "Continue the original objective: " + objective + ". The previous stage passed host validation and independent review. Use the saved source and any successful action receipts. Do not repeat completed actions or rewrite unchanged source. Prepare the next dependent stage, or report the verified final result."
            self.store.update(project.id, task_id, "WAITING")
        report.update(status="paused", phase="stage_limit", cloud_eligible=False,
                      message="Reached the configured stage budget. Progress and action receipts are saved; resume this task to continue.",
                      user_summary="The stage budget is reached. The task is saved and paused.", next_step="Use /resume to continue the remaining work.")
        self.store.update(project.id, task_id, "PAUSED", result=report)
        return report

    def _run_stage(self, project, task_id, message, actor, *, snapshot=None, cancel=None, references=(), runner=None):
        user_message = message
        token = cancel or Control(self.store).token(project.id)
        stage = "fabric_read"
        try:
            if snapshot is None and project.workspaces:
                saved = json.loads(self.store.task(project.id, task_id)["result"] or "{}")
                saved.update(status="waiting", phase="reading_fabric", user_summary=None, next_step=None)
                self.store.update(project.id, task_id, "WAITING", result=saved)
                snapshot = FabricGateway(
                    project, self.store, self.data_dir, actor=actor
                ).snapshot()
            token.check()
            cloud = CloudActions(project, self.store, self.data_dir)
            receipts = cloud.receipts(task_id)
            snapshot = dict(snapshot or {"project_id": project.id, "binding": project.binding})
            snapshot["action_receipts"] = receipts
            snapshot["reference_material"] = references
            read_failures = {}
            repeated = {}
            for read_round in range(project.config.execution.max_read_rounds + 1):
                stage = "model"
                report = Orchestrator(self.store, runner or self.runner).run(
                    project, task_id, message, snapshot=snapshot, cancel=token
                )
                if receipts:
                    report["cloud_plans"] = receipts
                if read_failures:
                    report["read_failures"] = list(read_failures.values())
                if receipts or read_failures:
                    self.store.update(project.id, task_id, self.store.task(project.id, task_id)["status"], result=report)
                requests = report.get("read_requests", [])
                guidance = report.get("guidance_requests", [])
                if not (requests or guidance) or report["status"] != "waiting":
                    break
                if read_round == project.config.execution.max_read_rounds:
                    report.update(status="blocked", message="Reached the configured investigation budget. Resume with a narrower verification request.", read_requests=[])
                    self.store.update(project.id, task_id, "BLOCKED", result=report)
                    return report
                stage = "fabric_read"
                gateway = FabricGateway(project, self.store, self.data_dir, actor=actor)
                gateway.cancel = token
                observations = []
                if guidance:
                    from .skills import search_guidance
                    for query in guidance:
                        observations.append({"request": {"guidance_query": query}, "source": "pinned_guidance",
                                             "captured_at": now(), "data": search_guidance(query, limit=3, budget=12000)})
                for request in requests:
                    token.check()
                    key = json.dumps(request, sort_keys=True)
                    if key in read_failures:
                        observations.append(read_failures[key])
                        continue
                    try:
                        observation = gateway.read(request, task_id=task_id)
                        observations.append(dict(observation, request=request))
                    except RayError as exc:
                        failure = {"request": request, "source": "host_read_error", "captured_at": now(),
                                   "error": describe_error(exc, "fabric_read")}
                        read_failures[key] = failure
                        observations.append(failure)
                token.check()
                from .task_context import observe
                observe(self.store, project.id, task_id, observations)
                signature = json.dumps([{k: v for k, v in row.items() if k != "captured_at"} for row in observations], sort_keys=True)
                repeated[signature] = repeated.get(signature, 0) + 1
                if repeated[signature] >= 3:
                    report.update(status="blocked", read_requests=[], guidance_requests=[],
                                  message="Repeated investigation returned no new evidence. Change the approach or narrow the request.")
                    self.store.update(project.id, task_id, "BLOCKED", result=report)
                    return report
                snapshot = dict(snapshot or {"project_id": project.id, "binding": project.binding})
                # Recent bounded observations also survive thread loss in task_context.
                snapshot["read_results"] = observations
                message = "Continue the original objective: " + self.store.task(project.id, task_id)["objective"] + ". Current user request: " + user_message + ". Use the host read_results as evidence; do not treat a preview as full-table verification. A host_read_error means that read failed, not that a successful deployment failed. Do not repeat failed reads or cloud actions; use other available evidence or report the precise verification limitation."
        except TaskStopped:
            self.store.update(project.id, task_id, "PAUSED")
            raise
        except Exception as exc:
            current = self.store.task(project.id, task_id)
            recorded = json.loads(current["result"] or "{}").get("error")
            # Orchestrator already records the precise author/review/validation stage.
            if stage != "model" or not recorded or current["status"] not in {"ERROR", "PAUSED"}:
                record_failure(self.store, project.id, task_id, exc, stage, "PAUSED" if isinstance(exc, TimeoutError) else "ERROR")
            raise
        if report["status"] != "completed" and not (report["status"] == "waiting" and report.get("cloud_eligible")):
            return report
        cloud = CloudActions(project, self.store, self.data_dir)
        try:
            proposals = report.get("cloud_actions", [])
            if proposals:
                report.update(status="waiting", phase="preparing_fabric")
                self.store.update(project.id, task_id, "WAITING", result=report)
            # Reject any unsupported target before starting a sequence of writes.
            for proposal in proposals:
                cloud.authorize_proposal(proposal)
            # Preserve explicit order: a job can validate a just-updated definition.
            for index, proposal in enumerate(proposals):
                token.check()
                plan = cloud.prepare(task_id, actor=actor, **proposal)
                token.check()
                if plan["state"] == "PENDING_APPROVAL":
                    if index + 1 < len(proposals):
                        # Dependent preflight must observe the preceding approved
                        # operation. Resume with a fresh source review after approval.
                        pending = json.loads(self.store.task(project.id, task_id)["result"])
                        pending.update(continue_work=True, remaining_cloud_actions=proposals[index + 1:])
                        self.store.update(project.id, task_id, "APPROVAL_REQUIRED", result=pending)
                    break
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
