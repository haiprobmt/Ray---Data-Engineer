from __future__ import annotations

import json
import sys

from .context import changed, load_context, manifest
from .runtime import clean_env
from .control import Control, TaskStopped
from .processes import run_checked
import hashlib
from .schemas import ReviewResult, TurnResult
from .errors import record_failure

STATES = {
    "working": "WAITING",
    "clarification": "CLARIFYING",
    "approval_required": "APPROVAL_REQUIRED",
    "completed": "COMPLETED",
    "blocked": "BLOCKED",
    "error": "ERROR",
}


def validate(project, cancel=None, commands=None):
    evidence = []
    for command in project.config.validation_commands if commands is None else commands:
        argv = [sys.executable if arg == "{python}" else arg for arg in command]
        try:
            code = run_checked(
                argv,
                cwd=project.repo,
                env=clean_env(),
                timeout=project.config.timeout_seconds,
                cancel=cancel,
            )
        except (OSError, TimeoutError):
            return False, evidence + ["Configured validation could not finish"]
        evidence.append(f"Validation {len(evidence) + 1}: exit {code}")
        if code != 0:
            return False, evidence
    return True, evidence


def repo_digest(project):
    return hashlib.sha256(
        json.dumps(manifest(project.repo), sort_keys=True).encode()
    ).hexdigest()


class Orchestrator:
    def __init__(self, store, runner):
        self.store, self.runner = store, runner

    def run(self, project, task_id, message, *, snapshot=None, cancel=None):
        cancel = cancel or Control(self.store).token(project.id)
        cancel.check()
        task = self.store.task(project.id, task_id)
        if task["status"] in {"WORKING", "VALIDATING", "REVIEWING"}:
            raise ValueError("Task needs recovery before continuing")
        writing = task["mode"] == "write"
        if writing and not project.config.policy.local_write:
            raise ValueError("Local writes are disabled")
        if writing and not project.config.validation_commands:
            raise ValueError(
                "Configure validation_commands before starting a write task"
            )
        prompt = (
            load_context(project, task, snapshot=snapshot)
            + "\n\nCurrent user request:\n"
            + message
        )
        before = manifest(project.repo)
        self.store.update(project.id, task_id, "WORKING")

        def checkpoint(thread_id):
            self.store.update(project.id, task_id, "WORKING", thread_id=thread_id)

        try:
            output = self.runner.run(
                project,
                prompt,
                TurnResult.model_json_schema(),
                thread_id=task["thread_id"],
                read_only=not writing,
                on_thread=checkpoint,
                cancel=cancel,
            )
            result = TurnResult.model_validate(output)
            if result.artifacts:
                if not writing:
                    raise ValueError("Authoring files requires a write task")
                from .artifacts import materialize
                materialize(project, result.artifacts)
            if writing:
                from .artifacts import compile_definitions
                compile_definitions(project)
            after = manifest(project.repo)
            edits = changed(before, after)
            report = result.model_dump()
            report.pop("artifacts", None)
            report["changed_files"] = edits
            report["host_validation"] = []
            report["review"] = None
            status = STATES[result.status]
            if not writing and edits:
                status = "BLOCKED"
                report["message"] = (
                    "Read-only run changed repository files; investigate the sandbox before continuing."
                )
            elif writing and (edits or status == "COMPLETED"):
                self.store.update(project.id, task_id, "VALIDATING")
                passed, evidence = validate(project, cancel)
                report["host_validation"] = evidence
                if not passed:
                    status = "BLOCKED"
                    report["message"] = (
                        "Validation failed. Changes remain local for inspection."
                    )
                else:
                    # Capture after tests because tests may generate files.
                    review_before = manifest(project.repo)
                    self.store.update(project.id, task_id, "REVIEWING")

                    def review_checkpoint(thread_id):
                        self.store.update(
                            project.id,
                            task_id,
                            "REVIEWING",
                            reviewer_thread_id=thread_id,
                        )

                    review_prompt = (
                        "Perform an independent read-only engineering review. Inspect the repository, "
                        "the requirement and the changed files. Assess correctness, idempotency, "
                        "incremental loads, schema/data quality, security, cost, observability and tests. "
                        "Do not change files.\n"
                        + load_context(project, task, snapshot=snapshot)
                        + "\nCurrent request: "
                        + message
                        + "\nChanged files: "
                        + json.dumps(edits)
                        + "\nValidation: "
                        + json.dumps(evidence)
                        + "\nProposed cloud actions (not executed): "
                        + json.dumps(report.get("cloud_actions", []))
                    )
                    review = ReviewResult.model_validate(
                        self.runner.run(
                            project,
                            review_prompt,
                            ReviewResult.model_json_schema(),
                            read_only=True,
                            on_thread=review_checkpoint,
                            cancel=cancel,
                        )
                    )
                    report["review"] = review.model_dump()
                    if changed(review_before, manifest(project.repo)):
                        status = "BLOCKED"
                        report["message"] = (
                            "Reviewer changed files; review evidence is invalid."
                        )
                    elif review.verdict in {"REWORK", "BLOCK"}:
                        status = "BLOCKED"
                        report["message"] = (
                            "Review requires follow-up: " + review.summary
                        )
            cancel.check()
            report["cloud_eligible"] = writing and status == "COMPLETED"
            report["repo_digest"] = repo_digest(project)
            report["status"] = status.lower()
            report["task_id"] = task_id
            self.store.update(project.id, task_id, status, result=report)
            return report
        except TimeoutError as exc:
            record_failure(self.store, project.id, task_id, exc, "model", "PAUSED")
            raise
        except (KeyboardInterrupt, TaskStopped):
            self.store.update(project.id, task_id, "PAUSED")
            raise
        except Exception as exc:
            record_failure(self.store, project.id, task_id, exc, "model")
            raise
