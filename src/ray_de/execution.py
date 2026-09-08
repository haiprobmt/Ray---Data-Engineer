"""Per-invocation execution budgets shared by author, reviewer and read loops."""
import time


class ExecutionLimit(TimeoutError):
    pass


class Budget:
    def __init__(self, config, cancel):
        self.config, self.cancel = config, cancel
        self.started = time.monotonic()
        self.calls = 0

    def check(self):
        self.cancel.check()
        if time.monotonic() - self.started >= self.config.max_seconds:
            raise ExecutionLimit("Task time budget reached")

    def wait(self, seconds):
        end = time.monotonic() + seconds
        while time.monotonic() < end:
            self.check()
            time.sleep(min(0.25, max(0, end - time.monotonic())))
        self.check()

    def snapshot(self):
        return {"model_calls": self.calls, "elapsed_seconds": round(time.monotonic() - self.started, 2),
                "limits": self.config.model_dump()}


class BudgetRunner:
    def __init__(self, runner, budget, store, project_id, task_id):
        self.runner, self.budget = runner, budget
        self.store, self.project_id, self.task_id = store, project_id, task_id

    def run(self, *args, **kwargs):
        self.budget.check()
        if self.budget.calls >= self.budget.config.max_model_calls:
            raise ExecutionLimit("Task model-call budget reached")
        self.budget.calls += 1
        kwargs["cancel"] = self.budget
        from .task_context import merge, read
        from .state import now
        before = time.monotonic()
        try:
            result = self.runner.run(*args, **kwargs)
            return result
        finally:
            context = read(self.store, self.project_id, self.task_id)
            runs = context.get("model_runs", [])
            metadata = getattr(self.runner, "last_metadata", None)
            runs.append({"captured_at": now(), "duration_seconds": round(time.monotonic() - before, 2),
                         "role": "reviewer" if "verdict" in args[2].get("properties", {}) else "engineer",
                         "runtime": metadata if isinstance(metadata, dict) else {"source": "not_reported"}})
            merge(self.store, self.project_id, self.task_id, model_runs=runs[-32:], execution=self.budget.snapshot())
