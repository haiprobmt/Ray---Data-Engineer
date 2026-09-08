"""Content-free SDK activity and bounded engineering turn deadlines."""
import json
import time


ACTIVITIES = {"working", "running_command", "writing_files", "preparing_reply"}


class ModelTimeout(TimeoutError):
    def __init__(self, reason, details):
        self.code = "MODEL_IDLE_TIMEOUT" if reason == "idle" else "MODEL_TIME_LIMIT"
        self.details = details
        super().__init__("Model activity stopped" if reason == "idle" else "Model turn reached its maximum duration")


class TurnDeadline:
    def __init__(self, seconds, *, engineering, now):
        self.started = self.last_activity = now
        self.idle_limit = seconds
        self.total_limit = min(seconds * 3, 7200) if engineering else seconds
        self.sequence = 0
        self.activity = "working"

    def observe(self, value, now):
        if (isinstance(value, dict) and set(value) == {"sequence", "activity"}
                and type(value["sequence"]) is int and self.sequence < value["sequence"] < 1_000_000_000
                and isinstance(value["activity"], str) and value["activity"] in ACTIVITIES):
            self.sequence = value["sequence"]
            self.activity = value["activity"]
            self.last_activity = now
            return True
        return False

    def snapshot(self, now):
        return {"activity": self.activity, "elapsed_seconds": max(0, int(now - self.started)),
                "idle_seconds": max(0, int(now - self.last_activity)),
                "idle_limit_seconds": self.idle_limit, "total_limit_seconds": self.total_limit}

    def check(self, now):
        if now - self.last_activity >= self.idle_limit:
            raise ModelTimeout("idle", self.snapshot(now))
        if now - self.started >= self.total_limit:
            raise ModelTimeout("total", self.snapshot(now))


def activity_for(event):
    """Use SDK event categories only. Never copy reasoning, commands or raw output."""
    method = event.method
    if method.startswith("item/reasoning/") or method in {"turn/plan/updated", "item/plan/delta"}:
        return "working"
    if method == "item/agentMessage/delta":
        return "preparing_reply"
    if method.startswith("item/commandExecution/"):
        return "running_command"
    if method.startswith("item/fileChange/"):
        return "writing_files"
    if method in {"item/started", "item/completed"}:
        item = getattr(event.payload, "item", None)
        item = getattr(item, "root", item)
        kind = getattr(item, "type", "")
        return {"commandExecution": "running_command", "fileChange": "writing_files", "agentMessage": "preparing_reply"}.get(kind, "working")
    return None


class ProgressWriter:
    def __init__(self, path):
        self.path = path
        self.sequence = 0
        self.last_write = float("-inf")
        self.activity = None

    def observe(self, event):
        activity = activity_for(event)
        now = time.monotonic()
        if activity is None or (now - self.last_write < 1 and activity == self.activity):
            return
        self.sequence += 1
        self.activity, self.last_write = activity, now
        temporary = self.path.with_suffix(".tmp")
        try:
            temporary.write_text(json.dumps({"sequence": self.sequence, "activity": activity}), encoding="utf-8")
            temporary.replace(self.path)
        except OSError:
            # Optional telemetry must not break a turn when Windows briefly locks a file.
            # A later SDK event retries; no synthetic heartbeat is emitted.
            pass
