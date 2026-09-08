import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from ray_de.codex_client import CodexRunner


def timed_worker(tmp_path, monkeypatch, *, progress=True, finish_at=12):
    """Drive the real parent wait loop with a clock and a controllable SDK worker."""
    clock = SimpleNamespace(value=0)
    monkeypatch.setattr("time.monotonic", lambda: clock.value)
    monkeypatch.setattr("time.sleep", lambda duration: setattr(clock, "value", clock.value + 1))
    monkeypatch.setattr("ray_de.codex_client.resolve_binary", lambda kind: "codex.exe")
    monkeypatch.setattr("ray_de.codex_client.subprocess.run", lambda *a, **kw: None)
    processes = []
    class Process:
        pid = 12345
        returncode = None
        def __init__(self, args, **kwargs):
            self.result = Path(args[4])
            self.checkpoint = Path(args[5])
            self.checkpoint.write_text(json.dumps({"thread_id": "saved-thread"}))
            self.progress = self.checkpoint.with_name("progress.json")
            processes.append(self)
        def poll(self):
            if progress and clock.value % 4 == 0:
                self.progress.write_text(json.dumps({"sequence": int(clock.value) + 1, "activity": "working"}))
            if clock.value >= finish_at:
                self.result.write_text('{"ok":true}')
                self.returncode = 0
            return self.returncode
        def wait(self, **kwargs):
            self.returncode = -1
            return -1
        def kill(self):
            self.returncode = -1
    monkeypatch.setattr("ray_de.codex_client.subprocess.Popen", Process)
    project = SimpleNamespace(id="timeout-demo", repo=tmp_path, config=SimpleNamespace(timeout_seconds=10, model=None))
    return project, clock, processes


def test_active_engineering_turn_is_not_killed_at_the_old_wall_clock_limit(tmp_path, monkeypatch):
    project, clock, processes = timed_worker(tmp_path, monkeypatch)
    checkpoints = []
    result = CodexRunner(tmp_path / "state").run(project, "Finish the draft", {}, read_only=False, on_thread=checkpoints.append)
    assert result == {"ok": True}
    assert clock.value >= 12 and checkpoints == ["saved-thread"]
    assert processes[0].returncode == 0


def test_silent_worker_still_times_out_and_preserves_thread_checkpoint(tmp_path, monkeypatch):
    project, clock, processes = timed_worker(tmp_path, monkeypatch, progress=False, finish_at=100)
    checkpoints = []
    with pytest.raises(TimeoutError):
        CodexRunner(tmp_path / "state").run(project, "Finish the draft", {}, on_thread=checkpoints.append)
    assert clock.value == 10 and checkpoints == ["saved-thread"]
    assert processes[0].returncode == -1


def test_active_worker_still_has_a_hard_limit(tmp_path, monkeypatch):
    from ray_de.model_progress import ModelTimeout
    project, clock, _ = timed_worker(tmp_path, monkeypatch, finish_at=100)
    with pytest.raises(ModelTimeout) as error:
        CodexRunner(tmp_path / "state").run(project, "Keep working", {})
    assert clock.value == 30 and error.value.code == "MODEL_TIME_LIMIT"
    assert error.value.details["total_limit_seconds"] == 30


def test_conversation_does_not_inherit_extended_engineering_budget(tmp_path, monkeypatch):
    project, clock, _ = timed_worker(tmp_path, monkeypatch)
    with pytest.raises(TimeoutError):
        CodexRunner(tmp_path / "state").run(project, "Hello", {}, conversation=True)
    assert clock.value == 10


@pytest.mark.parametrize("value", [None, {}, {"sequence": True, "activity": "working"},
    {"sequence": 2, "activity": "private command"}, {"sequence": 2, "activity": "working", "text": "private"}])
def test_invalid_activity_cannot_extend_deadline(value):
    from ray_de.model_progress import TurnDeadline, ModelTimeout
    watch = TurnDeadline(10, engineering=True, now=0)
    assert not watch.observe(value, 9)
    with pytest.raises(ModelTimeout) as error:
        watch.check(10)
    assert error.value.code == "MODEL_IDLE_TIMEOUT"


def test_repeated_checkpoint_and_unrelated_sdk_events_cannot_fake_activity(tmp_path):
    from ray_de.model_progress import TurnDeadline, ModelTimeout, ProgressWriter
    watch = TurnDeadline(10, engineering=True, now=0)
    assert watch.observe({"sequence": 1, "activity": "working"}, 1)
    assert not watch.observe({"sequence": 1, "activity": "working"}, 9)
    with pytest.raises(ModelTimeout):
        watch.check(11)
    path = tmp_path / "progress.json"
    ProgressWriter(path).observe(SimpleNamespace(method="account/rateLimits/updated", payload=None))
    assert not path.exists()


def test_cancel_interrupts_active_work_before_the_extended_deadline(tmp_path, monkeypatch):
    from ray_de.control import TaskStopped
    project, clock, processes = timed_worker(tmp_path, monkeypatch)
    class Cancel:
        def check(self):
            if clock.value >= 5:
                raise TaskStopped()
    with pytest.raises(TaskStopped):
        CodexRunner(tmp_path / "state").run(project, "Work", {}, cancel=Cancel())
    assert clock.value == 5 and processes[0].returncode == -1


def test_review_timeout_keeps_its_stage_and_pending_draft_in_the_host_service(project, store, tmp_path):
    from ray_de.model_progress import ModelTimeout
    from ray_de.service import TaskService
    from test_recovery import output
    task = store.create(project.id, "Finish the local draft", "write")
    class Runner:
        def run(self, p, prompt, schema, **kwargs):
            if "verdict" in schema["properties"]:
                raise ModelTimeout("idle", {"activity": "working", "elapsed_seconds": 600, "idle_seconds": 600,
                                           "idle_limit_seconds": 600, "total_limit_seconds": 1800})
            kwargs["on_progress"]({"activity": "writing_files", "elapsed_seconds": 2, "idle_seconds": 0,
                                    "idle_limit_seconds": 600, "total_limit_seconds": 1800})
            current = json.loads(store.task(project.id, task["id"])["result"])
            assert current["model_progress"]["activity"] == "writing_files"
            (p.repo / "main.py").write_text("answer = 3\n")
            return output()
    with pytest.raises(ModelTimeout):
        TaskService(store, Runner(), tmp_path / "state").run(project, task["id"], "Finish", "actor",
            snapshot={"project_id": project.id, "binding": project.binding})
    saved = store.task(project.id, task["id"])
    report = json.loads(saved["result"])
    assert saved["status"] == "PAUSED" and report["error"]["stage"] == "review"
    assert report["error"]["code"] == "MODEL_IDLE_TIMEOUT" and not report["cloud_eligible"]
    assert "main.py" in report["review_paths"]


def test_timeout_message_explains_the_actual_step_and_keeps_timing_technical():
    from ray_de.errors import describe_error, error_text
    from ray_de.model_progress import ModelTimeout
    from ray_de.telegram_format import friendly_error
    error = describe_error(ModelTimeout("idle", {"activity": "working", "elapsed_seconds": 700,
        "idle_seconds": 600, "idle_limit_seconds": 600, "total_limit_seconds": 1800}), "model")
    assert "draft files are saved" in friendly_error(error)
    assert "700s elapsed" in error_text(error)
    assert "700s" not in friendly_error(error)
    old_error = {"code": "TIMED_OUT", "stage": "model"}
    assert "preparing the code" in friendly_error(old_error)


@pytest.mark.parametrize("code", ["TIMED_OUT", "MODEL_IDLE_TIMEOUT", "MODEL_TIME_LIMIT"])
def test_chat_timeout_never_tells_the_user_to_resume_an_engineering_task(code):
    from ray_de.telegram_format import friendly_error
    message = friendly_error({"code": code, "stage": "conversation"})
    assert "Send your message again" in message
    assert "/resume" not in message and "code" not in message and "draft" not in message
