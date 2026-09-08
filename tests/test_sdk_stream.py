import json
from types import SimpleNamespace

import pytest

from openai_codex.models import Notification
from openai_codex.generated.v2_all import ItemCompletedNotification, TurnCompletedNotification
from ray_de.sdk_worker import consume_turn
from ray_de.errors import RayError


def message(text, phase="final_answer", turn_id="turn"):
    return Notification("item/completed", ItemCompletedNotification.model_validate({
        "threadId": "thread", "turnId": turn_id, "completedAtMs": 0,
        "item": {"id": "item", "type": "agentMessage", "phase": phase, "text": text}}))


def completed(status="completed", error=None):
    return Notification("turn/completed", TurnCompletedNotification.model_validate({
        "threadId": "thread", "turn": {"id": "turn", "items": [], "status": status, "error": error}}))


class StreamTurn:
    id = "turn"
    def __init__(self, events):
        self.events = events
        self.closed = False
    def stream(self):
        try:
            yield from self.events
        finally:
            self.closed = True


def test_stream_keeps_final_json_and_drops_private_payloads_from_progress(tmp_path):
    turn = StreamTurn([message("client_secret=never-store-in-progress", "commentary"), message('{"ok": true}'), completed()])
    path = tmp_path / "progress.json"
    assert consume_turn(turn, path) == {"ok": True}
    value = json.loads(path.read_text())
    assert set(value) == {"sequence", "activity"}
    assert "never-store" not in path.read_text() and turn.closed


def test_progress_file_lock_does_not_discard_a_successful_model_response(tmp_path, monkeypatch):
    from pathlib import Path
    def locked(*args, **kwargs):
        raise PermissionError("temporary Windows sharing conflict")
    monkeypatch.setattr(Path, "replace", locked)
    turn = StreamTurn([message('{"ok":true}'), completed()])
    assert consume_turn(turn, tmp_path / "progress.json") == {"ok": True}


@pytest.mark.parametrize("phase", [None, "final_answer"])
def test_stream_accepts_legacy_and_explicit_final_phases(tmp_path, phase):
    turn = StreamTurn([message('{"done":true}', phase), completed()])
    assert consume_turn(turn, tmp_path / "progress.json") == {"done": True}


@pytest.mark.parametrize("events,code", [
    ([message('{"done":true}', "commentary"), completed()], "MODEL_OUTPUT_INVALID"),
    ([message('{"done":true}', turn_id="other"), completed()], "MODEL_OUTPUT_INVALID"),
    ([message('{"done":true}')], "MODEL_UNAVAILABLE"),
    ([message('{"done":true}'), completed("failed", {"message": "rate limit 429 private-text", "additionalDetails": None, "codexErrorInfo": None})], "MODEL_LIMIT"),
    ([message('{"done":true}'), completed("interrupted")], "MODEL_UNAVAILABLE"),
])
def test_only_terminal_success_accepts_output(tmp_path, events, code):
    turn = StreamTurn(events)
    with pytest.raises(RayError) as error:
        consume_turn(turn, tmp_path / "progress.json")
    assert error.value.code == code and "private-text" not in str(error.value)
    assert turn.closed
