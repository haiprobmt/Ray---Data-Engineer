import json
import pytest
from ray_de.runtime import clean_env, resolve_binary
from ray_de.fabric import fabric_env
from ray_de.cli import doctor
from ray_de.skills import catalogue


def test_credentials_not_inherited(monkeypatch):
    monkeypatch.setenv("AZURE_CLIENT_SECRET", "secret")
    monkeypatch.setenv("RAY_TELEGRAM_BOT_TOKEN", "secret")
    monkeypatch.setenv("FAB_API_ENDPOINT_FABRIC", "evil.example")
    monkeypatch.setenv("OPENAI_API_KEY", "secret")
    env = clean_env()
    assert not any(
        k in env
        for k in [
            "AZURE_CLIENT_SECRET",
            "RAY_TELEGRAM_BOT_TOKEN",
            "FAB_API_ENDPOINT_FABRIC",
            "OPENAI_API_KEY",
        ]
    )


def test_fabric_profiles_are_separate(tmp_path):
    a = fabric_env(tmp_path, "a")
    b = fabric_env(tmp_path, "b")
    assert a["USERPROFILE"] != b["USERPROFILE"]
    assert a["HOME"] == a["USERPROFILE"]


def test_explicit_missing_binary_does_not_fall_back(monkeypatch, tmp_path):
    monkeypatch.setenv("SENIOR_DE_CODEX_BIN", str(tmp_path / "missing.exe"))
    with pytest.raises(FileNotFoundError):
        resolve_binary("codex")


def test_vendored_skills_match_lock():
    import hashlib
    import ray_de.skills

    lock = catalogue()["source"]
    assert len(lock["revision"]) == 40
    for rel, expected in lock["files"].items():
        assert (
            hashlib.sha256((ray_de.skills.ROOT / rel).read_bytes()).hexdigest()
            == expected
        )


def test_doctor_does_not_treat_fab_exit_zero_as_login(project, tmp_path, monkeypatch):
    monkeypatch.setattr("ray_de.cli.resolve_binary", lambda k: k)

    def probe(binary, args, **kw):
        if args == ["--version"]:
            return {"ok": True, "output": "version"}
        return (
            {"ok": True, "output": '{"logged_in":false,"token":"never-display"}'}
            if binary == "fab"
            else {"ok": False}
        )

    monkeypatch.setattr("ray_de.cli.probe", probe)
    report = doctor(project, tmp_path)
    assert report["checks"]["fab"]["authenticated"] is False
    assert "never-display" not in json.dumps(report)


@pytest.mark.parametrize("conversation", [False, True])
def test_worker_checkpoints_only_after_turn_is_accepted(tmp_path, monkeypatch, conversation):
    from types import SimpleNamespace
    import openai_codex
    from ray_de.sdk_worker import run

    checkpoint = tmp_path / "checkpoint.json"
    result_path = tmp_path / "result.json"
    request = tmp_path / "request.json"
    request.write_text(
        json.dumps(
            {
                "binary": "codex.exe",
                "repo": str(tmp_path),
                "read_only": True,
                "thread_id": None,
                "model": None,
                "prompt": "Inspect",
                "schema": {},
                "conversation": conversation,
            }
        )
    )
    seen = []

    class Turn:
        def run(self):
            assert json.loads(checkpoint.read_text())["thread_id"] == "accepted-id"
            return SimpleNamespace(status="completed", final_response='{"ok":true}')

    class Thread:
        id = "accepted-id"

        def turn(self, *args, **kwargs):
            assert not checkpoint.exists()
            seen.append(kwargs)
            return Turn()

    class Codex:
        def __init__(self, config):
            seen.append(config)

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def thread_start(self, **kwargs):
            assert not checkpoint.exists()
            seen.append(kwargs)
            return Thread()

    monkeypatch.setattr(openai_codex, "Codex", Codex)
    run(request, result_path, checkpoint)
    assert json.loads(result_path.read_text()) == {"ok": True}
    assert seen[1]["approval_mode"] == openai_codex.ApprovalMode.deny_all
    assert seen[1]["sandbox"] == openai_codex.Sandbox.read_only
    assert "sandbox_workspace_write.network_access=false" in seen[0].config_overrides
    if conversation:
        assert seen[1]["ephemeral"] is True
        assert "features.shell_tool=false" in seen[0].config_overrides
        assert "tools.view_image=false" in seen[0].config_overrides
        assert "project_doc_max_bytes=0" in seen[0].config_overrides
        assert "friendly AI companion" in seen[1]["developer_instructions"]
    else:
        assert "ephemeral" not in seen[1]


def test_unaccepted_first_turn_does_not_checkpoint_an_unresumable_id(
    tmp_path, monkeypatch
):
    import openai_codex
    from ray_de.sdk_worker import run

    checkpoint = tmp_path / "checkpoint.json"
    request = tmp_path / "request.json"
    request.write_text(
        json.dumps(
            {
                "binary": "codex.exe",
                "repo": str(tmp_path),
                "read_only": True,
                "thread_id": None,
                "model": None,
                "prompt": "Inspect",
                "schema": {},
            }
        )
    )

    class Thread:
        id = "unusable-id"

        def turn(self, *args, **kwargs):
            raise RuntimeError("turn rejected")

    class Codex:
        def __init__(self, *a):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *a):
            pass

        def thread_start(self, **kw):
            return Thread()

    monkeypatch.setattr(openai_codex, "Codex", Codex)
    with pytest.raises(RuntimeError):
        run(request, tmp_path / "result.json", checkpoint)
    assert not checkpoint.exists()
