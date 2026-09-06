import json, os, threading, time, zipfile
import pytest
from ray_de.control import Control, TaskStopped
from ray_de.memory import save_decision, recall, backup, verify_backup
from ray_de.demo import fixture
from ray_de.state import StateStore
from ray_de.processes import run_checked
from ray_de.evaluation import record_rating, summarize, export
from ray_de.monitor import check_once


def test_stop_generation_survives_restart_and_resume(project, store):
    control = Control(store)
    token = control.token(project.id)
    control.stop(project.id)
    reopened = Control(StateStore(store.path))
    with pytest.raises(TaskStopped):
        reopened.token(project.id)
    reopened.resume(project.id)
    with pytest.raises(TaskStopped):
        token.check()
    reopened.token(project.id).check()


@pytest.mark.parametrize(
    "fault", ["actor", "project", "expired", "replay", "superseded", "choice"]
)
def test_decisions_are_scoped_one_shot(project, store, fault):
    c = Control(store)
    t = store.create(project.id, "choose", "read")
    id = c.decide(
        "a",
        project.id,
        t["id"],
        "clarification",
        {"options": ["yes", "no"]},
        ttl=-1 if fault == "expired" else 3600,
    )
    if fault == "replay":
        c.answer(id, "a", project.id, 0)
    if fault == "superseded":
        c.decide("a", project.id, t["id"], "clarification", {"options": ["other"]})
    with pytest.raises(ValueError):
        c.answer(
            id,
            "other" if fault == "actor" else "a",
            "b" if fault == "project" else project.id,
            8 if fault == "choice" else 0,
        )


def test_memory_search_and_backup_exclude_profiles(tmp_path):
    p, s, t, f, c = fixture(tmp_path / "case")
    save_decision(
        p,
        "Incremental watermark",
        "Need a restart key",
        "Use event time",
        "Late rows require lookback",
        "local:test",
    )
    assert "event time" in recall(p, "watermark")[0]["text"]
    secret = s.path.parent / "secrets"
    secret.mkdir()
    (secret / "telegram.dpapi").write_bytes(b"NEVER_COPY")
    out = tmp_path / "backup.zip"
    backup(s, [p], out)
    assert verify_backup(out)["verified_files"] == 4
    with zipfile.ZipFile(out) as z:
        assert not any("secret" in n or "codex" in n for n in z.namelist())
    restored = tmp_path / "restored.db"
    with zipfile.ZipFile(out) as z:
        restored.write_bytes(z.read("ray.db"))
    assert StateStore(restored).task(p.id, t["id"])["status"] == "COMPLETED"


def test_memory_rejects_secret(project):
    with pytest.raises(ValueError):
        save_decision(project, "Secret", "password=abcdefgh", "x", "x", "a")


def test_backup_detects_modified_db(tmp_path):
    p, s, t, f, c = fixture(tmp_path / "case")
    out = tmp_path / "backup.zip"
    backup(s, [p], out)
    with zipfile.ZipFile(out) as z:
        entries = {n: z.read(n) for n in z.namelist()}
    entries["ray.db"] = b"corrupt"
    with zipfile.ZipFile(out, "w") as z:
        for n, b in entries.items():
            z.writestr(n, b)
    with pytest.raises(ValueError, match="integrity"):
        verify_backup(out)


def test_cancel_kills_validation_process(project, store, tmp_path):
    import sys

    c = Control(store)
    token = c.token(project.id)
    timer = threading.Timer(0.3, lambda: c.stop(project.id))
    timer.start()
    start = time.monotonic()
    with pytest.raises(TaskStopped):
        run_checked(
            [sys.executable, "-c", "import time; time.sleep(20)"],
            cwd=project.repo,
            env=os.environ.copy(),
            timeout=30,
            cancel=token,
        )
    timer.join()
    assert time.monotonic() - start < 5


@pytest.mark.skipif(os.name != "nt", reason="Windows DPAPI")
def test_dpapi_roundtrip_dummy_token(tmp_path):
    from ray_de.secrets import save_token, load_token

    token = "123456:" + ("x" * 30)
    save_token(tmp_path, token)
    assert token.encode() not in (tmp_path / "secrets" / "telegram.dpapi").read_bytes()
    assert load_token(tmp_path) == token


def test_ratings_and_escaped_evaluation(project, store, tmp_path):
    t = store.create(project.id, "task", "read")
    record_rating(store, project.id, t["id"], 4, "<script>alert(1)</script>")
    assert summarize(store, project.id)["average_usefulness"] == 4
    export(store, project.id, tmp_path / "report.html")
    assert "<script>" not in (tmp_path / "report.html").read_text()
    with pytest.raises(ValueError):
        record_rating(store, project.id, t["id"], 5)


def test_monitor_quiet_until_change_or_failure(project, store, tmp_path):
    class Fake:
        data = []
        fail = False

        def snapshot(self):
            if self.fail:
                raise RuntimeError("secret-token")
            return {"workspaces": self.data}

    fake = Fake()
    assert not check_once(project, store, tmp_path, gateway=fake)["notify"]
    assert not check_once(project, store, tmp_path, gateway=fake)["notify"]
    fake.data = [{"id": "new"}]
    assert check_once(project, store, tmp_path, gateway=fake)["notify"]
    fake.fail = True
    assert check_once(project, store, tmp_path, gateway=fake)["notify"]
    assert not check_once(project, store, tmp_path, gateway=fake)["notify"]
    assert "secret-token" not in json.dumps(
        Control(store).setting("monitor:" + project.id)
    )


def test_service_shutdown_preserves_decisions_but_interrupts_workers(project, store):
    c = Control(store)
    task = store.create(project.id, "Choose a key", "read")
    id = c.decide(
        "actor", project.id, task["id"], "clarification", {"options": ["A", "B"]}
    )
    token = c.token(project.id)
    c.interrupt_running(project.id)
    with pytest.raises(TaskStopped):
        token.check()
    reopened = Control(StateStore(store.path))
    reopened.token(project.id).check()
    assert reopened.answer(id, "actor", project.id, 0)[1] == "A"
