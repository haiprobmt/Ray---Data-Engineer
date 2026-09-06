import yaml
import asyncio, threading
import pytest
from ray_de.demo import fixture
from ray_de.telegram import Gateway, GatewayConfig, chunks
from ray_de.control import TaskStopped
from ray_de.state import StateStore


class API:
    def __init__(self):
        self.calls = []
        self.fail = False

    async def call(self, method, payload):
        self.calls.append((method, payload))
        if self.fail:
            raise RuntimeError("do not echo bot token")
        return True


class Service:
    def __init__(self, store):
        self.store = store
        self.calls = []
        self.clarify = False
        self.block = False
        self.started = threading.Event()

    def run(self, p, id, text, actor, **kw):
        self.calls.append((p.id, id, text, actor))
        self.started.set()
        if self.block:
            while True:
                kw["cancel"].wait(0.03)
        report = {
            "status": "clarifying" if self.clarify else "completed",
            "message": "Fixture response",
            "question": "Which key?",
            "recommendation": "Business key",
            "options": ["Business key", "Surrogate key"],
            "host_validation": [],
        }
        self.store.update(
            p.id, id, "CLARIFYING" if self.clarify else "COMPLETED", result=report
        )
        self.clarify = False
        return report


def setup(tmp_path):
    p, s, t, f, c = fixture(tmp_path / "fixture")
    api = API()
    service = Service(s)
    config = GatewayConfig.model_validate(
        {
            "projects": {p.id: str(p.config_path)},
            "conversational": False,
            "access": [{"user_id": 123, "chat_id": 123, "projects": [p.id]}],
        }
    )
    gateway = Gateway(
        config, tmp_path / "gateway.yaml", s, s.path.parent, api, service=service
    )
    return gateway, api, service, p, s


def update(id, text, user=123, chat=123, type="private"):
    return {
        "update_id": id,
        "message": {
            "text": text,
            "from": {"id": user},
            "chat": {"id": chat, "type": type},
        },
    }


def test_allowlist_private_only_and_deduplication(tmp_path):
    async def run():
        g, a, svc, p, s = setup(tmp_path)
        for u in [
            update(1, "hello", user=999),
            update(2, "hello", chat=999),
            update(3, "hello", type="group"),
        ]:
            await g.handle(u)
        assert not a.calls and not svc.calls
        await g.handle(update(4, "hello"))
        await asyncio.gather(*g.jobs.values())
        await g.handle(update(4, "hello"))
        assert len(svc.calls) == 1
        with s.connect() as db:
            assert db.execute("SELECT count(*) FROM inbox").fetchone()[0] == 1

    asyncio.run(run())


def test_callback_survives_restart_and_cannot_replay(tmp_path):
    async def run():
        g, a, svc, p, s = setup(tmp_path)
        svc.clarify = True
        await g.handle(update(1, "clarify"))
        await asyncio.gather(*g.jobs.values())
        row = g.control.pending("telegram:123:123", p.id)[0]
        reopened = Gateway(
            g.config,
            tmp_path / "gateway.yaml",
            StateStore(s.path),
            s.path.parent,
            a,
            service=svc,
        )
        cb = {
            "update_id": 2,
            "callback_query": {
                "id": "cb",
                "from": {"id": 123},
                "message": {"chat": {"id": 123, "type": "private"}},
                "data": row["id"] + ":0",
            },
        }
        await reopened.handle(cb)
        await asyncio.gather(*reopened.jobs.values())
        assert len(svc.calls) == 2 and svc.calls[-1][2] == "Decision: Business key"
        cb["update_id"] = 3
        await reopened.handle(cb)
        assert len(svc.calls) == 2

    asyncio.run(run())


def test_stop_is_processed_while_task_runs(tmp_path):
    async def run():
        g, a, svc, p, s = setup(tmp_path)
        svc.block = True
        await g.handle(update(1, "long task"))
        for _ in range(100):
            if svc.started.is_set():
                break
            await asyncio.sleep(0.01)
        assert svc.started.is_set()
        await g.handle(update(2, "/stop"))
        await asyncio.wait_for(asyncio.gather(*g.jobs.values()), 3)
        session = g.control.session("telegram:123:123")
        assert s.task(p.id, session["task_id"])["status"] == "PAUSED"
        with pytest.raises(TaskStopped):
            g.control.token(p.id)

    asyncio.run(run())


def test_unauthorized_project_and_task_resume(tmp_path):
    async def run():
        g, a, svc, p, s = setup(tmp_path)
        private = s.create(p.id, "CLI private task", "read")
        await g.handle(update(1, "/project hidden"))
        await g.handle(update(2, "/resume " + private["id"]))
        assert (
            not svc.calls and g.control.session("telegram:123:123")["task_id"] is None
        )

    asyncio.run(run())


def test_message_chunking_counts_utf16():
    result = list(chunks("x" * 3499 + "😀" * 3000))
    assert "".join(result) == "x" * 3499 + "😀" * 3000
    assert all(len(piece.encode("utf-16-le")) // 2 <= 3500 for piece in result)


def test_uncertain_send_is_durable_without_retry(tmp_path):
    async def run():
        g, a, svc, p, s = setup(tmp_path)
        a.fail = True
        await g.send("telegram:123:123", "hello")
        with s.connect() as db:
            row = db.execute("SELECT * FROM outbox").fetchone()
        assert row["state"] == "UNCERTAIN"
        await g.deliver(row["id"])
        assert len(a.calls) == 1

    asyncio.run(run())


def test_status_and_actions_never_launch_model(tmp_path):
    async def run():
        g, a, svc, p, s = setup(tmp_path)
        await g.handle(update(1, "/status"))
        await g.handle(update(2, "/actions"))
        assert not svc.calls

    asyncio.run(run())


def test_forwarded_requests_ignored(tmp_path):
    async def run():
        g, a, svc, p, s = setup(tmp_path)
        u = update(1, "change data")
        u["message"]["forward_origin"] = {"type": "user"}
        await g.handle(u)
        assert not a.calls

    asyncio.run(run())


def test_switch_projects_does_not_reuse_task_or_context(tmp_path):
    from ray_de.context import load_context
    from ray_de.memory import save_decision

    async def run():
        g, a, svc, p, s = setup(tmp_path)
        other = tmp_path / "other"
        (other / "repo").mkdir(parents=True)
        cfg = p.config.model_dump()
        cfg.update(project_id="other", repo_path="repo")
        (other / "config.yaml").write_text(yaml.safe_dump(cfg))
        (other / "CONTEXT.md").write_text("PRIVATE_OTHER_CONTEXT")
        save_decision(
            p,
            "Private A memory",
            "Private A context",
            "A-only choice",
            "A consequence",
            "a",
        )
        config = GatewayConfig.model_validate(
            {
                "conversational": False,
                "projects": {
                    p.id: str(p.config_path),
                    "other": str(other / "config.yaml"),
                },
                "access": [
                    {"user_id": 123, "chat_id": 123, "projects": [p.id, "other"]}
                ],
            }
        )
        g = Gateway(config, tmp_path / "gateway.yaml", s, s.path.parent, a, service=svc)
        await g.handle(update(1, "first task"))
        await asyncio.gather(*g.jobs.values())
        first = svc.calls[-1][1]
        await g.handle(update(2, "/project other"))
        await g.handle(update(3, "second task"))
        await asyncio.gather(*g.jobs.values())
        assert svc.calls[-1][0] == "other" and svc.calls[-1][1] != first
        task = s.task("other", svc.calls[-1][1])
        context = load_context(g.projects["other"], task)
        assert "PRIVATE_OTHER_CONTEXT" in context and "A-only choice" not in context
        await g.handle(update(4, "/resume " + first))
        assert svc.calls[-1][0] == "other"

    asyncio.run(run())


def test_gateway_rejects_overlapping_projects(tmp_path):
    g, a, svc, p, s = setup(tmp_path)
    cfg = p.config.model_dump()
    cfg["project_id"] = "overlap"
    alternate = p.directory / "alternate.yaml"
    alternate.write_text(yaml.safe_dump(cfg))
    config = GatewayConfig.model_validate(
        {
            "projects": {p.id: str(p.config_path), "overlap": str(alternate)},
            "access": [],
        }
    )
    with pytest.raises(ValueError, match="overlap"):
        Gateway(config, tmp_path / "gateway.yaml", s, s.path.parent, a, service=svc)
