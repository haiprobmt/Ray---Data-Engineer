import asyncio
import json

import pytest

from ray_de.conversation import ConversationService, ConversationResult
from ray_de.telegram import Gateway, GatewayConfig
from test_telegram import setup, update


ACTOR = "telegram:123:123"


class Runner:
    def __init__(self):
        self.calls = []
    def run(self, project, prompt, schema, **kwargs):
        self.calls.append((project.id, json.loads(prompt), kwargs))
        return {"message": "Hey, we can just talk. How was your day?", "offer_work": False}


def test_chat_has_continuity_without_tasks_fabric_or_project_files(tmp_path):
    g, api, service, p, store = setup(tmp_path)
    runner = Runner()
    chat = ConversationService(store, runner)
    chat.reply(p, p, ACTOR, "I am learning guitar")
    reopened = ConversationService(store, runner)
    reopened.reply(p, p, ACTOR, "Remember what I am learning?")
    _, prompt, options = runner.calls[-1]
    assert prompt["recent_conversation"][0]["text"] == "I am learning guitar"
    assert options["conversation"] and options["read_only"]
    assert "thread_id" not in options
    reopened.reply(p, p, "telegram:456:456", "hello")
    assert runner.calls[-1][1]["recent_conversation"] == []
    assert not service.calls
    chat.clear(ACTOR)
    chat.reply(p, p, ACTOR, "start fresh")
    assert runner.calls[-1][1]["recent_conversation"] == []


def test_chat_history_is_bounded_and_rejects_credentials(tmp_path):
    g, api, service, p, store = setup(tmp_path)
    chat = ConversationService(store, Runner())
    for i in range(22):
        chat.reply(p, p, ACTOR, f"message {i}")
    with store.connect() as db:
        assert db.execute("SELECT count(*) FROM conversations").fetchone()[0] == 40
    with pytest.raises(ValueError, match="secret"):
        chat.reply(p, p, ACTOR, "client_secret=private-secret")


class Chat:
    def __init__(self):
        self.offer = False
        self.calls = []
    def begin(self, actor):
        return None
    def reply(self, auth_project, project, actor, text, **kwargs):
        self.calls.append(text)
        return ConversationResult(message="We can talk about that.", offer_work=self.offer)
    def stop(self, actor):
        pass
    def clear(self, actor):
        pass


def chat_gateway(tmp_path):
    g, api, service, p, store = setup(tmp_path)
    cfg = g.config.model_dump()
    cfg["conversational"] = True
    chat = Chat()
    gateway = Gateway(GatewayConfig.model_validate(cfg), tmp_path / "gateway.yaml", store, store.path.parent, api, service=service, conversation=chat)
    return gateway, api, service, chat, p, store


def test_plain_chat_does_not_create_engineering_task_or_status_wrappers(tmp_path):
    async def run():
        g, api, service, chat, p, store = chat_gateway(tmp_path)
        before = len(store.list_tasks(p.id))
        await g.handle(update(1, "I just want a friend to talk to"))
        await asyncio.gather(*g.jobs.values())
        assert chat.calls == ["I just want a friend to talk to"]
        assert not service.calls and len(store.list_tasks(p.id)) == before
        assert len(api.calls) == 1
        assert api.calls[0][1]["text"] == "We can talk about that."
    asyncio.run(run())


def test_model_can_only_offer_work_and_button_uses_original_user_request(tmp_path):
    async def run():
        g, api, service, chat, p, store = chat_gateway(tmp_path)
        chat.offer = True
        await g.handle(update(1, "Inspect my pipeline"))
        await asyncio.gather(*g.jobs.values())
        assert not service.calls
        session = g.control.session(ACTOR)
        decision = next(r for r in g.control.pending(ACTOR, p.id) if r["kind"] == "work_handoff")
        callback = {"update_id": 2, "callback_query": {
            "id": "cb", "from": {"id": 123}, "message": {"chat": {"id": 123, "type": "private"}},
            "data": decision["id"] + ":0",
        }}
        reopened = Gateway(g.config, tmp_path / "gateway.yaml", store, store.path.parent, api, service=service, conversation=chat)
        await reopened.handle(callback)
        await asyncio.gather(*reopened.jobs.values())
        assert len(service.calls) == 1
        assert service.calls[0][2] == "Inspect my pipeline"
        callback["update_id"] = 3
        await reopened.handle(callback)
        assert len(service.calls) == 1
        assert service.calls[0][1] == session["task_id"]
    asyncio.run(run())


def test_work_command_is_explicit_and_bypasses_conversation(tmp_path):
    async def run():
        g, api, service, chat, p, store = chat_gateway(tmp_path)
        await g.handle(update(1, "/work Inspect my pipeline"))
        await asyncio.gather(*g.jobs.values())
        assert service.calls[0][2] == "Inspect my pipeline"
        assert not chat.calls
    asyncio.run(run())


def test_stop_before_chat_worker_starts_cannot_be_undone(tmp_path):
    from ray_de.control import TaskStopped
    g, api, service, p, store = setup(tmp_path)
    runner = Runner()
    chat = ConversationService(store, runner)
    token = chat.begin(ACTOR)
    chat.stop(ACTOR)
    with pytest.raises(TaskStopped):
        chat.reply(p, p, ACTOR, "hello", cancel=token)
    assert not runner.calls
    # A later direct message can chat again without resuming engineering work.
    chat.reply(p, p, ACTOR, "hello again")
    assert len(runner.calls) == 1
