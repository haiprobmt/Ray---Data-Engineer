import asyncio

import pytest

from test_telegram import setup, update


@pytest.mark.parametrize("confirmation", ["Ok sure go ahead", "yes please", "Go ahead!"])
def test_text_confirmation_starts_the_selected_request(tmp_path, confirmation):
    async def run():
        gateway, api, service, project, store = setup(tmp_path)
        gateway.config = gateway.config.model_copy(update={"conversational": True})
        actor = "telegram:123:123"
        task = store.create(project.id, "Apply the agreed changes to the existing pipeline", "read")
        with store.connect() as db:
            db.execute("INSERT INTO channel_tasks VALUES (?,?,?)", (actor, project.id, task["id"]))
        gateway.control.set_session(actor, project.id, task["id"], "read")
        decision = gateway.control.decide(actor, project.id, task["id"], "work_handoff", {"options": ["Work on this", "Just chatting"]})
        gateway.launch_conversation = lambda *a, **kw: pytest.fail("A confirmation must not create a vague new task")
        await gateway.handle(update(1, confirmation))
        await asyncio.gather(*gateway.jobs.values())
        assert service.calls[0][1:3] == (task["id"], task["objective"])
        assert len(store.list_tasks(project.id)) == 2  # Includes the setup fixture's task.
        assert not gateway.control.pending(actor, project.id)
        assert not any(method == "answerCallbackQuery" for method, _ in api.calls)
        # The same chat update cannot start the task twice.
        await gateway.handle(update(1, confirmation))
        assert len(service.calls) == 1
    asyncio.run(run())


@pytest.mark.parametrize("case", ["approval", "expired", "different_task", "different_actor", "additional_instruction"])
def test_text_confirmation_never_consumes_an_unrelated_decision(tmp_path, case):
    async def run():
        gateway, api, service, project, store = setup(tmp_path)
        gateway.config = gateway.config.model_copy(update={"conversational": True})
        actor = "telegram:123:123"
        task = store.create(project.id, "Current task", "read")
        other = store.create(project.id, "Other task", "read")
        with store.connect() as db:
            db.execute("INSERT INTO channel_tasks VALUES (?,?,?)", (actor, project.id, task["id"]))
        gateway.control.set_session(actor, project.id, task["id"], "read")
        gateway.control.decide("another-actor" if case == "different_actor" else actor, project.id,
            other["id"] if case == "different_task" else task["id"],
            "approval:exact-action" if case == "approval" else "work_handoff",
            {"options": ["Approve", "Reject"]}, ttl=-1 if case == "expired" else 300)
        conversations = []
        gateway.launch_conversation = lambda *a, **kw: conversations.append(a[-1])
        text = "Go ahead and delete everything" if case == "additional_instruction" else "Go ahead"
        await gateway.handle(update(1, text))
        assert conversations == [text] and not service.calls
        with store.connect() as db:
            assert db.execute("SELECT status FROM decisions").fetchone()[0] == "PENDING"
    asyncio.run(run())
