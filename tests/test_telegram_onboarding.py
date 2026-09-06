import asyncio
import json

import pytest

from ray_de.config import load_project
from ray_de.onboarding import workspace_input
from ray_de.telegram import Gateway, GatewayConfig
from ray_de.telegram_setup import pair, pairing_actor
from test_telegram import setup, update


WS = "11111111-1111-1111-1111-111111111111"
ACTOR = "telegram:123:123"


def enabled(tmp_path):
    g, api, service, p, store = setup(tmp_path)
    cfg = g.config.model_dump()
    cfg["access"][0]["allow_workspace_setup"] = True
    cfg["workspace_root"] = str(tmp_path / "enrolled")
    config = GatewayConfig.model_validate(cfg)
    gateway = Gateway(config, tmp_path / "gateway.yaml", store, store.path.parent, api, service=service)
    return gateway, api, service, p, store


def test_enrollment_persists_owner_policy_and_fresh_task(tmp_path):
    async def run():
        g, api, service, original, store = enabled(tmp_path)
        await g.handle(update(1, "first task"))
        await asyncio.gather(*g.jobs.values())
        old_session = g.control.session(ACTOR)
        await g.handle(update(2, f"/connect DEV https://app.fabric.microsoft.com/groups/{WS}/list?experience=fabric"))
        session = g.control.session(ACTOR)
        assert session["project_id"] != original.id
        assert session["task_id"] is None and session["mode"] == "read"
        project = g.projects[session["project_id"]]
        assert project.config.fabric.workspaces[0].id == WS
        assert not project.config.policy.local_write
        assert not project.config.policy.fabric_dev_write
        assert not project.config.fabric.write_targets
        assert not project.config.fabric.allow_definition_export
        assert load_project(original.config_path).binding == original.binding
        assert len(service.calls) == 1
        reopened = Gateway(g.config, tmp_path / "gateway.yaml", store, store.path.parent, api, service=service)
        assert reopened.session(ACTOR, g.config.access[0])["project_id"] == project.id
        assert reopened.projects[project.id].binding == project.binding
        await reopened.handle(update(3, "/resume " + old_session["task_id"]))
        assert len(service.calls) == 1
        await reopened.handle(update(4, f"/connect DEV {WS}"))
        assert reopened.registry.owned(ACTOR) == [project.id]
        assert reopened.registry.owned("telegram:456:456") == []
        await reopened.handle(update(5, f"/connect TEST {WS}"))
        assert reopened.control.session(ACTOR)["project_id"] != project.id
        assert reopened.projects[project.id].binding == project.binding
    asyncio.run(run())


def test_explicit_authoring_command_uses_new_binding_and_reconnects(tmp_path):
    async def run():
        g, api, service, original, store = enabled(tmp_path)
        await g.handle(update(1, f"/connect DEV {WS}"))
        old = g.projects[g.control.session(ACTOR)["project_id"]]
        old_binding = old.binding
        await g.handle(update(2, "/workspace enable-write"))
        session = g.control.session(ACTOR)
        author = g.projects[session["project_id"]]
        assert author.id != old.id and session["mode"] == "write"
        assert author.config.fabric.create_items and author.config.fabric.workspace_write
        assert author.config.policy.fabric_dev_write and author.config.validation_commands
        assert load_project(old.config_path).binding == old_binding
        assert not service.calls
        await g.handle(update(3, f"/connect DEV {WS}"))
        assert g.control.session(ACTOR)["project_id"] == author.id
        with pytest.raises(ValueError, match="owner"):
            g.registry.enable_authoring("telegram:456:456", old, store.path.parent)
        prod = g.registry.enroll(ACTOR, f"PROD {WS}")
        with pytest.raises(ValueError, match="DEV or TEST"):
            g.registry.enable_authoring(ACTOR, prod, store.path.parent)
    asyncio.run(run())


@pytest.mark.parametrize("value", [
    "DEV nope", f"DEV https://evil.example/groups/{WS}", f"DEV https://app.fabric.microsoft.com.evil/groups/{WS}",
    f"DEV https://app.fabric.microsoft.com@evil.example/groups/{WS}",
    f"DEV http://app.fabric.microsoft.com/groups/{WS}", f"DEV {WS} enable-writes",
    f"STAGE {WS}", f"DEV ../../{WS}",
])
def test_workspace_parser_rejects_untrusted_targets(value):
    with pytest.raises(ValueError):
        workspace_input(value)


def test_setup_requires_explicit_allowlist_and_ignores_forwarded_commands(tmp_path):
    async def run():
        g, api, service, p, store = enabled(tmp_path)
        for u in [update(1, f"/connect DEV {WS}", user=999), update(2, f"/connect DEV {WS}", type="group")]:
            await g.handle(u)
        forwarded = update(3, f"/connect DEV {WS}")
        forwarded["message"]["forward_origin"] = {"type": "user"}
        await g.handle(forwarded)
        assert not g.registry.owned(ACTOR) and not api.calls
        cfg = g.config.model_dump()
        cfg["access"][0]["allow_workspace_setup"] = False
        disabled = Gateway(GatewayConfig.model_validate(cfg), tmp_path / "gateway.yaml", store, store.path.parent, api, service=service)
        await disabled.handle(update(4, f"/connect DEV {WS}"))
        assert not g.registry.owned(ACTOR) and not service.calls
    asyncio.run(run())


def test_other_allowed_actor_cannot_select_enrolled_project(tmp_path):
    async def run():
        g, api, service, p, store = enabled(tmp_path)
        await g.handle(update(1, f"/connect DEV {WS}"))
        private = g.control.session(ACTOR)["project_id"]
        cfg = g.config.model_dump()
        cfg["access"].append({"user_id": 456, "chat_id": 456, "projects": [p.id], "allow_workspace_setup": True})
        other = Gateway(GatewayConfig.model_validate(cfg), tmp_path / "gateway.yaml", store, store.path.parent, api, service=service)
        await other.handle(update(2, "/project " + private, user=456, chat=456))
        assert other.control.session("telegram:456:456")["project_id"] == p.id
        assert not service.calls
    asyncio.run(run())


def test_credentials_never_enter_tasks_or_outbox(tmp_path):
    async def run():
        g, api, service, p, store = enabled(tmp_path)
        secret = "123456789:" + "x" * 35
        await g.handle(update(1, secret))
        await g.handle(update(2, "/connect DEV client_secret=" + secret))
        assert not service.calls and not g.registry.owned(ACTOR)
        with store.connect() as db:
            for table in ("tasks", "inbox", "outbox", "actions"):
                assert secret not in json.dumps([dict(r) for r in db.execute("SELECT * FROM " + table)])
        assert secret not in json.dumps(api.calls)
    asyncio.run(run())


def test_workspace_check_uses_host_gateway_and_no_model(tmp_path, monkeypatch):
    async def run():
        g, api, service, p, store = enabled(tmp_path)
        await g.handle(update(1, f"/connect DEV {WS}"))
        calls = []
        def snapshot(gateway):
            calls.append((gateway.project.config.fabric.workspaces[0].id, gateway.actor))
            return {"read_only": True}
        monkeypatch.setattr("ray_de.fabric.FabricGateway.snapshot", snapshot)
        await g.handle(update(2, "/workspace check"))
        await asyncio.gather(*g.jobs.values())
        assert calls == [(WS, ACTOR)] and not service.calls
        assert "check passed" in api.calls[-1][1]["text"]
        assert "does not test SQL authentication" in api.calls[-1][1]["text"]
        def fail(gateway):
            raise RuntimeError("SECRET_RAW_ERROR")
        monkeypatch.setattr("ray_de.fabric.FabricGateway.snapshot", fail)
        await g.handle(update(3, "/workspace check"))
        await asyncio.gather(*g.jobs.values())
        assert "check failed" in api.calls[-1][1]["text"]
        assert "SECRET_RAW_ERROR" not in json.dumps(api.calls)
    asyncio.run(run())


def test_sql_login_displays_explicit_local_command_without_starting_task(tmp_path):
    async def run():
        g, api, service, p, store = enabled(tmp_path)
        await g.handle(update(1, f"/connect DEV {WS}"))
        await g.handle(update(2, "/workspace login-sql"))
        text = api.calls[-1][1]["text"]
        assert "login sql" in text and "Complete Microsoft sign-in" in text
        assert not service.calls
    asyncio.run(run())


def test_root_cannot_overlap_state_or_existing_project(tmp_path):
    g, api, service, p, store = enabled(tmp_path)
    for root in (store.path.parent, p.repo, p.directory, tmp_path):
        cfg = g.config.model_dump()
        cfg["workspace_root"] = str(root)
        with pytest.raises(ValueError, match="separate"):
            Gateway(GatewayConfig.model_validate(cfg), tmp_path / "gateway.yaml", store, store.path.parent, api, service=service)


def test_pairing_requires_exact_private_nonce():
    code = "test-pairing-nonce"
    assert pairing_actor(update(1, "/pair " + code), code) == (123, 123)
    assert pairing_actor(update(1, "/pair wrong"), code) is None
    assert pairing_actor(update(1, "/pair " + code, type="group"), code) is None
    forwarded = update(1, "/pair " + code)
    forwarded["message"]["forward_origin"] = {"type": "user"}
    assert pairing_actor(forwarded, code) is None


def test_pairing_poll_returns_actor_and_consumed_offset():
    class API:
        async def call(self, method, payload):
            assert method == "getUpdates"
            return [update(10, "/pair wrong", user=999), update(11, "/pair nonce")]
    assert asyncio.run(pair(API(), "nonce")) == (123, 123, 12)


def test_secure_setup_writes_usable_config_without_credentials(tmp_path, monkeypatch):
    import sys
    import yaml
    from ray_de import telegram_setup
    from ray_de.control import Control
    from ray_de.state import StateStore

    token = "123456789:" + "x" * 35
    saved = []
    class SetupAPI:
        def __init__(self, value):
            assert value == token
        async def call(self, method, payload):
            if method == "getMe":
                return {"is_bot": True, "username": "ray_test_bot"}
            if method == "getWebhookInfo":
                return {"url": ""}
            assert method == "getUpdates"
            return [update(7, "/pair nonce")]
    monkeypatch.delenv("RAY_TELEGRAM_BOT_TOKEN", raising=False)
    monkeypatch.setattr(sys.stdin, "isatty", lambda: True)
    monkeypatch.setattr(telegram_setup.getpass, "getpass", lambda prompt: token)
    monkeypatch.setattr(telegram_setup.secrets, "token_urlsafe", lambda length: "nonce")
    monkeypatch.setattr(telegram_setup, "TelegramAPI", SetupAPI)
    monkeypatch.setattr(telegram_setup, "save_token", lambda directory, value: saved.append((directory, value)))
    root = tmp_path / "ray"
    asyncio.run(telegram_setup.configure(root))
    assert saved == [(root / "state", token)]
    raw = (root / "gateway.yaml").read_text()
    assert token not in raw
    config = GatewayConfig.model_validate(yaml.safe_load(raw))
    store = StateStore(root / "state" / "ray.db")
    g = Gateway(config, root / "gateway.yaml", store, root / "state", SetupAPI(token))
    assert config.access[0].allow_workspace_setup
    assert g.identify(update(8, "/start"))[0] == ACTOR
    assert Control(store).setting("telegram_offset") == 8
    with pytest.raises(ValueError, match="already configured"):
        asyncio.run(telegram_setup.configure(root))
