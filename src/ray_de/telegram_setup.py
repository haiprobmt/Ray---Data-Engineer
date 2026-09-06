"""Interactive local Telegram setup. Never accepts tokens in command arguments."""

import argparse
import asyncio
import getpass
import os
import re
import secrets
import time
from pathlib import Path

import yaml

from .config import ProjectConfig
from .control import Control
from .secrets import save_token
from .state import StateStore, project_lock
from .telegram import GatewayConfig, TelegramAPI


def pairing_actor(update, code):
    message = update.get("message", {})
    user, chat = message.get("from", {}), message.get("chat", {})
    if (type(update.get("update_id")) is not int
            or message.get("text") != "/pair " + code
            or message.get("forward_origin") or user.get("is_bot")
            or chat.get("type") != "private"
            or type(user.get("id")) is not int or user["id"] <= 0
            or type(chat.get("id")) is not int or chat["id"] <= 0):
        return None
    return user["id"], chat["id"]


async def pair(api, code, *, timeout=600):
    deadline, offset = time.monotonic() + timeout, 0
    while time.monotonic() < deadline:
        updates = await api.call("getUpdates", {
            "offset": offset, "timeout": 20, "allowed_updates": ["message", "callback_query"]
        })
        for update in updates:
            actor = pairing_actor(update, code)
            offset = max(offset, update["update_id"] + 1)
            if actor:
                return *actor, offset
    raise RuntimeError("Pairing expired. Run setup again to get a new pairing code.")


async def configure(root):
    root = root.resolve()
    config_path, data_dir = root / "gateway.yaml", root / "state"
    if config_path.exists():
        raise ValueError("Telegram is already configured here. Start Ray with the existing gateway.yaml.")
    if not __import__("sys").stdin.isatty():
        raise ValueError("Run setup in an interactive local terminal so token input can be hidden.")
    if os.environ.get("RAY_TELEGRAM_BOT_TOKEN"):
        raise ValueError("Clear RAY_TELEGRAM_BOT_TOKEN in this terminal before secure setup; it would override the stored token.")
    token = getpass.getpass("Paste the BotFather token (hidden; press Enter): ").strip()
    if not re.fullmatch(r"\d{6,}:[A-Za-z0-9_-]{25,}", token):
        raise ValueError("Invalid Telegram bot token format")
    api = TelegramAPI(token)
    identity = await api.call("getMe", {})
    username = identity.get("username", "")
    if not identity.get("is_bot") or not re.fullmatch(r"[A-Za-z0-9_]{5,32}", username):
        raise RuntimeError("Telegram did not return a valid bot identity")
    webhook = await api.call("getWebhookInfo", {})
    if webhook.get("url"):
        raise RuntimeError("This bot has an active webhook. Use a dedicated Ray bot or remove its webhook before pairing.")
    store = StateStore(data_dir / "ray.db")
    with project_lock(data_dir / "locks", "telegram-gateway"):
        code = secrets.token_urlsafe(24)
        print(f"\nOpen https://t.me/{username} in Telegram and send this exact message:", flush=True)
        print(f"\n/pair {code}\n", flush=True)
        print("Waiting up to 10 minutes for your private chat. Keep the pairing code private.", flush=True)
        user_id, chat_id, offset = await pair(api, code)
        lobby = root / "lobby"
        (lobby / "repo").mkdir(parents=True, exist_ok=True)
        project_path = lobby / "config.yaml"
        config = ProjectConfig(project_id="ray-lobby", name="Ray", repo_path="repo")
        if project_path.exists():
            from .config import load_project
            existing = load_project(project_path)
            if existing.config != config or existing.repo != (lobby / "repo").resolve():
                raise ValueError("An existing lobby has different settings. Choose a new setup root.")
        else:
            with project_path.open("x", encoding="utf-8") as output:
                output.write(yaml.safe_dump(config.model_dump(), sort_keys=False))
        gateway = GatewayConfig.model_validate({
            "projects": {"ray-lobby": str(project_path)},
            "access": [{"user_id": user_id, "chat_id": chat_id, "projects": ["ray-lobby"],
                        "allow_workspace_setup": True}],
            "workspace_root": str(root / "workspaces"),
        })
        save_token(data_dir, token)
        control = Control(store)
        control.set_setting("telegram_offset", offset)
        with config_path.open("x", encoding="utf-8") as output:
            output.write(yaml.safe_dump(gateway.model_dump(), sort_keys=False))
        print(f"\nPaired Telegram user {user_id}, private chat {chat_id}.", flush=True)
        print(f"Token stored with Windows current-user protection. Config: {config_path}", flush=True)
        print("Ray is ready to start. Run scripts\\login-ray-chat.ps1 to enable model conversations, then say hello in Telegram. Workspace setup can wait.", flush=True)


def main(argv=None):
    parser = argparse.ArgumentParser(description="Secure local setup for Ray's private Telegram bot")
    parser.add_argument("--root", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        asyncio.run(configure(args.root))
        return 0
    except (KeyboardInterrupt, EOFError):
        print("Setup cancelled.")
        return 1
    except Exception as exc:
        # Network exceptions can contain the token-bearing request URL.
        if type(exc) in {ValueError, RuntimeError}:
            print(str(exc))
        else:
            print("Setup failed (" + type(exc).__name__ + "). No credential details are displayed.")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
