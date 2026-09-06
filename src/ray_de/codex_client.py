from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

from .runtime import clean_env, resolve_binary
from .errors import RayError


class CodexRunner:
    """Run the official SDK in a short-lived worker; parent owns durable state.

    Each project uses a separate Codex home. No user plugins, hooks, or MCP
    configuration are copied into it. Authenticate it with `ray login`.
    """

    def __init__(self, data_dir: Path):
        self.data_dir = data_dir

    def home(self, project):
        home = self.data_dir / "codex" / project.id
        home.mkdir(parents=True, exist_ok=True)
        return home

    def run(
        self,
        project,
        prompt,
        schema,
        *,
        thread_id=None,
        read_only=True,
        on_thread=None,
        cancel=None,
        conversation=False,
    ):
        if cancel:
            cancel.check()
        binary = resolve_binary("codex")
        home = self.home(project)
        repo = project.repo
        if conversation:
            if thread_id or not read_only:
                raise ValueError("Conversation must use a fresh read-only thread")
            repo = self.data_dir / "conversation-empty"
            repo.mkdir(parents=True, exist_ok=True)
        env = clean_env()
        env["CODEX_HOME"] = str(home)
        # Worker imports the installed package or this source checkout.
        env["PYTHONPATH"] = str(Path(__file__).resolve().parents[1])
        request = {
            "binary": binary,
            "repo": str(repo),
            "model": project.config.model,
            "home": str(home),
            "thread_id": thread_id,
            "read_only": read_only,
            "prompt": prompt,
            "schema": schema,
            "conversation": conversation,
        }
        # Separate result and immediate checkpoint files avoid mixing model output
        # with SDK logs and preserve the thread ID even when a turn fails.
        self.data_dir.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix="turn-", dir=self.data_dir) as scratch:
            scratch = Path(scratch)
            req = scratch / "request.json"
            result_path = scratch / "result.json"
            checkpoint = scratch / "thread.json"
            req.write_text(json.dumps(request), encoding="utf-8")
            process = subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "ray_de.sdk_worker",
                    str(req),
                    str(result_path),
                    str(checkpoint),
                ],
                env=env,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                creationflags=subprocess.CREATE_NEW_PROCESS_GROUP
                if os.name == "nt"
                else 0,
                start_new_session=os.name != "nt",
            )
            import time

            deadline = time.monotonic() + project.config.timeout_seconds
            saved = None
            try:
                while process.poll() is None:
                    if cancel:
                        cancel.check()
                    if checkpoint.exists() and saved is None:
                        saved = json.loads(checkpoint.read_text(encoding="utf-8"))[
                            "thread_id"
                        ]
                        if on_thread:
                            on_thread(saved)
                    if time.monotonic() >= deadline:
                        raise TimeoutError(
                            "Codex turn timed out; task paused, no automatic retry"
                        )
                    time.sleep(0.1)
                if checkpoint.exists() and saved is None and on_thread:
                    on_thread(
                        json.loads(checkpoint.read_text(encoding="utf-8"))["thread_id"]
                    )
                if process.returncode != 0 or not result_path.exists():
                    code = "MODEL_UNAVAILABLE"
                    if result_path.exists():
                        try:
                            failure = json.loads(result_path.read_text(encoding="utf-8"))
                            code = failure.get("_ray_error", {}).get("code", code)
                        except (ValueError, AttributeError):
                            pass
                    raise RayError(code)
                if cancel:
                    cancel.check()
                return json.loads(result_path.read_text(encoding="utf-8"))
            finally:
                if process.poll() is None:
                    if os.name == "nt":
                        subprocess.run(
                            ["taskkill", "/PID", str(process.pid), "/T", "/F"],
                            capture_output=True,
                            timeout=15,
                            check=False,
                        )
                    else:
                        import signal

                        os.killpg(process.pid, signal.SIGTERM)
                    try:
                        process.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=5)
