from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

from .runtime import codex_env, resolve_binary
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

    def probe(self, project):
        """Check a real sandboxed shell without a model turn or cloud access."""
        return self.run(project, "", {}, diagnostic=True)

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
        diagnostic=False,
        on_progress=None,
    ):
        self.last_metadata = None
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
        env = codex_env()
        env["CODEX_HOME"] = str(home)
        # Worker imports the installed package or this source checkout.
        env["PYTHONPATH"] = str(Path(__file__).resolve().parents[1])
        request = {
            "binary": binary,
            "repo": str(repo),
            "model": project.config.model,
            "reasoning_effort": getattr(project.config, "reasoning_effort", None),
            "home": str(home),
            "thread_id": thread_id,
            "read_only": read_only,
            "prompt": prompt,
            "schema": schema,
            "conversation": conversation,
            "diagnostic": diagnostic,
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

            from .model_progress import TurnDeadline
            watch = TurnDeadline(30 if diagnostic else project.config.timeout_seconds,
                                 engineering=not conversation and not diagnostic, now=time.monotonic())
            progress_path = scratch / "progress.json"
            last_notice = float("-inf")
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
                    now = time.monotonic()
                    try:
                        with progress_path.open(encoding="utf-8") as stream:
                            encoded = stream.read(513)
                        activity = json.loads(encoded) if len(encoded) <= 512 else None
                    except (OSError, ValueError):
                        activity = None
                    watch.observe(activity, now)
                    if on_progress and now - last_notice >= 20:
                        on_progress(watch.snapshot(now))
                        last_notice = now
                    watch.check(now)
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
                runtime_path = scratch / "runtime.json"
                if runtime_path.exists():
                    try:
                        from .memory import redact_data
                        self.last_metadata = redact_data(json.loads(runtime_path.read_text(encoding="utf-8")[:4096]))
                    except (OSError, ValueError):
                        self.last_metadata = None
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

                        try:
                            os.killpg(process.pid, signal.SIGTERM)
                        except ProcessLookupError:
                            pass  # Worker exited between poll and termination.
                    try:
                        process.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=5)
