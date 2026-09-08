"""Private SDK subprocess. Inputs and state paths are supplied by the host."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from .errors import RayError, model_error_code


def strict_output_schema(value):
    if isinstance(value, list):
        return [strict_output_schema(v) for v in value]
    if not isinstance(value, dict):
        return value
    schema = {k: strict_output_schema(v) for k, v in value.items() if k != "default"}
    if "properties" in schema:
        schema["required"] = list(schema["properties"])
        schema["additionalProperties"] = False
    return schema


def atomic_json(path, value):
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value), encoding="utf-8")
    temporary.replace(path)


def consume_turn(turn, progress_path, metadata=None):
    """Consume the public SDK stream while preserving final-response semantics."""
    from .model_progress import ProgressWriter
    from openai_codex.generated.v2_all import ItemCompletedNotification, TurnCompletedNotification, AgentMessageThreadItem
    from openai_codex.generated.v2_all import ThreadTokenUsageUpdatedNotification
    progress = ProgressWriter(progress_path)
    final = fallback = None
    stream = turn.stream()
    try:
        for event in stream:
            payload = event.payload
            progress.observe(event)
            if metadata is not None and isinstance(payload, ThreadTokenUsageUpdatedNotification) and payload.turn_id == turn.id:
                metadata["usage"] = payload.token_usage.last.model_dump()
                atomic_json(progress_path.with_name("runtime.json"), metadata)
            if isinstance(payload, ItemCompletedNotification) and payload.turn_id == turn.id:
                item = getattr(payload.item, "root", payload.item)
                if isinstance(item, AgentMessageThreadItem):
                    phase = getattr(item.phase, "value", item.phase)
                    if phase == "final_answer":
                        final = item.text
                    elif phase is None:
                        fallback = item.text
            if isinstance(payload, TurnCompletedNotification) and payload.turn.id == turn.id:
                status = getattr(payload.turn.status, "value", payload.turn.status)
                if status != "completed":
                    raise RayError(model_error_code(payload.turn.error))
                text = final if final is not None else fallback
                if not text:
                    raise RayError("MODEL_OUTPUT_INVALID")
                return json.loads(text)
        raise RayError("MODEL_UNAVAILABLE")
    finally:
        stream.close()


def probe_shell(config):
    """Exercise the configured SDK command path; return categories, never raw logs."""
    import shutil
    from openai_codex.client import CodexClient
    from openai_codex.generated.v2_all import CommandExecResponse

    shell = (shutil.which("pwsh") or shutil.which("powershell")) if os.name == "nt" else shutil.which("sh")
    if not shell:
        return {"shell_available": False, "error_code": "LOCAL_RESOURCE"}
    command = [shell, "-NoProfile", "-NonInteractive", "-Command", "Write-Output ray-runtime-ok"] if os.name == "nt" else [shell, "-c", "printf ray-runtime-ok"]
    try:
        with CodexClient(config) as client:
            client.initialize()
            result = client.request("command/exec", {
                "command": command, "cwd": config.cwd,
                "sandboxPolicy": {"type": "readOnly"},
                "timeoutMs": 10000,
            }, response_model=CommandExecResponse)
        ready = result.exit_code == 0 and result.stdout.strip() == "ray-runtime-ok"
        return {"shell_available": ready, "shell": shell, "error_code": None if ready else "SANDBOX_FAILED",
                "scope": "one read-only shell command; no model, Fabric write or containment acceptance"}
    except Exception:
        return {"shell_available": False, "shell": shell, "error_code": "SANDBOX_FAILED"}


def run(request_path, result_path, checkpoint):
    from openai_codex import Codex, CodexConfig, Sandbox, ApprovalMode

    request = json.loads(request_path.read_text(encoding="utf-8"))
    conversation = request.get("conversation", False)
    if conversation and (not request["read_only"] or request["thread_id"]):
        raise ValueError("Conversation requires a fresh read-only thread")
    sandbox = Sandbox.read_only if request["read_only"] else Sandbox.workspace_write
    # Project-specific CODEX_HOME is set by the parent. SDK env is additive,
    # so the parent strips credentials before creating this worker.
    shell_path = os.environ.get("PATH", "")
    overrides = (
        'approval_policy="never"',
        "sandbox_workspace_write.network_access=false",
        'shell_environment_policy.inherit="none"',
        "shell_environment_policy.set.PATH=" + json.dumps(shell_path),
        'web_search="disabled"',
        "features.apps=false",
        "features.multi_agent=false",
        "allow_login_shell=false",
    )
    if os.name == "nt":
        # A fresh isolated CODEX_HOME has no Windows sandbox selection. With
        # approval=never that rejects even Get-ChildItem before process creation.
        # Select the native restricted-token sandbox without admin setup or
        # permission escalation. Enterprise requirements still take precedence.
        mode = "unelevated"
        if request.get("home"):
            config_path = Path(request["home"]) / "config.toml"
            if config_path.is_file():
                import tomllib
                with config_path.open("rb") as stream:
                    mode = tomllib.load(stream).get("windows", {}).get("sandbox", mode)
        if mode not in {"elevated", "unelevated"}:
            raise ValueError("Unsupported Windows sandbox implementation")
        overrides += ("windows.sandbox=" + json.dumps(mode),)
        for key in ("SYSTEMROOT", "WINDIR", "COMSPEC", "PATHEXT", "TEMP", "TMP"):
            value = next((v for k, v in os.environ.items() if k.upper() == key), None)
            if value is not None:
                overrides += ("shell_environment_policy.set." + key + "=" + json.dumps(value),)
    if conversation:
        overrides += (
            "features.shell_tool=false", "tools.view_image=false",
            "project_doc_max_bytes=0",
        )
    config = CodexConfig(
        codex_bin=request["binary"],
        cwd=request["repo"],
        config_overrides=overrides,
        client_name="ray_fabric_engineer",
    )
    if request.get("diagnostic"):
        if conversation or not request["read_only"] or request["thread_id"]:
            raise ValueError("Runtime probe requires a fresh read-only request")
        atomic_json(result_path, probe_shell(config))
        return
    instructions = (
        Path(__file__)
        .with_name("prompts")
        .joinpath("conversation.md" if conversation else "persona.md")
        .read_text(encoding="utf-8")
    )
    params = {
        "cwd": request["repo"],
        "sandbox": sandbox,
        "approval_mode": ApprovalMode.deny_all,
        "developer_instructions": instructions,
    }
    if request["model"]:
        params["model"] = request["model"]
    if conversation:
        params["ephemeral"] = True
    with Codex(config) as codex:
        thread = (
            codex.thread_resume(request["thread_id"], **params)
            if request["thread_id"]
            else codex.thread_start(**params)
        )
        from openai_codex.generated.v2_all import ReasoningEffort
        effort = request.get("reasoning_effort")
        turn_options = {"effort": ReasoningEffort(effort)} if effort else {}
        metadata = {"source": "codex_sdk", "configured_model": request["model"],
                    "resolved_model": None, "reasoning_effort": effort, "usage": None,
                    "sdk_version": "0.147.0"}
        # The high-level Thread API does not expose the resolved model; never
        # present a requested/default model as an observed one.
        atomic_json(checkpoint.with_name("runtime.json"), metadata)
        turn = thread.turn(
            request["prompt"],
            output_schema=strict_output_schema(request["schema"]),
            approval_mode=ApprovalMode.deny_all,
            sandbox=sandbox,
            **turn_options,
        )
        # A thread/start ID has no resumable rollout until turn/start succeeds.
        # Checkpoint the accepted turn before consuming its model output.
        atomic_json(checkpoint, {"thread_id": thread.id})
        response = consume_turn(turn, checkpoint.with_name("progress.json"), metadata)
        atomic_json(result_path, response)


if __name__ == "__main__":
    paths = [Path(p) for p in sys.argv[1:]]
    try:
        run(*paths)
    except Exception as exc:
        code = exc.code if isinstance(exc, RayError) else model_error_code(exc)
        if isinstance(exc, json.JSONDecodeError):
            code = "MODEL_OUTPUT_INVALID"
        atomic_json(paths[1], {"_ray_error": {"code": code}})
        raise SystemExit(1)
