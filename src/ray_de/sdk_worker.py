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
    )
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
        turn = thread.turn(
            request["prompt"],
            output_schema=strict_output_schema(request["schema"]),
            approval_mode=ApprovalMode.deny_all,
            sandbox=sandbox,
        )
        # A thread/start ID has no resumable rollout until turn/start succeeds.
        # Checkpoint the accepted turn before consuming its model output.
        atomic_json(checkpoint, {"thread_id": thread.id})
        response = turn.run()
        status = getattr(response.status, "value", response.status)
        if status != "completed" or not response.final_response:
            raise RayError(model_error_code(getattr(response, "error", None)))
        atomic_json(result_path, json.loads(response.final_response))


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
