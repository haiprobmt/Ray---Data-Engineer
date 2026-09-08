"""Small, lossless Telegram text renderer; model text never becomes HTML."""

import re


def units(text):
    return len(text.encode("utf-16-le")) // 2


def render(text):
    # Recognize a deliberately small Markdown subset. Unsupported markup stays
    # literal, including HTML and links. Code contents are never reinterpreted.
    pattern = re.compile(r"```(?:[\w+-]+\n)?(?P<pre>[\s\S]*?)```|`(?P<code>[^`\n]+)`|\*\*(?P<bold>[^*\n]+)\*\*")
    parts, entities, position, offset = [], [], 0, 0
    for match in pattern.finditer(text):
        prefix = text[position:match.start()]
        parts.append(prefix)
        offset += units(prefix)
        kind = match.lastgroup
        value = match.group(kind)
        parts.append(value)
        length = units(value)
        if length:
            entities.append({"type": kind, "offset": offset, "length": length})
        offset += length
        position = match.end()
    parts.append(text[position:])
    return "".join(parts), entities


def formatted_chunks(text, limit=3500):
    text, entities = render(text)
    base = 0
    while text:
        length, end = 0, 0
        for char in text:
            size = units(char)
            if length + size > limit:
                break
            length += size
            end += 1
        if not end:
            raise ValueError("Message limit is too small")
        # Keep paragraphs/words together where possible, without dropping text.
        if end < len(text):
            boundary = max(text.rfind("\n", end // 2, end), text.rfind(" ", end // 2, end))
            if boundary >= 0:
                end = boundary + 1
        piece = text[:end]
        length = units(piece)
        spans = []
        for entity in entities:
            start = max(base, entity["offset"])
            stop = min(base + length, entity["offset"] + entity["length"])
            if start < stop:
                spans.append({"type": entity["type"], "offset": start - base, "length": stop - start})
        yield piece, spans
        base += length
        text = text[end:]


PHASES = {
    "reading_fabric": "I'm checking the current data and items in Fabric.",
    "authoring": "I'm working on the next step.",
    "validating": "I'm checking the files on this computer.",
    "reviewing": "I'm checking the proposed changes before using them in Fabric.",
    "source_ready": "The code is ready. The change in Fabric is still pending.",
    "preparing_fabric": "I'm checking the target in Fabric before making the change.",
    "applying_changes": "I'm applying the change in Fabric.",
    "running_job": "The pipeline is running in Fabric. I'm waiting for its result.",
    "preparing_next_step": "This step finished. I'm preparing the next step.",
    "awaiting_approval": "Please approve the proposed change before I continue.",
    "finished": "This step is complete.",
}


def friendly_error(error):
    code = error.get("code") if isinstance(error, dict) else None
    code = code if isinstance(code, str) else "UNKNOWN"
    if code in {"TIMED_OUT", "MODEL_IDLE_TIMEOUT", "MODEL_TIME_LIMIT"} and isinstance(error, dict) and error.get("stage") == "conversation":
        return "I couldn't finish this reply in time. Send your message again to continue the conversation."
    if code == "MODEL_OUTPUT_INVALID" and isinstance(error, dict) and error.get("stage") == "conversation":
        return "I couldn't read the reply for this conversation. Send your message again so I can answer."
    messages = {
        "FABRIC_SQL_TABLE_UNAVAILABLE": "Fabric could not read this table yet. The data check is still pending; this does not mean the earlier load failed.",
        "FABRIC_READ_TOO_LARGE": "The data sample was too large. I need to check totals or use a smaller sample.",
        "FABRIC_SQL_SIGNIN_REQUIRED": "Ray needs you to sign in again before it can read the tables. Use /workspace login-sql for instructions.",
        "MODEL_SCHEMA_REJECTED": "Ray could not read the reply it received. Ray's response handling needs a fix before this step can continue.",
        "MODEL_OUTPUT_INVALID": "Ray couldn't use the reply for this step. Progress is saved. Use /resume to continue the same task.",
        "MODEL_IDLE_TIMEOUT": "Ray stopped receiving progress while working on the code. Your task and any draft files are saved. Use /resume to continue the same task.",
        "MODEL_TIME_LIMIT": "This code step reached its maximum time. Your task and any draft files are saved. Use /resume to finish the remaining work in a smaller step.",
        "TIMED_OUT": "This step took too long. Its result needs checking before it is tried again.",
        "FABRIC_DEFINITION_CHANGED": "The version in Fabric differs from the checked files. They need to be compared before running it.",
        "UNKNOWN": "I couldn't finish this step. Check /details technical for the saved information.",
    }
    if code in messages:
        if code == "TIMED_OUT" and isinstance(error, dict):
            step = {"model": "preparing the code", "review": "checking the code", "validation": "running the local checks", "fabric_read": "reading Fabric", "cloud_action": "waiting for Fabric", "conversation": "preparing the reply"}.get(error.get("stage"))
            if step:
                return "Ray took too long while " + step + ". Progress is saved. Check /details before resuming the same task."
        return messages[code]
    from .errors import ERRORS
    message, next_step = ERRORS.get(code, ERRORS["UNKNOWN"])
    return message + " " + next_step


def action_summary(plan):
    remote = plan.get("remote") or {}
    operation = plan.get("operation") or {"created_item": "create_item", "updated_item": "update_item", "job": "run_job"}.get(remote.get("kind"))
    item_type = plan.get("item_type") or remote.get("item_type")
    noun = {"Notebook": "notebook", "DataPipeline": "pipeline", "Lakehouse": "lakehouse"}.get(item_type, "Fabric item")
    if operation == "tenant_action" and plan.get("state") == "SUCCEEDED":
        return {"create_workspace": "Created the configured Fabric workspace.",
                "assign_capacity": "Assigned the configured capacity.",
                "provision_identity": "Provisioned the workspace identity.",
                "assign_workspace_role": "Verified the workspace role assignment.",
                "create_folder": "Created the workspace folder.", "move_item": "Moved the item and preserved its identity.",
                "create_connection": "Created and verified the connection.", "update_connection": "Updated and verified the connection.",
                "assign_connection_role": "Verified the connection role assignment.",
                "create_application": "Created the execution application.", "create_service_principal": "Created the execution service principal.",
                "create_application_credential": "Stored the new execution credential in protected local storage.",
                "create_group": "Created the configured security group.", "add_group_member": "Verified the security group membership.",
                "github_create_repository": "Created the private GitHub repository.", "github_publish": "Published the reviewed source to GitHub.",
                "git_connect": "Connected the workspace to its configured Git directory.",
                "git_initialize": "Initialized Git; inspect the receipt for the required synchronization step.",
                "git_commit": "Saved the initial workspace baseline to Git.",
                "git_update": "Deployed the reviewed Git commit and verified the items and folders."
                }.get(plan.get("tenant_operation"), "Verified the configured tenant operation.")
    if plan.get("state") == "SUCCEEDED":
        return {"create_item": f"Created a {noun} in Fabric.", "update_item": f"Updated the {noun}'s details.",
                "update_definition": f"Updated the {noun} in Fabric.", "deploy_to_test": f"Updated the {noun} in TEST.",
                "run_job": f"The {noun} run finished successfully.",
                "publish_environment": "Published the Fabric environment."}.get(operation, "A Fabric step finished successfully.")
    return {"FAILED": "A Fabric step failed.", "UNCERTAIN": "A Fabric result still needs verification.",
            "VALIDATION_FAILED": "The change was sent to Fabric, but its checks did not pass.",
            "PENDING_APPROVAL": "A change is waiting for your approval.", "APPROVED": "The approved change is waiting to start.",
            "READY": "A Fabric change is ready to start.", "EXECUTING": f"The {noun} is running in Fabric." if operation == "run_job" else "A change is being applied in Fabric.",
            "CANCELLED": "A planned change was cancelled.", "SUPERSEDED": "An earlier plan was replaced."}.get(plan.get("state"), "A Fabric result needs checking.")


def task_message(report, *, details=False):
    status, phase = report.get("status", ""), report.get("phase")
    plans = report.get("cloud_plans", [])
    executing = next((p for p in plans if p.get("state") == "EXECUTING"), None)
    if executing:
        status = "waiting"
        phase = "running_job" if executing.get("operation") == "run_job" else "applying_changes"
    elif status == "completed" and report.get("continue_work"):
        status, phase = "waiting", "preparing_next_step"
    title = {"completed": "✅ Done", "blocked": "⚠️ Blocked",
             "clarifying": "❓ Your input is needed", "paused": "⏸ Paused",
             "error": "⚠️ Couldn’t finish", "waiting": "⏳ In progress", "working": "⏳ Still working",
             "validating": "⏳ Checking the files", "reviewing": "⏳ Checking the changes",
             "approval_required": "❓ Your approval is needed"}.get(status, "Update")
    active = phase in {"reading_fabric", "authoring", "validating", "reviewing", "preparing_fabric", "applying_changes", "running_job", "preparing_next_step"}
    body = PHASES.get(phase) if active else report.get("user_summary")
    if phase in {"authoring", "reviewing"} and status in {"working", "reviewing"}:
        progress = report.get("model_progress") or {}
        activity = progress.get("activity")
        detail = {"working": "I'm working through this step.", "running_command": "I'm running a local command.",
                  "writing_files": "I'm updating the draft files.", "preparing_reply": "I'm preparing the response for this step."}.get(activity)
        if detail:
            body = detail
    if executing:
        body = action_summary(executing) + " I'm waiting for its result."
    if not body:
        body = PHASES.get(phase, "I'm working on this request." if status in {"waiting", "working"} else "")
        if not details and not report.get("user_summary") and not report.get("cloud_actions"):
            body = report.get("message", body)
    if status in {"blocked", "error", "paused"}:
        body = report.get("user_summary") or {"blocked": "This request needs attention before I can continue.",
            "error": "I couldn't finish this step.", "paused": "Work is paused. Use /resume when you're ready to continue."}[status]
        if not details and not report.get("error"):
            body = report.get("message", body)
    parts = [f"**{title}**", body]
    finished = list(dict.fromkeys(action_summary(p) for p in plans if p.get("state") == "SUCCEEDED"))
    if finished:
        parts.append("**Finished**\n" + "\n".join("• " + line for line in finished[-5:]))
    notes = list(report.get("user_notes") or [])
    review = report.get("review") or {}
    if details and not active and review:
        check = review.get("user_summary") or {"PASS": "The code checks passed.",
            "PASS_WITH_COMMENTS": "The code checks passed, with notes to follow up.",
            "REWORK": "The code needs changes before I can continue.", "BLOCK": "The code check found a problem that must be resolved."}.get(review.get("verdict"), "A code check was recorded.")
        parts.append("**Checks**\n" + check)
        notes.extend(review.get("user_notes") or [])
    notes.extend(action_summary(p) for p in plans if p.get("state") in {"UNCERTAIN", "VALIDATION_FAILED"})
    current_ids = report.get("stage_plan_ids")
    notes.extend(action_summary(p) for p in plans if p.get("state") == "FAILED" and (current_ids is None or p.get("id") in current_ids))
    if report.get("error"):
        notes.append(friendly_error(report["error"]))
    notes.extend(friendly_error(f.get("error")) for f in report.get("read_failures", []) if f.get("error"))
    if notes:
        parts.append("**Needs attention**\n" + "\n".join("• " + note for note in dict.fromkeys(notes)))
    if report.get("next_step") and not active:
        parts.append("**Next**\n" + report["next_step"])
    parts.append("/details technical — full logs and IDs" if details else "/details — progress and checks")
    return "\n\n".join(part for part in parts if part)


def task_details(project_id, task, report, *, technical=False):
    if not technical:
        return task_message(dict(report, status=task["status"].lower()), details=True)
    parts = ["**Task details**", f"Project: `{project_id}`\nTask: `{task['id']}`\nState: {task['status']}"]
    review = report.get("review") or {}
    if review:
        parts.append("**Review — " + review.get("verdict", "Recorded") + "**\n" + review.get("summary", ""))
        if review.get("findings"):
            parts.append("**Reviewer findings**\n" + "\n".join("• " + finding for finding in review["findings"]))
    if report.get("error"):
        from .errors import error_text
        parts.append(error_text(report["error"]))
    for label, values in [("Validation", report.get("host_validation")), ("Evidence", report.get("evidence"))]:
        if values:
            parts.append(f"**{label}**\n" + "\n".join("• " + str(value) for value in values))
    for plan in report.get("cloud_plans", []):
        remote = plan.get("remote") or {}
        parts.append(f"Action `{plan['id']}`: {plan['state']}" + (f"\nRemote: `{remote.get('id')}`" if isinstance(remote, dict) and remote.get("id") else ""))
    return "\n\n".join(parts)
