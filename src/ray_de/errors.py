"""Public error descriptions. Raw exception/HTTP/model text is never persisted."""

import json
import subprocess
from pydantic import ValidationError


# Fixed schema diagnostics only: input values, arbitrary keys, exception context
# and service messages must never reach saved reports or chat.
VALIDATION_FIELDS = frozenset("response status message user_summary next_step user_notes recommendation question options evidence skills_used cloud_actions artifacts read_requests continue_work operation workspace_id item_id definition_path path content schema_name table_name job_id verdict summary findings offer_work read_paths".split())
VALIDATION_TYPES = {
    "missing": "MISSING_FIELD", "literal_error": "INVALID_CHOICE", "extra_forbidden": "UNEXPECTED_FIELD",
    "string_too_long": "TOO_LONG", "string_too_short": "TOO_SHORT", "too_long": "TOO_LONG",
    "too_short": "TOO_SHORT", "string_pattern_mismatch": "INVALID_FORMAT",
}
VALIDATION_RULES = {
    "Read requests require a working turn without writes or artifacts": "READ_WRITE_MIX",
    "Continuation requires a completed source stage": "CONTINUATION_STATUS",
    "Clarification requires one question and a recommendation": "CLARIFICATION_INCOMPLETE",
    "Completion requires evidence": "COMPLETION_EVIDENCE_MISSING",
    "A blocked recovery question requires a recommendation": "RECOVERY_INCOMPLETE",
    "A question belongs to a clarification or blocked result": "QUESTION_STATUS",
    "A job ID belongs only to a notebook output read": "JOB_READ_MISMATCH",
    "A table name is required for SQL reads": "TABLE_REQUIRED",
}
VALIDATION_CODES = frozenset(VALIDATION_TYPES.values()) | frozenset(VALIDATION_RULES.values()) | {"INVALID_VALUE", "MALFORMED_JSON"}


def validation_issues(exc):
    if isinstance(exc, json.JSONDecodeError):
        return [{"field": "response", "code": "MALFORMED_JSON"}]
    issues = []
    for issue in exc.errors(include_input=False, include_context=False, include_url=False)[:5]:
        location = issue.get("loc", ())
        field = location[0] if location and location[0] in VALIDATION_FIELDS else "response"
        code = VALIDATION_RULES.get(issue.get("msg", "").removeprefix("Value error, "),
                                    VALIDATION_TYPES.get(issue.get("type"), "INVALID_VALUE"))
        item = {"field": field, "code": code}
        if item not in issues:
            issues.append(item)
    return issues


ERRORS = {
    "MODEL_IDLE_TIMEOUT": ("The model stopped reporting progress while preparing the local work.", "The task and any draft files are saved. Resume the same task to continue; review is still required before any new Fabric change."),
    "MODEL_TIME_LIMIT": ("The model reached the maximum time for one step.", "The task and any draft files are saved. Resume the same task and finish the remaining work in a smaller stage."),
    "FABRIC_DEFINITION_CHANGED": ("The live Fabric definition differs from the reviewed source. Ray has not submitted the job.", "Inspect the live definition, reconcile the local source, and obtain fresh validation and review before running. Do not repeat item creation."),
    "FABRIC_SQL_SIGNIN_REQUIRED": ("Ray could not obtain SQL authorization for this profile. SQL authentication is separate from Fabric metadata access.", "Use /workspace login-sql and run its local PowerShell command to authorize SQL access, then resume the task. /workspace check tests metadata only."),
    "FABRIC_SQL_SETUP": ("Ray's SQL read transport needs pyodbc 5.3.0 and Microsoft ODBC Driver 18 for SQL Server.", "Install Ray's Fabric dependencies and ODBC Driver 18 on the host, then resume."),
    "FABRIC_SQL_NOT_READY": ("The Lakehouse SQL analytics endpoint is not ready.", "Check its provisioning status in Fabric, then resume when it is ready."),
    "FABRIC_SQL_TABLE_UNAVAILABLE": ("The requested table is not visible to the SQL analytics endpoint.", "Check the table and schema against Lakehouse metadata. Newly written Delta tables may still be synchronizing; preserve successful job receipts and explicitly recheck later."),
    "FABRIC_SQL_UNAVAILABLE": ("Ray could not query the Lakehouse SQL analytics endpoint.", "Check SQL read permissions, local Fabric sign-in, and network access to port 1433, then resume."),
    "FABRIC_READ_TOO_LARGE": ("The Fabric read exceeded Ray's bounded result size.", "Request table schema or row count instead of a wide preview."),
    "MODEL_SCHEMA_REJECTED": ("The model service rejected Ray's response schema before answering.", "Update Ray's schema handling, then explicitly resume the task."),
    "MODEL_AUTH_REQUIRED": ("The model service rejected Ray's sign-in.", "Sign in to this project's Codex profile, then explicitly resume."),
    "MODEL_LIMIT": ("The model service reported a usage or rate limit.", "Check your account allowance or wait for the limit to reset before resuming."),
    "MODEL_CONTEXT_LIMIT": ("This conversation exceeded the model's context limit.", "Start a new task with a shorter request."),
    "MODEL_UNAVAILABLE": ("Ray could not complete the request with the model service.", "Check the model connection and /doctor, then explicitly resume."),
    "MODEL_OUTPUT_INVALID": ("The model's reply did not match the response format Ray needs.", "Explicitly resume to request a new reply; the invalid reply was not accepted."),
    "SANDBOX_FAILED": ("The model runtime could not use the configured sandbox.", "Check the local runtime and sandbox configuration with /doctor."),
    "FABRIC_AUTH_REQUIRED": ("Fabric rejected Ray's sign-in (HTTP 401).", "Use /workspace login to sign in locally, then check /workspace check."),
    "FABRIC_SIGNIN_REQUIRED": ("Ray's local Fabric sign-in is missing or cannot renew silently.", "Use /workspace login and complete Microsoft sign-in locally, then use /workspace check. Chat with Ray remains available."),
    "FABRIC_FORBIDDEN": ("Fabric denied access to the requested resource (HTTP 403).", "Check this account's workspace permissions, then use /workspace check."),
    "FABRIC_NOT_FOUND": ("Fabric could not find the requested resource (HTTP 404).", "Check the configured workspace or item ID."),
    "FABRIC_LIMIT": ("Fabric rate-limited the request (HTTP 429).", "Wait before explicitly retrying the read."),
    "FABRIC_UNAVAILABLE": ("Ray could not read the required Fabric data.", "Check /workspace login and /workspace check before resuming."),
    "FABRIC_REQUEST_REJECTED": ("Fabric rejected the item payload or operation (HTTP 400).", "Check the reviewed payload against the Fabric API for this item type, correct it and create a new reviewed action."),
    "FABRIC_CONFLICT": ("Fabric reported an item or operation conflict (HTTP 409).", "Inspect the existing item and action receipts before planning another write."),
    "FABRIC_ACTION_FAILED": ("Fabric reported that the action failed.", "Inspect the saved action receipt and Fabric job history. Ray has not retried the write."),
    "FABRIC_VERIFICATION_FAILED": ("The Fabric item was returned, but its metadata or definition did not match the reviewed source.", "Inspect and reconcile the recorded item ID. Do not repeat creation."),
    "TIMED_OUT": ("The operation did not finish within Ray's time limit.", "Check the current state before explicitly resuming. No automatic retry was started."),
    "POLICY_REJECTED": ("The requested operation is outside this project's allowed policy.", "Check the selected workspace and /mode. For an enrolled DEV or TEST workspace, /workspace enable-write enables authoring."),
    "LOCAL_RESOURCE": ("A required local file, executable, or directory is unavailable.", "Check the project configuration and /doctor."),
    "INVALID_REQUEST": ("Ray could not accept the request or configuration.", "Check the command inputs and selected project. Use /help for supported commands."),
    "UNKNOWN": ("Ray encountered an unexpected error. Its detailed cause was not safely available.", "Check /doctor and the current task state before explicitly resuming."),
}


class RayError(RuntimeError):
    def __init__(self, code):
        self.code = code if isinstance(code, str) and code in ERRORS else "UNKNOWN"
        super().__init__(ERRORS[self.code][0])


def model_error_code(error):
    """Use SDK error metadata and known phrases only to select fixed public text."""
    if hasattr(error, "model_dump"):
        error = error.model_dump(mode="json")
    if not isinstance(error, dict):
        error = {"message": str(error)}
    info = error.get("codex_error_info", error.get("codexErrorInfo"))
    info = getattr(info, "value", info)
    known = {"unauthorized": "MODEL_AUTH_REQUIRED", "usageLimitExceeded": "MODEL_LIMIT",
             "sessionBudgetExceeded": "MODEL_LIMIT", "contextWindowExceeded": "MODEL_CONTEXT_LIMIT",
             "sandboxError": "SANDBOX_FAILED"}
    if isinstance(info, str) and info in known:
        return known[info]
    message = str(error.get("message", "")).lower()
    if "invalid schema" in message or "invalid_json_schema" in message:
        return "MODEL_SCHEMA_REJECTED"
    if "unauthorized" in message or "authentication required" in message or "401" in message:
        return "MODEL_AUTH_REQUIRED"
    if any(term in message for term in ("usage limit", "rate limit", "quota exceeded", "429")):
        return "MODEL_LIMIT"
    if "context window" in message and "exceed" in message:
        return "MODEL_CONTEXT_LIMIT"
    return "MODEL_UNAVAILABLE"


def describe_error(exc, stage="task"):
    from .model_progress import ModelTimeout
    if isinstance(exc, ModelTimeout):
        code = exc.code
    elif isinstance(exc, RayError):
        code = exc.code
    elif isinstance(exc, (TimeoutError, subprocess.TimeoutExpired)):
        code = "TIMED_OUT"
    elif isinstance(exc, (ValidationError, json.JSONDecodeError)):
        code = "MODEL_OUTPUT_INVALID" if stage in {"model", "conversation", "review"} else "INVALID_REQUEST"
    elif isinstance(exc, (FileNotFoundError, PermissionError)):
        code = "LOCAL_RESOURCE"
    elif stage == "fabric_read":
        code = "FABRIC_UNAVAILABLE"
    elif isinstance(exc, ValueError):
        from .fabric import PolicyError
        code = "POLICY_REJECTED" if isinstance(exc, PolicyError) else "INVALID_REQUEST"
    else:
        code = "UNKNOWN"
    stage = stage if stage in {"task", "model", "review", "validation", "fabric_read", "cloud_action", "conversation"} else "task"
    message, next_step = ERRORS[code]
    result = {"code": code, "stage": stage, "message": message, "next_step": next_step}
    if isinstance(exc, ModelTimeout):
        result["timing"] = exc.details
    if code == "MODEL_OUTPUT_INVALID" and isinstance(exc, (ValidationError, json.JSONDecodeError)):
        result["validation_issues"] = validation_issues(exc)
    return result


def error_text(error):
    # Rehydrate fixed catalog text even when reading stored data.
    code = error.get("code") if isinstance(error, dict) else "UNKNOWN"
    code = code if isinstance(code, str) and code in ERRORS else "UNKNOWN"
    message, next_step = ERRORS[code]
    text = f"Error: {message}\nNext step: {next_step}\nReference: {code}"
    issues = error.get("validation_issues") if isinstance(error, dict) else None
    if code == "MODEL_OUTPUT_INVALID" and isinstance(issues, list):
        for issue in issues[:5]:
            if (isinstance(issue, dict) and isinstance(issue.get("field"), str) and issue["field"] in VALIDATION_FIELDS
                    and isinstance(issue.get("code"), str) and issue["code"] in VALIDATION_CODES):
                text += f"\nReply check: {issue['field']} ({issue['code']})."
    if isinstance(error, dict) and error.get("timing"):
        timing = error["timing"]
        if isinstance(timing, dict) and all(type(timing.get(k)) is int for k in ("elapsed_seconds", "idle_seconds", "total_limit_seconds")):
            text += f"\nTiming: {timing['elapsed_seconds']}s elapsed; {timing['idle_seconds']}s without progress; {timing['total_limit_seconds']}s maximum."
    return text


def record_failure(store, project_id, task_id, exc, stage="task", status="ERROR"):
    error = describe_error(exc, stage)
    previous = store.task(project_id, task_id)
    report = json.loads(previous["result"]) if previous.get("result") else {}
    report.update(status=status.lower(), message=error["message"], error=error, cloud_eligible=False)
    store.update(project_id, task_id, status, result=report)
    return error
