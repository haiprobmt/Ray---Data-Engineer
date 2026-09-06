"""Read-only, opt-in single polling cycle; scheduler ownership stays with the user."""

import hashlib, json
from .control import Control
from .fabric import FabricGateway
from .state import now


def check_once(project, store, data_dir, *, gateway=None):
    control = Control(store)
    control.token(project.id).check()
    key = "monitor:" + project.id
    try:
        snapshot = (gateway or FabricGateway(project, store, data_dir)).snapshot()
        # Ignore capture time so unchanged inventory stays quiet.
        signature = hashlib.sha256(
            json.dumps(snapshot["workspaces"], sort_keys=True).encode()
        ).hexdigest()
        old = control.setting(key)
        changed = old is not None and (
            old.get("hash") != signature or old.get("state") != "ok"
        )
        result = {"state": "ok", "hash": signature, "time": now()}
        control.set_setting(key, result)
        return {
            "project_id": project.id,
            "notify": changed,
            "event": "inventory_changed_or_recovered"
            if changed
            else ("baseline" if old is None else "unchanged"),
        }
    except Exception as exc:
        old = control.setting(key)
        control.set_setting(
            key, {"state": "error", "error": type(exc).__name__, "time": now()}
        )
        return {
            "project_id": project.id,
            "notify": not old or old.get("state") != "error",
            "event": "monitor_error",
            "error": type(exc).__name__,
        }
