"""Explicit local SP enrollment and project-bound Windows credential loading.

Only Fabric/SQL workers load these credentials. No dotenv values enter the
orchestrator, model environment, task results, or command-line arguments.
"""

import contextlib
import io
import json
import os
import sys
from importlib.metadata import version
from pathlib import Path
from uuid import UUID

from .secrets import _crypt


def _credential_path(auth):
    auth_file = getattr(auth, "auth_file", None)
    return Path(auth_file).with_name("ray-service-principal.dpapi") if auth_file else None


def load_service_principal(auth):
    path = _credential_path(auth)
    if path is None or not path.exists() or auth.get_identity_type() != "service_principal":
        return auth
    try:
        from fabric_cli.core import fab_constant
        if path.stat().st_size > 32768:
            raise ValueError()
        data = json.loads(_crypt(path.read_bytes(), True))
        if (set(data) != {"version", "profile", "tenant_id", "client_id", "client_secret"}
                or data["version"] != 1
                or data["profile"] != str(Path(auth.auth_file).resolve())
                or data["tenant_id"] != auth.get_tenant_id()
                or data["client_id"] != auth._get_auth_property(fab_constant.FAB_SPN_CLIENT_ID)
                or not isinstance(data["client_secret"], str)
                or not 1 <= len(data["client_secret"]) <= 8192):
            raise ValueError()
        auth.set_spn(data["client_id"], password=data["client_secret"])
    except Exception:
        raise PermissionError("Project service principal credential needs local enrollment") from None
    return auth


def read_enrollment(path):
    """Parse only the three supported literal values; never expand shell syntax."""
    try:
        path = Path(path)
        if path.stat().st_size > 16384:
            raise ValueError()
        values = {}
        for line in path.read_text(encoding="utf-8-sig").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            key, value = line.split("=", 1)
            key, value = key.strip(), value.strip()
            if key in values:
                raise ValueError()
            if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
                value = value[1:-1]
            values[key] = value
        if set(values) != {"FAB_TENANT_ID", "FAB_SPN_CLIENT_ID", "FAB_SPN_CLIENT_SECRET"}:
            raise ValueError()
        for key in ("FAB_TENANT_ID", "FAB_SPN_CLIENT_ID"):
            if str(UUID(values[key])) != values[key].lower():
                raise ValueError()
            values[key] = values[key].lower()
        if not 1 <= len(values["FAB_SPN_CLIENT_SECRET"]) <= 8192:
            raise ValueError()
        return values
    except Exception:
        raise ValueError("Local SP file must contain tenant ID, client ID and a nonempty client secret") from None


def enroll(path):
    if version("ms-fabric-cli") != "1.7.0" or os.name != "nt":
        raise RuntimeError("SP enrollment requires Windows and the pinned Fabric adapter")
    from fabric_cli.core.fab_auth import FabAuth
    import msal
    values = read_enrollment(path)
    tenant, client, secret = (values[k] for k in
        ("FAB_TENANT_ID", "FAB_SPN_CLIENT_ID", "FAB_SPN_CLIENT_SECRET"))
    # A new in-memory cache proves the supplied credential actually works;
    # an existing Fabric access token cannot make a bad secret appear valid.
    app = msal.ConfidentialClientApplication(client_id=client, client_credential=secret,
        authority="https://login.microsoftonline.com/" + tenant, token_cache=msal.TokenCache())
    for scope in ("https://api.fabric.microsoft.com/.default", "https://database.windows.net/.default"):
        result = app.acquire_token_for_client(scopes=[scope])
        if not isinstance(result, dict) or not result.get("access_token"):
            raise PermissionError("Service principal could not acquire required resource tokens")
    auth = FabAuth()
    auth.set_tenant(tenant)
    auth.set_spn(client, password=secret)
    data = dict(version=1, profile=str(Path(auth.auth_file).resolve()),
                tenant_id=tenant, client_id=client, client_secret=secret)
    protected = _crypt(json.dumps(data).encode())
    target = _credential_path(auth)
    temporary = target.with_suffix(".tmp")
    temporary.write_bytes(protected)
    os.replace(temporary, target)
    return {"service_principal": "ready", "fabric_token": "verified_fresh",
            "sql_token": "verified_fresh", "storage": "Windows DPAPI, project-bound",
            "note": "Resource permissions require separate live access checks."}


def main(path):
    try:
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            result = enroll(path)
        print(json.dumps(result))
        return 0
    except Exception:
        # Never serialize third-party exception text, parsed values or tokens.
        print('{"error_code":"FABRIC_SP_ENROLLMENT_FAILED"}')
        return 1


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("Expected a local enrollment file path")
    raise SystemExit(main(sys.argv[1]))
