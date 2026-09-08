"""Windows protected, project/tenant/client-bound secrets, loaded only by host workers."""
import json
import os
import re
import secrets
from pathlib import Path

from .secrets import _crypt
from .tenant_config import KEY, identifier


def secret_path(profile, name):
    if not re.fullmatch(KEY, name):
        raise ValueError("Invalid credential reference")
    return Path(profile).resolve() / "ray-secrets" / (name + ".dpapi")


def save(profile, tenant_id, client_id, name, value):
    identifier(tenant_id)
    identifier(client_id)
    if not isinstance(value, str) or not value or len(value) > 8192 or any(c in value for c in "\r\n\x00"):
        raise ValueError("Credential must be one nonempty line of at most 8192 characters")
    path = secret_path(profile, name)
    path.parent.mkdir(parents=True, exist_ok=True)
    data = dict(version=1, profile=str(Path(profile).resolve()), tenant_id=tenant_id.lower(),
                client_id=client_id.lower(), name=name, value=value)
    temporary = path.with_name(secrets.token_hex(12) + ".tmp")
    try:
        temporary.write_bytes(_crypt(json.dumps(data).encode()))
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def load(profile, tenant_id, client_id, name):
    try:
        path = secret_path(profile, name)
        if path.stat().st_size > 32768:
            raise ValueError()
        data = json.loads(_crypt(path.read_bytes(), True))
        expected = dict(version=1, profile=str(Path(profile).resolve()), tenant_id=tenant_id.lower(),
                        client_id=client_id.lower(), name=name)
        if {k: v for k, v in data.items() if k != "value"} != expected or not isinstance(data["value"], str):
            raise ValueError()
        return data["value"]
    except Exception:
        raise PermissionError("Credential needs local enrollment for this tenant and identity") from None


def resolve(value, loader):
    if isinstance(value, dict):
        if "secretRef" in value:
            if set(value) != {"secretRef"}:
                raise ValueError("Invalid secret reference")
            return loader(value["secretRef"])
        return {k: resolve(v, loader) for k, v in value.items()}
    if isinstance(value, list):
        return [resolve(v, loader) for v in value]
    return value
