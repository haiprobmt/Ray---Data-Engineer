"""One fixed-provider host request. No redirects, credential output or write retries."""
import contextlib
import io
import json
import re
import sys
import urllib.error
import urllib.request
from importlib.metadata import version
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlsplit

from .fabric_worker import NoRedirect
from .tenant_config import identifier, secret_references
from .tenant_credentials import load, resolve

BASES = {"fabric": "https://api.fabric.microsoft.com/v1/", "graph": "https://graph.microsoft.com/v1.0/",
         "github": "https://api.github.com/"}
GUID = r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
SHA = r"[0-9a-f]{40}"


def endpoint_path(provider, method, endpoint, github=None):
    uri = urlsplit(endpoint)
    if (provider not in BASES or method not in {"get", "post", "patch"} or uri.scheme or uri.netloc
            or uri.fragment or endpoint.startswith("/") or ".." in uri.path.split("/") or "%" in uri.path
            or "\\" in endpoint or any(ord(c) < 32 for c in endpoint)):
        raise ValueError("Invalid managed endpoint")
    query = parse_qs(uri.query, keep_blank_values=True, strict_parsing=True)
    allowed = {"fabric": {"continuationToken", "format"}, "github": {"recursive", "per_page", "page"},
               "graph": {"$select", "$top", "$skiptoken"}}[provider]
    if set(query) - allowed or any(len(v) != 1 for v in query.values()):
        raise ValueError("Unsupported managed query")
    path = uri.path
    if provider == "fabric":
        routes = {
            "get": [r"workspaces", r"capacities", r"connections(?:/supportedConnectionTypes)?",
                    rf"connections/{GUID}(?:/roleAssignments)?", rf"operations/{GUID}(?:/result)?",
                    rf"workspaces/{GUID}(?:/(?:items|folders|roleAssignments|git/(?:connection|status|myGitCredentials)))?",
                    rf"workspaces/{GUID}/(?:items|folders|sqlDatabases)/{GUID}"],
            "post": [r"workspaces", r"connections", rf"workspaces/{GUID}/(?:assignToCapacity|provisionIdentity|roleAssignments|folders)",
                     rf"workspaces/{GUID}/git/(?:connect|initializeConnection|commitToGit|updateFromGit)",
                     rf"workspaces/{GUID}/items/{GUID}/(?:move|getDefinition)", rf"connections/{GUID}/roleAssignments"],
            "patch": [rf"connections/{GUID}", rf"workspaces/{GUID}/git/myGitCredentials", rf"workspaces/{GUID}/roleAssignments/{GUID}"],
        }
    elif provider == "graph":
        routes = {"get": [rf"(?:applications|servicePrincipals|groups)/{GUID}", rf"groups/{GUID}/members"],
                  "post": [r"applications", r"servicePrincipals", r"groups", rf"groups/{GUID}/members/\$ref", rf"applications/{GUID}/addPassword"], "patch": []}
    else:
        if not github or set(github) != {"owner", "repository", "branch", "credential_ref"}:
            raise ValueError("GitHub must be bound to an enrolled repository")
        repo = re.escape(f"repos/{github['owner']}/{github['repository']}")
        branch = re.escape(github["branch"])
        routes = {"get": [r"user", repo, repo + rf"/git/ref/heads/{branch}", repo + rf"/git/(?:commits|trees|blobs)/{SHA}"],
                  "post": [r"user/repos", repo + r"/git/(?:blobs|trees|commits|refs)"],
                  "patch": [repo + rf"/git/refs/heads/{branch}"]}
    if not any(re.fullmatch(pattern, path) for pattern in routes[method]):
        raise ValueError("Managed endpoint is outside the implemented routes")
    return path + ("?" + urlencode({k: v[0] for k, v in query.items()}) if query else "")


def authentication(tenant, client):
    if version("ms-fabric-cli") != "1.7.0":
        raise RuntimeError("Pinned Fabric adapter required")
    from fabric_cli.core.fab_auth import FabAuth
    from fabric_cli.core import fab_constant
    from .fabric_auth import load_service_principal
    auth = FabAuth()
    if (auth.get_identity_type() != "service_principal" or auth.get_tenant_id().lower() != tenant.lower()
            or auth._get_auth_property(fab_constant.FAB_SPN_CLIENT_ID).lower() != client.lower()):
        raise PermissionError("Enrolled service principal does not match the configured tenant/client")
    return load_service_principal(auth)


def request(envelope, *, opener=None, token=None, secret_loader=None, secret_writer=None):
    if set(envelope) - {"provider", "method", "endpoint", "payload", "tenant_id", "client_id", "github"}:
        raise ValueError("Unknown request fields")
    provider, method = envelope["provider"], envelope["method"]
    tenant, client = identifier(envelope["tenant_id"]), identifier(envelope["client_id"])
    github = envelope.get("github")
    path = endpoint_path(provider, method, envelope["endpoint"], github)
    payload = envelope.get("payload")
    secret_references(payload)
    if token is None:
        auth = authentication(tenant, client)
        profile = Path(auth.auth_file).resolve().parent
        secret_loader = lambda name: load(profile, tenant, client, name)
        from .tenant_credentials import save
        secret_writer = lambda name, value: save(profile, tenant, client, name, value)
        token = secret_loader(github["credential_ref"]) if provider == "github" else auth.get_access_token(
            [BASES[provider].split("/v1")[0] + "/.default"], interactive_renew=False)
    if not isinstance(token, str) or not token:
        raise PermissionError("Managed provider authentication unavailable")
    # Resolve secrets inside this process only, after target validation. Parent
    # plans, temporary files, command arguments and stdout only carry references.
    values = [token]
    def get_secret(name):
        if secret_loader is None:
            raise PermissionError("Secret reference was not enrolled")
        secret = secret_loader(name)
        values.append(secret)
        return secret
    payload = resolve(payload, get_secret)
    store_ref = None
    if provider == "graph" and method == "post" and path.endswith("/addPassword"):
        if not isinstance(payload, dict) or set(payload) != {"passwordCredential", "storeSecretRef"} or secret_writer is None:
            raise ValueError("Application credentials require protected local storage")
        payload = dict(payload)
        store_ref = payload.pop("storeSecretRef")
    headers = {"Authorization": "Bearer " + token, "Content-Type": "application/json"}
    if provider == "github":
        headers.update({"Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28", "User-Agent": "Ray"})
    if provider == "fabric":
        headers["x-ms-fabric-skill"] = "git-integration-operations-cli"
    req = urllib.request.Request(BASES[provider] + path, method=method.upper(), headers=headers,
        data=json.dumps(payload).encode() if payload is not None else (b"" if method == "post" else None))
    try:
        response = (opener or urllib.request.build_opener(NoRedirect())).open(req, timeout=60)
    except urllib.error.HTTPError as exc:
        response = exc
    with response:
        raw = response.read(16_000_001)
        if len(raw) > 16_000_000:
            raise RuntimeError("Managed response exceeds limit")
        body = json.loads(raw) if raw and response.status in {200, 201, 202, 204} else {}
        if store_ref and response.status == 200:
            secret = body.pop("secretText", None)
            if not isinstance(secret, str) or not secret:
                raise RuntimeError("Application credential response did not contain a secret")
            secret_writer(store_ref, secret)
            values.append(secret)
            body.update(credential_stored=True, credential_ref=store_ref)
        def sanitize(value):
            if isinstance(value, dict):
                result = {k: sanitize(v) for k, v in value.items() if k.lower() not in
                          {"credentials", "passwordcredentials", "keycredentials", "accesstoken", "secrettext", "hint"}}
                if "passwordCredentials" in value:
                    result["passwordCredentials"] = [{k: entry[k] for k in ("keyId", "displayName", "endDateTime") if k in entry} for entry in value["passwordCredentials"]]
                return result
            if isinstance(value, list):
                return [sanitize(v) for v in value]
            if isinstance(value, str):
                for secret in values:
                    if secret and secret in value:
                        value = value.replace(secret, "[redacted]")
            return value
        return {"status_code": response.status, "text": sanitize(body),
                "headers": {k: sanitize(v) for k, v in response.headers.items() if k.lower() in
                            {"location", "x-ms-operation-id", "retry-after"}}}


def main():
    try:
        envelope = json.loads(sys.stdin.read(8_000_001))
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            result = request(envelope)
        print(json.dumps(result))
        return 0
    except Exception:
        print('{"error":"Managed request unavailable; inspect before retrying any write"}')
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
