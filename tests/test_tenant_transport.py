"""Credential/HTTP boundary tests use synthetic responses, never real credentials."""
import io
import json
import urllib.error
from email.message import Message

import pytest

from ray_de.tenant_worker import request, authentication, endpoint_path
from ray_de.tenant_config import TenantConfig
from ray_de.tenant_credentials import save, load
from ray_de.fabric import TenantGateway
from ray_de.schemas import TurnResult

TENANT = "11111111-1111-1111-1111-111111111111"
CLIENT = "22222222-2222-2222-2222-222222222222"
WS = "33333333-3333-3333-3333-333333333333"
ITEM = "44444444-4444-4444-4444-444444444444"
GITHUB = {"owner": "hai", "repository": "fabric", "branch": "main", "credential_ref": "github"}


def envelope(provider="fabric", method="get", endpoint="workspaces"):
    return dict(provider=provider, method=method, endpoint=endpoint, tenant_id=TENANT, client_id=CLIENT, github=GITHUB)


class Response(io.BytesIO):
    def __init__(self, body, status=200):
        super().__init__(json.dumps(body).encode())
        self.status, self.headers = status, Message()


class Opener:
    def __init__(self, body=None, status=200):
        self.calls, self.body, self.status = [], body or {"value": []}, status
    def open(self, req, timeout):
        self.calls.append(req)
        return Response(self.body, self.status)


@pytest.mark.parametrize("provider,method,path", [
    ("fabric", "post", "workspaces"), ("fabric", "post", f"workspaces/{WS}/provisionIdentity"),
    ("fabric", "post", f"workspaces/{WS}/git/connect"), ("fabric", "post", "connections"),
    ("graph", "post", "applications"), ("graph", "post", f"groups/{WS}/members/$ref"),
    ("github", "post", "user/repos"), ("github", "post", "repos/hai/fabric/git/commits"),
    ("github", "patch", "repos/hai/fabric/git/refs/heads/main"),
])
def test_supported_worker_routes_reach_only_fixed_hosts(provider, method, path):
    opener = Opener()
    request(envelope(provider, method, path), token="synthetic-token", opener=opener)
    assert len(opener.calls) == 1
    assert opener.calls[0].method == method.upper()
    assert "Bearer synthetic-token" == opener.calls[0].headers["Authorization"]
    if provider == "fabric":
        assert opener.calls[0].headers["X-ms-fabric-skill"] == "git-integration-operations-cli"


@pytest.mark.parametrize("provider,method,path", [
    ("fabric", "delete", f"workspaces/{WS}"), ("fabric", "post", "admin/tenantsettings"),
    ("fabric", "post", f"workspaces/{WS}/delete"), ("fabric", "get", "https://evil.example/workspaces"),
    ("fabric", "get", "workspaces/../connections"), ("fabric", "get", "workspaces/%2e%2e/connections"),
    ("graph", "post", "roleManagement/directory/roleAssignments"), ("graph", "patch", f"applications/{WS}"),
    ("github", "patch", "repos/other/fabric/git/refs/heads/main"),
    ("github", "patch", "repos/hai/fabric/git/refs/heads/production"),
    ("github", "post", "repos/hai/fabric/actions/workflows/run/dispatches"),
])
def test_worker_rejects_unimplemented_or_out_of_scope_routes_before_auth(provider, method, path):
    opener = Opener()
    with pytest.raises(ValueError): request(envelope(provider, method, path), token="synthetic", opener=opener)
    assert not opener.calls


def test_connection_secret_resolved_only_inside_worker_and_never_returned():
    secret = "synthetic-private-credential"
    data = envelope("fabric", "post", "connections")
    data["payload"] = {"credentialDetails": {"credentials": {"credentialType": "Key", "key": {"secretRef": "github"}}}}
    opener = Opener({"id": ITEM, "message": "credential: " + secret, "credentialDetails": {"credentials": {"key": secret}}})
    result = request(data, token="synthetic-bearer", secret_loader=lambda name: secret, opener=opener)
    assert secret in opener.calls[0].data.decode()
    assert secret not in json.dumps(result)
    assert data["payload"]["credentialDetails"]["credentials"]["key"] == {"secretRef": "github"}


def test_raw_credential_payload_cannot_reach_http():
    data = envelope("fabric", "post", "connections")
    data["payload"] = {"credentials": {"key": "synthetic-raw-secret"}}
    opener = Opener()
    with pytest.raises(ValueError): request(data, token="synthetic", opener=opener)
    assert not opener.calls


def test_error_response_neither_retried_nor_exposed():
    opener = Opener({"message": "synthetic-sensitive-error"}, 403)
    result = request(envelope("fabric", "post", "workspaces"), token="synthetic", opener=opener)
    assert result["text"] == {} and len(opener.calls) == 1


def test_wrong_enrolled_tenant_rejected_without_token_request(monkeypatch):
    from fabric_cli.core import fab_auth
    class Auth:
        def get_identity_type(self): return "service_principal"
        def get_tenant_id(self): return WS
        def get_access_token(self, *args, **kwargs): raise AssertionError("must not acquire token")
    monkeypatch.setattr(fab_auth, "FabAuth", Auth)
    with pytest.raises(PermissionError, match="tenant/client"): authentication(TENANT, CLIENT)


def test_protected_store_binds_tenant_client_and_profile(tmp_path, monkeypatch):
    import ray_de.tenant_credentials as credentials
    # Synthetic crypt boundary: this test asserts binding, not DPAPI acceptance.
    monkeypatch.setattr(credentials, "_crypt", lambda value, *args: value)
    save(tmp_path, TENANT, CLIENT, "github", "synthetic-private")
    assert load(tmp_path, TENANT, CLIENT, "github") == "synthetic-private"
    with pytest.raises(PermissionError): load(tmp_path, WS, CLIENT, "github")
    with pytest.raises(PermissionError): load(tmp_path, TENANT, WS, "github")
    with pytest.raises(ValueError): save(tmp_path, TENANT, CLIENT, "../escape", "synthetic")


def test_generated_application_secret_is_saved_before_receipt():
    data = envelope("graph", "post", f"applications/{ITEM}/addPassword")
    data["payload"] = {"passwordCredential": {"displayName": "Execution"}, "storeSecretRef": "execution"}
    secret = "synthetic-generated-password"
    opener = Opener({"secretText": secret, "hint": secret[:3], "keyId": WS})
    stored = []
    result = request(data, token="synthetic-bearer", opener=opener, secret_writer=lambda name, value: stored.append((name, value)))
    assert stored == [("execution", secret)]
    assert result["text"]["credential_stored"] is True
    assert secret not in json.dumps(result) and "hint" not in result["text"]
    assert "storeSecretRef" not in json.loads(opener.calls[0].data)


def test_source_schema_remains_closed_for_codex_structured_output():
    schema = TurnResult.model_json_schema()
    def visit(value):
        if isinstance(value, dict):
            if value.get("type") == "object":
                assert value.get("additionalProperties") is False
            for child in value.values(): visit(child)
        elif isinstance(value, list):
            for child in value: visit(child)
    visit(schema)
