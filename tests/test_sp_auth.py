import io
import json
import os
from pathlib import Path

import pytest

from ray_de import fabric_worker, sql_worker
from ray_de.secrets import _crypt


@pytest.fixture
def enrolled_auth(tmp_path, monkeypatch):
    if os.name != "nt":
        pytest.skip("Windows DPAPI credential enrollment")
    from fabric_cli.core import fab_auth, fab_constant
    from fabric_cli.core.fab_exceptions import FabricCLIError

    class Auth:
        auth_file = str(tmp_path / "auth.json")
        credential = None
        def get_identity_type(self):
            return "service_principal"
        def get_tenant_id(self):
            return "11111111-1111-1111-1111-111111111111"
        def _get_auth_property(self, key):
            return "22222222-2222-2222-2222-222222222222"
        def set_spn(self, client_id, password):
            assert client_id == self._get_auth_property(None)
            self.credential = password
        def get_access_token(self, scopes, interactive_renew):
            assert interactive_renew is False
            if self.credential != "synthetic-local-secret":
                raise FabricCLIError("No credential", fab_constant.ERROR_AUTHENTICATION_FAILED)
            return "synthetic-token"

    auth = Auth()
    payload = dict(version=1, profile=str(Path(auth.auth_file).resolve()),
                   tenant_id=auth.get_tenant_id(), client_id=auth._get_auth_property(None),
                   client_secret="synthetic-local-secret")
    protected = tmp_path / "ray-service-principal.dpapi"
    protected.write_bytes(_crypt(json.dumps(payload).encode()))
    monkeypatch.setattr(fab_auth, "FabAuth", lambda: auth)
    return auth, protected, payload


@pytest.mark.parametrize("scope", ["fabric", "sql"])
def test_fresh_worker_can_acquire_uncached_sp_token(enrolled_auth, scope):
    auth, _, _ = enrolled_auth
    if scope == "sql":
        assert sql_worker.sql_token() == "synthetic-token"
    else:
        class Response(io.BytesIO):
            status = 200
            headers = {}
        class Opener:
            def open(self, request, timeout):
                assert request.get_header("Authorization") == "Bearer synthetic-token"
                return Response(b'{"id":"ok"}')
        assert fabric_worker.request("get", "workspaces/example", opener=Opener())["status_code"] == 200
    assert auth.credential == "synthetic-local-secret"


def test_credential_cannot_be_reused_by_another_project(enrolled_auth):
    auth, protected, payload = enrolled_auth
    payload["profile"] = str(Path(auth.auth_file).parent / "other" / "auth.json")
    protected.write_bytes(_crypt(json.dumps(payload).encode()))
    with pytest.raises(PermissionError):
        sql_worker.sql_token()
    assert auth.credential is None


def test_enrollment_validates_both_scopes_without_existing_tokens(enrolled_auth, monkeypatch, tmp_path, capsys):
    import msal
    from ray_de import fabric_auth
    auth, protected, _ = enrolled_auth
    auth.set_tenant = lambda tenant: None
    scopes = []
    class App:
        def __init__(self, **kwargs):
            assert kwargs["client_credential"] == "synthetic-local-secret"
            assert isinstance(kwargs["token_cache"], msal.TokenCache)
            assert not list(kwargs["token_cache"].search("AccessToken"))
        def acquire_token_for_client(self, **kwargs):
            scopes.extend(kwargs["scopes"])
            print("synthetic-token")
            return {"access_token": "synthetic-token"}
    monkeypatch.setattr(msal, "ConfidentialClientApplication", App)
    source = tmp_path / ".env"
    source.write_text("FAB_TENANT_ID=" + auth.get_tenant_id() + "\nFAB_SPN_CLIENT_ID="
                      + auth._get_auth_property(None) + "\nFAB_SPN_CLIENT_SECRET=synthetic-local-secret\n")
    assert fabric_auth.main(source) == 0
    assert scopes == ["https://api.fabric.microsoft.com/.default", "https://database.windows.net/.default"]
    output = capsys.readouterr().out
    assert "synthetic-token" not in output and "synthetic-local-secret" not in output
    assert b"synthetic-local-secret" not in protected.read_bytes()
    assert json.loads(output)["storage"] == "Windows DPAPI, project-bound"


def test_enrollment_failure_never_leaks_input(tmp_path, capsys):
    from ray_de.fabric_auth import main
    source = tmp_path / ".env"
    source.write_text("FAB_SPN_CLIENT_SECRET=synthetic-private-value\n")
    assert main(source) == 1
    assert capsys.readouterr().out.strip() == '{"error_code":"FABRIC_SP_ENROLLMENT_FAILED"}'


@pytest.mark.parametrize("change", ["tenant_id", "client_id"])
def test_stale_identity_cannot_load_saved_credential(enrolled_auth, change):
    auth, protected, payload = enrolled_auth
    payload[change] = "33333333-3333-3333-3333-333333333333"
    protected.write_bytes(_crypt(json.dumps(payload).encode()))
    with pytest.raises(PermissionError):
        sql_worker.sql_token()
    assert auth.credential is None


def test_sp_enrollment_cli_accepts_only_file_path():
    from ray_de.cli import parser
    args = parser().parse_args(["--project", "config.yaml", "login", "fabric", "--service-principal-env", ".env"])
    assert args.service_principal_env == Path(".env")
