import io, json, urllib.error
from email.message import Message
import pytest
from ray_de.fabric_worker import request, NoRedirect


class Response(io.BytesIO):
    status = 200

    def __init__(self, body):
        super().__init__(json.dumps(body).encode())
        self.headers = Message()


class Opener:
    def __init__(self):
        self.calls = []

    def open(self, req, timeout):
        self.calls.append(req)
        return Response({"value": [], "continuationToken": "next"})


def test_single_request_no_hidden_pagination_and_one_encoding():
    opener = Opener()
    r = request(
        "get",
        "workspaces/a/items?continuationToken=a%26b%3D%2F%2B%2C",
        token="dummy",
        opener=opener,
    )
    assert len(opener.calls) == 1 and r["text"]["continuationToken"] == "next"
    assert opener.calls[0].full_url.endswith("continuationToken=a%26b%3D%2F%2B%2C")


def test_post_error_not_retried_or_payload_exposed():
    class Error:
        calls = 0

        def open(self, req, timeout):
            self.calls += 1
            raise urllib.error.HTTPError(
                req.full_url, 429, "busy", Message(), io.BytesIO(b"secret-detail")
            )

    opener = Error()
    result = request(
        "post",
        "workspaces/a/items/b/updateDefinition",
        {},
        token="dummy",
        opener=opener,
    )
    assert opener.calls == 1 and result["status_code"] == 429 and result["text"] == {}


def test_redirects_never_forward_token():
    assert NoRedirect().redirect_request(None, None, None, None, None, None) is None


@pytest.mark.parametrize(
    "path",
    [
        "https://evil.com/a",
        "/workspaces/a",
        "workspaces/../secret",
        "workspaces/a?evil=true",
        "workspaces/a?format=evil",
    ],
)
def test_worker_endpoint_rejected(path):
    with pytest.raises(ValueError):
        request("get", path, token="dummy", opener=Opener())


def test_auth_adapter_uses_scope_list_without_interactive_renew(monkeypatch):
    from fabric_cli.core import fab_auth

    calls = []

    class Auth:
        def get_access_token(self, scopes, interactive_renew):
            calls.append((scopes, interactive_renew))
            return "synthetic-token"

    monkeypatch.setattr(fab_auth, "FabAuth", Auth)
    request("get", "workspaces/a", opener=Opener())
    assert calls == [(["https://api.fabric.microsoft.com/.default"], False)]


def test_expired_profile_reports_local_signin_without_http(monkeypatch):
    from fabric_cli.core import fab_auth, fab_constant
    from fabric_cli.core.fab_exceptions import FabricCLIError
    from ray_de.fabric_worker import SignInRequired
    class Auth:
        def get_access_token(self, *a, **kw):
            raise FabricCLIError("private auth detail", status_code=fab_constant.ERROR_AUTHENTICATION_FAILED)
    monkeypatch.setattr(fab_auth, "FabAuth", Auth)
    opener = Opener()
    with pytest.raises(SignInRequired) as error:
        request("get", "workspaces/a", opener=opener)
    assert "private auth detail" not in str(error.value) and not opener.calls
