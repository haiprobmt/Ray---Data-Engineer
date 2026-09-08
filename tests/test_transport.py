import io, json, urllib.error
from email.message import Message
import pytest
from ray_de.fabric_worker import request, NoRedirect


def test_notebook_output_beta_route_is_read_only():
    path = "workspaces/11111111-1111-1111-1111-111111111111/notebooks/22222222-2222-2222-2222-222222222222/jobs/execute/instances/33333333-3333-3333-3333-333333333333?beta=true"
    assert request("get", path, token="synthetic", opener=Opener())["status_code"] == 200
    for method, endpoint in [("post", path), ("get", path.replace("true", "false")),
                             ("get", "workspaces/example/items?beta=true")]:
        with pytest.raises(ValueError):
            request(method, endpoint, token="synthetic", opener=Opener())


def test_environment_publish_version_route_reaches_transport():
    path = "workspaces/11111111-1111-1111-1111-111111111111/environments/22222222-2222-2222-2222-222222222222/staging/publish?beta=false"
    opener = Opener()
    assert request("post", path, token="synthetic", opener=opener)["status_code"] == 200
    assert len(opener.calls) == 1
    assert opener.calls[0].method == "POST"
    assert opener.calls[0].full_url == "https://api.fabric.microsoft.com/v1/" + path
    for method, endpoint in [("get", path), ("patch", path), ("post", path.replace("false", "true")),
                             ("post", path + "&format=FabricGitSource"),
                             ("post", path.replace("/staging/publish", "/publish"))]:
        with pytest.raises(ValueError):
            request(method, endpoint, token="synthetic", opener=Opener())


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
