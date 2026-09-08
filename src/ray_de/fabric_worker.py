"""Pinned Fabric CLI authentication with exactly one host-controlled HTTP request.
Runs in a subprocess with the project's isolated Fabric profile. Never prints tokens.
"""

import contextlib, io, json, sys, urllib.request, urllib.error, re
from importlib.metadata import version
from pathlib import Path
from urllib.parse import urlsplit, parse_qs, urlencode


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


class SignInRequired(RuntimeError):
    pass


def request(method, endpoint, payload=None, *, token=None, opener=None):
    if method not in {"get", "post", "patch"}:
        raise ValueError("Unsupported method")
    uri = urlsplit(endpoint)
    if (
        uri.scheme
        or uri.netloc
        or uri.fragment
        or endpoint.startswith("/")
        or ".." in uri.path.split("/")
        or not uri.path.startswith(("workspaces/", "operations/"))
    ):
        raise ValueError("Invalid Fabric endpoint")
    query = parse_qs(uri.query, keep_blank_values=True, strict_parsing=True)
    if set(query) - {"continuationToken", "format", "beta"} or any(
        len(v) != 1 for v in query.values()
    ):
        raise ValueError("Unsupported query")
    if "beta" in query:
        guid = r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
        notebook_output = (method == "get" and query == {"beta": ["true"]} and re.fullmatch(
            rf"workspaces/{guid}/notebooks/{guid}/jobs/execute/instances/{guid}", uri.path))
        environment_publish = (method == "post" and query == {"beta": ["false"]} and re.fullmatch(
            rf"workspaces/{guid}/environments/{guid}/staging/publish", uri.path))
        if not (notebook_output or environment_publish):
            raise ValueError("Unsupported versioned Fabric route")
    if "format" in query and (not uri.path.endswith("/getDefinition") or not re.fullmatch(r"[A-Za-z][A-Za-z0-9_-]{0,79}", query["format"][0])):
        raise ValueError("Unsupported format")
    url = "https://api.fabric.microsoft.com/v1/" + uri.path
    if query:
        url += "?" + urlencode({k: v[0] for k, v in query.items()})
    if token is None:
        if version("ms-fabric-cli") != "1.7.0":
            raise RuntimeError("Fabric auth adapter requires ms-fabric-cli 1.7.0")
        from fabric_cli.core.fab_auth import FabAuth
        from fabric_cli.core.fab_exceptions import FabricCLIError
        from fabric_cli.core import fab_constant
        from ray_de.fabric_auth import load_service_principal
        try:
            token = load_service_principal(FabAuth()).get_access_token(
                ["https://api.fabric.microsoft.com/.default"], interactive_renew=False
            )
        except PermissionError:
            raise SignInRequired("Local SP credential needs enrollment") from None
        except FabricCLIError as exc:
            if exc.status_code == fab_constant.ERROR_AUTHENTICATION_FAILED:
                raise SignInRequired("Local Fabric sign-in needs renewal") from None
            raise
    if not isinstance(token, str) or not token:
        raise SignInRequired("Fabric profile needs explicit login")
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode()
        if payload is not None
        else (b"" if method == "post" else None),
        headers={
            "Authorization": "Bearer " + str(token),
            "Content-Type": "application/json",
        },
        method=method.upper(),
    )
    opener = opener or urllib.request.build_opener(NoRedirect())
    try:
        response = opener.open(req, timeout=60)
    except urllib.error.HTTPError as exc:
        response = exc
    with response:
        raw = response.read(16_000_001)
        if len(raw) > 16_000_000:
            raise RuntimeError("Fabric response too large")
        status = response.status
        # Error payloads can contain customer data; the parent only needs a status.
        body = json.loads(raw) if raw and status in {200, 201, 202, 204} else {}
        return {"status_code": status, "text": body, "headers": dict(response.headers)}


def main():
    try:
        payload = (
            json.loads(Path(sys.argv[3]).read_text(encoding="utf-8"))
            if len(sys.argv) > 3
            else None
        )
        with (
            contextlib.redirect_stdout(io.StringIO()),
            contextlib.redirect_stderr(io.StringIO()),
        ):
            result = request(sys.argv[1], sys.argv[2], payload)
        print(json.dumps(result))
        return 0
    except SignInRequired:
        print('{"error_code":"FABRIC_SIGNIN_REQUIRED"}')
        return 1
    except BaseException:
        print(
            '{"error":"Fabric request unavailable; inspect before retrying a mutation"}'
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
