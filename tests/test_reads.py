import json
import io

import pytest
from pydantic import ValidationError

from conftest import WS, ITEM
from ray_de.fabric import FabricGateway, PolicyError
from ray_de.schemas import TurnResult
from ray_de.service import TaskService
from test_recovery import output


def test_notebook_output_read_is_workspace_bound_and_receipt_checked(project, store, tmp_path):
    job = "33333333-3333-3333-3333-333333333333"
    calls = []
    def execute(endpoint):
        calls.append(endpoint)
        return {"id": job, "itemId": ITEM, "properties": {"exitValue": "ok"}}
    gate = FabricGateway(project, store, tmp_path, executor=execute)
    assert gate.notebook_job(WS, ITEM, job)["properties"]["exitValue"] == "ok"
    assert calls == [f"workspaces/{WS}/notebooks/{ITEM}/jobs/execute/instances/{job}?beta=true"]
    with pytest.raises(PolicyError):
        gate.notebook_job(job, ITEM, job)
    assert len(calls) == 1
    with pytest.raises(PolicyError):
        gate.notebook_job(WS, ITEM, WS)


def test_model_can_read_real_notebook_receipt_output_without_cli(project, store, tmp_path):
    job = "33333333-3333-3333-3333-333333333333"
    calls = []
    def execute(endpoint):
        calls.append(endpoint)
        return {"id": job, "itemId": ITEM, "properties": {"exitValue": '{"rows":42}'}}
    gate = FabricGateway(project, store, tmp_path, executor=execute)
    req = dict(operation="get_notebook_job", workspace_id=WS, item_id=ITEM, job_id=job)
    result = gate.read(req)
    assert result["source"] == "provided_executor"
    assert result["data"]["properties"]["exitValue"] == '{"rows":42}'
    assert calls == [f"workspaces/{WS}/notebooks/{ITEM}/jobs/execute/instances/{job}?beta=true"]
    for bad in (dict(req, workspace_id=ITEM), dict(req, job_id="../escape"), dict(req, operation="get_lakehouse")):
        with pytest.raises((PolicyError, ValidationError)):
            gate.read(bad)
    assert len(calls) == 1


def test_sql_auth_failure_identifies_sql_instead_of_workspace_login(monkeypatch, capsys):
    from ray_de import sql_worker
    monkeypatch.setattr(sql_worker.sys, "stdin", io.StringIO(json.dumps(sql_request())))
    def fail(*a, **kw): raise PermissionError("private auth details")
    monkeypatch.setattr(sql_worker, "execute", fail)
    assert sql_worker.main() == 1
    assert json.loads(capsys.readouterr().out) == {"error_code": "FABRIC_SQL_SIGNIN_REQUIRED"}


def test_missing_sql_table_has_a_safe_specific_error(monkeypatch, capsys):
    import pyodbc
    from ray_de import sql_worker
    monkeypatch.setattr(sql_worker.sys, "stdin", io.StringIO(json.dumps(sql_request())))
    def fail(*a, **kw): raise pyodbc.ProgrammingError("42S02", "private table detail (208)")
    monkeypatch.setattr(sql_worker, "execute", fail)
    assert sql_worker.main() == 1
    assert json.loads(capsys.readouterr().out) == {"error_code": "FABRIC_SQL_TABLE_UNAVAILABLE"}


def test_read_failure_keeps_receipts_and_does_not_retry_automatically(project, store, tmp_path, monkeypatch):
    from ray_de.errors import RayError
    from ray_de.cloud import CloudActions
    task = store.create(project.id, "Verify completed ingestion", "read")
    calls, reads = [], []
    receipts = [{"id": "original-plan", "state": "SUCCEEDED", "remote": {"id": "original-job"}}]
    monkeypatch.setattr(CloudActions, "receipts", lambda *a: receipts)
    def fail(*a, **kw):
        reads.append(1)
        raise RayError("FABRIC_SQL_UNAVAILABLE")
    monkeypatch.setattr(FabricGateway, "read", fail)
    class Runner:
        def run(self, p, prompt, schema, **kwargs):
            calls.append(prompt)
            assert "original-job" in prompt
            if len(calls) > 1:
                assert 'host_read_error' in prompt and 'FABRIC_SQL_UNAVAILABLE' in prompt
            if len(calls) <= 2:
                return dict(output("working"), read_requests=[request()])
            return dict(output("blocked"), message="Ingestion receipt succeeded; independent SQL verification is unavailable.")
    result = TaskService(store, Runner(), tmp_path).run(project, task["id"], "Verify", "actor",
                snapshot={"project_id": project.id, "binding": project.binding})
    assert len(reads) == 1
    assert result["status"] == "blocked" and not result["cloud_eligible"]
    assert result["cloud_plans"] == receipts
    assert len(result["read_failures"]) == 1


def test_cli_accepts_explicit_sql_login():
    from ray_de.cli import parser
    args = parser().parse_args(["--project", "config.yaml", "login", "sql"])
    assert args.service == "sql"


def test_sql_browser_login_reuses_isolated_cache_and_never_returns_token(monkeypatch, capsys):
    from types import SimpleNamespace
    from ray_de import sql_worker
    from ray_de.cli import parser
    from fabric_cli.core import fab_auth, fab_constant
    import msal
    args = parser().parse_args(["--project", "config.yaml", "login", "sql", "--browser"])
    assert args.browser
    cache = object()
    auth = SimpleNamespace(get_identity_type=lambda: "user",
                           _get_app=lambda: SimpleNamespace(token_cache=cache),
                           _get_authority_url=lambda: "https://login.microsoftonline.com/test-tenant")
    monkeypatch.setattr(fab_auth, "FabAuth", lambda: auth)
    calls = []
    class BrowserApp:
        def __init__(self, **kwargs):
            assert kwargs["token_cache"] is cache
            assert kwargs["client_id"] == fab_constant.AUTH_DEFAULT_CLIENT_ID
            assert kwargs["authority"] == auth._get_authority_url()
            assert kwargs["enable_broker_on_windows"] is False
            assert kwargs["enable_broker_on_mac"] is False
        def acquire_token_interactive(self, **kwargs):
            calls.append(kwargs)
            print("synthetic-private-token")
            return {"access_token": "synthetic-private-token"}
    monkeypatch.setattr(msal, "PublicClientApplication", BrowserApp)
    assert sql_worker.main(login=True, browser=True) == 0
    assert calls[0]["scopes"] == ["https://database.windows.net/.default"]
    assert 0 < calls[0]["timeout"] <= 300
    text = capsys.readouterr().out
    assert "synthetic-private-token" not in text
    assert json.loads(text)["sql_signin"] == "ready"


def test_sql_browser_auth_requires_explicit_interactive_login():
    from ray_de.sql_worker import sql_token
    with pytest.raises(ValueError):
        sql_token(browser=True)


def test_explicit_sql_login_is_interactive_and_does_not_print_token(monkeypatch, capsys):
    from ray_de import sql_worker
    from fabric_cli.core import fab_auth
    calls = []
    class Auth:
        def get_access_token(self, scopes, interactive_renew):
            calls.append((scopes, interactive_renew))
            return "synthetic-private-token"
    monkeypatch.setattr(fab_auth, "FabAuth", Auth)
    assert sql_worker.main(login=True) == 0
    text = capsys.readouterr().out
    assert calls == [(["https://database.windows.net/.default"], True)]
    assert "synthetic-private-token" not in text
    assert json.loads(text)["sql_signin"] == "ready"


def request(operation="lakehouse_count", **kw):
    return dict(operation=operation, workspace_id=WS, item_id=ITEM,
                schema_name="dbo", table_name="Sales", job_id="", **kw)


def test_read_task_gets_live_evidence_without_cloud_actions(project, store, tmp_path, monkeypatch):
    task = store.create(project.id, "Verify Sales row count", "read")
    calls = []
    class Runner:
        def run(self, p, prompt, schema, **kw):
            calls.append(prompt)
            kw["on_thread"]("primary")
            if len(calls) == 1:
                return dict(output("working"), read_requests=[request()])
            assert '"row_count": 42' in prompt
            assert 'provided_executor' in prompt
            return output()
    def read(gateway, req, **kw):
        assert req == request()
        assert kw["task_id"] == task["id"]
        return {"request": req, "source": "provided_executor", "data": {"row_count": 42}}
    monkeypatch.setattr(FabricGateway, "read", read, raising=False)
    snapshot = {"project_id": project.id, "binding": project.binding, "workspaces": []}
    result = TaskService(store, Runner(), tmp_path).run(project, task["id"], "Verify", "actor", snapshot=snapshot)
    assert result["status"] == "completed"
    assert len(calls) == 2
    with store.connect() as db:
        assert db.execute("SELECT count(*) FROM plans").fetchone()[0] == 0


@pytest.mark.parametrize("field,value", [("workspace_id", ITEM), ("item_id", "../items"),
    ("schema_name", "dbo]; DELETE FROM x--"), ("table_name", "x.y"), ("operation", "execute_sql")])
def test_invalid_read_never_reaches_transport(project, store, tmp_path, field, value):
    calls = []
    gate = FabricGateway(project, store, tmp_path, executor=lambda x: calls.append(x))
    req = request(); req[field] = value
    with pytest.raises((PolicyError, ValidationError)):
        gate.read(req)
    assert not calls


def test_read_requests_cannot_mix_with_writes():
    with pytest.raises(ValidationError):
        TurnResult.model_validate(dict(output("completed"), read_requests=[request()], artifacts=[{"path": "x.py", "content": "x=1"}]))


def sql_request(operation="lakehouse_preview"):
    return dict(server="example.datawarehouse.fabric.microsoft.com", database="Sales Lakehouse",
                operation=operation, schema_name="dbo", table_name="Sales")


@pytest.mark.parametrize("key,value", [("server", "evil.example"), ("server", "example.datawarehouse.fabric.microsoft.com;PWD=bad"),
    ("database", "Sales;DROP TABLE X"), ("table_name", "Sales]--"), ("operation", "DELETE"), ("query", "SELECT 1")])
def test_sql_worker_rejects_untrusted_connection_and_query_fields(key, value):
    from ray_de.sql_worker import execute
    req = sql_request(); req[key] = value
    with pytest.raises(ValueError):
        execute(req, connect=lambda *a, **kw: pytest.fail("Invalid request connected"), token="synthetic")


def test_sql_transport_caps_preview_and_closes_resources():
    from ray_de.sql_worker import execute
    calls = []
    class Cursor:
        description = [("id",)]
        def execute(self, sql): calls.append(sql)
        def fetchmany(self, count):
            assert count == 101
            return [(i,) for i in range(101)]
        def close(self): calls.append("cursor closed")
    class Connection:
        def cursor(self): return Cursor()
        def close(self): calls.append("connection closed")
    def connect(connection_string, **kw):
        assert "Database={Sales Lakehouse}" in connection_string
        assert "TrustServerCertificate=no" in connection_string
        assert "synthetic" not in connection_string
        assert kw["timeout"] == 15
        return Connection()
    data = execute(sql_request(), connect=connect, token="synthetic")
    assert len(data["rows"]) == 100 and data["truncated"]
    assert calls == ["SELECT TOP (101) * FROM [dbo].[Sales]", "cursor closed", "connection closed"]


def test_gateway_resolves_sql_target_and_audits_without_rows(project, store, tmp_path):
    requests = []
    def metadata(endpoint):
        if "/lakehouses/" in endpoint:
            return {"id": ITEM, "properties": {"sqlEndpointProperties": {
                "id": WS, "provisioningStatus": "Success", "connectionString": sql_request()["server"]}}}
        return {"id": WS, "type": "SQLEndpoint", "displayName": "Sales Lakehouse"}
    def execute(req):
        requests.append(req)
        return {"columns": ["row_count"], "rows": [[42]], "truncated": False}
    result = FabricGateway(project, store, tmp_path, executor=metadata, sql_executor=execute).read(request())
    assert requests == [sql_request("lakehouse_count")]
    assert result["source"] == "provided_executor"
    assert result["data"]["rows"] == [[42]]
    with store.connect() as db:
        audit = json.dumps([dict(row) for row in db.execute("SELECT * FROM actions")])
    assert 'row_count' not in audit and 'SUCCEEDED' in audit


def test_sql_failure_records_category_only(project, store, tmp_path):
    def metadata(endpoint):
        if "/lakehouses/" in endpoint:
            return {"id": ITEM, "properties": {"sqlEndpointProperties": {
                "id": WS, "provisioningStatus": "Success", "connectionString": sql_request()["server"]}}}
        return {"id": WS, "type": "SQLEndpoint", "displayName": "Sales Lakehouse"}
    def fail(req): raise RuntimeError("password=synthetic-secret")
    with pytest.raises(RuntimeError):
        FabricGateway(project, store, tmp_path, executor=metadata, sql_executor=fail).read(request())
    with store.connect() as db:
        audit = json.dumps([dict(row) for row in db.execute("SELECT * FROM actions")])
    assert "synthetic-secret" not in audit and 'FAILED' in audit


def test_read_loop_is_bounded(project, store, tmp_path, monkeypatch):
    task = store.create(project.id, "Verify", "read")
    calls = []
    class Runner:
        def run(self, *a, **kw):
            return dict(output("working"), read_requests=[request()])
    monkeypatch.setattr(FabricGateway, "read", lambda *a, **kw: calls.append(1) or {"data": {}})
    snapshot = {"project_id": project.id, "binding": project.binding}
    result = TaskService(store, Runner(), tmp_path).run(project, task["id"], "Verify", "actor", snapshot=snapshot)
    assert len(calls) == 4 and result["status"] == "blocked"


def test_expired_sql_auth_uses_sql_scope_without_connecting(monkeypatch):
    from fabric_cli.core import fab_auth, fab_constant
    from fabric_cli.core.fab_exceptions import FabricCLIError
    from ray_de.sql_worker import execute
    calls = []
    class Auth:
        def get_access_token(self, scopes, *, interactive_renew):
            calls.append((scopes, interactive_renew))
            raise FabricCLIError("synthetic secret", fab_constant.ERROR_AUTHENTICATION_FAILED)
    monkeypatch.setattr(fab_auth, "FabAuth", Auth)
    with pytest.raises(PermissionError):
        execute(sql_request(), connect=lambda *a, **kw: pytest.fail("Expired auth connected"))
    assert calls == [(["https://database.windows.net/.default"], False)]


def test_cancelled_read_round_does_not_issue_another_read(project, store, tmp_path, monkeypatch):
    from ray_de.control import TaskStopped
    task = store.create(project.id, "Verify", "read")
    class Token:
        cancelled = False
        def check(self):
            if self.cancelled: raise TaskStopped("stopped")
    token = Token()
    calls = []
    class Runner:
        def run(self, *a, **kw):
            return dict(output("working"), read_requests=[request(), request()])
    def read(*a, **kw):
        calls.append(1)
        token.cancelled = True
        return {"data": {}}
    monkeypatch.setattr(FabricGateway, "read", read)
    snapshot = {"project_id": project.id, "binding": project.binding}
    with pytest.raises(TaskStopped):
        TaskService(store, Runner(), tmp_path).run(project, task["id"], "Verify", "actor", snapshot=snapshot, cancel=token)
    assert calls == [1]
    assert store.task(project.id, task["id"])["status"] == "PAUSED"


@pytest.mark.parametrize("transport", ["fabric", "sql"])
def test_worker_timeouts_are_safe_read_failures(project, store, tmp_path, monkeypatch, transport):
    import subprocess
    from ray_de.errors import RayError
    def timeout(*args, **kwargs):
        raise subprocess.TimeoutExpired("sensitive-command", 60, output="synthetic-private-value")
    monkeypatch.setattr("ray_de.fabric.subprocess.run", timeout)
    gateway = FabricGateway(project, store, tmp_path)
    with pytest.raises(RayError) as caught:
        gateway._execute("workspaces/" + WS) if transport == "fabric" else gateway._execute_sql(sql_request())
    assert caught.value.code == "TIMED_OUT"
    assert "synthetic-private-value" not in str(caught.value)


def test_structured_credentials_are_redacted_from_read_results(project, store, tmp_path):
    secret = "synthetic-private-value"
    gateway = FabricGateway(project, store, tmp_path, executor=lambda endpoint: {"id": ITEM, "properties": {"access_token": secret}})
    result = gateway.read(dict(request(), operation="get_item", table_name=""))
    assert secret not in json.dumps(result)


def test_sql_preview_credentials_are_redacted_by_column_name():
    from ray_de.memory import redact_data
    result = redact_data({"columns": ["customer_id", "access_token", "refresh_token"],
                          "rows": [["001", "synthetic-private-value", "second-private-value"]]})
    assert result["rows"] == [["001", "[REDACTED]", "[REDACTED]"]]


def test_wide_sql_preview_keeps_size_error_through_worker_and_gateway(project, store, tmp_path, monkeypatch, capsys):
    import io
    from types import SimpleNamespace
    from ray_de import sql_worker
    from ray_de.errors import RayError
    def oversized(request):
        raise OverflowError("synthetic data must not appear")
    monkeypatch.setattr(sql_worker, "execute", oversized)
    monkeypatch.setattr(sql_worker.sys, "stdin", io.StringIO(json.dumps(sql_request())))
    assert sql_worker.main() == 1
    envelope = capsys.readouterr().out
    assert json.loads(envelope) == {"error_code": "FABRIC_READ_TOO_LARGE"}
    monkeypatch.setattr("ray_de.fabric.subprocess.run", lambda *a, **k: SimpleNamespace(returncode=1, stdout=envelope))
    with pytest.raises(RayError) as caught:
        FabricGateway(project, store, tmp_path)._execute_sql(sql_request())
    assert caught.value.code == "FABRIC_READ_TOO_LARGE"
