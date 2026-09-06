import json
import io

import pytest
from pydantic import ValidationError

from conftest import WS, ITEM
from ray_de.fabric import FabricGateway, PolicyError
from ray_de.schemas import TurnResult
from ray_de.service import TaskService
from test_recovery import output


def test_sql_auth_failure_identifies_sql_instead_of_workspace_login(monkeypatch, capsys):
    from ray_de import sql_worker
    monkeypatch.setattr(sql_worker.sys, "stdin", io.StringIO(json.dumps(sql_request())))
    def fail(*a, **kw): raise PermissionError("private auth details")
    monkeypatch.setattr(sql_worker, "execute", fail)
    assert sql_worker.main() == 1
    assert json.loads(capsys.readouterr().out) == {"error_code": "FABRIC_SQL_SIGNIN_REQUIRED"}


def test_cli_accepts_explicit_sql_login():
    from ray_de.cli import parser
    args = parser().parse_args(["--project", "config.yaml", "login", "sql"])
    assert args.service == "sql"


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
                schema_name="dbo", table_name="Sales", **kw)


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
