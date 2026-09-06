import json
import subprocess
import pytest
from pydantic import ValidationError
from ray_de.config import ProjectConfig, load_project
from ray_de.fabric import FabricGateway, PolicyError, authorize
from conftest import WS, ITEM


@pytest.mark.parametrize(
    "operation",
    [
        "delete",
        "drop_table",
        "import_dev_item",
        "run_dev_job",
        "deploy_to_test",
        "api",
        "get_item_definition",
    ],
)
def test_unsupported_actions_never_reach_executor(project, store, tmp_path, operation):
    calls = []
    gate = FabricGateway(project, store, tmp_path, executor=lambda x: calls.append(x))
    with pytest.raises(PolicyError):
        gate.call(operation, WS, ITEM)
    assert calls == []
    with store.connect() as db:
        assert db.execute("SELECT status FROM actions").fetchone()[0] == "DENIED"


@pytest.mark.parametrize(
    "workspace",
    [
        "33333333-3333-3333-3333-333333333333",
        "../workspaces/prod",
        WS + "/items",
        "--help",
        "https://evil.example",
        "",
    ],
)
def test_wrong_workspace_never_executes(project, store, tmp_path, workspace):
    def fail(_):
        raise AssertionError("Unauthorized execution")

    with pytest.raises(PolicyError):
        FabricGateway(project, store, tmp_path, executor=fail).call(
            "list_items", workspace
        )


@pytest.mark.parametrize("environment", ["DEV", "TEST", "PROD"])
def test_read_operations_have_same_policy_in_every_environment(project, environment):
    cfg = project.config.model_dump()
    cfg["fabric"]["workspaces"][0]["environment"] = environment
    project.config = ProjectConfig.model_validate(cfg)
    assert authorize(project, "get_item", WS, ITEM) == (
        environment,
        f"workspaces/{WS}/items/{ITEM}",
    )
    with pytest.raises(PolicyError):
        authorize(project, "run_dev_job", WS, ITEM)


def test_pagination_ignores_untrusted_uri(project, store, tmp_path):
    calls = []

    def execute(endpoint):
        calls.append(endpoint)
        if len(calls) == 1:
            return {
                "value": [{"id": ITEM}],
                "continuationToken": "abc&x=/prod",
                "continuationUri": "https://evil.example",
            }
        return {"value": [{"id": "second"}]}

    result = FabricGateway(project, store, tmp_path, executor=execute).call(
        "list_items", WS
    )
    assert len(result["value"]) == 2
    assert calls[1] == f"workspaces/{WS}/items?continuationToken=abc%26x%3D%2Fprod"
    with store.connect() as db:
        assert (
            db.execute(
                "SELECT count(*) FROM actions WHERE status='SUCCEEDED'"
            ).fetchone()[0]
            == 3
        )


def test_pagination_loop_fails_without_partial_snapshot(project, store, tmp_path):
    gate = FabricGateway(
        project,
        store,
        tmp_path,
        executor=lambda _: {"value": [], "continuationToken": "again"},
    )
    with pytest.raises(RuntimeError, match="repeated"):
        gate.snapshot()


def test_every_failure_is_audited_without_exception_secret(project, store, tmp_path):
    def execute(_):
        raise RuntimeError("secret-token-value")

    gate = FabricGateway(project, store, tmp_path, executor=execute)
    with pytest.raises(RuntimeError):
        gate.call("get_workspace", WS)
    with store.connect() as db:
        rows = [dict(r) for r in db.execute("SELECT * FROM actions")]
    assert rows[0]["status"] == "FAILED"
    assert "secret-token-value" not in json.dumps(rows)


def test_list_workspaces_queries_only_configured_ids(project, store, tmp_path):
    calls = []
    gate = FabricGateway(
        project,
        store,
        tmp_path,
        executor=lambda endpoint: (calls.append(endpoint) or {"id": WS}),
    )
    assert gate.list_workspaces() == [{"id": WS}]
    assert calls == [f"workspaces/{WS}"]


def test_snapshot_includes_lakehouse_connection_metadata(project, store, tmp_path):
    calls = []
    details = {"id": ITEM, "type": "Lakehouse", "properties": {
        "sqlEndpointProperties": {"id": WS, "provisioningStatus": "Success",
            "connectionString": "example.datawarehouse.fabric.microsoft.com"}}}

    def execute(endpoint):
        calls.append(endpoint)
        if endpoint.endswith("/items"):
            return {"value": [{"id": ITEM, "type": "Lakehouse"}]}
        if endpoint.endswith("/lakehouses/" + ITEM):
            return details
        return {"id": WS}

    snapshot = FabricGateway(project, store, tmp_path, executor=execute).snapshot()
    assert snapshot["workspaces"][0]["lakehouses"] == [details]
    assert calls[-1] == f"workspaces/{WS}/lakehouses/{ITEM}"
    assert snapshot["source"] == "provided_executor"


def test_lakehouse_details_are_workspace_bound(project):
    assert authorize(project, "get_lakehouse", WS, ITEM)[1] == f"workspaces/{WS}/lakehouses/{ITEM}"
    with pytest.raises(PolicyError):
        authorize(project, "get_lakehouse", ITEM, ITEM)
    with pytest.raises(PolicyError):
        authorize(project, "get_lakehouse", WS, "../items")


def test_restricted_lakehouse_does_not_hide_workspace_inventory(project, store, tmp_path):
    from ray_de.errors import RayError
    def execute(endpoint):
        if endpoint.endswith("/items"):
            return {"value": [{"id": ITEM, "type": "Lakehouse"}]}
        if "/lakehouses/" in endpoint:
            raise RayError("FABRIC_FORBIDDEN")
        return {"id": WS}
    snapshot = FabricGateway(project, store, tmp_path, executor=execute).snapshot()
    ws = snapshot["workspaces"][0]
    assert ws["items"] and not ws["lakehouses"]
    assert ws["lakehouse_read_errors"] == [{"item_id": ITEM, "error_code": "FABRIC_FORBIDDEN"}]


@pytest.mark.parametrize("field", ["fabric_test_write", "fabric_prod_write"])
def test_config_cannot_enable_cloud_writes(project, field):
    cfg = project.config.model_dump()
    cfg["policy"][field] = True
    with pytest.raises(ValidationError):
        ProjectConfig.model_validate(cfg)


def test_duplicate_workspace_environment_rejected(project):
    cfg = project.config.model_dump()
    cfg["fabric"]["workspaces"].append({"id": WS, "environment": "PROD"})
    with pytest.raises(ValidationError):
        ProjectConfig.model_validate(cfg)


def test_duplicate_yaml_key_rejected(tmp_path):
    p = tmp_path / "config.yaml"
    p.write_text("project_id: a\nproject_id: b\n")
    with pytest.raises(ValueError, match="Duplicate"):
        load_project(p)


def test_actual_fab_response_envelope_parsed(project, store, tmp_path, monkeypatch):
    seen = []

    def run(argv, **kwargs):
        seen.append((argv, kwargs))
        return subprocess.CompletedProcess(
            argv, 0, json.dumps({"status_code": 200, "text": {"id": WS}}), ""
        )

    monkeypatch.setattr("ray_de.fabric.subprocess.run", run)
    assert FabricGateway(project, store, tmp_path).call("get_workspace", WS) == {
        "id": WS
    }
    assert seen[0][0][2:] == ["get", f"workspaces/{WS}"]
    assert seen[0][1]["shell"] is False


def test_http_error_even_with_cli_exit_zero(project, store, tmp_path, monkeypatch):
    monkeypatch.setattr(
        "ray_de.fabric.subprocess.run",
        lambda argv, **kw: subprocess.CompletedProcess(
            argv, 0, '{"status_code":403,"text":{}}', ""
        ),
    )
    with pytest.raises(RuntimeError, match="HTTP"):
        FabricGateway(project, store, tmp_path).call("get_workspace", WS)
