import json
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest

from ray_de import fmd
from ray_de.artifacts import compile_definitions, validate_sources
from ray_de.schemas import CloudProposal


def guid():
    return str(uuid4())


@pytest.fixture
def source(tmp_path, monkeypatch):
    root = tmp_path / "source"
    root.mkdir()
    files = {
        "config/lakehouse_deployment.json": [{"name": "LH_DATA_LANDINGZONE.Lakehouse", "type": "Lakehouse", "id": guid()}],
        "config/data_deployment.json": [{"id": guid(), "database": "old-database", "endpoint": "old.database.fabric.microsoft.com"}],
        "config/item_deployment.json": [
            {"name": name, "type": name.rsplit(".", 1)[1], "id": guid()} for name in [
                "NB_FMD_LOAD_BRONZE_SILVER.Notebook", "PL_FMD_LOAD_ALL.DataPipeline", "ENV_FMD.Environment", "VAR_CONFIG_FMD.VariableLibrary",
                "PL_FMD_LDZ_COMMAND_ADF.DataPipeline"]],
    }
    config = {"workspaces": {"workspace_" + x: guid() for x in ["config", "code", "data"]},
              "connections": {x: guid() for x in ["CON_FMD_FABRIC_SQL", "CON_FMD_FABRIC_PIPELINES", "CON_FMD_FABRIC_NOTEBOOKS"]},
              "database": {"endpoint": "old.datawarehouse.fabric.microsoft.com,1433"}}
    files["config/item_config.yaml"] = json.dumps(config)
    files["src/ENV_FMD.Environment/Setting/Sparkcompute.yml"] = "runtime_version: 2.0\n"
    header = '# Fabric notebook source\n# METADATA ********************\n# META {"kernel_info":{"name":"synapse_pyspark"},"dependencies":{}}\n# CELL ********************\n'
    files["src/NB_FMD_LOAD_BRONZE_SILVER.Notebook/notebook-content.py"] = header + '''from datetime import datetime
start_audit_time = datetime(2025, 1, 1)
end_audit_time = datetime(2025, 1, 2)
result_data = {"StartTime" : start_audit_time, "EndTime" : end_audit_time}
'''
    files["src/NB_FMD_CUSTOM_DQ_CLEANSING.Notebook/notebook-content.py"] = header + "pass\n"
    files["src/VAR_CONFIG_FMD.VariableLibrary/variables.json"] = {"variables": [{"name": "fmd_fabric_db_name", "type": "String", "value": ""}]}
    files["src/VAR_CONFIG_FMD.VariableLibrary/settings.json"] = {"valueSetsOrder": ["Production"]}
    pipeline_id = files["config/item_deployment.json"][1]["id"]
    adf_id = files["config/item_deployment.json"][-1]["id"]
    files["src/PL_FMD_LOAD_ALL.DataPipeline/pipeline-content.json"] = {"properties": {"activities": [
        {"type": "InvokePipeline", "name": "PL_FMD_LOAD_BRONZE", "typeProperties": {"pipelineId": pipeline_id, "workspaceId": fmd.ZERO},
         "dependsOn": [{"activity": "PL_FMD_LOAD_LANDINGZONE", "dependencyConditions": ["Completed"]}],
         "externalReferences": {"connection": config["connections"]["CON_FMD_FABRIC_PIPELINES"]}},
        {"type": "Fail", "name": "FA_THROW_ERROR_LDZ", "dependsOn": [{"activity": "SP_FAIL_LDZ_AUDIT_PIPELINE", "dependencyConditions": ["Completed"]},
              {"activity": "PL_FMD_LOAD_SILVER", "dependencyConditions": ["Completed"]}]},
        {"type": "InvokePipeline", "name": "ADF", "typeProperties": {"pipelineId": adf_id, "workspaceId": fmd.ZERO}},
        {"type": "SqlServerStoredProcedure", "name": "audit", "externalReferences": {"connection": config["connections"]["CON_FMD_FABRIC_SQL"]}},
    ]}}
    files["src/Config_Database/Security/integration.sql"] = "CREATE SCHEMA [integration];\nGO\n"
    files["src/Config_Database/integration/Tables/AChild.sql"] = "CREATE TABLE [integration].[AChild] (id int REFERENCES [integration].[ZParent](id));\nGO\n"
    files["src/Config_Database/integration/Tables/ZParent.sql"] = "CREATE TABLE [integration].[ZParent] (id int PRIMARY KEY);\nGO\n"
    files["src/Config_Database/integration/StoredProcedures/Check.sql"] = "CREATE PROCEDURE [integration].[Check] AS SELECT 1;\nGO\n"
    for name, content in files.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(content) if isinstance(content, (dict, list)) else content, encoding="utf-8")
    lock = tmp_path / "lock.json"
    lock.write_text(json.dumps({"files": {name: fmd.source_hash(name, (root / name).read_bytes()) for name in files}}))
    monkeypatch.setattr(fmd, "LOCK", lock)
    return root


def bindings(root):
    return fmd.Bindings(workspaces={x: guid() for x in ["configuration", "code", "data"]},
        items={i["name"]: guid() for i in fmd.inventory(root)},
        connections={x: guid() for x in ["CON_FMD_FABRIC_SQL", "CON_FMD_FABRIC_PIPELINES", "CON_FMD_FABRIC_NOTEBOOKS"]},
        sql_server="example.database.fabric.microsoft.com", sql_database="SQL_FMD_FRAMEWORK")


def test_pinned_source_tampering_fails_before_output(source, tmp_path):
    (source / "config/item_config.yaml").write_text("changed")
    with pytest.raises(ValueError, match="differs"):
        fmd.inspect_source(source)
    assert not (tmp_path / "output").exists()


@pytest.mark.parametrize("stage", ["definitions", "sql", "metadata", "verify"])
def test_missing_sql_connection_prevents_partial_deployment(source, tmp_path, stage):
    b = bindings(source).model_copy(update={"connections": {}})
    with pytest.raises(ValueError, match="after the database exists"):
        fmd.build(source, b, tmp_path / "output", stage=stage)
    assert not (tmp_path / "output").exists()


def test_foundations_are_inert_and_do_not_require_connections(source, tmp_path):
    b = fmd.Bindings(workspaces=bindings(source).workspaces)
    bundle = fmd.build(source, b, tmp_path / "out", stage="foundations")
    assert all(x["operation"] == "create_item" for x in bundle["cloud_actions"])
    assert all(x["item_id"] == "" for x in bundle["cloud_actions"])
    assert "RAY_FMD_NOT_BOUND" in str(bundle["files"])
    assert "definition has not been bound" in str(bundle["files"])
    assert not any("ADF" in x["definition_path"] for x in bundle["cloud_actions"])


def test_real_binding_repairs_and_compilation(source, tmp_path):
    b = bindings(source)
    repo = tmp_path / "repo"
    repo.mkdir()
    out = repo / "bound"
    bundle = fmd.build(source, b, out, stage="definitions")
    pipeline = json.loads(bundle["files"]["PL_FMD_LOAD_ALL.DataPipeline/pipeline-content.json.txt"])
    bronze, fail, adf, audit = pipeline["properties"]["activities"]
    assert bronze["typeProperties"]["workspaceId"] == b.workspaces["code"]
    assert bronze["dependsOn"][0]["dependencyConditions"] == ["Succeeded"]
    assert len(fail["dependsOn"]) == 1  # Must still fail when Silver was skipped.
    assert adf["type"] == "Fail"
    assert audit["externalReferences"]["connection"] == b.connections["CON_FMD_FABRIC_SQL"]
    notebook = bundle["files"]["NB_FMD_LOAD_BRONZE_SILVER.Notebook/notebook-content.py.txt"]
    namespace = {}
    exec(notebook, namespace)
    assert json.loads(json.dumps(namespace["result_data"]))["EndTime"] == "2025-01-02T00:00:00"
    fmd.write_stage(bundle, repo, out)
    for action in bundle["cloud_actions"]:
        CloudProposal.model_validate(action)
    compile_definitions(SimpleNamespace(repo=repo))
    assert validate_sources(repo) > 1


def test_inactive_or_unresolved_audit_is_rejected(source):
    b = bindings(source)
    with pytest.raises(ValueError, match="Inactive"):
        fmd.pipeline_source({"state": "Inactive", "onInactiveMarkAs": "Succeeded"}, b)
    with pytest.raises(ValueError, match="connection"):
        fmd.pipeline_source({"externalReferences": {"connection": "lookup error"}}, b)
    with pytest.raises(ValueError, match="binding"):
        fmd.pipeline_source({"notebookId": guid()}, b)


def test_sql_orders_dependencies_and_refuses_existing_install(source):
    objects = fmd.sql_objects(source)
    assert [o["name"] for o in objects][1:3] == ["[integration].[ZParent]", "[integration].[AChild]"]
    assert "database is not empty" in fmd.install_code(source)
    assert "CREATE TABLE" in fmd.install_code(source)
    assert "CREATE PROCEDURE" in fmd.install_code(source)


def test_sql_metadata_uses_code_workspace_and_parameters(source, tmp_path):
    b = bindings(source)
    code = fmd.seed_code(b, fmd.inventory(source))
    statements = []
    class Cursor:
        def execute(self, sql, parameters):
            statements.append((sql, parameters))
        def nextset(self):
            return None
    exec(code, {"cursor": Cursor()})
    pipelines = [parameters for sql, parameters in statements if "sp_UpsertPipeline" in sql]
    assert pipelines and all(p[1] == b.workspaces["code"] for p in pipelines)
    for stage in ["sql", "metadata", "verify"]:
        bundle = fmd.build(source, b, tmp_path / stage, stage=stage)
        assert [x["operation"] for x in bundle["cloud_actions"]] == ["update_definition", "run_job"]
    assert '"full_operation_accepted": False' in fmd.verification_code(source, b, fmd.inventory(source))


def test_runtime_does_not_create_missing_notebook(source):
    text = '# META {"dependencies":{}}\nif not nb_exists:\n    requests.post("cloud")\n\n# METADATA ********************\n'
    patched = fmd.notebook_source(text, bindings(source))
    assert "requests.post" not in patched
    assert "raise RuntimeError" in patched


@pytest.mark.parametrize("change", [{"environment": "PROD"}, {"environment": "TEST"}, {"sql_server": "evil.example"},
    {"sql_database": "db;password=secret"}, {"connections": {"unknown": str(uuid4())}}])
def test_bindings_fail_closed(source, change):
    with pytest.raises(ValueError):
        fmd.Bindings.model_validate(bindings(source).model_dump() | change)
