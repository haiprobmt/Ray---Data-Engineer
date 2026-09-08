"""Offline FMD deployment compiler. It never authenticates or calls a cloud API.

Generated proposals go through Ray's ordinary validation, independent review and
CloudActions executor. Repository assets and this manifest grant no permissions.
"""
from __future__ import annotations

import argparse
import ast
import hashlib
import json
import re
from pathlib import Path
from typing import Literal

import yaml
from pydantic import Field, model_validator

from .config import StrictModel
from .fabric import canonical_id
from .memory import safe_text

REVISION = "ebe97d4f9259da249a063827f824d43c973448f0"
REPOSITORY = "https://github.com/edkreuk/FMD_FRAMEWORK"
LOCK = Path(__file__).with_name("playbooks") / "fmd-source.json"
ZERO = "00000000-0000-0000-0000-000000000000"
EXCLUDED = ("PURVIEW", "_ADF", "LOAD_DEMO_DATA")
SQL_NAME = "SQL_FMD_FRAMEWORK.SQLDatabase"
BOOTSTRAP = "NB_RAY_FMD_SQL_INSTALL.Notebook"
SEED = "NB_RAY_FMD_METADATA.Notebook"
VERIFY = "NB_RAY_FMD_VERIFY.Notebook"


class Bindings(StrictModel):
    environment: Literal["DEV"] = "DEV"
    workspaces: dict[str, str]
    items: dict[str, str] = Field(default_factory=dict)
    connections: dict[str, str] = Field(default_factory=dict)
    sql_server: str = ""
    sql_database: str = ""
    lakehouse_schema_enabled: bool = True

    @model_validator(mode="after")
    def valid(self):
        if set(self.workspaces) != {"configuration", "code", "data"}:
            raise ValueError("Supply configuration, code and data DEV workspaces")
        ids = [canonical_id(x) for x in self.workspaces.values()]
        if len(set(ids)) != 3 or ZERO in ids:
            raise ValueError("FMD requires three distinct, non-placeholder workspace IDs")
        for value in list(self.items.values()) + list(self.connections.values()):
            if canonical_id(value) == ZERO:
                raise ValueError("Placeholder IDs are not deployment bindings")
        if len(set(x.lower() for x in self.items.values())) != len(self.items):
            raise ValueError("Item bindings must be unique")
        if set(self.connections) - {"CON_FMD_FABRIC_SQL", "CON_FMD_FABRIC_PIPELINES", "CON_FMD_FABRIC_NOTEBOOKS"}:
            raise ValueError("Only FMD SQL, notebook and pipeline connections are in scope")
        if self.sql_server and not re.fullmatch(r"[a-zA-Z0-9-]+\.database\.(?:fabric\.microsoft\.com|windows\.net)", self.sql_server):
            raise ValueError("Use serverFqdn returned by get_sql_database")
        if self.sql_database and not re.fullmatch(r"[A-Za-z0-9_ -]{1,128}", self.sql_database):
            raise ValueError("Use databaseName returned by get_sql_database")
        return self


def read_source(root, relative):
    path = (root / relative).resolve(strict=True)
    if not path.is_relative_to(root) or path.stat().st_size > 6_000_000:
        raise ValueError("FMD source escapes the checkout or exceeds the size limit")
    return path.read_bytes()


def source_hash(relative, content):
    # git's Windows checkout conversion changes text line endings, not semantics.
    if Path(relative).suffix.lower() in {".py", ".sql", ".md", ".json", ".yaml", ".yml", ".xml", ".ipynb", ".sqlproj", ".txt"} or Path(relative).name in {".platform", "LICENSE"}:
        content = content.replace(b"\r\n", b"\n")
    return hashlib.sha256(content).hexdigest()


def inspect_source(root):
    """Check every input byte against the inspected revision, including the installer."""
    root = Path(root).resolve(strict=True)
    lock = json.loads(LOCK.read_text(encoding="utf-8"))
    for relative, expected in lock["files"].items():
        if source_hash(relative, read_source(root, relative)) != expected:
            raise ValueError(f"FMD source differs from the inspected revision: {relative}")
    return {"repository": REPOSITORY, "revision": REVISION, "files_checked": len(lock["files"]),
            "items": [{k: item[k] for k in ("name", "type", "workspace")} for item in inventory(root)],
            "source": "local_pinned_source", "live_acceptance": False,
            "external_prerequisites": ["Three configured DEV workspaces on an active capacity",
                "Tenant settings and execution identity/group/role setup",
                "Authenticated FabricSql connection after SQL database creation",
                "Authenticated FabricDataPipelines and Notebook connections and cross-workspace access"],
            "excluded": ["PROD", "TEST", "Gold", "Purview", "ADF", "domains", "deletion", "workspace administration", "taskflow import (optional UI setup)"],
            "repairs": ["Silver datetime audit serialization", "Success-only Bronze/Silver dependencies",
                "Explicit cross-workspace and connection bindings", "Pipeline metadata uses the code workspace",
                "Include the custom DQ notebook missing from the upstream item manifest"],
            "acceptance_required": ["SQL schema and metadata verified", "Audit activities active and connected",
                "Metadata-driven first load with audit records", "Repeat load with expected row/key counts and audit records"]}


def inventory(root):
    def config(name):
        return json.loads(read_source(root, "config/" + name))
    result = []
    for item in config("lakehouse_deployment.json"):
        result.append(dict(item, workspace="data"))
    result.append({"name": SQL_NAME, "type": "SQLDatabase", "workspace": "configuration",
                   "id": config("data_deployment.json")[0]["id"]})
    for item in config("item_deployment.json"):
        if not any(word in item["name"] for word in EXCLUDED):
            result.append(dict(item, workspace="code"))
    for name in ["NB_FMD_CUSTOM_DQ_CLEANSING.Notebook", BOOTSTRAP, SEED, VERIFY]:
        result.append({"name": name, "type": "Notebook", "workspace": "code"})
    return result


def mappings(root, bindings, items):
    config = yaml.safe_load(read_source(root, "config/item_config.yaml"))
    result = {config["workspaces"]["workspace_" + key]: value
              for key, value in {"config": bindings.workspaces["configuration"],
                                 "code": bindings.workspaces["code"], "data": bindings.workspaces["data"]}.items()}
    result.update({item["id"]: bindings.items[item["name"]] for item in items if "id" in item})
    result.update({config["connections"][name]: value for name, value in bindings.connections.items()})
    database = json.loads(read_source(root, "config/data_deployment.json"))[0]
    result[database["database"]] = bindings.sql_database
    result[database["endpoint"]] = bindings.sql_server
    result[config["database"]["endpoint"]] = bindings.sql_server
    return result


def substitute(text, replacements):
    # One pass prevents a replacement value from being interpreted as another source ID.
    pattern = "|".join(re.escape(k) for k in sorted(replacements, key=len, reverse=True))
    return re.sub(pattern, lambda m: replacements[m.group()], text) if pattern else text


def notebook_source(text, bindings):
    """Bind the top-level Fabric notebook metadata without rewriting cell metadata."""
    pattern = r"(?m)^# META .*$(?:\n# META .*$)*"
    match = re.search(pattern, text)
    if not match:
        raise ValueError("Missing Fabric notebook metadata")
    metadata = json.loads("\n".join(line[7:] for line in match.group().splitlines()))
    metadata["dependencies"] = {"environment": {"environmentId": bindings.items["ENV_FMD.Environment"],
        "workspaceId": bindings.workspaces["code"]}}
    replacement = "\n".join("# META " + line for line in json.dumps(metadata, indent=2).splitlines())
    text = text[:match.start()] + replacement + text[match.end():]
    # The first-load branch passed datetime objects to both json.dumps and notebook.exit.
    text = text.replace('"StartTime" : start_audit_time,', '"StartTime" : start_audit_time.isoformat(),')
    text = text.replace('"EndTime" : end_audit_time', '"EndTime" : end_audit_time.isoformat()')
    # The parallel runner used to create a notebook directly if a lookup failed.
    # Dependency creation belongs to the host's reviewed item stage.
    text = re.sub(r'(?ms)^if not nb_exists:\n.*?(?=^# METADATA \*+)',
                  'if not nb_exists:\n    raise RuntimeError("Deploy NB_FMD_CUSTOM_DQ_CLEANSING through Ray before running FMD")\n\n', text)
    return text


def pipeline_source(value, bindings, *, excluded_pipeline_ids=()):
    def walk(node):
        if isinstance(node, dict):
            if node.get("state") == "Inactive":
                raise ValueError("Inactive pipeline activities are not acceptable for FMD deployment")
            if node.get("type") == "InvokePipeline" and node.get("typeProperties", {}).get("pipelineId") in excluded_pipeline_ids:
                name, dependencies = node["name"], node.get("dependsOn", [])
                node.clear()
                node.update(name=name, type="Fail", dependsOn=dependencies,
                    typeProperties={"message": "This source connector is outside the reviewed FMD deployment scope", "errorCode": "RAY_FMD_SOURCE_OUT_OF_SCOPE"})
            if node.get("type") == "InvokePipeline":
                node["typeProperties"]["workspaceId"] = bindings.workspaces["code"]
                if node.get("name") in {"PL_FMD_LOAD_BRONZE", "PL_FMD_LOAD_SILVER"}:
                    for dependency in node.get("dependsOn", []):
                        if dependency["activity"] in {"PL_FMD_LOAD_LANDINGZONE", "PL_FMD_LOAD_BRONZE"}:
                            dependency["dependencyConditions"] = ["Succeeded"]
            if node.get("type") == "TridentNotebook":
                node["typeProperties"]["workspaceId"] = bindings.workspaces["code"]
            if node.get("name") == "FA_THROW_ERROR_LDZ":
                # Silver is now skipped after Landingzone failure; waiting for its
                # completion would suppress the terminal Fail activity.
                node["dependsOn"] = [d for d in node.get("dependsOn", []) if d["activity"] != "PL_FMD_LOAD_SILVER"]
            connection = node.get("externalReferences", {}).get("connection")
            if connection is not None and connection not in bindings.connections.values() and connection not in {
                "@item().ConnectionGuid", "@pipeline().parameters.ConnectionGuid"
            }:
                raise ValueError("An activity has an unresolved or out-of-scope connection")
            for field in ("pipelineId", "notebookId", "artifactId", "workspaceId"):
                ref = node.get(field)
                if isinstance(ref, str) and not ref.startswith("@") and ref not in {
                    *bindings.items.values(), *bindings.workspaces.values()
                }:
                    raise ValueError("Unresolved static Fabric binding: " + field)
            for child in node.values():
                walk(child)
        elif isinstance(node, list):
            for child in node:
                walk(child)
    walk(value)
    return value


def sql_objects(root):
    """Compile the reviewed SQL sources, ordering tables by foreign-key dependency."""
    lock = json.loads(LOCK.read_text(encoding="utf-8"))
    objects = []
    for path in lock["files"]:
        if not path.startswith("src/Config_Database/") or not path.endswith(".sql"):
            continue
        text = read_source(root, path).decode("utf-8-sig")
        match = re.search(r"CREATE\s+(SCHEMA|TABLE|VIEW|PROCEDURE)\s+(\[\w+\](?:\.\[\w+\])?)", text, re.I)
        if not match:
            raise ValueError("Unsupported SQL deployment object")
        kind, name = match.groups()
        batches = [b.strip() for b in re.split(r"(?im)^\s*GO\s*$", text) if b.strip()]
        refs = re.findall(r"REFERENCES\s+(\[\w+\]\.\[\w+\])", text, re.I)
        objects.append({"kind": kind.upper(), "name": name, "batches": batches, "requires": refs})
    ordered = []
    for kind in ["SCHEMA", "TABLE", "VIEW", "PROCEDURE"]:
        pending = [o for o in objects if o["kind"] == kind]
        while pending:
            ready = [o for o in pending if set(o["requires"]) <= {x["name"] for x in ordered}]
            if not ready:
                raise ValueError("Unresolved SQL object dependencies")
            ordered.extend(ready)
            pending = [o for o in pending if o not in ready]
    return ordered


def sql_notebook(bindings, code):
    header = """# Fabric notebook source
# METADATA ********************
# META {"kernel_info":{"name":"synapse_pyspark"},"dependencies":{}}
# CELL ********************
import json, struct, pyodbc
"""
    connection = f'''
server = {bindings.sql_server!r}
database = {bindings.sql_database!r}
raw = notebookutils.credentials.getToken("pbi").encode("utf-16-le")
conn = pyodbc.connect("DRIVER={{ODBC Driver 18 for SQL Server}};SERVER=tcp:" + server + ",1433;DATABASE=" + database + ";Encrypt=yes;TrustServerCertificate=no;", attrs_before={{1256: struct.pack("<I", len(raw)) + raw}}, timeout=30, autocommit=False)
del raw
try:
    cursor = conn.cursor()
    cursor.execute("SET XACT_ABORT ON")
{''.join('    ' + line + chr(10) for line in code.splitlines())}    conn.commit()
except Exception:
    conn.rollback()
    raise RuntimeError("FMD SQL stage failed; inspect Fabric diagnostics before continuing") from None
finally:
    conn.close()
notebookutils.notebook.exit(json.dumps(result))
'''
    ast.parse(header + connection)
    return notebook_source(header + connection, bindings)


def install_code(root):
    objects = sql_objects(root)
    expected = [o["name"] for o in objects if o["kind"] != "SCHEMA"]
    batches = [b for o in objects for b in o["batches"]]
    # Fresh install only: never silently skip/overwrite a partly installed database.
    return f'''
cursor.execute("SELECT COUNT(*) FROM sys.objects WHERE is_ms_shipped = 0 AND SCHEMA_NAME(schema_id) IN ('integration','execution','logging')")
if cursor.fetchone()[0]:
    raise RuntimeError("FMD database is not empty; use verification or a separately reviewed migration")
for batch in {batches!r}:
    cursor.execute(batch)
for name in {expected!r}:
    cursor.execute("SELECT OBJECT_ID(?)", name)
    if cursor.fetchone()[0] is None:
        raise RuntimeError("FMD SQL object missing after installation")
result = {{"ok": True, "stage": "sql_install", "objects_verified": {len(expected)}, "revision": {REVISION!r}}}
'''


def seed_code(bindings, items):
    statements = []
    for role, ws in bindings.workspaces.items():
        statements.append(("EXEC integration.sp_UpsertWorkspace @WorkspaceId=?, @Name=?", [ws, "FMD " + role.upper()]))
    for item in items:
        if item["type"] in {"DataPipeline", "Lakehouse"}:
            kind = "Pipeline" if item["type"] == "DataPipeline" else "Lakehouse"
            statements.append((f"EXEC integration.sp_Upsert{kind} @{kind}Id=?, @WorkspaceId=?, @Name=?",
                [bindings.items[item["name"]], bindings.workspaces[item["workspace"]], item["name"].rsplit(".", 1)[0]]))
    for name, connection in bindings.connections.items():
        statements.append(("EXEC integration.sp_UpsertConnection @ConnectionGuid=?, @Name=?, @Type=?, @IsActive=1",
                           [connection, name, {"CON_FMD_FABRIC_SQL": "FabricSql", "CON_FMD_FABRIC_PIPELINES": "FabricDataPipelines", "CON_FMD_FABRIC_NOTEBOOKS": "Notebook"}[name]]))
    for guid, name, kind in [(ZERO, "CON_FMD_ONELAKE", "ONELAKE"), (ZERO[:-1] + "1", "CON_FMD_NOTEBOOK", "NOTEBOOK")]:
        statements.append(("EXEC integration.sp_UpsertConnection @ConnectionGuid=?, @Name=?, @Type=?, @IsActive=1", [guid, name, kind]))
    for name, namespace, kind, guid in [("LH_DATA_LANDINGZONE", "ONELAKE", "ONELAKE_TABLES_01", ZERO),
            ("LH_DATA_LANDINGZONE", "ONELAKE", "ONELAKE_FILES_01", ZERO), ("CUSTOM_NOTEBOOK", "NB", "NOTEBOOK", ZERO[:-1] + "1")]:
        statements.append(("DECLARE @c INT=(SELECT ConnectionId FROM integration.Connection WHERE ConnectionGuid=?); "
            "DECLARE @d INT=(SELECT DataSourceId FROM integration.DataSource WHERE ConnectionId=@c AND Name=? AND Type=?); "
            "EXEC integration.sp_UpsertDataSource @ConnectionId=@c, @DataSourceId=@d, @Name=?, @Namespace=?, @Type=?, @Description=?, @IsActive=1",
            [guid, name, kind, name, namespace, kind, kind]))
    return f'''
for statement, parameters in {statements!r}:
    cursor.execute(statement, parameters)
    while cursor.nextset():
        pass
result = {{"ok": True, "stage": "metadata_seed", "statements": {len(statements)}, "entity_onboarding_required": True}}
'''


def verification_code(root, bindings, items):
    names = [o["name"] for o in sql_objects(root) if o["kind"] != "SCHEMA"]
    pipelines = [[bindings.items[x["name"]], bindings.workspaces["code"]] for x in items if x["type"] == "DataPipeline"]
    return f'''
for name in {names!r}:
    cursor.execute("SELECT OBJECT_ID(?)", name)
    if cursor.fetchone()[0] is None:
        raise RuntimeError("Required FMD SQL object is missing")
for pipeline_id, workspace_id in {pipelines!r}:
    cursor.execute("SELECT COUNT(*) FROM integration.Pipeline WHERE PipelineGuid=? AND WorkspaceGuid=? AND IsActive=1", pipeline_id, workspace_id)
    if cursor.fetchone()[0] != 1:
        raise RuntimeError("FMD pipeline metadata binding is missing or incorrect")
counts = {{}}
for table in ["integration.Workspace", "integration.Connection", "integration.DataSource", "integration.Lakehouse", "integration.LandingzoneEntity", "integration.BronzeLayerEntity", "integration.SilverLayerEntity", "logging.PipelineExecution", "logging.NotebookExecution", "logging.CopyActivityExecution"]:
    cursor.execute("SELECT COUNT_BIG(*) FROM " + table)
    counts[table] = cursor.fetchone()[0]
result = {{"ok": True, "stage": "installation_verification", "objects_verified": {len(names)}, "counts": counts,
    "full_operation_accepted": False, "remaining": "Correlate first and repeat load runs with entity counts and new audit rows"}}
'''


def build(root, bindings, output, *, stage, selected=()):
    root = Path(root).resolve(strict=True)
    inspection = inspect_source(root)
    items = inventory(root)
    if set(bindings.items) - {x["name"] for x in items}:
        raise ValueError("Unknown or excluded item in FMD bindings")
    if set(selected) - {x["name"] for x in items}:
        raise ValueError("Unknown or excluded item selected for this stage")
    if stage != "foundations":
        missing = {x["name"] for x in items} - bindings.items.keys()
        if missing:
            raise ValueError("Capture successful item creation receipts before binding: " + ", ".join(sorted(missing)))
        if set(bindings.connections) != {"CON_FMD_FABRIC_SQL", "CON_FMD_FABRIC_PIPELINES", "CON_FMD_FABRIC_NOTEBOOKS"} or not bindings.sql_server or not bindings.sql_database:
            raise ValueError("Create/authenticate the SQL connection after the database exists; supply live SQL metadata and all three connection IDs")
    output = Path(output).resolve()
    if output.exists():
        raise ValueError("Use a new output folder for each reviewed FMD stage")
    # Compute everything before writing; a failed binding check leaves no partial stage.
    files = {}
    proposals = []

    def emit(item, parts=None, operation="update_definition", extra=None):
        payload = {}
        if operation == "create_item":
            payload.update(displayName=item["name"].rsplit(".", 1)[0], type=item["type"])
        if extra:
            payload.update(extra)
        if parts is not None:
            payload["definition"] = {"parts": []}
            if item["type"] == "Notebook":
                payload["definition"]["format"] = "fabricGitSource"
            for part_name, content in parts.items():
                # .txt holds Fabric magic syntax; this is readable source for review.
                source = item["name"] + "/" + part_name.replace("/", "_") + ".txt"
                files[source] = content
                # Relative to the output directory. The CLI below relocates these to
                # repository-relative paths; no cloud payload accepts SourceFile.
                payload["definition"]["parts"].append({"path": part_name, "source": source, "payloadType": "SourceFile"})
        filename = item["name"] + ".json"
        files[filename] = payload
        proposals.append({"operation": operation, "workspace_id": bindings.workspaces[item["workspace"]],
            "item_id": "" if operation == "create_item" else bindings.items[item["name"]], "definition_path": filename})

    if stage == "foundations":
        for item in items:
            if selected and item["name"] not in selected:
                continue
            if item["name"] in bindings.items:
                continue
            # Empty items cannot execute unbound upstream workloads.
            extra = {"creationPayload": {"enableSchemas": bindings.lakehouse_schema_enabled}} if item["type"] == "Lakehouse" else None
            parts = None
            if item["type"] == "Notebook":
                parts = {"notebook-content.py": '# Fabric notebook source\n# METADATA ********************\n# META {"kernel_info":{"name":"synapse_pyspark"},"dependencies":{}}\n# CELL ********************\nraise RuntimeError("FMD definition has not been bound and reviewed")\n'}
            elif item["type"] == "DataPipeline":
                parts = {"pipeline-content.json": json.dumps({"properties": {"activities": [{"name": "NotDeployed", "type": "Fail", "typeProperties": {"message": "FMD definition has not been bound and reviewed", "errorCode": "RAY_FMD_NOT_BOUND"}}]}})}
            elif item["type"] == "VariableLibrary":
                parts = {"variables.json": '{"variables": []}', "settings.json": '{"valueSetsOrder": []}'}
            elif item["type"] == "Environment":
                parts = {"Setting/Sparkcompute.yml": read_source(root, "src/ENV_FMD.Environment/Setting/Sparkcompute.yml").decode("utf-8-sig")}
            emit(item, parts, operation="create_item", extra=extra)
    elif stage == "definitions":
        replacements = mappings(root, bindings, items)
        for item in items:
            name = item["name"]
            if selected and name not in selected:
                continue
            if name in {BOOTSTRAP, SEED, VERIFY} or item["type"] in {"SQLDatabase", "Lakehouse"}:
                continue
            directory = "src/" + name
            parts = {}
            for relative in json.loads(LOCK.read_text(encoding="utf-8"))["files"]:
                if not relative.startswith(directory + "/"):
                    continue
                part = relative[len(directory) + 1:]
                if part.startswith(".") or part.startswith("valueSets/") or part == "notebook-settings.json":
                    continue
                text = substitute(read_source(root, relative).decode("utf-8-sig").replace("\r\n", "\n"), replacements)
                if item["type"] == "Notebook":
                    text = notebook_source(text, bindings)
                elif item["type"] == "DataPipeline":
                    excluded = [x["id"] for x in json.loads(read_source(root, "config/item_deployment.json")) if any(w in x["name"] for w in EXCLUDED)]
                    # Compact JSON keeps both the readable source and its compiled
                    # base64 payload within the independent review evidence budget.
                    text = json.dumps(pipeline_source(json.loads(text), bindings, excluded_pipeline_ids=excluded), separators=(",", ":"))
                elif item["type"] == "VariableLibrary":
                    value = json.loads(text)
                    if part == "settings.json":
                        value["valueSetsOrder"] = []
                    values = {"fmd_fabric_db_connection": bindings.sql_server, "fmd_fabric_db_name": bindings.sql_database,
                        "fmd_config_database_guid": bindings.items[SQL_NAME], "fmd_config_workspace_guid": bindings.workspaces["configuration"],
                        "lakehouse_schema_enabled": bindings.lakehouse_schema_enabled, "key_vault_uri_name": "", "purview_account_name": ""}
                    for variable in value.get("variables", []):
                        variable["value"] = values[variable["name"]]
                    text = json.dumps(value, indent=2)
                parts[part] = text
            if not parts:
                raise ValueError("FMD item has no definition parts: " + name)
            emit(item, parts)
    elif stage in {"sql", "metadata", "verify"}:
        name, code = {"sql": (BOOTSTRAP, lambda: install_code(root)), "metadata": (SEED, lambda: seed_code(bindings, items)),
                      "verify": (VERIFY, lambda: verification_code(root, bindings, items))}[stage]
        item = next(x for x in items if x["name"] == name)
        emit(item, {"notebook-content.py": sql_notebook(bindings, code())})
        proposals.append(dict(proposals[-1], operation="run_job"))
    else:
        raise ValueError("Unknown FMD stage")
    return {"inspection": inspection, "stage": stage, "environment": "DEV", "cloud_actions": proposals,
            "execution": "Proposals only. Validate sources and obtain fresh independent review before CloudActions.",
            "files": files}


def write_stage(bundle, repo, output):
    repo, output = Path(repo).resolve(strict=True), Path(output).resolve()
    if not output.is_relative_to(repo) or output == repo or output.exists():
        raise ValueError("Output must be a new directory inside the reviewed project repository")
    prefix = output.relative_to(repo).as_posix()
    for value in bundle["files"].values():
        if isinstance(value, dict):
            for part in value.get("definition", {}).get("parts", []):
                part["source"] = prefix + "/" + part["source"]
    for proposal in bundle["cloud_actions"]:
        proposal["definition_path"] = prefix + "/" + proposal["definition_path"]
    pending = []
    for name, content in bundle.pop("files").items():
        text = json.dumps(content, indent=2) if isinstance(content, dict) else content
        safe_text(text, limit=6_000_000)
        path = (output / name).resolve()
        if not path.is_relative_to(output):
            raise ValueError("Invalid FMD artifact path")
        pending.append((path, text))
    for path, text in pending:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    output.mkdir(parents=True, exist_ok=True)
    (output / "deployment.json").write_text(json.dumps(bundle, indent=2), encoding="utf-8")
    return bundle


def main(argv=None):
    parser = argparse.ArgumentParser(description="Ray offline, pinned FMD deployment compiler")
    parser.add_argument("operation", choices=["inspect", "build"])
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--bindings", type=Path)
    parser.add_argument("--repo", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--stage", choices=["foundations", "definitions", "sql", "metadata", "verify"])
    parser.add_argument("--item", action="append", default=[], help="Build only this item; repeat for a small review stage")
    args = parser.parse_args(argv)
    if args.operation == "inspect":
        result = inspect_source(args.source)
    else:
        if not all([args.bindings, args.repo, args.output, args.stage]):
            parser.error("build requires --bindings, --repo, --output and --stage")
        bindings = Bindings.model_validate_json(args.bindings.read_text(encoding="utf-8"))
        result = write_stage(build(args.source, bindings, args.output, stage=args.stage, selected=args.item), args.repo, args.output)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
