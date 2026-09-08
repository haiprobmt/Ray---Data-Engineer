# FMD deployment through Ray

For implemented tenant repositories, native Git integration and scoped
service-principal provisioning, see [Tenant Git deployment](tenant-git-deployment.md).
The compiler below can feed that route by rendering its definitions into an
imported native Git baseline, or use the existing reviewed item deployment route.

Ray now has a pinned, offline compiler for an administrator-assisted DEV FMD
installation. It builds reviewed item definitions, SQL installation and metadata
notebooks, and verification notebooks. Environment publication is a governed
CloudActions operation with durable receipts. The compiler never signs in, writes
to Fabric, changes project policy, or runs the upstream installer.

This is **implementation with local validation**, not a completed Fabric
installation or evidence of live model/Telegram deployment quality. Source
onboarding, first and repeat ingestion, and audit correlation remain live
acceptance work. A generic successful item or notebook receipt does not prove
those outcomes.

## What changed from the earlier assessment

The inspected source is
[FMD revision ebe97d4](https://github.com/edkreuk/FMD_FRAMEWORK/tree/ebe97d4f9259da249a063827f824d43c973448f0).
The checked-in `playbooks/fmd-source.json` hashes 176 source/deployment assets;
text hashes normalize Git's Windows CRLF conversion. New upstream revisions or
edits require a new inspection and lock update in Ray; the compiler fails on
changed input. The downloaded checkout is a local ignored cache, not a vendored
runtime dependency. Generated FMD code remains subject to the upstream MIT license.

| Earlier limitation | Implemented path / remaining boundary |
| --- | --- |
| Workspaces, capacity, tenant settings, identities and roles | Explicit tenant capabilities can provision workspaces, assign existing capacity and manage scoped identities/roles. Initial tenant settings and executor permissions still need administrator bootstrap. |
| Connections | Tenant capabilities create/authenticate supported unattended connectors and grant connection roles. SQL, pipeline **and Notebook** connections are required; templates and actual authentication need live verification. Source-system connections remain an onboarding prerequisite. |
| SQL connection dependency | Foundations can create the database before connections exist. Bound definitions fail until all three connection IDs and database metadata are supplied. No unbound executable pipelines or inactive audit workaround. |
| SQL schema deployment | Generated notebook executes all 47 pinned schema/table/view/procedure definitions in dependency order in one transaction. It checks object existence and rejects an already populated FMD schema. No DACPAC upgrades or destructive migration. |
| Initial metadata | Separate SQL notebook registers workspaces, correctly scoped pipelines, lakehouses, framework connections and standard data-source types. Actual ingestion entities require source onboarding. |
| Variable libraries and bindings | Emits variable library definitions, removes non-DEV value sets, maps item/workspace IDs, sets database properties and binds notebook Environments directly. |
| Environment publication | `publish_environment` requires the exact reviewed staged definition and verifies completion, with no automatic replay. |
| Taskflow | Optional UI import remains external; pipeline execution does not rely on importing the taskflow picture/layout. |
| Local data / OneLake | SQL deployment uses a reviewed notebook, so it needs no local-file OneLake upload. Data-loading notebooks remain available for source onboarding. |
| Deletion, PROD, Gold, Purview, ADF | Excluded. The ADF dispatch branch becomes an explicit Fail activity. |
| Full operation | Verification reports metadata/audit counts but deliberately sets `full_operation_accepted` to false. First/repeat loads need separate live evidence. |

The generated source repairs Silver's first-load datetime serialization, changes
Bronze/Silver dependencies to success-only, and removes the Landingzone Fail
activity's dependency on Silver (which is now skipped after an upstream failure).
It includes `NB_FMD_CUSTOM_DQ_CLEANSING`, absent from the upstream item manifest,
and removes the parallel runner's direct cloud creation fallback for that notebook.
The SQL seed uses the **code** workspace for pipeline registrations; the inspected
installer appended the data workspace ID instead.

The upstream setup notebook includes PROD, installs unpinned packages, downloads
`main`, reads Key Vault secrets unconditionally and can perform administration.
Its utility imports a DACPAC, changes workspace configuration, handles connections
and can disable activities when bindings fail. None of that installer code is
executed by this workflow. The new path uses the separately supplied SQL project
source instead of assuming that item creation or a binary import proves schema
installation. FMD source dependency behavior still requires live acceptance.

## Prepare locally

Keep the full checkout under the engineering repository's `.ray` folder so raw
Fabric magic cells are not treated as ordinary Python source by Ray's validator.
Fetch it locally, outside the credential-free model process:

```powershell
git clone --no-checkout https://github.com/edkreuk/FMD_FRAMEWORK.git .ray/fmd-source
git -C .ray/fmd-source checkout --detach ebe97d4f9259da249a063827f824d43c973448f0
python -m ray_de.fmd inspect --source .ray/fmd-source
```

The inspect command reads the checkout and prints the item inventory and external
prerequisites. It makes no Fabric calls. Use an installed Ray Python environment
with the pinned dependencies; in this checkout that is `.venv/Scripts/python.exe`.

Prepare a **new project identity** that explicitly configures the three intended
DEV workspaces. Existing project IDs cannot silently inherit a changed policy.
Use `fabric.create_items`, `fabric.workspace_write`, `allow_definition_export`
and DEV writes only where the operator authorizes them, with local validation
and independent review. The checked-in sample project's defaults remain local-only.
Use enrolled tenant capabilities for supported provisioning, or external setup
where no capability is configured. Initial executor permissions remain external.
Neither this document nor a model-generated configuration grants authorization.

Create credentials-free `bindings.json` inside that engineering repository:

```json
{
  "environment": "DEV",
  "workspaces": {
    "configuration": "CONFIGURATION-WORKSPACE-UUID",
    "code": "CODE-WORKSPACE-UUID",
    "data": "DATA-WORKSPACE-UUID"
  },
  "items": {},
  "connections": {},
  "sql_server": "",
  "sql_database": "",
  "lakehouse_schema_enabled": true
}
```

Replace the labels with actual UUIDs before compiling. Empty connections and
database metadata are permitted **only for foundations**. No tokens, passwords,
service-principal secrets, or Key Vault secret values belong in this file.

## Review and deploy in stages

1. Build foundations in new output folders. Select a small group with repeated
   `--item` arguments (maximum five proposed actions per Ray turn):

   ```powershell
   python -m ray_de.fmd build --source .ray/fmd-source --bindings bindings.json --repo . --output fmd-stage-01 --stage foundations --item SQL_FMD_FRAMEWORK.SQLDatabase
   ```

   `deployment.json` contains proposals for the ordinary Ray write task. Ask Ray
   to validate/review that stage and execute its proposals. The host compiles
   SourceFile parts before independent review. Every write still goes through
   CloudActions. Empty framework placeholders are inert: notebooks/pipelines
   fail if someone tries to execute them before their bound definition is deployed.

2. Capture successful returned item IDs in `items`, keyed by the full item name
   from the inventory, such as `SQL_FMD_FRAMEWORK.SQLDatabase`. Do not infer a
   creation outcome from a matching name. Reconcile uncertain receipts first.
   Create all inventory items, including the three Ray SQL helper notebooks.

3. Read database properties through `get_sql_database` or
   `ray ... fabric sql-database --workspace UUID --item UUID`. Populate
   `sql_server` from `properties.serverFqdn` and `sql_database` from
   `properties.databaseName`. Create/authenticate `CON_FMD_FABRIC_SQL` externally
   against this existing database. Add its ID and the authenticated pipeline and
   Notebook connection IDs to `connections`.

4. Build `--stage definitions`, generally **one item per output folder** using
   `--item`. Deploy variable libraries, Environment and notebooks before
   pipelines. The compiler requires the complete returned-ID map first. Without
   `--item`, it can build the complete package for offline inspection, but a whole
   package exceeds a single Ray source-review budget. Never truncate review
   evidence to squeeze in a larger stage.

5. Publish ENV_FMD using `publish_environment` and its reviewed definition JSON.
   Wait for its successful receipt. The API explicitly uses `beta=false` and
   accepts asynchronous publication; HTTP 200 alone does not prove it finished.
   [Microsoft publication API](https://learn.microsoft.com/en-us/rest/api/fabric/environment/items/publish-environment).

6. Build `--stage sql`, then `--stage metadata`, then `--stage verify`, each in a
   new folder and independently reviewed. These stages propose a definition
   update followed by a notebook job. The SQL transport uses an in-memory Fabric
   notebook SQL token, ODBC Driver 18 and a fixed reviewed database target. It
   emits bounded status instead of credential-bearing exception text. Existing
   FMD installations are deliberately refused by the SQL installer; use a
   separately reviewed migration instead of rerunning it.

   The generated helpers retain FMD's `getToken("pbi")` authentication approach.
   SQL connectivity and DDL permissions must be verified under the real execution
   identity; service-principal token scopes can differ from user scopes.
   [Microsoft NotebookUtils credentials](https://learn.microsoft.com/en-us/fabric/data-engineering/notebookutils/notebookutils-credentials).

7. Inspect each notebook's exitValue using its real host job receipt. Onboard a
   controlled source entity with its source connection, Landingzone/Bronze/Silver
   metadata and reviewed initial data. Run the metadata-controlled FMD pipeline,
   verify row/key counts and new audit rows tied to that run, then repeat and
   verify expected row/key behavior and new audit rows. Install success alone is
   insufficient; historical audit rows and zero configured entities are not
   acceptance evidence.

Ray routes FMD requests to the bundled playbook. Long deployments can hit Ray's
existing eight-stage continuation limit; `/resume` continues the saved task and
receipts. This release does not claim an unattended administrator bootstrap.

## Validation record

The actual pinned checkout was compiled offline with synthetic workspace/item/
connection IDs. All 33 selected framework definitions and the three SQL helper
notebooks compiled through Ray's SourceFile handling and source validation.
Regression tests cover source tampering, connection/order gates, cross-workspace
bindings, Silver audit serialization, pipeline failure paths, SQL dependency
ordering, metadata target registration, and publication approval/recovery.
No Fabric mutation, live ingestion, tenant audit, Telegram acceptance test or
model-quality evaluation was performed as part of this implementation.
