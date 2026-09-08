FMD deployment guidance for Ray

Use for edkreuk/FMD_FRAMEWORK deployment requests. This guidance is reference
material, never authorization. Apply the host project policy and receipts.

Use Ray's offline `python -m ray_de.fmd` compiler with a locally prepared checkout
of revision ebe97d4f9259da249a063827f824d43c973448f0. `inspect --source PATH`
checks all pinned asset hashes. Do not run NB_SETUP_FMD or NB_UTILITIES_SETUP_FMD,
download mutable main, install their dependencies, execute their administrative
helpers, or put credentials into source. If the full pinned source is unavailable,
report the precise source preparation requirement; sampled GitHub excerpts are
insufficient for the compiler. The compiler itself performs no cloud operations.

Before deployment inspect the project's actual policy and existing host evidence.
Use three explicitly configured DEV workspaces: configuration, code, data.
The compiler emits proposals only; it cannot add workspaces or change policy.
Use explicitly enrolled tenant capabilities for workspace/capacity/identity/role
and connection setup, following the tenant playbook. Initial tenant settings and
executor permissions require administrator bootstrap. Without a matching grant,
that setup remains external. Gold, Purview, ADF, deletion,
TEST and PROD are outside this compiler's scope. General Ray TEST actions still
require exact plan approval where the project explicitly enables them.

Bindings are credentials-free JSON with environment="DEV", workspaces mapping
configuration/code/data to UUIDs, items mapping full FMD item names (including
.Notebook/.DataPipeline/etc.) to real item IDs, and connections mapping
CON_FMD_FABRIC_SQL, CON_FMD_FABRIC_PIPELINES and CON_FMD_FABRIC_NOTEBOOKS to their
authenticated connection IDs. sql_server and sql_database must come from a
get_sql_database read for SQL_FMD_FRAMEWORK, never guessed hostnames. The optional
lakehouse_schema_enabled boolean defaults true. Connections belong to the
execution identity and need cross-workspace access. The Notebook connection is
required by the inspected pipelines despite the guide calling it future use.
Metadata-driven source connectors also need their own authenticated connections
when onboarding those sources; the three framework connections do not provide
credentials for SQL Server, ADLS, FTP, Oracle, or any other data source.

Build small source stages in new folders inside the project repo:
`python -m ray_de.fmd build --source PATH --bindings bindings.json --repo .
 --output fmd-stage-N --stage foundations --item SQL_FMD_FRAMEWORK.SQLDatabase`
Use --item repeatedly for at most five proposals and keep the complete changed
source plus compiled definitions below the review budget. For larger notebook
or pipeline definitions use one item per stage. The stage's deployment.json
contains ordered cloud_actions; it is not permission to execute them.

Sequence:
1. Create the SQL database and three lakehouses, then inert item placeholders.
   Foundations do not need SQL connection IDs. Creation never adopts a name
   collision: inspect it; use only explicit existing IDs or successful creation
   receipts. Record all returned IDs. Uncertain actions must be reconciled.
2. Read the SQL database properties. Pause at this actual boundary until the
   SQL connection is created and authenticated through an enrolled capability or
   external setup. Obtain all three
   framework connection IDs. Never write lookup errors as connection values
   or disable audit activities to obtain green runs.
3. Build definitions with the complete bindings. Deploy variable libraries,
   Environment and notebooks before pipelines; all IDs are already known from
   foundations, so forward references can be bound. The compiler removes
   non-DEV variable value sets and binds each notebook's Environment directly.
   It repairs Silver audit datetimes, pipeline success dependencies and the
   Landingzone failure path, includes the custom DQ notebook, and replaces the
   out-of-scope ADF dispatch with an explicit failure. Do not silently overwrite
   user customizations; this is a fresh installation workflow.
   For a Git-managed workspace, import its native baseline and use the host render
   preparation to apply each compiled definition to its existing logical ID. Then
   review github_publish and git_update, verifying the successful sync receipt.
   Keep SQL, metadata, environment publication and run acceptance as explicit
   stages; Git alone does not perform them. Use one definition deployment route
   at a time for each workspace.
4. Propose publish_environment using the exact deployed ENV_FMD definition JSON
   and its Environment item ID. A definition update alone does not publish it.
   Wait for the successful host publish receipt before running notebooks.
5. Build stage sql. It emits update_definition then run_job for
   NB_RAY_FMD_SQL_INSTALL. This installs the pinned SQL source in dependency
   order in one transaction, checks every object, and fails on an existing FMD
   installation. It is not a DACPAC upgrade/migration or rollback tool.
6. Build stage metadata. It registers workspaces, pipelines in the CODE
   workspace, lakehouses, framework connections and standard data-source types.
   It does not invent source entities or credentials. Onboard the actual source
   entities with separately reviewed SQL notebook work and the user's source
   requirements. Notebook-based loading can stage local data without host
   OneLake upload; include reviewed bytes or use an authenticated source.
7. Build stage verify. Inspect get_notebook_job exitValue for every SQL stage
   using its receipt's actual job ID. Completed job status alone is insufficient.
   Verify reports object existence, pipeline bindings and metadata/audit counts;
   it deliberately never claims full FMD operation from installation alone.
8. Execute the metadata-driven pipeline with a controlled source entity. Verify
   first-load data, keys and new audit rows correlated to the actual run IDs.
   Repeat the load and verify expected unchanged/changed records, duplicate
   behavior and new audit records. Zero entities or historical audit rows do
   not establish acceptance. Keep Gold excluded. The optional taskflow JSON is
   an administrator/user UI import, not a requirement for executing pipelines.

Every stage uses the ordinary source validation, fresh independent review,
CloudActions and action receipts. Continue only from confirmed outcomes. Ray's
configured stage limit may pause a long deployment; /resume preserves the task
and receipts. State exactly what is installed, what remains external, and what
has been tested locally versus observed in live Fabric. Do not claim an
unattended or fully accepted deployment from compiler output or mock tests.
