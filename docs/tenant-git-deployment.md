# Tenant repositories and Git deployment

Ray implements tenant-bound provisioning and local source -> personal GitHub ->
Fabric Git deployment. The implementation has offline contract tests; no live
tenant, GitHub repository, credentials or workspace enrollment is supplied by this
checkout. The [example configuration](examples/tenant-config.yaml) has all cloud
writes disabled and contains synthetic IDs.

## What Ray can do

- Initialize a local Git repository for one tenant, import native workspace source
  from an exact Git commit, and allocate item directories to functional folders.
- Create workspaces on an enrolled active capacity, assign that capacity, provision
  workspace identities, grant specific workspace roles, create folders and move
  items while preserving their physical IDs.
- Create single-tenant execution applications and service principals, issue an
  expiring credential directly into Windows protected storage, create static
  security groups and add specific members to groups created by this project.
- Create/authenticate shareable cloud connections using supported unattended
  credentials, update their credentials, and grant specific connection roles.
  SQL connection endpoints can be discovered from an exact Fabric SQL database
  or its verified creation receipt, resolving FMD's intervening connection step.
- Create a private repository under an enrolled personal GitHub account; publish
  reviewed source; connect and initialize workspace Git; capture an existing
  workspace baseline; and deploy a verified publication to Fabric.

These are explicit per-operation capabilities in operator-owned project config.
The model chooses a capability key and supplies its narrow arguments, while the
host constructs the request. It cannot choose arbitrary admin endpoints or grant
itself additional privileges. Reads live in `fabric.py`; writes live in `cloud.py`.
Every cloud mutation requires validated source, a fresh independent review,
matching tenant/client and environment policy, and a durable action receipt. TEST
requires exact approval; PROD and deletion remain disabled.

## Repository structure

```text
fabric-tenant/
  tenant.json
  README.md
  deployment/                       # allocation and deployment manifests
  workspaces/
    dev-fmd-configuration/
      Configuration/SQL_FMD_FRAMEWORK.SQLDatabase/
        .platform
        ...                          # exported native SQL project
    dev-fmd-code/
      Orchestration/PL_FMD_LOAD_ALL.DataPipeline/
        .platform
        pipeline-content.json
      Transformation/NB_FMD_LOAD_BRONZE_SILVER.Notebook/
        .platform
        notebook-content.py
      Shared/ENV_FMD.Environment/
        .platform
        ...
    dev-fmd-data/
      Bronze/LH_BRONZE.Lakehouse/
        .platform
        ...
      Silver/LH_SILVER.Lakehouse/
        .platform
        ...
```

Each workspace connects to its own nonoverlapping `workspaces/...` directory on
one enrolled branch. Native `Name.ItemType` directories follow Microsoft's
[fabric-cicd sample](https://github.com/microsoft/fabric-cicd/tree/main/sample/workspace).
The tenant wrapper and functional folder names are Ray conventions. This uses
Fabric's native Git integration; installing fabric-cicd is not required.

Keep `.platform` version 2 metadata and `config.logicalId` from the exported
baseline. A REST `definition.parts` envelope is not a native Git directory.
Keep manifests outside item directories: Fabric can remove unrelated files inside
them. Fabric retains nested workspace folders in Git; see its
[source format](https://learn.microsoft.com/en-us/fabric/cicd/git-integration/source-code-format)
and [folder handling](https://learn.microsoft.com/en-us/fabric/cicd/git-integration/git-integration-process#handling-folder-changes-safely).

## Enroll once

1. An administrator initially provides Ray's executor service principal with the
   applicable Fabric tenant settings, capacity Contributor/Admin access and the
   permissions needed for the selected capabilities. Existing managed workspaces
   require the appropriate workspace role; Git setup requires Admin. Graph app and
   group operations require separately consented Microsoft Graph application
   permissions. Ray does not grant tenant admin consent, directory roles, capacity
   access to itself, or enable tenant settings. See Microsoft's
   [workspace creation requirements](https://learn.microsoft.com/en-us/rest/api/fabric/core/workspaces/create-workspace)
   and [Git automation setup](https://learn.microsoft.com/en-us/fabric/cicd/git-integration/git-automation).
2. Keep local enrollment in the Ray codebase's `projects/` directory. The current
   draft is `projects/actual-fabric/tenant-config.yaml`. For another tenant, copy
   the example to `projects/<tenant>/tenant-config.yaml`. Set `repo_path: repo`
   for a separate source checkout beside the config. Replace synthetic tenant,
   client, capacity and GitHub values, and select only the required grants. Use a new Ray
   project identity when changing an existing project's policy. `@create-code`
   resolves only after that creation grant has a successful verified receipt;
   existing workspace IDs must also appear in `fabric.workspaces` with the same
   environment. Keep the config outside its nested `repo/` engineering directory;
   runtime state and credentials remain under `%LOCALAPPDATA%/Ray/state`.
   Before the first Ray command, finalize all IDs, grants and policy flags. For
   authorized DEV testing, enable `policy.local_write` and `fabric_dev_write`;
   retain disabled TEST/PROD writes. Enable `fabric.create_items`,
   `workspace_write` and `allow_definition_export` if testing FMD foundations or
   direct item operations as well. CLI commands bind the project ID to the entire
   configuration, so changing these settings after login requires a new project
   ID and separate authentication. Keep the shipped template itself local-only.

   To make a person Admin in every managed workspace, put their Entra user Object
   ID in `fabric.tenant.workspace_admins.primary.id` and use
   `principal: {principalRef: primary}` in each `assign_workspace_role` grant.
   Changing that one ID updates every such grant. Workspace creation and role
   assignment remain separate reviewed operations with separate receipts, so run
   the applicable `*-user-admin` grant after each workspace exists. Changing this
   security enrollment changes the project binding and therefore requires a new
   project ID and fresh local authentication. For an already-created workspace,
   enroll its exact physical ID under `fabric.workspaces` and use that ID in the
   Git binding and role grant instead of its former `@create-*` reference.
3. From the Ray codebase root, create the local repository using the installed
   Ray Python environment:

   ```powershell
   .\.venv\Scripts\python.exe -m ray_de.tenant_repository .\projects\actual-fabric\repo --tenant TENANT_UUID --workspace dev-fmd-configuration --workspace dev-fmd-code --workspace dev-fmd-data
   ```

   `repo_path: repo` resolves to that directory. It must be new or empty. For a write task
   whose repository is already an empty directory, Ray can also request the host
   `tenant_read` preparation kind `scaffold`.
4. Authenticate the isolated project locally; never paste credentials into chat:

   ```powershell
   $ray = (Resolve-Path .\.venv\Scripts\ray.exe).Path
   $project = (Resolve-Path .\projects\actual-fabric\tenant-config.yaml).Path
   $state = Join-Path $env:LOCALAPPDATA 'Ray\state'
   & $ray --project $project --data-dir $state login fabric --service-principal-env C:/Private/ray-executor.env
   & $ray --project $project --data-dir $state tenant secret-set --name github
   & $ray --project $project --data-dir $state tenant capabilities
   ```

   The env file uses the existing Ray SP login format (`FAB_TENANT_ID`,
   `FAB_SPN_CLIENT_ID`, `FAB_SPN_CLIENT_SECRET`). The GitHub command prompts locally.
   Credentials are DPAPI protected and bound to this profile, tenant and client.
   The GitHub PAT needs access to the enrolled private repository with Contents
   read/write. Creating a repository additionally needs permission to create it;
   an existing personal repository is supported. Its configured branch must exist.
   See [GitHub prerequisites](https://learn.microsoft.com/en-us/fabric/cicd/git-integration/git-get-started).
5. Sign in to the new project's isolated Codex profile and check enrollment:

   ```powershell
   & $ray --project $project --data-dir $state login codex
   & $ray --project $project --data-dir $state tenant read --kind capacities
   & $ray --project $project --data-dir $state tenant read --kind github_repository
   & $ray --project $project --data-dir $state tenant read --kind github_branch
   ```

   Repository/branch checks expect the repository to exist already. The capabilities
   command reports local configuration; it does not prove actual service permissions.

### Register with the existing Telegram gateway

Use the same `--data-dir` as the running gateway for every enrollment command.
On Hai's current installation this is `C:/Users/haing/AppData/Local/Ray/state`,
already selected by `$state` above. Otherwise the bot would
look in a different profile for its credentials and action receipts.

In `C:/Users/haing/AppData/Local/Ray/gateway.yaml`, add the new project to the
existing `projects` mapping, without replacing the lobby or other entries:

```yaml
projects:
  # Retain the existing entries.
  fabric-tenant-dev: C:/Work/Work/Personal/Ray_Complete_Project/ray/projects/actual-fabric/tenant-config.yaml
```

Append `fabric-tenant-dev` to the `projects` list in your existing `access` entry.
Preserve its user/chat IDs, existing project entries and other settings. Keep the
lobby first in that list because it supplies conversational authentication. The
project directory and source directory must not overlap existing gateway projects,
the registered workspace root or Ray's state directory.

Reload the existing gateway once it is idle, using its existing gateway config
and state directory. `/stop` pauses work; it does not terminate the gateway, and
starting a second copy is not a restart. The new project then becomes selectable
in Telegram. Send these as separate messages:

```text
/project fabric-tenant-dev
/mode read
/work Check the enrolled tenant, capacity and GitHub repository access and report readiness.
```

For the first Git test, an existing private personal GitHub repository initialized
with a README and the configured branch avoids needing repository-creation token
permissions. Use a fine-grained PAT scoped to it with Contents read/write. Microsoft
documents the Fabric/GitHub prerequisites in its
[Git setup guide](https://learn.microsoft.com/en-us/fabric/cicd/git-integration/git-get-started).

## Adopt and organize an existing workspace

Ask Ray to inspect the enrolled workspace inventory, connections and Git state.
It must report definitions that cannot be exported. Exports and Git contain item
definitions, not lakehouse data, SQL rows, connection secrets or a complete backup.

For an unconnected workspace, provision the GitHub connection, create or inspect
the repository, and publish each workspace directory's README. Export a baseline
to a new JSON source file, connect Git, and initialize with `PreferWorkspace`.
Commit the reviewed baseline using `git_commit`, which is single-use per grant.
An existing Git connection must match the enrollment; Ray never silently replaces
it. Initialization receipts expose `required_action`; initialization alone is not
deployment completion.

Import the resulting exact commit locally. Assign existing logical IDs to folders
in an allocation JSON file such as `{"LOGICAL_UUID": "Transformation"}`. Ray's
host preparation kinds `export_to_source`, `import`, `allocate` and `render` are
available only in locally enabled write tasks. They modify local source only.
Equivalent inspection/import/allocation CLI commands are:

```powershell
ray --project CONFIG --data-dir STATE tenant read --kind export --workspace WORKSPACE --output deployment/baseline.json
ray --project CONFIG --data-dir STATE tenant import --workspace WORKSPACE --id FULL_COMMIT_SHA
ray --project CONFIG --data-dir STATE tenant allocate --workspace WORKSPACE --allocation ALLOCATION_JSON --apply
```

Allocation moves whole native directories and preserves `.platform`. Publish all
new item parts and list all previous paths in `remove` for each move. The host
rejects item deletion, logical-ID replacement, type changes and incomplete moves.
Alternatively, initial allocation can use enrolled `create_folder` and `move_item`
operations before capturing the workspace baseline.

## Later deployments

1. Prepare native source and a JSON arguments artifact for `github_publish`:

   ```json
   {"expected_head":"FULL_CURRENT_SHA","message":"Update Bronze loading","files":["workspaces/dev-fmd-code/Transformation/Load.Notebook/.platform","workspaces/dev-fmd-code/Transformation/Load.Notebook/notebook-content.py"],"remove":[]}
   ```

2. Ray proposes `tenant_action`, using the grant key as `item_id`, the exact grant
   `workspace` string as `workspace_id` (empty for repository grants), and the
   arguments artifact as `definition_path`. Every referenced source file must be
   included in the fresh review. Fixed-parameter capabilities take `{}`.
3. The host publishes using GitHub's Git database API and a non-forced branch
   update. It checks the expected branch head before submitting. It does not run
   local Git hooks or source scripts. The local directory has `.git`, but these
   API publications do not advance its local HEAD or configure a `git push` remote;
   the authoritative publication commit is recorded in Ray's receipt. Existing
   GitHub repository automation may respond to a push according to its own setup.
4. Use the current Fabric `workspaceHead` and the verified publication SHA in a
   reviewed `git_update` artifact. Workspace drift, conflicts, deletions or an
   unexpected remote commit block the update. Every changed file must match source
   from a verified publication receipt; a reviewed README change cannot carry
   unreviewed external code changes into the sync. Ray waits for the asynchronous
   operation and verifies clean Git state, physical item IDs, folder placement and
   exported item definitions against the published source.

One publication is bounded to 500 files and 180 KB of source; the overall review
budget may require smaller batches. Import accepts ordinary UTF-8 native source,
not binary assets, symlinks or arbitrary hidden files. Unsupported item formats
or caller limitations stop verification and require a specific adapter. A clean
Git status alone is not accepted as proof that unsupported items were deployed.
Uncertain operations retain receipts for read-only reconciliation and are never
automatically resubmitted. Repository and workspace mutations are serialized with
other unresolved actions on the same tenant/workspace.

## FMD integration and remaining acceptance

Use [FMD foundations](fmd-deployment.md) to create the database, lakehouses and
inert placeholders. Create the SQL connection after discovering that database's
real endpoint. Capture/import the native baseline, then compile complete bound
definitions. The host `render` preparation takes a baseline logical ID and the
compiler definition path, writes its parts into that existing native item and
preserves `.platform`. Publish and sync these reviewed changes through Git.

SQL installation, initial metadata, environment publication and pipeline execution
remain explicit post-deployment stages. A Git sync does not install SQL metadata,
authenticate source connectors, publish a Spark environment or prove repeat-load
and audit behavior. Use one definition deployment route per workspace at a time.
Gold, Purview, ADF, destructive rollback and PROD remain outside this FMD scope.

Before live acceptance, supply the actual tenant/client, capacity, workspace
selection and personal GitHub repository, then exercise the configured operations
with that service principal. Offline tests establish host behavior only; they do
not establish real tenant permissions, connector authentication, Fabric item-type
support, a deployed FMD instance or Telegram/model quality.

## Verification record — 7 September 2026

The full offline suite passed 467 tests. After adding the external-commit source
coverage check, all nine focused Git tests passed, including that additional
regression. All 16 FMD compiler tests also passed after the guidance update.
`git diff --check` passed, and the built wheel was checked against all 42 top-level
Python modules in the checkout. Existing runtime project bindings were preserved;
the idle local Ray process was reloaded after a SQLite backup. These results do
not constitute a live Fabric deployment or a model-quality assessment.
