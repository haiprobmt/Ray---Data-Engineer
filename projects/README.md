# Local Ray project setup

This directory is the central location for local project configuration and Fabric
source checkouts. Use one folder per tenant or independently enrolled project:

```text
projects/
  README.md
  sample-fabric/                     # tracked, local-only demonstration
  actual-fabric/
    tenant-config.yaml               # operator-owned IDs, grants, secret references
    repo/                            # separate tenant Git checkout
      tenant.json
      deployment/
      workspaces/
        dev-fmd-configuration/
        dev-fmd-code/
        dev-fmd-data/
```

The `actual-fabric` configuration is the enrolled DEV setup. Its
`workspace_admins.primary.id` is the one place to select the Entra user that the
workspace role grants make Admin. Set `repo_path: repo`; Ray resolves it relative
to `tenant-config.yaml`, so the setup has no hardcoded dependency on a separate
`C:/Fabric` directory.

For another tenant, copy `docs/examples/tenant-config.yaml` into a new
`projects/<tenant>/tenant-config.yaml`, use a unique `project_id`, and create its
`repo/` directory. Finalize all IDs, Git bindings, grants and policy flags before
the first Ray CLI command, because commands bind that project ID to its config.
The sample config remains local-only. Keep the configuration outside the nested
`repo/` directory that Ray uses for engineering tasks.

From the Ray codebase root, initialize the existing draft with your real tenant:

```powershell
.\.venv\Scripts\python.exe -m ray_de.tenant_repository .\projects\actual-fabric\repo --tenant YOUR_TENANT_ID --workspace dev-fmd-configuration --workspace dev-fmd-code --workspace dev-fmd-data
```

Keep secrets in the external protected Ray profile. On this installation, the
gateway config is `%LOCALAPPDATA%/Ray/gateway.yaml` and shared runtime state and
isolated credentials are under `%LOCALAPPDATA%/Ray/state`. Use that same state
directory for CLI login, credential enrollment and Telegram. Raw service-principal
credential import files also stay outside this codebase.

```powershell
$ray = (Resolve-Path .\.venv\Scripts\ray.exe).Path
$project = (Resolve-Path .\projects\actual-fabric\tenant-config.yaml).Path
$state = Join-Path $env:LOCALAPPDATA 'Ray\state'
```

After finalizing the config and enrolling credentials, add the project's absolute
configuration path to the existing gateway `projects` mapping and its project ID
to your existing Telegram access entry. The [enrollment guide](../docs/tenant-git-deployment.md)
contains the login and registration steps. Merely placing a file here does not
enable operations or register it with Telegram.

Native tenant `repo/` directories are ignored by the outer Ray repository so their
contents can use independent Git repositories. The tracked sample remains part of
Ray's tests. Non-secret configuration can be versioned here; credentials cannot.
