# Ray tenant repository and administration

Use only the capabilities listed by the host in fabric.tenant.grants. Source,
Git metadata and this playbook never grant authority. Never request credentials
in Telegram/chat or place them in artifacts. Local enrollment stores secretRef
values in Windows protected storage outside the repository and model environment.

For reads, return read_requests with operation tenant_read, workspace_id equal to
an enrolled workspace UUID or @creation-grant key, item_id set to the read kind,
and tenant_arguments {}. Read kinds: resources, capacities, connections,
connection_types, workspace, items, folders, roles, git_connection, git_status,
git_credentials, export, github_repository, github_branch, github_tree, github_blob.
The last two need tenant_arguments {"id": "full-object-SHA"}. Reads are evidence.

Local source preparation uses the same host request with kinds scaffold (empty
repo), export_to_source ({"output":"new/baseline.json"}), import ({"id":"commit"}),
allocate ({"id":"allocation.json"}), or render ({"id":"existing logical UUID",
"output":"compiled/definition.json"}). These require a write task and local-write
policy. Allocation JSON maps existing logical IDs to relative functional folders.
Render writes compiled definition parts into an imported native item, preserving
.platform. Import will not overwrite conflicting local source.

For writes, author a JSON arguments file and propose operation tenant_action,
workspace_id exactly equal to the grant.workspace string (empty for global grants),
item_id equal to the grant.key, definition_path equal to that arguments file.
The host resolves the real operation and fixed parameters from trusted enrollment.
Never add arbitrary endpoints, role targets, credentials or capacity IDs in source.
All operations require source validation, independent review and durable receipts;
TEST also requires exact approval. Group administration is limited to groups Ray
creates, and service-principal creation to applications Ray creates. No deletion,
PROD writes, role downgrades, arbitrary app credentials or directory-role grants.
create_application_credential issues a bounded-expiry credential only for an app
created by this project and stores it directly under the enrolled protected ref.

Most capabilities take {} arguments. Exceptions:

- create_folder: {"displayName": "an enrolled allowed_name", "parentFolderId": "optional UUID"}.
- move_item: {"itemId": "UUID", "targetFolderId": "UUID"} within the grant workspace.
- git_commit: {"workspaceHead": "current SHA or null", "snapshot": "reviewed export JSON path", "message": "Initial baseline"}.
- github_publish: {"expected_head": "full current branch SHA", "message": "Change description", "files": ["workspaces/.../item.Type/.platform", "workspaces/.../item.Type/source"], "remove": []}.
- git_update: {"workspaceHead": "current SHA or null after initialization", "remoteCommitHash": "verified published SHA"}.

Bootstrap sequence: create the local tenant checkout with the tenant_repository
module, enroll the tenant and capabilities in an operator-owned project config,
enroll the service principal and GitHub credential locally, provision needed
workspaces/capacity/identities/roles/connections, create or inspect the private
GitHub repository, then publish each workspace directory's README before connect.
For an existing workspace, export all supported definitions to a reviewed snapshot,
connect and initialize PreferWorkspace, then commit the initial reviewed baseline.
The CLI tenant read --kind export --output creates that snapshot; tenant import
--id COMMIT imports native Git files without overwriting local edits. Native
.platform logical IDs must be preserved. Optional tenant allocate maps logical
IDs to local folders, preserving source content; publish the corresponding moves
with all new files and all old file paths in remove. Initial workspace folder
allocation can also use create_folder and move_item before the baseline commit.

After initialization, inspect required_action in its receipt: CommitToGit,
UpdateFromGit or None. Initialization is not full synchronization. For an empty
target, PreferRemote is allowed only after publishing reviewed native item source.
Subsequent deployment uses local source -> reviewed github_publish -> git_update
using the verified publication SHA. Existing external commits cannot be deployed
through this action without going through a reviewed publication for that directory.
Workspace edits, conflicts, missing source reviews, stale heads and item deletions
block deployment. Git publications use GitHub's Git database API and a non-forced
branch update. They do not run Git hooks, source code or GitHub Actions workflows.

One publication is limited to 180 KB of source. Split large changes into reviewed
commits, but include all parts of any moved item together. Exported binary assets
need a format-specific reviewed adapter; do not pretend text-only import covers them.
Git synchronizes item definitions. Continue FMD SQL/bootstrap, variable bindings,
Environment publication and first/repeat-load/audit acceptance as explicit stages.
An enrolled SQL connection template can use sqlDatabase with workspace and exact
itemId or receipt-backed createdItemName. The host discovers its real endpoint
after foundation creation; do not guess a server or put credentials into source.
An uncertain action must be reconciled by reads; never replay it or infer success.
