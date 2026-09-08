# Implementation status — Ray 0.2.0

## Tenant and Git deployment extension

Ray now implements tenant-bound workspace creation/capacity assignment, execution
identities, scoped roles and groups, protected connection administration, native
Git import/folder allocation, personal GitHub publication and Fabric Git sync.
All are explicit capabilities under the existing source/reviewer/receipt policy;
TEST needs exact approval and PROD/deletion remain disabled. Initial administrator
bootstrap and actual tenant/repository enrollment are still required. See
[tenant deployment](tenant-git-deployment.md) for supported operations, configuration,
native format limits and the distinction between offline tests and live acceptance.

## Lakehouse read extension

The host now enriches inventory snapshots with bounded Lakehouse detail reads and
supports structured requests for metadata, table inventory, SQL table schema,
row count and a capped row preview. Read requests continue within the same task;
only fixed SELECT templates reach the isolated SQL transport. Connection metadata
comes from authorized Fabric reads. Existing mutation review and approval controls
remain in effect. Tests use synthetic responses; live SQL acceptance is outstanding.

## Workspace authoring extension

An explicit `/workspace enable-write` command now creates a separately bound authoring
project for an enrolled DEV/TEST workspace. Generic item creation, metadata PATCH,
definition updates and supported job execution use `cloud.py`, scoped to configured
workspaces. `workspace_write` resolves existing item types from live metadata instead
of requiring a manually maintained item list; `create_items` enables Items API creation.
PROD/destructive operations remain unavailable. Administration requires the explicit
tenant capabilities described above. Checked-in examples stay local-only.

Structured source artifacts and SourceFile definition compilation support runtimes
whose model file tools are unavailable. Validation and fresh independent review precede
each stage. Created item/operation receipts support dependent stages and read-only
reconciliation. No creation is retried or adopted by name after an uncertain response.
Creation, update, job and TEST approval tests use synthetic service responses.

Private runtime configuration has been enabled separately for the user's DEV workspace;
this is not a claim that every Fabric API or item type has been tested live. Exact live
outcomes are recorded in that runtime's action receipts. Direct host OneLake upload,
binary definition payloads and general admin APIs are not implemented.

## Original local-only delivery record

The user authorized all remaining phases and selected **local-only for now**.
The local application implements Sprints 1–6. No live services or startup jobs were enabled.

| Phase | Delivered | Live acceptance still required |
| --- | --- | --- |
| 1 | Official SDK, explicit runtime, doctor, strict config, checkpoints, local tests and independent review | Real model behavior and Windows sandbox containment |
| 2 | Allow-listed discovery, snapshots, pagination, audit and 260 pinned skill files | Actual Fabric discovery |
| 3 | Private Telegram polling, identity/project guards, controls, durable buttons, inbox/outbox and Windows logon scripts | Bot connection and reboot/logon test |
| 4 | SQLite memory, Markdown decisions, relevant context, isolation tests and verified backup | Full conversation restore with separately retained rollouts |
| 5 | Existing Notebook/DataPipeline updates, DEV jobs, source/reviewer gates, post-validation and reconciliation | Actual definition round trips and business-data checks |
| 6 | Exact TEST approvals, reviewed rollback, fabric-cicd ADR, incident playbook, monitor-once and evaluation report | Live promotion and model evaluation |

The final suite contains 117 automated tests. Both sample inventory tests pass.
The offline demonstration passes ten checks using real files, SQLite and validation
subprocesses with synthetic model/Fabric responses. See verification.json for evidence.
No human usefulness scores or live success rates were invented.

Source was formatted and checked with Ruff. PowerShell scripts were parsed statically.
Startup registration and its preview were not executed. Dependencies, wheel contents
and vendored hashes were checked. Doctor correctly reports empty workspace configuration
and unauthenticated isolated profiles.

Service shutdown interrupts workers while preserving pending user decisions. Explicit
/stop cancels pending decisions and actions. A regression covers this distinction.

Before live DEV use, evaluate the blueprint gates: zero protected write violations
or context leaks, at least 90% correct environment targeting, at least 80% local/read
task success, complete audit and demonstrated approval/rollback behavior. Synthetic
tests do not establish those live rates. Real credentials, permissions, linked
resources, data checks and Windows containment remain environment-specific acceptance.

At original delivery, PROD/destructive/admin actions were unsupported. Teams, WhatsApp, voice, email, GitHub
and broader Power BI operations remain the blueprint's explicitly deferred backlog.

Sources: [Codex SDK](https://learn.chatgpt.com/docs/codex-sdk),
[Fabric CLI](https://github.com/microsoft/fabric-cli),
[pinned skills](https://github.com/microsoft/skills-for-fabric/tree/714ea2f9431179344ecd9bc673a9881a773c9f47),
[Telegram API](https://core.telegram.org/bots/api), and API links in cloud-actions.md.
Installed SDK/CLI source was inspected for exact signatures and request behavior.
