# Operations runbook

All commands use the same absolute `--project` and `--data-dir` paths. Keep state
outside project repositories. Do not move a bound project or edit its policy under
an existing ID; use a new ID for changed bindings.

## Health and logs

`doctor` reports runtime versions, executable paths, isolated authentication readiness
and configured policy. `status` lists durable tasks. In Telegram, `/status` shows the
selected task and pending decisions. SQLite is the structured log: `events` records
state transitions; `actions` records actor, task, endpoint, environment, status and
error category; `plans` records exact payload/source/rollback bindings and outcomes;
`inbox`/`outbox` track delivery; `settings.telegram_health` records last polling health.
No raw model/process logs are sent to chat. Secret redaction is pattern-based, so
avoid including confidential data in requests and decision text.

Telegram `/status` and `/details` now give short, plain-English updates. Use
`/details technical` for the review, command results, error codes and Fabric IDs.
The source stage remains in progress while Fabric changes or jobs are pending;
current action receipts override a stale saved completion message. SourceFile
definitions can be validated directly in memory without temporary-folder rewrites.

The `reading_material` table holds bounded, redacted file/repository text by actor,
project and binding. It is included in the state database backup. `/forget` clears
the actor's recent chat/reference cache; it does not delete saved task evidence.
See [document and GitHub reading](document-and-github-reading.md).

## Interruption and recovery

Engineering model turns use `timeout_seconds` as the inactivity limit while
bounded SDK activity is observed, with a hard limit of three times that value
(at most two hours). Chat and runtime probes keep their original total limit.
At the default 600 seconds, an active engineering turn has up to 30 minutes;
an unresponsive one still stops after 10 minutes. `/status` shows the current
activity; `/details technical` distinguishes inactivity from the overall limit.
See the [Gold timeout repair](model-timeout-repair-20260906.md).

1. Issue `stop` to halt new work. Already submitted jobs continue in Fabric.
2. After the old process exits, run `recover` under the project lock. Local execution
   pauses. An executing plan becomes uncertain; no external action is resubmitted.
3. Inspect `actions list`, `actions show`, task status and platform evidence. Use
   `actions reconcile` for an uncertain operation with sufficient recorded evidence.
4. `unpause` permits new work. `resume TASK MESSAGE` explicitly continues the same
   primary thread. Old cancellation tokens stay invalid after resume.

Telegram startup recovers local execution and in-flight channel delivery records.
Pending questions persist. `/status` re-displays them; `/actions` reissues pending
approval buttons with the same exact plan. Uncertain outgoing messages are not retried
because Telegram lacks an application idempotency key here. Check task status if a
reply was lost. Deduplicated received updates are never automatically replayed.

## Backup and restore

Use `scripts/backup.ps1 -Project CONFIG -DataDir DATA -Output NEW.zip` or the CLI.
The online SQLite backup is transactionally consistent; project Markdown/config files
are copied under the selected project's lock. The archive holds the complete task
registry and selected project documents, plus SHA-256 hashes. Credential stores,
conversation rollout files and repository content are excluded. Store repositories
in source control and retain any needed rollout files separately under the same
protected OS account.

For restore, stop all Ray processes, verify the archive with `verify-backup`, and
extract it to a new inspection directory. Preserve the current state as a separate
backup. Restore `ray.db` only while no process is using the data directory. Restore
config/context/decision files at their original bound paths; do not overwrite an
active WAL database. Reauthenticate profiles as needed. The included backup alone
can restore task/decision evidence but cannot recreate a missing Codex rollout. If
rollouts are unavailable, start a new task using the restored summary.

## Optional monitoring

`monitor-once` only reads configured workspace inventory. It stores a baseline hash
and reports `notify: true` for a change, recovery or new error. It does not send a
message or install a background task. A user-managed scheduler can call this command
and route meaningful events after live setup. Repeated unchanged errors stay quiet.
Do not run concurrent monitor and engineering jobs for the same project; both use
the project lock.

## Release acceptance still needed before live operation

### Lakehouse connection metadata and row verification

Snapshots now include details for up to 20 Lakehouses per workspace, including
`properties.sqlEndpointProperties` when supplied by Fabric. Per-item errors and
omitted details are explicit. Ray can request a particular Lakehouse's metadata or
table inventory through the host when the initial snapshot is insufficient.

Engineering turns can also request `lakehouse_schema`, `lakehouse_count` and
`lakehouse_preview`. The host in `fabric.py` validates the configured workspace and
Lakehouse ID, resolves the SQL endpoint and database name from Fabric, and invokes
an isolated SQL worker with fixed SELECT templates. The model cannot supply SQL or
connection settings. These reads do not enable cloud writes or alter TEST approvals.
There are at most three requests per round and four read rounds per stage.

Install `.[fabric]` (including pinned `pyodbc==5.3.0`) and Microsoft ODBC Driver 18
for SQL Server on the host. The worker uses the project's isolated Fabric sign-in
to obtain a SQL audience token without interactive renewal. It requires SQL read
permissions and outbound TCP 1433. Missing prerequisites and unavailable sign-in
are reported as specific errors; tokens and raw driver errors are never returned.

SQL results are limited to 100 rows and 16 KB per request, with a 45-second query
timeout. Table/schema identifiers currently support letters, digits and underscores,
starting with a letter or underscore. Database names support letters, digits, spaces,
underscores and hyphens. Preview order is unspecified and truncation is explicit.
Counts reflect rows visible through SQL at query time; SQL endpoint synchronization
may lag Delta changes. Notebook literals and synthetic executors are not live evidence.

For `FABRIC_SIGNIN_REQUIRED`, use `/workspace login`, finish Microsoft sign-in,
then `/workspace check`. Restart the Ray host after installing updated code, and
explicitly resume the blocked task. Resuming an authoring task can proceed through
its normal validation, review and enabled cloud-action policy.

For `FABRIC_SQL_SIGNIN_REQUIRED`, use `/workspace login-sql` and run the displayed
local PowerShell command (`ray ... login sql`). This explicitly acquires SQL
authorization in the same project profile; background queries never open interactive
sign-in. SQL login confirms token acquisition only, not database permissions or
connectivity. `/workspace check` tests metadata and cannot establish SQL readiness.
Resume the failed task after sign-in so its actual SQL read can verify access.
If the Windows broker fails, run `ray ... login sql --browser` explicitly. This
uses MSAL browser sign-in with the same tenant, client and encrypted project cache,
with a five-minute timeout. Background SQL queries never launch either sign-in flow.

Authenticate isolated profiles, provide real IDs, check sensitivity/export permissions,
validate actual business-data reconciliation and Notebook/Pipeline round trips, verify
Windows sandbox containment, and test logon/reboot on the intended machine. Then run
the evaluation scenarios and record actual model quality and human interventions.
No local synthetic test result is a live acceptance result.

Telegram task replies show a readable outcome and summary. `/details technical` displays
the current task's full identifiers, evidence, validations and action receipts;
`/status` remains available for operational status. Ordinary conversation and
task replies support bold labels, inline code and fenced code blocks using
Telegram text entities. Long replies split without breaking Unicode offsets.
Blocked outcomes, failures and pending approvals stay visible in the summary.
`/details technical` includes the independent review verdict and findings before background
evidence, so a review block has actionable reasons. SourceFile notebook compilation
normalizes cell source strings to Fabric-compatible line arrays before validation
and review; compiled payloads and local notebook source remain byte-consistent.
Fabric can add notebook metadata after creation. A later run still requires an
exact match to the reviewed definition. `FABRIC_DEFINITION_CHANGED` means to
inspect the export and obtain fresh review of reconciled source, not to enable
more permissions or repeat creation.

For unattended service-principal authentication on Windows, put
`FAB_TENANT_ID`, `FAB_SPN_CLIENT_ID`, and `FAB_SPN_CLIENT_SECRET` in a local,
Git-ignored `.env`. Run `ray --project <config.yaml> --data-dir <state> login
fabric --service-principal-env <absolute-path-to-.env>` locally. This verifies
both Fabric and SQL token acquisition with a new in-memory cache, then stores
the credential using Windows user-bound DPAPI, bound to this project's profile,
tenant and application. Fabric and SQL workers reload it for token renewal.
The model environment does not receive these variables. The `.env` stays on
disk; it is not needed by workers after enrollment. After secret rotation,
update it and rerun enrollment. Workspace and SQL permissions still require
live checks. Browser SQL login applies only to human user profiles.

`doctor` now reports `ready_for_command_execution` separately from model/Fabric
authentication. It runs one fixed read-only shell command through the same pinned
SDK configuration, without a model turn. A successful probe establishes that command
execution works; it does not establish sandbox containment or Fabric write access.
Fresh Windows profiles select `unelevated` explicitly, because an unset native
sandbox rejects commands with escalation denied. An explicit `elevated` selection
in that profile's `config.toml` is preserved. Enterprise restrictions still apply;
Ray never falls back to full access. Store PowerShell PATH entries are excluded only
from Ray's subprocess environment; standalone PowerShell/Codex paths remain available.
Local shell profiles are disabled, and Python resolves to Ray's installed environment.

Review source prioritizes current changes, pending changes from a resumed task and
proposed definitions over older repository files. Required review source is never
silently omitted when its budget is exhausted. Host structural validation remains
distinct from executing authored tests; Ray and its reviewer can now run those tests
in the native sandbox. The model can request `get_item`, `list_items` and
`get_notebook_job` through the workspace-bound read gateway, including Notebook
exit values for actual job IDs. These reads keep credentials in the host workers.

During model-requested reads, classified service failures are returned as
`source=host_read_error` with fixed public error text. Successful action receipts
remain available independently of those observations. Identical failed reads are
not retried within that stage; Ray must use other evidence or report the limitation.
`FABRIC_SQL_TABLE_UNAVAILABLE` means SQL returned missing-table SQLSTATE `42S02`;
check schema/table metadata and allow for newly written Delta tables to synchronize.
It is not proof that the preceding pipeline failed or that sign-in must be renewed.

Source manifests are checkpointed in SQLite before an author turn. Exceptions and
restart recovery retain unfinished changes for the next independent review;
validation-generated files join that scope. A completed source stage no longer
consumes the next stage's required-source budget. The added checkpoint table is
created automatically when Ray opens its state database.

A new source turn supersedes unused action plans and approval decisions from the
previous turn. Successful and failed historical receipts stay visible. Current
stage plans determine progress, while unresolved remote actions still prevent a
completion claim. Reconciliation records a subsequently observed terminal job
failure without repeating the job.

Dependent TEST actions are prepared one stage at a time. Telegram continues after
an approved action succeeds, obtains fresh source review, and presents the next
exact approval when needed. With the CLI, resume the task after executing an
approved action whose task state remains WAITING. Cancellation during reads now
persists PAUSED, and worker timeouts/oversized previews retain their specific
error categories. See [the broader reliability audit](reliability-audit-20260906.md)
for regression cases and verification limits.
