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

## Interruption and recovery

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

Authenticate isolated profiles, provide real IDs, check sensitivity/export permissions,
validate actual business-data reconciliation and Notebook/Pipeline round trips, verify
Windows sandbox containment, and test logon/reboot on the intended machine. Then run
the evaluation scenarios and record actual model quality and human interventions.
No local synthetic test result is a live acceptance result.
