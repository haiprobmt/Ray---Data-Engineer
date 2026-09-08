# Ray intelligence upgrade — 8 September 2026

Version 0.3.0 improves the host workflow around the existing Codex model. It keeps
Telegram, laptop hosting, isolated project profiles, the official pinned SDK and
the current Fabric write policy. It does not claim a measured improvement in live
model quality, or configure a different model on your laptop.

## Behavior changes

### Local diagnosis and repair

Validation now drains stdout/stderr concurrently and retains up to 12,000
characters of complete, sanitized diagnostic lines. Credential-bearing and
oversized lines are omitted; URLs and recognizable tokens are removed. Process
output is not written to a raw log. The ordinary Telegram message stays concise;
technical validation evidence is available to the next engineering turn and review.
Redaction is best-effort: trusted validation commands must not deliberately print
secrets or encode credentials in arbitrary output.

A failed local validation or reviewer `REWORK` can trigger up to three automatic
repair attempts. Each attempt receives the failure evidence, reuses the task's
engineering thread and gets new host validation and a fresh reviewer. Repeating
an unchanged source/failure category stops the loop. Reviewer `BLOCK`, cancellation,
permissions, missing executables, unresolved business decisions and cloud failures
do not enter this loop. A failed or uncertain Fabric write is never resubmitted by
this mechanism. Cloud execution remains downstream of successful host gates.

### Conversation handoff and durable task context

A work offer and a new `/work` task retain a bounded brief from the same Telegram
actor and selected project. The original task objective is preserved verbatim.
User messages and assistant proposals remain separately labeled; assistant prose
cannot become a business decision or cloud approval. Up to 16 recent messages
(20,000 characters) are retained. Earlier conversation rows without a project ID
remain usable for personal chat but are not imported into engineering briefs.

SQLite now stores each task's brief, recent user requests, optional structured
plan, bounded observations, execution counters and model-run records. Task plans
include acceptance criteria, steps and unresolved questions; model claims of
completed steps are not completion gates. Observations carry timestamps and replace
older observations of the same request. Recheck them after cloud changes.

Relevant earlier completed tasks can be retrieved when local validation/review or
successful cloud receipts support their recorded outcome. Historical summaries
remain evidence, never accepted business rules. All retrieval stays project scoped.
Backups already include the complete SQLite database, including the new table.
Conversation rollout recovery still requires the separately retained Codex profile.

### Skill and reference retrieval

The router searches Markdown sections throughout the pinned `skills`, `common`
and Ray playbook directories. It ranks content, filenames and headings, includes
line coverage and file hashes, and retrieves material from later sections and
reference files. Vietnamese accents are normalized and common Fabric phrases
expanded to English search terms. This is lexical retrieval, not an embedding
model or a claim of complete Vietnamese language coverage.

The model can issue `guidance_requests` during a working turn. Those requests
return additional local excerpts without changing files or granting permissions.
There is no direct model web access, automatic skill installation or script execution.
The in-memory index refreshes when Ray restarts after a pinned skill update.

### Fabric investigation

The existing schema/count/preview and notebook output reads remain available.
Additional typed operations are:

| Operation | Scope and output |
| --- | --- |
| `lakehouse_profile` | Row count, null count and non-null distinct count for selected columns; optional filters. |
| `lakehouse_aggregate` | SUM/AVG/MIN/MAX/COUNT/non-null distinct count, optional grouping and filters. At most 100 groups returned; truncation explicit. |
| `lakehouse_compare` | Missing/extra row multiplicities for selected columns between two tables in the same Lakehouse SQL endpoint, including nulls and duplicate rows. |
| `get_item_definition` | Policy-gated definition part index, then sanitized text excerpts by part path and offset. Supports bounded asynchronous export polling. |
| `get_job_status` | Generic job status for an explicit item/job ID; mismatched response IDs rejected. |

Example analytical read:

```json
{
  "operation": "lakehouse_aggregate",
  "workspace_id": "11111111-1111-1111-1111-111111111111",
  "item_id": "22222222-2222-2222-2222-222222222222",
  "schema_name": "dbo",
  "table_name": "Sales",
  "analytics": {
    "group_by": ["plant"],
    "metrics": [{"function": "sum", "column": "amount"}],
    "filters": [{"column": "year", "operator": "eq", "value": 2026}]
  }
}
```

The host discovers connection metadata from the authorized Lakehouse. SQL is
compiled from a fixed grammar; filters use bound parameters. No raw SQL, server
input, cross-database target, mutation or arbitrary SQL function is accepted.
Existing connection and query timeouts apply. These queries can scan large tables;
result limits do not limit scanned data or Fabric consumption. SQL collation,
unsupported source data types and endpoint synchronization remain runtime concerns.
Comparison covers selected columns only and is not transactionally coordinated
with concurrent writers. Definition text is sanitized, not an exact deployment
payload or proof of byte-for-byte equality. Binary parts are unsupported.

### Execution budgets and model comparison

Default limits per explicit invocation are 24 stages, 12 serviced investigation
rounds per stage, 64 total author/reviewer calls, and a 3,600-second time budget.
Three identical investigation results stop an unproductive loop. Useful read-only
investigations continue through read/guidance requests without creating files.
Purely local source-stage continuation still requires real, validated changes.

The budget is checked between operations and during model progress/cancellation.
Already-submitted Fabric jobs are not cancelled; a blocking external call can run
until its own transport timeout. `/resume` starts a new bounded invocation using
saved context and receipts. Source repair is capped at three attempts per stage.

Optional project settings:

```yaml
# Set an exact model available to YOUR authenticated Codex runtime.
# model: your-available-model-id
reasoning_effort: high
execution:
  repair_attempts: 3
  max_stages: 24
  max_read_rounds: 12
  max_model_calls: 64
  max_seconds: 3600
```

Defaults preserve existing project binding hashes. Explicitly changing these
settings or the model changes the binding: use a new project ID and fresh task as
required by Ray's existing configuration contract. Do not overwrite live state.
Unsupported model/effort combinations are reported by the runtime, not silently
replaced with another model.

The pinned SDK's public high-level Thread API does not expose the resolved model
in the returned Thread handle. Run records therefore separate `configured_model`
from `resolved_model: null`; they do not guess the model when configuration is
unset. Available SDK token-usage events, requested effort, duration, call count and
role are recorded. Missing token usage stays null, never zero. No price or cost is
inferred without a verified pricing source.

Use separate run directories and the same evaluation input pack for comparisons:

```powershell
.\.venv\Scripts\python.exe scripts/run-scenario-tests.py `
  --pack C:\RayTests --run-root C:\RayRuns\improved-current `
  --auth-home C:\YourEnrolledCodexProfile `
  --model YOUR_CURRENT_MODEL --reasoning-effort high --variant improved-current
```

Repeat with another available model and a different run root. For a pre-upgrade
baseline, run the earlier commit from a separate checkout. Do not reuse an enrolled
project ID across settings. The evaluator answer key stays outside Ray's input.
Measure verified task completion, correct environment targeting, unnecessary
questions, human interventions, repair success, latency and observed usage. The
existing evaluation export now includes repair/runtime/context records. Human
usefulness and live business acceptance are still scored separately.

## Update the laptop

1. Stop the Telegram gateway after checking for active or uncertain cloud actions.
2. Back up the state database and retain the isolated authentication profiles.
3. In the existing checkout, run `git pull --ff-only`, then `scripts/setup.ps1`.
4. Run `/doctor` and the existing workspace/SQL checks; restart the gateway through
   your normal `scripts/start-ray.ps1` or setup flow.
5. Try the acceptance tasks below in an enrolled DEV project.

This GitHub update does not restart or modify your privately running laptop process.
No new cloud host, database service, embedding service or GPU is required. Additional
model calls and Fabric query execution consume their respective resources.

## Verification and live acceptance

Automated regressions exercise real local files/processes/SQLite with synthetic
model and Fabric responses. They cover repair gates, bounded diagnostics,
conversation isolation/restart, knowledge retrieval, typed queries, duplicate/null
comparison semantics, export restrictions, and execution budgets. The existing
policy/approval/receipt/recovery suite remains required.

The repository's vendored hash test now normalizes LF/CRLF before comparing the
original CRLF-based hashes. No upstream source contents or pins were replaced.
The missing-table unit test now supplies the SQLSTATE exception contract without
requiring a native ODBC driver. Worker termination handles a process that exits
between polling and signaling; timeout/cancellation regressions cover this race.

Live acceptance remains to be run on the laptop:

- Discuss two designs, then say “build option 2, keep my table names”; inspect the
  transferred brief and resulting implementation.
- Introduce a local assertion failure and a reviewer REWORK; verify correction,
  fresh validation/review, and no repeated cloud mutation.
- Ask in Vietnamese for a report or incremental-load task; inspect retrieved sources.
- Reconcile DEV tables containing duplicates/nulls and compare to independent SQL.
- Read a real item definition, including an asynchronous export when Fabric returns
  one, and inspect a real failed job.
- Interrupt a multi-step task, restart the gateway, resume, and check receipt reuse.

No live model success rate, Fabric query performance, Windows containment, Telegram
interaction or laptop restart acceptance is claimed by the offline test suite.

### Local verification record

- Python 3.12, Linux: `python -m pytest tests -q` — **495 passed, 8 skipped**
  in 40.53 seconds. Skips are existing platform/runtime-dependent cases.
- 31 new intelligence regressions included in the total.
- Dependency consistency and `git diff --check` passed.
- Built `ray_fabric_engineer-0.3.0-py3-none-any.whl` offline with setuptools 84;
  verified the new modules and persona match the source checkout. This is a
  packaging check; the historical 0.2.0 wheel in `distribution/` is unchanged.
- No authenticated Fabric, Telegram or live model execution performed.
