# Runtime and authoring repair — 6 September 2026

The reported Bronze pipeline task stopped before any Fabric mutation. Its source
stage passed structural validation, but the independent reviewer lacked the new
test contents. Alphabetical source collection consumed the 180,000-character budget
on older definitions before reaching the tests. Its attempted shell read then failed.

The actual isolated SDK profile reported Windows sandbox readiness `notConfigured`.
An actual `Get-ChildItem -Name -Force` model turn reproduced the policy rejection.
Selecting the native non-admin sandbox exposed a second failure: Store PowerShell
could not start with the restricted token. Merely supplying another PowerShell path
to the model tool did not fix it: this Codex version resolves that path by shell type
and searches PATH again. Removing the incompatible Store PowerShell entries from
Ray's subprocess PATH allowed Windows PowerShell to execute in the same sandbox.

Changes:

- Explicit Windows sandbox selection, preserving an existing elevated selection,
  denied escalation, disabled shell profiles and Ray's own Python environment.
- Changed/pending source and proposed definitions precede unrelated source in review.
  Required source exceeding the evidence budget fails before a review can be accepted.
- A fixed SDK shell probe in `doctor`, reported separately from authentication.
- Workspace-bound model reads for item inventory/metadata and Notebook job output.
- Specific SQL table-visibility errors, preserved action receipts through read
  failures, and bounded error feedback to the model without automatically retrying
  the failed read. A read error cannot turn a succeeded action receipt into failure.
- Pending review source survives reviewer interruption, and equivalent definition
  paths are normalized before required-source selection.

Verified evidence:

- The actual model shell command failed before the repair and succeeded afterward.
- The repaired `doctor` shell probe, Codex login and Fabric login checks passed.
- All 249 repository tests passed. These use synthetic cloud responses and are
  separate from the live checks below.
- The blocked Bronze task resumed with all four required source files supplied.
  Both the real author and fresh independent reviewer ran the nine parser tests
  successfully. The reviewer also decoded and compared the Notebook payload.
  Verdict: `PASS_WITH_COMMENTS`.
- Live workspace metadata/inventory reads succeeded. The new model read route
  retrieved the existing discovery Notebook job's Completed status and exit value.
- The Bronze Notebook was created through `CloudActions`, after validation and
  fresh review. Action `cgsiJDONqCCZf8fKUXWlmg` succeeded; Notebook ID
  `8c1eb835-e758-4104-81e4-84a37c3f3830`. Metadata and definition readback matched.
- Ray then created the dependent DataPipeline using that Notebook ID, with another
  fresh independent review. Action `Glpc9QppnwVHC2xOMKvQHg` succeeded; pipeline ID
  `80d4c2d6-005e-41c1-a194-4d54d5507f9c`. Metadata and definition readback matched;
  separate live `get_item` reads also confirmed both new items.
- Pipeline execution action `MhlLWSBtnafdUDZ8cw-jgA` succeeded. Job
  `07917bdc-210c-4185-91f8-34b30d5a0b02` ran from 10:02:06 to 10:07:09 UTC;
  a separate live API read confirmed `Completed`. The Notebook child job
  `c9c38973-951b-4db3-bed8-586d0b373325` also reported `Completed`.
- A separate Lakehouse API read listed all seven new Bronze Delta tables: the five
  raw-source tables, source-file archive and ingestion-batch manifest. The executed
  notebook checks persisted counts/content and immediate replay against the bytes
  it reads. Those assertions ran within this pipeline; no independent numeric table
  counts are claimed from source literals.
- A separate governed description-only update on the new pipeline passed fresh
  review and live readback. Task `b87baf73-cb9b-499d-b797-b15b50fa4590`, action
  `c7v0716I2bzK9QYu1GIpIA`, state `SUCCEEDED`. The 10:23:13 UTC live snapshot
  confirmed the exact requested description and unchanged name/type. No job was
  run for this metadata update; definition update behavior was not retested here.

The first follow-up SQL queries could not resolve the new tables: actual ODBC
SQLSTATE `42S02`, native error 208. A comparison query to the pre-existing
`member_acceptance` table succeeded and returned 3 at 10:16:14 UTC. New-table SQL
visibility therefore remains a separate verification limitation, consistent with
metadata synchronization delay, rather than a general sign-in failure. The beta
Notebook exit-value route returned 404 for this pipeline-triggered child job, so
its exit report was not obtained. Neither read failure resubmitted ingestion.
The original task was explicitly resumed for reporting after the repair and passed
fresh review. It now reports `COMPLETED` for pipeline creation/execution while
preserving the SQL verification limitation and all three successful receipts.
The idle Telegram gateway was restarted after verifying that no tasks or actions
were executing. Its new process recorded successful polling at 10:25:49 UTC.
This establishes startup/polling after the repair; no outbound Telegram message
or end-to-end message-delivery test was performed in this session.

The task is `f5db6f09-ba7c-4aaf-88e8-2f11e52cbdcc`, in configured DEV workspace
`7753594d-a4c7-4ce3-a905-6980339c87bc`. Credentials and private runtime configuration
remain outside the checkout. This is not acceptance of every Fabric item type,
TEST/PROD, concurrent ingestion, or Windows sandbox containment.

References: [official Windows sandbox guidance](https://developers.openai.com/codex/windows)
and [Codex 0.153.4 shell resolution](https://github.com/openai/codex/blob/rust-v0.153.4/codex-rs/shell-command/src/shell_detect.rs).
