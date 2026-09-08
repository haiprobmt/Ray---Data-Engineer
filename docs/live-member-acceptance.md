# Fabric Member workflow acceptance — 2026-09-06

Acceptance is **partial**. Live workspace reads, four item creates, Notebook
and pipeline updates, separate pipeline reruns, and SQL reads succeeded. The
project now uses a service principal with protected credential renewal. This
is not a claim that every Member permission,
every Fabric item type, or the account's exact workspace role was verified.

Workspace `ws-test` (`7753594d-a4c7-4ce3-a905-6980339c87bc`) is classified as DEV
by the enrolled project policy. The test used Ray's real pinned Codex runtime,
fresh independent reviews, Fabric transport, and durable action receipts.
The KPI rerun used Telegram Desktop; other lifecycle scenarios used the CLI's
shared TaskService. No synthetic executor established a live result.

| Check | Evidence and outcome |
| --- | --- |
| Workspace metadata and inventory | Live Fabric API snapshot and subsequent snapshots succeeded. |
| Lakehouse creation/readback | `ray_acceptance_20260906_lh`, ID `d75b960e-faf2-4930-a27a-1d139a870859`; successful metadata verification. |
| Notebook creation/readback | `ray_acceptance_20260906_nb`, ID `6911f319-f1fb-4147-8369-b7d37adbc4cb`; metadata and definition verified. |
| SP discovery Notebook creation/readback | `ray_acceptance_20260906_discovery`, ID `2953fc20-52f9-4bb0-bca0-e17738d31001`; action `pS4nDEfc6HOATecm_tAgbw` succeeded with metadata and definition verification. |
| SP discovery Notebook run/output | Job `2c1da3e2-08d6-4937-9a10-d5480d566e54` completed. Exit report captured at 09:19:16 UTC: five Files entries and one Delta table, complete inventory and row coverage, no failed reads or output omissions. Rows: customers 14, inventory 35, plant mapping 4, sales 66, rules 1, member_acceptance 3 at Delta version 7. Profiling is partial for two omitted nested JSON distinct metrics. |
| Pipeline creation/readback | `ray_acceptance_20260906_pipeline`, ID `9316e852-b5cb-4b0c-82e3-f41d3eaa67b9`; metadata and definition verified. |
| Initial pipeline execution | Job `d2b4bf0e-ea3b-4606-9389-b512bea7aea1` completed successfully. |
| Persisted table discovery | Live Lakehouse table API listed managed Delta table `member_acceptance`. |
| Notebook metadata update | Plan `0y0EqCtJ0Af6CeAHXgUy3A` succeeded; phase 2 description verified. |
| Notebook definition update | Plan `ymW5V-OuJNNbqnFjdpmzOQ` succeeded; operation `3ba7bced-e2c5-4d86-bd3a-480ad87e705b`. |
| Direct Notebook execution | Job `a4e8f207-2874-4b1d-a1ec-b1d3d1bfd36c` reconciled SUCCEEDED using the SP without resubmission. Its actual exit value records two successful cycles, each with three rows, total 65 and exact-row equality. |
| SQL schema/count/preview | All three live SQL reads passed using the SP. Schema: member_id int, label varchar, quantity int; count 3. Latest preview at 07:51:31 UTC returned alpha 10, beta 20 and gamma 35. |
| Pipeline definition update and two separate reruns | SP update plan `5EhRH_PVFe36BtRg3cQ47Q` succeeded. Separate reviewed runs `936e3bc4-b059-49cb-9250-4fc5960df7df` and `00af8c8a-16e3-41aa-aa4f-40df4988bb65` both completed. |
| Telegram SP SQL check after restart | Task `e9d6d350-599e-4afa-97b2-c863dd630e2b` completed. Ray requested real schema/count/preview reads, reported three rows and returned-row total 65 with timestamps, and made no writes. |

The initial Notebook source asserts three exact persisted rows, quantity total
60, schema/ID checks, and two overwrite/read cycles. The phase 2 source changes
gamma from 30 to 35 and the asserted total to 65. The direct Notebook exit value
independently reports total 65 at 05:19:16 UTC. SQL initially returned the older
gamma 30 at 07:47:37 UTC; a subsequent query returned gamma 35 at 07:51:31 UTC.
This demonstrates SQL synchronization lag and why source constants must not be
reported as queried metrics. Two successful pipeline runs support logical
repeatability for this bounded overwrite fixture; incremental/concurrent behavior
remains outside this particular test.

The initial creates and Notebook updates used the previous user identity. The
SP checks verified metadata/table reads, pipeline update/execution, job-output
access, SQL reads, and creation of the separate discovery Notebook.

Five approved input files were copied byte-for-byte into
`inputs/acceptance_20260906` in the enrolled authoring repository. Evaluator-only
sentinel expectations were not supplied to Ray.

| Data scenario | Rerun outcome |
| --- | --- |
| T01 customer clarification | Identified leading zeros, duplicates and missing values; asked a consequential activity-evidence question with a recommendation. |
| T03 incremental Gold | Detected conflicting SO5005 versions, invalid date, missing customer, negative amount, unknown plant and late modifications. Assessment only; executable aggregation and runtime acceptance remain pending. |
| T04 inventory Silver | Detected duplicate, invalid quantity, missing plant, negative values and conflicting warehouse mappings. Asked a consequential mapping question. Assessment only; transformation implementation remains pending. |
| T05 discrepancy | Correct conditional bridge: 2,413,500 − 36,000 − 25,000 + 7,500 − 1,260,000 = 1,100,000. Explicitly blocked end-to-end attribution because actual report output/filter definitions were absent. |
| T06 KPI ambiguity | Live Telegram reply identified conflicting definitions, absent completion status and insufficient 12-month coverage. Preserved blocked status and displayed the recovery question. |

Fixes made during acceptance:

- Accept a blocked response containing a recovery question and recommendation;
  preserve blocked state and prohibit cloud eligibility. Show that guidance in
  Telegram without approval buttons.
- Give the independent reviewer the current author's status and claims. A
  clarification assessment is no longer implicitly presented as a completion.
  The T04 and T05 live reruns exercised this corrected review context.
- Add explicit `login sql --browser`, using MSAL with the existing enrolled
  tenant, client and encrypted project cache. Background reads remain silent.
- Distinguish rejected mutations from failed status/definition reads. A polling
  authentication failure leaves a submitted operation UNCERTAIN and reconcilable.
  A regression verifies that reconciliation does not submit the job again.
- Import an explicitly selected Git-ignored local dotenv credential into Windows
  DPAPI storage bound to the project profile, tenant and client ID. Both workers
  load it silently. Fresh, empty-cache Entra requests verified Fabric and SQL
  token acquisition; subsequent real SQL workers verified database access.
  A final fresh subprocess loaded the DPAPI credential, replaced only its
  in-memory MSAL cache with an empty cache, and acquired both resource tokens
  silently. The persisted token cache was not cleared or modified for this check.
- Add bounded host Notebook job-output diagnostics using the documented beta
  endpoint. Only the exact GET route accepts beta=true; unrelated beta routes
  and mutations are rejected.

The existing direct-run plan `WSTaeHJ_cFivST2Nmi0m7Q` had been incorrectly marked
FAILED. Its stored successful submission and failed status-read audit establish
uncertainty, not terminal failure. Its state was conservatively corrected to
UNCERTAIN with an audit record; its payload, digest and job receipt were retained.
It has now been reconciled successfully with its original job ID.

The discovery review reproduced loss of directory partition metadata and leaking
exception messages into report output. Revised notebook helpers preserve string
partition metadata and emit only fixed error categories. Eight helper tests pass
offline; those tests do not establish partitioned Spark runtime acceptance.
Fabric rejected an initial creation for string cell sources. The source compiler
now normalizes SourceFile notebook cells to line arrays before validation/review.
The failed creation receipt remains intact; the corrected creation succeeded.
Later preflight identified Fabric-added metadata and an omitted ipynb export
format selector. Local run source was reconciled and freshly reviewed; exact
definition matching remains enforced. Telegram `/details technical` includes review
findings, and definition mismatch errors give specific recovery guidance.

Validation: `.venv/Scripts/python.exe -m pytest tests -q` — **236 passed**.
This includes policy, exact TEST approval, replay, stale-source, uncertain-action,
read-target isolation, credential-redaction and the new regression checks.
These checks are offline evidence, not live TEST/PROD or sandbox acceptance.

Remaining acceptance work:

1. Complete T03/T04 executable acceptance using explicitly stated test-only
   quarantine/mapping policies, then run the transformations against isolated
   acceptance tables. Production business decisions remain unresolved.

Machine-local evidence and prepared follow-up prompts are under
`C:/Users/haing/.codex/tmp/ray-evaluation-20260906/`, particularly
`member-evidence.json`, `poll-state-repair.json` and `dq_prompt.txt`.
Created acceptance items are retained. No unrelated items, permission assignments,
PROD resources or destructive/admin operations were changed.

Scope references: [Microsoft workspace roles](https://learn.microsoft.com/en-us/fabric/fundamentals/roles-workspaces)
and [MSAL token acquisition](https://learn.microsoft.com/en-us/entra/msal/python/getting-started/acquiring-tokens).
