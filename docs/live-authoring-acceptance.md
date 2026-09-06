# DEV authoring acceptance — 6 September 2026 (Singapore)

The user authorized enabling Fabric write/create capabilities in the previously
enrolled DEV workspace `ws-test`. A new immutable authoring project was created
outside this checkout and selected in Telegram write mode. The old read-only
project and its tasks were preserved. Authentication remained in private runtime state.

The actual Codex model authored local source artifacts, the host validated them,
and fresh independent model reviews passed with comments before each cloud stage.
The workflow used the live Fabric APIs, not synthetic responses.

| Resource | Live outcome |
| --- | --- |
| `ray_excel_demo_lh` | Created and metadata verified; ID `96bdd9ea-3c2a-47a0-a73a-b5f5e7281174` |
| `ray_excel_demo_nb` | Created; metadata and exported definition verified; ID `0d4a0af2-d261-42ac-8f9c-8caf80eec522` |
| `ray_excel_demo_pipeline` | Created; metadata and complete JSON definition verified; ID `30b656a5-7fd4-426e-98e7-84c417860a83` |
| Pipeline job | Fabric reported `Completed`; ID `8352b238-7986-4885-9dc0-16df01574326` |
| Lakehouse table | A separate live table-inventory read confirmed managed Delta table `sample_sales` |

The executed notebook generates `Files/ray_excel_demo/sample_sales.xlsx`, reads the
persisted workbook back and writes `Tables/sample_sales`. Runtime assertions check
the exact five rows, types, unique IDs, positive quantities and quantity total 15.
Acceptance relies on the successful execution of that verified definition plus
separate table discovery; workbook download, separate SQL row queries, repeated-run
idempotency, TEST promotion, all other item types and sandbox containment were not tested.

Fabric reformatted the pipeline JSON on export. The original byte comparison
conservatively marked creation uncertain. After confirming the complete JSON values
were identical, comparison was corrected to ignore JSON whitespace/key order while
preserving every field and embedded source string. The recorded item ID was reconciled;
creation was not repeated.

Local Fabric authentication later could not renew silently. The local Microsoft sign-in
flow renewed it, and a fresh live table-inventory read succeeded. Ray now distinguishes
local sign-in renewal failures from HTTP rejections and reports the recovery command.

Validation: the complete suite passed 179 tests after the final production-code change.
These automated tests use synthetic service responses; live evidence above is recorded
separately in runtime task `1c8dfc70-8c75-45d2-bc65-38530cf3f086` and its four action receipts.
