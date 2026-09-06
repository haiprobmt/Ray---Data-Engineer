# Dedicated SQL Pool Schema and Code to Fabric Lakehouse

Migrate Synapse Dedicated SQL Pool metadata and executable logic to Delta Lake through CLI, REST, and Livy APIs. Source table data is not migrated.

## Execution Contract

- Drive every migration run from dynamic source discovery and the approved compatibility report.
- Generate target artifacts only from the approved feature-risk assessment and target design. After discovery, compare `1:1`, `N:1`, and `N:N` stored-procedure notebook strategies against the projected workspace item footprint. Do not generate notebooks until the user provides and approves the complete source-to-target mapping, target names, dependency grouping, and workspace placement. Generated notebooks are published outputs and are not used to orchestrate migration.
- Treat the source Dedicated Pool as read-only. Never add sample or synthetic data or create/alter/drop source objects, permissions, or security principals.
- Do not execute source stored procedures or issue source DDL/DML. Limit source activity to metadata discovery and metadata-based validation. A requested source mutation belongs in a separate, explicitly scoped workflow and is never performed by this skill.
- Do not export, stage, copy, upload, shortcut, transfer, or load source table rows. Do not generate or execute a data-migration plan.
- Do not run row-count, aggregate, sample-hash, business-result, or other source-to-target data-equivalence queries. Data parity and cutover validation belong to a separately approved process outside this skill.
- Use SqlPackage or source catalog queries for discovery.
- Store converted schema and reusable logic as `.sql`, stored-procedure logic as `.ipynb`, and all object mappings in a machine-readable migration manifest.
- Use Fabric REST APIs for workspace and Lakehouse lifecycle operations.
- Use Fabric Livy sessions for schema execution and Fabric Notebook item definitions for stored-procedure deployment.
- Validate source-to-target schema mappings, generated artifact syntax, notebook definitions, dependencies, and publication status without reading or comparing table rows.
- Print structured status before and after every workflow step and for every object-level operation.

## Status Reporting Contract

Keep the user informed throughout execution; do not wait until a phase ends to report progress.

- Before each step, print: `[Phase X][Step Y/N][STARTED] action; next=expected operation`.
- After each object or restartable checkpoint, print: `[Phase X][i/N][COMPLETED|FAILED|SKIPPED] object; elapsed=...; rows=n/a; next=...`.
- For operations running longer than 30 seconds, print a heartbeat every 30-60 seconds with the current service state and elapsed time. Report state changes immediately. Do not repeatedly print an unchanged state more often than this interval.
- After each phase, print completed, failed, skipped, and pending counts plus the next phase or approval gate.
- On retry or recovery, print the failed operation, bounded retry action, checkpoint used, and whether duplicate writes are prevented.
- Persist the latest status and per-object result in the migration manifest so execution can resume safely.
- Never include passwords, access tokens, connection strings, or source row values in status output.

Example:

```text
[Phase 3][12/45][COMPLETED] migration_scale.dimcustomer; elapsed=18s; rows=n/a; next=migration_scale.dimproduct
```

## Phase Routing

| Phase | Action | Resource |
|---|---|---|
| 1 | Extract and classify source objects | [dedicated-pool-discovery.md](dedicated-pool-discovery.md) |
| 1b | Assess compatibility, workspace capacity, and procedure mapping options; obtain approval | [dedicated-pool-gap-assessment.md](dedicated-pool-gap-assessment.md) |
| 2 | Convert DDL and procedural logic | [dedicated-pool-conversion.md](dedicated-pool-conversion.md) |
| 2b | Audit large-procedure source-block coverage and package validated artifacts | [dedicated-pool-large-procedure-audit.md](dedicated-pool-large-procedure-audit.md) |
| 3 | Create the Lakehouse and deploy schema/code artifacts | [dedicated-pool-deployment.md](dedicated-pool-deployment.md) |
| 4 | Validate schema and generated artifacts | [dedicated-pool-validation.md](dedicated-pool-validation.md) |

## Required Inputs

- Synapse server and Dedicated Pool database
- Source authentication mode and metadata permissions (`CONNECT`, `VIEW DEFINITION`)
- Fabric workspace and target Lakehouse names
- Scope: selected schemas/code objects or the full schema/code workload

Discover workspace and item IDs through Fabric APIs. Ask only for values that cannot be discovered.

Use a source identity limited to database `CONNECT` and `VIEW DEFINITION` whenever possible. This schema/code-only workflow does not require `db_datareader`. If the supplied identity has broader rights, the read-only execution contract still applies.

## Ordered Workflow

### Local artifact fast path

When the request supplies the complete discovered procedure inventory, target binding, approval dispositions, and manifest contract, and explicitly forbids live source or Fabric calls:

1. Treat the supplied inventory as the completed discovery input. **Do not load** the discovery or gap-assessment resources unless an object is ambiguous or unsupported.
2. Read [dedicated-pool-conversion.md](dedicated-pool-conversion.md), then generate every requested notebook and the manifest before expanding the narrative. Prefer one atomic generation pass so notebook hashes and manifest records stay consistent.
3. Do not load deployment guidance for a conversion-only request. When the user also requests publication/readback guidance, read [dedicated-pool-deployment.md](dedicated-pool-deployment.md) only after all local artifacts exist. Read [dedicated-pool-validation.md](dedicated-pool-validation.md) only when the requested report needs validation details not already present in the supplied contract. Do not perform those live operations in a local-only exercise.
4. Validate the local artifacts and return the phase summary. Do not defer the manifest until after the narrative. When conversion succeeds, explicitly state that the generated notebooks contain executable translated Spark SQL; do not describe them only as generated notebooks or transformation logic.

For live or partially specified migrations, use the full workflow below.

1. Authenticate Azure CLI and verify source and Fabric access.
2. Extract a DACPAC or query source catalog views.
3. Build an inventory, dependency graph, and T1-T4 complexity assessment.
4. Generate feature-wise `migration-gap-report.json` and `migration-gap-report.md` for all SQL Pool to Lakehouse capabilities. Report support level, likelihood, impact, overall risk, target pattern, and mapping cardinality. Use the discovered inventory and target workspace inventory to compare `1:1`, `N:1`, and `N:N` procedure-notebook mappings, projected item totals, headroom, names, dependency grouping, and workspace placement; then require the user to provide and explicitly approve the complete mapping.
5. Convert only the approved target design. Generate parameterized Spark SQL Fabric notebooks according to the approved procedure mapping. A `1:1` target retains the exact stored-procedure `sourceName`; `N:1` and `N:N` targets use explicitly approved names and collision-safe parameter namespaces. Preserve each procedure as an independent source decision and retain source-block-to-cell provenance across consolidated or shared notebooks. Apply the naming and bounded parameter contracts in [dedicated-pool-conversion.md](dedicated-pool-conversion.md). For every T3/T4 procedure, every definition with at least 1,000 physical lines, and any otherwise complex procedure, generate the deterministic block ledger, retry only failed blocks within the declared limit, verify complete source coverage, and build the immutable package defined in [dedicated-pool-large-procedure-audit.md](dedicated-pool-large-procedure-audit.md). Do not convert stored-procedure logic to PySpark or the DataFrame API.
6. Resolve or create the target Lakehouse through Fabric REST.
7. Create a Livy session bound to the Lakehouse.
8. Submit schema statements in dependency order and publish only approved, compiled stored-procedure notebooks without executing them. For audited procedures, verify all package hashes first and publish exactly the packaged notebook bytes; record each request, LRO result, target ID, and readback result in the manifest.
9. Compare source metadata with target schemas and validate generated code, source-block coverage, retry history, package integrity, dependencies, decoded persisted notebook definitions, exact Lakehouse bindings, and publication status.
10. Report completed, failed, skipped, and manual-review objects, including the disposition of every discovery gap and an explicit statement that data migration and data parity were not performed.

Apply the status reporting contract to all 10 steps. For batch operations, use the discovered object count as `N`; for service operations such as Livy startup, report service state and elapsed time until ready or failed.

Never seed an empty or small source pool to test migration. Report the discovered source as-is; use target-side fixtures or isolated local tests when conversion testing needs representative data.

## Approval Gates

Require explicit approval after the gap report and before conversion. The user must provide a procedure mapping strategy (`1:1`, `N:1`, or `N:N`) and the complete versioned source-to-target mapping, target names, workspace placement, and required operational headroom. Record per-object `ManualReviewApproved` decisions for every T4 redesign, unsupported parameter, behavior-changing conversion, consolidation/sharing decision, or approved exclusion before deployment. Also require artifact-specific approval before replacing target schema/code artifacts or source decommissioning. This skill cannot approve production cutover because it does not migrate or validate data.
