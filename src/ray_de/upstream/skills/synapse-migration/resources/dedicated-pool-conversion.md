# Dedicated Pool T-SQL Conversion

Convert extracted Synapse objects into Spark SQL artifacts for Fabric Lakehouse execution and generated Spark SQL Fabric notebooks for stored procedures. Keep the translated SQL approachable for customers familiar with Synapse Dedicated Pool T-SQL.

## Inputs and Outputs

**Inputs**
- Source SQL files or catalog definitions
- Object inventory, dependencies, and complexity tier
- Approved `migration-gap-report.json` with accepted scope, procedure mapping strategy, versioned source-to-target relationships, workspace placement, and dispositions
- Target schema naming policy

**Outputs**
- `schema/*.sql`: idempotent Spark SQL Delta DDL
- `logic/*.sql`: reusable Spark SQL generated from non-procedural source logic
- `notebooks/*.ipynb`: Spark SQL Fabric notebook components defined by the approved `1:1`, `N:1`, or `N:N` procedure mapping
- `source/<sourceStableId>.sql`: immutable exact source evidence for each audited procedure
- `procedure-audit/`: source-block ledgers, bounded attempt records, generated run-specific scripts, conversion-manifest projection, and deployment package
- `migration-manifest.json`: source features/objects, approved target components, mapping cardinality, dependencies, tier, status, warnings, and validation cases

Generate artifacts only from discovered definitions, dependency metadata, the feature-wise risk assessment, and approved target-design dispositions. Do not infer, consolidate, split, or rename procedure notebook components beyond the approved versioned mapping.

## Conversion Rules

| Source pattern | Target pattern |
|---|---|
| `CREATE TABLE ... DISTRIBUTION` | `CREATE TABLE IF NOT EXISTS ... USING DELTA`; omit MPP distribution and index clauses |
| `CTAS` | Generate a non-executed Spark SQL conversion artifact; do not materialize data |
| `MERGE` | Generate non-executed Delta `MERGE INTO` logic with explicit match clauses |
| Temp tables | Spark SQL temporary views with unique names scoped to the notebook session |
| Stored procedure parameters | Externally overridable Fabric Notebook Activity parameters consumed through `%%configure` substitution in `%%sql` cells |
| Output parameters | Final Spark SQL result set with clearly named output columns |
| Transactions | Idempotent stages and Delta atomic writes; redesign multi-statement transaction assumptions |
| Cursors and loops | Set-based Spark SQL transformations; mark irreducible cases for redesign instead of falling back to PySpark |
| Dynamic SQL | Resolve bounded variants explicitly; mark unbounded generation as manual review |
| Views | Spark SQL view definitions where supported; otherwise generate a non-executed redesign artifact |

Preserve decimal precision, nullability, timestamps, identifiers, and source dependencies. Record unsupported constraints instead of implying they are enforced.

## Large-Procedure Source Audit

For every T3/T4 procedure and every source definition with at least 1,000 physical lines, apply [dedicated-pool-large-procedure-audit.md](dedicated-pool-large-procedure-audit.md). Also apply it below that threshold when complexity or whole-procedure review could conceal an omission. A run may apply the same audit to all procedures for consistency.

Generate a T-SQL-aware preprocessing script and verifier under the migration artifact directory. Partition the exact discovered source into deterministic, gap-free, non-overlapping source blocks; do not use an LLM, line count, semicolon split, or regular expression alone to choose boundaries. Map every converted target statement back to source block IDs and preserve source audit/logging statements by default.

Convert in bounded block batches and retry only failed retryable blocks. Declare `maxAttemptsPerBlock` before conversion, default it to three total attempts, and never regenerate successful blocks merely to repair another block. Packaging is blocked until byte coverage is exactly 100%, every block is `Converted`, `ApprovedExclusion`, or `ManualReviewApproved`, cross-block validation passes, and `deployment-package.json` has verdict `ReadyForPublication`.

## Stored-Procedure Parameter Contract

Fabric documents pipeline substitution inside a first-cell `%%configure`, but does not document arbitrary Python-variable interpolation into `%%sql`. Generated stored-procedure notebooks therefore use this bounded SQL-only contract:

1. Preserve the discovered source signature before conversion. For every supported source input, record its name, source type, mapped Spark SQL type, whether a source default exists, and the exact source default. Do not invent an interactive, sample, sentinel, or preview default.
2. Put bare `%%configure` (with no flags such as `-f`) in the first code cell. Under `conf`, map each supported source input exactly once to a unique `spark.synapseMigration.<procedure-key>.<parameter-key>` property whose value is an object containing `parameterName` and `defaultValue`; a scalar configuration value is invalid. Retain the procedure key even when a notebook has multiple contributing procedures so parameter namespaces cannot collide. `parameterName` is the externally overridable Fabric Notebook Activity base-parameter name, and `defaultValue` must equal the discovered source default without semantic coercion.
3. Reference that property at every semantic use of the source input in later `%%sql` cells as `${spark.synapseMigration.<procedure-key>.<parameter-key>}` and immediately cast the substitution to the mapped Spark SQL type, for example `CAST(${spark.synapseMigration.dbo_usp_LoadFilteredCustomers.MinCustomerId} AS BIGINT)`. Never replace a parameter reference with its default, a fixture value, a literal in a predicate, or a one-row temporary view containing constants.
4. A source input without a source default cannot satisfy this automatic `%%configure` contract because Fabric requires an interactive fallback. Mark it `ManualReviewRequired` until the approved design defines a behavior-preserving required-input strategy; do not manufacture a default merely to make the notebook runnable.
5. Permit automatic runtime substitution only for integers, fixed-scale decimals, and `true`/`false`. The approved caller must validate each override against the source type and canonical lexical form before submitting the Fabric Notebook activity: signed base-10 digits for integers, signed base-10 digits plus the declared scale for decimals, and lowercase `true` or `false` for booleans. Record that validation control in the manifest. Interactive execution uses only the exact source default. Direct execution with arbitrary overrides is unsupported.
6. Resolve identifiers only from discovered, allow-listed metadata and quote them during generation. Never accept table, schema, column, expression, clause, or arbitrary SQL text through a runtime parameter.
7. Mark strings, dates/timestamps, null-bearing runtime values, binary values, table-valued parameters, output parameters that cannot be represented as a final result set, and any parameter used to construct dynamic SQL as `ManualReviewRequired`. Do not publish those notebooks until the manifest records per-object approval and the approved redesign.

Do not add a Python parameter cell, Python escaping helper, `spark.sql(...)` wrapper, or DataFrame transformation to bypass these limits. The manifest must record each source parameter, mapped type, configuration key, source-default presence and value, emitted `defaultValue`, validation rule, and disposition. A parameterized procedure is not converted when its SQL no longer consumes the runtime mapping, even if the emitted literal equals the source default.

## Tier Strategy

- **T1**: deterministic conversion; syntax validation required.
- **T2**: deterministic conversion plus schema mapping and parser tests.
- **T3**: convert one object at a time with dependency context and targeted static tests.
- **T4**: produce a redesign note and testable skeleton; do not claim automatic parity.

## Artifact Requirements

- Make schema DDL idempotent.
- Parameterize workspace, Lakehouse, schema, and environment values.
- Keep secrets out of generated artifacts.
- For an approved `1:1` mapping, preserve the exact discovered `sourceName` as both the local notebook basename (`<sourceName>.ipynb`) and published Fabric Notebook display name (`<sourceName>`), including case and punctuation. For `N:1` or `N:N`, use only the explicitly approved target component names and paths; never derive a rename or grouping automatically. Keep every contributing `sourceSchema`, `sourceName`, and stable ID separately in the manifest.
- Emit `migration-manifest.json` with source-object decisions and target-component records. Each procedure source decision must include its stable source ID, feature IDs, gap IDs, approved mapping cardinality, disposition, approval evidence, and all target component IDs. Each Notebook target component must include all contributing source IDs, approved display name/workspace, artifact path, dependencies, tier, parameter mappings, source-block/cell provenance, deployment state, hashes, warnings, errors, and timestamps. Preserve `sourceStableId`, `targetArtifact`, and notebook lifecycle fields on a `1:1` notebook record for backward compatibility. Use only these state transitions: `Discovered` -> `Assessed` -> `DesignApproved` -> `Converted` -> `PublishPending` -> `Published` -> `ReadbackValidated`, with `ManualReviewRequired`, `ManualReviewApproved`, `ApprovedExclusion`, `Deferred`, or `Failed` as explicit gated states.
- For audited procedures, add the source hash, ledger path/hash, block totals by disposition, source-byte coverage, retry policy, attempt-record root, deployment-package path/hash, and package verdict to the manifest target component. Retain ledgers and attempt evidence after publication.
- Include source-to-target type mappings and conversion warnings.
- Emit at least one executable Spark SQL transformation cell for every converted stored procedure across its approved target components. Each transformation cell must start with `%%sql` and record its contributing source/block IDs. The magic command selects Spark SQL; do not depend on a particular `metadata.language` value. A notebook that only embeds, comments, or displays the source T-SQL is not a conversion.
- Do not emit PySpark, Python `spark.sql(...)` wrappers, or DataFrame API transformation logic for stored procedures. If Spark SQL cannot preserve a procedural construct, emit the supported SQL stages plus a precise manual-review/redesign finding rather than changing languages.
- Keep the original procedure text in the discovery evidence, not in executable notebook cells. Reject generated notebooks containing `CREATE PROCEDURE`, `CREATE PROC`, or a `source_procedure` placeholder.
- Use valid nbformat v4.5-or-newer JSON with top-level `cells`, `metadata`, `nbformat: 4`, and `nbformat_minor >= 5`. Every cell ID must be unique and match `^[A-Za-z0-9_-]{1,64}$`. Every code cell must have `cell_type: code`, `metadata`, `source`, `outputs: []`, and `execution_count: null`; markdown cells must have `cell_type: markdown`, `metadata`, and `source`.
- Set `metadata.dependencies.lakehouse.default_lakehouse`, `default_lakehouse_workspace_id`, and `default_lakehouse_name` to the resolved target values. Do not substitute `metadata.trident.lakehouse` or another alternate path for this required binding. Include Fabric-compatible kernel/language metadata copied from a newly created target-workspace Spark notebook; do not invent kernel identifiers.
- Require this order: optional introductory markdown; first code cell `%%configure` when runtime parameters or session configuration are needed; then one or more `%%sql` transformation cells. Generated notebooks must contain no saved outputs.
- Validate notebook JSON, validate every `%%sql` cell against the target Spark parser, reject PySpark/DataFrame code, verify parameter mappings against the discovered source signature, reject hardcoded replacements for source inputs, and assert each notebook has executable translated Spark SQL logic before deployment.
- Publish each successful notebook as a Fabric Notebook item through a definition payload using `format: "ipynb"`, part path `notebook-content.ipynb`, and `payloadType: "InlineBase64"`.
- Do not execute generated transformations or notebooks, export source rows, or populate target tables.

## Completion Gate

Do not start conversion when either migration gap report or the procedure mapping approval is missing or unapproved. An object is deployable only when its discovery gaps have dispositions, its dependencies are resolved, generated artifacts parse, its approved target notebooks contain executable translated `%%sql` logic rather than pasted source T-SQL or PySpark/DataFrame code, every supported source input remains externally overridable with its exact source default, and the manifest records `ManualReviewApproved` for every behavior-changing redesign. The mapping must cover every procedure and target component without missing, orphan, duplicate, or unapproved relationships. An audited procedure also requires 100% verified source-block coverage across all mapped target components, no non-deployable block or exhausted failure, and a hash-verified `ReadyForPublication` package. Missing mappings, invented defaults, hardcoded parameter uses, acknowledged/skipped/unknown states, package drift, and unresolved reviews are not deployable.
