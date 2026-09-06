# Dedicated Pool Validation

Validate converted artifacts locally and compare deployed Lakehouse schema metadata with the Synapse source catalog. Validation does not create or execute notebooks and does not read or compare source/target table rows.

## Validation Layers

| Layer | Check | Execution surface | Blocking |
|---|---|---|---|
| L1 | Notebook JSON and Spark SQL syntax | nbformat checks plus target Spark parser | Yes |
| L2 | Schema names, types, precision, scale, nullability | Source catalog plus Livy/SQL endpoint | Yes |
| L3 | Object coverage, dependencies, parameters, and conversion dispositions | Manifest plus generated artifacts | Yes |
| L4 | Notebook definition and publication status | Fabric Items API | Yes |
| L5 | Source-block coverage, bounded attempts, and deployment-package integrity | Audit ledgers plus package verifier | Yes for audited procedures |

## Local Checks

- Parse every `.sql` artifact and every `%%sql` notebook cell with the target Spark parser without executing transformations.
- Parse every `.ipynb` as nbformat v4 JSON and require the complete cell shape, stable IDs, empty outputs, null execution counts, and resolved Fabric dependency metadata defined by the conversion contract.
- Reject unresolved placeholders, embedded secrets, workspace or item IDs in executable SQL/cells, and IDs that do not match the approved resolved target. Require resolved workspace and Lakehouse IDs only in the notebook dependency metadata, manifest, and deployment fields defined by the conversion contract.
- Confirm every manifest dependency exists.
- Verify the approved procedure cardinality and mapping graph. Every discovered procedure must reference all and only its approved target components, and every target Notebook must list all and only its approved contributing procedures. Reject missing, orphan, duplicate, or unapproved relationships, names, workspace placement, decompositions, or sharing.
- Verify every source object has an approved target-component relationship, `ApprovedExclusion`, or approved retirement. `Deferred`, a skip reason, or `ManualReviewRequired` status is not completion.
- For each audited procedure, rerun the generated verifier and require exact source and ledger hashes, contiguous non-overlapping spans covering 100% of source bytes, unique deterministic block IDs, complete attempt history within `maxAttemptsPerBlock`, and only `Converted`, `ApprovedExclusion`, or `ManualReviewApproved` block dispositions.
- Require every converted or manually approved block to map to existing notebook cell IDs and target statement hashes, and every target transformation statement to map back to source blocks or justified generated scaffolding. Validate cross-block control flow, parameters, dependencies, temporary objects, outputs, error handling, and retained audit/logging behavior.
- Recompute every `deployment-package.json` artifact hash and canonical conversion-manifest projection hash, and require `ReadyForPublication`. The projection excludes package self-reference and mutable deployment/readback fields. Reject an unlisted artifact, changed notebook, missing ledger/attempt record, source hash mismatch, or package created before validation completed.

## Source-to-Target Schema Comparison

For each converted table definition, compare metadata only:

- Column count and ordered schema mapping
- Column names, mapped types, precision, and scale
- Nullability and supported defaults
- Table, schema, view, and dependency names
- Unsupported constraints and physical-design features recorded in the gap report

Do not run row-count, aggregate, sample, hash, or business-result queries.

## Logic Artifact Validation

Validate Spark SQL with the target parser, notebook JSON with nbformat rules, the bounded `%%configure` parameter mapping against source metadata, dependencies against the manifest, and notebook names/workspaces against the approved mapping. For `1:1`, require the local basename and persisted Fabric display name to equal the exact discovered `sourceName`. For `N:1` and `N:N`, require exact approved target names, contributing source IDs, dependency order, output boundaries, and source-block-to-cell provenance. For every supported source input, require exactly one collision-safe procedure-qualified mapping, require `parameterName` to match the approved Fabric Notebook Activity base parameter, require `defaultValue` to equal the discovered source default, and require every semantic use in `%%sql` to consume the mapped `${spark.synapseMigration...}` property with an immediate type cast. Reject invented defaults, missing mappings, source inputs replaced by literals or constant temporary views, transformation cells containing PySpark/DataFrame APIs, Python `spark.sql(...)`, `CREATE PROCEDURE`, `CREATE PROC`, `source_procedure`, unresolved placeholders, saved outputs, or non-SQL transformation logic. A required source input without a source default remains `ManualReviewRequired` unless an approved behavior-preserving design is recorded.

For every published Notebook, call `POST /v1/workspaces/{workspaceId}/notebooks/{notebookId}/getDefinition?format=ipynb` with `{}`. Accept direct `200`; for `202`, honor `Retry-After`, poll `Location` to `Succeeded` within a deadline, then retrieve `GET {Location}/result`. Decode the `notebook-content.ipynb` `InlineBase64` part and rerun all local assertions against the persisted JSON. Compare its canonical hash with the publish candidate and require exact target values for:

- `metadata.dependencies.lakehouse.default_lakehouse`
- `metadata.dependencies.lakehouse.default_lakehouse_workspace_id`
- `metadata.dependencies.lakehouse.default_lakehouse_name`

Validation is read-only: do not create, update, or execute notebooks, and do not call any notebook job, Livy notebook execution, `%run`, or `notebookutils.notebook.run` surface. Record unsupported behavior and manual-review requirements without claiming behavioral equivalence.

## Report

Produce a concise report containing:

- Source and target identifiers
- Feature totals by support level and risk, plus object/component totals by status, complexity tier, and mapping cardinality
- Schema comparison and artifact-validation results
- Conversion warnings and accepted structural differences
- Manual-review items
- Artifact-readiness verdict
- Audited-procedure block totals, source-byte coverage, retry exhaustion, package verdict, and retained evidence paths
- Explicit statement that data migration, data parity, runtime behavior, and cutover readiness were not validated

## Completion Gate

Artifact readiness is blocked by syntax failures, missing or duplicate required objects, unapproved or incomplete procedure mappings, unapproved notebook names/workspace placement or naming collisions, projected workspace-limit violations, schema mismatches, publication/readback/binding failures, unresolved placeholders, missing parameter mappings, invented defaults, hardcoded parameter uses, discovery blind spots, unknown classifications, open accepted-scope gaps, `ManualReviewRequired` objects, and skips without `ApprovedExclusion`. For audited procedures, less than 100% source-byte coverage, gaps/overlaps, non-terminal blocks, retry-limit violations, untracked target statements, missing audit evidence, or deployment-package/hash drift also block readiness. Production cutover is always outside this skill because data migration and data-equivalence validation are excluded.
