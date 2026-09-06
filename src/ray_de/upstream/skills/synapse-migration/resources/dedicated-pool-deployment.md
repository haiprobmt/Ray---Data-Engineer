# Dedicated Pool Deployment to Fabric

Convert approved source T-SQL stored-procedure logic into executable Spark SQL `%%sql` notebook cells before publication. Then create or resolve the target Lakehouse through Fabric REST, execute schema-only artifacts through Lakehouse-bound Livy, and publish the generated notebooks to the Fabric workspace without executing them.

## Generated Notebook Publication

Create or update each approved target Notebook component. An approved `1:1` component uses the exact source procedure name; approved `N:1` and `N:N` components use their approved target names and contain logic from multiple procedures according to the user-provided mapping. Every source procedure remains independently traceable, every notebook has an explicit default Lakehouse binding, and publication never invokes or uses the notebooks for orchestration.

## API Flow

Apply the skill-level `x-ms-fabric-skill: synapse-migration` telemetry header to every Fabric REST request and long-running-operation poll.

1. Acquire a Fabric token for `https://api.fabric.microsoft.com`.
2. Resolve the workspace by display name.
3. List Lakehouses and resolve the target by display name.
4. If absent and approved, create it with `POST /v1/workspaces/{workspaceId}/lakehouses`.
5. Handle synchronous `200`/`201` responses directly. For `202`, capture `Location` and `x-ms-operation-id`, honor `Retry-After`, and poll with a declared deadline until `Succeeded`, `Failed`, or `Cancelled`. Timeout, missing `Location`, terminal failure, malformed response, or exhausted bounded retries is a deployment failure and must be persisted; never poll indefinitely.
6. Create a Livy session bound to the target Lakehouse.
7. Wait for the Livy session to become `idle` within a bounded deadline. Submit schema-only artifacts in manifest dependency order; a statement succeeds only when its state is `available` and `output.status` is `ok`. Treat `error`, `cancelled`, `dead`, timeout, missing output, or session termination as failure. Do not submit row inserts, CTAS materialization, DataFrame writes, or other data-loading statements.
8. Before any Notebook create or update, verify `deployment-package.json` for every audited procedure. Require verdict `ReadyForPublication`, 100% source-block coverage, deployable terminal dispositions, attempts within the declared retry limit, exact source, ledger, SQL-artifact, and notebook hashes, and an exact canonical conversion-manifest projection hash that excludes package self-reference and mutable deployment/readback fields. A missing package, changed byte, or verifier failure blocks deployment. Never repair or rewrite an artifact during deployment.
9. Compile each generated `.ipynb`: validate JSON/nbformat, the first-cell `%%configure` contract when parameters are present, parameter declarations, every `%%sql` cell, forbidden constructs, empty outputs, and Spark SQL parser compatibility.
10. Resolve each approved target Notebook component by target component ID and verify its complete set of contributing source IDs. For `1:1`, require the display name to equal the exact discovered `sourceName`. For `N:1` and `N:N`, require the exact approved target display name and workspace; reject generated or unapproved names and placement. Create an empty Notebook only when the approved target mapping has no existing Notebook ID and no name collision exists. A collision blocks publication until an explicit target-name mapping is approved. Update content with `POST /v1/workspaces/{workspaceId}/notebooks/{notebookId}/updateDefinition` and this required body shape: `definition.format` is `ipynb`; the content part path is `notebook-content.ipynb`; its payload is the Base64-encoded notebook JSON; and `payloadType` is `InlineBase64`. For audited procedures, Base64-encode the exact packaged notebook bytes.
11. Accept `200` or poll `202` as defined in step 5. Then perform mandatory persisted readback with `POST /v1/workspaces/{workspaceId}/notebooks/{notebookId}/getDefinition?format=ipynb` and body `{}`. Accept a direct `200`; for `202`, poll `Location`, then call `GET {Location}/result` after `Succeeded`.
12. Locate the `notebook-content.ipynb` part, require `InlineBase64`, decode and parse it, and compare its canonical content with the publish candidate and packaged content hash. Ignore only documented volatile metadata; never ignore cells, sources, parameters, dependency binding, outputs, execution counts, or target naming. Verify the persisted Fabric item display name and workspace against the approved target component; for `1:1`, also verify the exact source procedure name. Verify `metadata.dependencies.lakehouse.default_lakehouse`, `default_lakehouse_workspace_id`, and `default_lakehouse_name` against the resolved target.
13. Persist the stable source ID, source schema/name, target display name and Notebook ID, workspace/Lakehouse IDs, canonical content hash, package and ledger hashes, source coverage, operation ID/URL, status, attempts, timestamps, readback status/hash, approval evidence, and sanitized error in the manifest.
14. Close the Livy session.

Use the Fabric Spark consumption/authoring core documents for the current Livy endpoint and payload shape rather than duplicating version-sensitive API details here.

## Deployment Order

1. Schemas and empty Delta tables
2. Non-materializing view definitions that are supported by the target
3. Converted procedural logic as published Notebook items

Converted procedural logic is published as Notebook items. Do not invoke generated notebooks or transformations as part of this skill.

## Idempotency and Recovery

- Check manifest status before submitting an artifact.
- Use `IF NOT EXISTS` and manifest checkpoints where appropriate, but compare the resulting target metadata with the expected schema after every create or no-op. A no-op against incompatible metadata is drift, not success.
- Bind each approved target component ID and its complete contributing source-ID set to exactly one target Notebook item ID. A source procedure may reference multiple target components and a target component may reference multiple procedures only when the approved mapping permits it. If the expected name belongs to another item or the mapped item has changed type/name/content, stop for artifact-specific replacement approval; do not create a duplicate or overwrite an unrelated item.
- Never replace an existing target schema/code artifact without recorded artifact-specific approval and a pre-update readback.
- On statement failure, capture state, output, and the affected object; stop dependent objects.
- Recreate an expired Livy session and resume from the first incomplete manifest item.
- Never call the Job Scheduler run endpoint, `notebookutils.notebook.run`, `%run`, or submit a generated notebook through Livy. Publication and readback are allowed; execution is not.
- Do not log access tokens, connection strings, or secrets.

## Completion Gate

Deployment completes only when all required manifest items have terminal success states, every Notebook has passed persisted readback and exact Lakehouse-binding checks, every audited procedure was published from a verified immutable package with 100% source-block coverage, and every behavior-changing object is `ManualReviewApproved`. Failed, skipped without `ApprovedExclusion`, unknown, review-required, unpackaged, or hash-mismatched items must be reported explicitly and block artifact readiness.
Deployment success means schema/code artifact readiness only; it does not establish data parity or production cutover readiness.
