# Cloud setup and action flow

This is an opt-in reference; its example IDs are not live targets.
Choose a new project ID when enabling policy; IDs below are illustrative UUIDs.

For enrolled Telegram workspaces, `/workspace enable-write` creates a new DEV/TEST
authoring project and selects write mode. To configure an equivalent project locally,
set `fabric.workspace_write: true` and `fabric.create_items: true`, retain the relevant
environment write policy and `allow_definition_export`, and configure source validation
such as `['{python}', '-m', 'ray_de.artifacts']`. This validator checks source structure;
business/data correctness belongs in source tests, independent review and runtime assertions.
Workspace authoring uses built-in remote verification and additionally runs any configured
post-validation commands. The legacy explicit-target configuration below is also supported.

`create_item` takes a reviewed JSON payload containing `displayName` and Fabric `type`,
with optional description, folderId and either definition or creationPayload. Its item_id
is empty; terminal planning omits `--item`. `update_item` takes displayName/description
fields only, and captures previous values before PATCH. Definition updates and supported
job execution can resolve existing types across the configured workspace when
workspace_write is enabled. Creation additionally requires create_items.

Source JSON can contain a local definition part such as
`{"path":"notebook-content.ipynb","source":"notebooks/demo.ipynb","payloadType":"SourceFile"}`.
The host encodes the referenced reviewed-repository file before validating and reviewing
the compiled payload; only InlineBase64 reaches Fabric. Re-emit the source reference
when changing the file. Binary payloads and direct host OneLake upload are unavailable.

Each create action checks for an existing name, persists the returned item or operation
ID and verifies metadata plus definitions when supplied. Dependent stages use those
receipts. An uncertain creation with no receipt cannot be reconciled by guessing its name.

```yaml
project_id: my-fabric-dev-v2
name: My Fabric DEV project
repo_path: repo
policy:
  local_write: true
  fabric_dev_write: true
  fabric_test_write: approval
  fabric_prod_write: false
fabric:
  workspaces:
    - id: 11111111-1111-1111-1111-111111111111
      environment: DEV
    - id: 33333333-3333-3333-3333-333333333333
      environment: TEST
  allow_definition_export: true
  write_targets:
    - workspace_id: 11111111-1111-1111-1111-111111111111
      item_id: 22222222-2222-2222-2222-222222222222
      item_type: Notebook
      job_type: RunNotebook
    - workspace_id: 33333333-3333-3333-3333-333333333333
      item_id: 44444444-4444-4444-4444-444444444444
      item_type: Notebook
validation_commands:
  - ['{python}', '-m', 'unittest', 'discover', '-s', 'tests']
post_validation_commands:
  - ['{python}', 'checks/reconcile_target.py']
```

Supply your own `checks/reconcile_target.py` with real target and business-data
checks before enabling live writes. Ray does not infer row-count acceptance or
sensitivity-label permissions. Authenticate the project's isolated Fabric profile
using `ray --project CONFIG --data-dir DATA login fabric`.

A source definition contains only `definition`, optional `format`, and 1–100 inline
base64 parts, with at most 4 MB decoded content. Notebook formats supported here are
`ipynb` (`notebook-content.ipynb`) and `fabricGitSource` (`notebook-content.py`). Pipeline
payloads require `pipeline-content.json`, with item type `DataPipeline` and optional
job type `Pipeline`. Author a payload accepted by the item's API; the host validates
the envelope, while semantic correctness belongs in repository tests and live checks.
`.platform` metadata is excluded from definition operations; labels and permissions are
not changed. Display names and descriptions use the separate reviewed update_item action.
Other linked workspaces/resources inside a notebook or pipeline require source review
and least-privilege Fabric identity; item allow-listing cannot restrict code executed
by a remote notebook identity.

The planner captures the previous definition and verifies the remote item type.
`run_job` also requires the remote definition to match reviewed source. Each write
rechecks source, policy and current remote content immediately before submitting.
JSON definition parts compare their complete parsed values, ignoring only whitespace
and key ordering; embedded code strings and every metadata field remain significant.
Other text parts use decoded-content hashes. Service changes to values may report
uncertainty; inspect the recorded receipt before reconciling.

Example command sequence, substituting actual values (global options precede commands):

```text
ray --project CONFIG --data-dir DATA actions plan --task TASK --action deploy_to_test --workspace TEST_UUID --item ITEM_UUID --definition definitions/notebook.json
ray --project CONFIG --data-dir DATA actions show --id PLAN
ray --project CONFIG --data-dir DATA actions approve --id PLAN --digest EXACT_ACTION_HASH
ray --project CONFIG --data-dir DATA actions execute --id PLAN
ray --project CONFIG --data-dir DATA actions reconcile --id PLAN
ray --project CONFIG --data-dir DATA actions rollback-export --id PLAN --output NEW_LOCAL_FILE.json
```

Terminal plans belong to the local Windows username. Telegram plans belong to the
exact user/chat identity and are approved in that conversation. Changing source,
policy or target after approval invalidates execution. Plans expire after one hour;
approval and execution transitions are atomic and one-shot.

On a lost response, inspect the recorded operation/job ID and reconcile it. This
only polls or reads current state, then reruns post-validation. A target with an
executing, uncertain or validation-failed action cannot receive another plan. If no
job ID was received, Ray cannot identify the submitted job automatically; manual
platform investigation is required. Stop never promises cancellation of remote work.

Rollback exports the captured prior definition to a new local file. Put that content
into the repository, inspect it, run a **new reviewed task**, and prepare a new action.
TEST rollback requires its own exact approval. Ray never automatically reverses a
write or assumes that a source rollback undoes data changes produced by a job.

API references checked against Microsoft documentation:

- [Get item definition](https://learn.microsoft.com/en-us/rest/api/fabric/core/items/get-item-definition)
- [Create item](https://learn.microsoft.com/en-us/rest/api/fabric/core/items/create-item)
- [Update item](https://learn.microsoft.com/en-us/rest/api/fabric/core/items/update-item)
- [Update item definition](https://learn.microsoft.com/en-us/rest/api/fabric/core/items/update-item-definition)
- [Notebook definitions](https://learn.microsoft.com/en-us/rest/api/fabric/articles/item-management/definitions/notebook-definition)
- [Run item job](https://learn.microsoft.com/en-us/rest/api/fabric/core/job-scheduler/run-on-demand-item-job)
- [Get job instance](https://learn.microsoft.com/en-us/rest/api/fabric/core/job-scheduler/get-item-job-instance)
