# Ray — Fabric Senior Data Engineer

Ray is a local engineering assistant built from the supplied implementation blueprint.
Version 0.2.0 implements the local application for Sprints 1–6: Codex engineering turns,
Fabric discovery, durable Telegram controls, project memory, governed DEV operations,
and approval-bound TEST promotion. The supplied configuration is **local-only**.

The checked-in examples configure no bot, Fabric workspace or startup task.
Private runtime enrollment and authentication live outside this checkout.
Automated cloud/channel tests use synthetic responses and do not establish live
service acceptance. See [implementation status](docs/implementation-status.md).

Ray can read attached MD, DOCX, PDF and Excel files, and inspect public GitHub
repositories from a pasted link. Follow-up questions can use the recent file text.
See [file and GitHub reading](docs/document-and-github-reading.md) for supported
formats, examples and reading limits.

Ray can prepare a pinned FMD Framework DEV installation for configured workspaces,
including SQL schema and metadata notebooks, bound item definitions, Environment
publication and verification. See [FMD deployment](docs/fmd-deployment.md) for the staged workflow
and the live acceptance work that remains.

Ray also supports tenant-local Git repositories, preserving native Fabric item
identities during folder allocation, and reviewed local -> personal GitHub ->
Fabric deployment. Explicit service-principal capabilities cover workspace and
capacity assignment, execution identities, scoped roles/groups and authenticated
connections. Initial tenant permissions and live enrollment are still required.
See [tenant Git setup](docs/tenant-git-deployment.md) and the
[disabled example configuration](docs/examples/tenant-config.yaml).

Telegram failures include a plain-language error and a next step. `/details technical`
keeps the full checks, error references and item IDs available when needed.
Task failures are retained for `/status` across restarts, including errors while
reading Fabric before the model starts. Raw exceptions, credential-bearing URLs,
and service response bodies are not sent or saved as error details. Unknown causes
are reported as unknown. Failed operations are not automatically retried.

## Run the offline demonstration

From this project folder in PowerShell, with Python 3.12 available:

```powershell
.\scripts\setup.ps1 -Python python
.\scripts\offline-demo.ps1 -Output .\demo-results
```

Choose a new output directory for each demonstration. Setup downloads pinned Python
dependencies and runs tests. The demonstration itself makes **no network requests**;
it uses real SQLite, files and validation subprocesses with synthetic model/Fabric
responses. It produces `report.json`, `evaluation.html`, a verified backup and a
sample project. It demonstrates DEV update/job execution, exact TEST approval,
replay rejection, durable stop and a separately reviewed rollback.

For source-only execution after installing dependencies:

```powershell
python -m ray_de.demo --output .\another-demo
python -m pytest tests -q
```

## Use Ray with a local repository

Use [`projects/`](projects/README.md) as the central location for local setup:
`projects/<tenant>/tenant-config.yaml` holds enrollment, and its sibling `repo/`
holds the tenant's separate Git checkout. The existing tenant draft is
`projects/actual-fabric/tenant-config.yaml`. Runtime state and protected credentials
stay under `%LOCALAPPDATA%/Ray/state`.

The sample project lives in `projects/sample-fabric`. Copy it to a new project
folder, set a unique `project_id` and point `repo_path` at an existing repository.
Keep Ray's state outside every engineering repository. `CONTEXT.md` records project
conventions; `decisions/` stores accepted decisions.

```powershell
$ray = '.\.venv\Scripts\ray.exe'
$project = '.\projects\sample-fabric\config.yaml'
& $ray --project $project --data-dir .\data doctor
```

`doctor` exits 2 until the isolated Codex profile is authenticated. An actual model
turn needs authentication and network access. When you choose to enable that:

```powershell
& $ray --project $project --data-dir .\data login codex
& $ray --project $project --data-dir .\data run 'Explain the inventory transformation' --mode read
& $ray --project $project --data-dir .\data run 'Add a regression for duplicate records' --mode write
& $ray --project $project --data-dir .\data status
& $ray --project $project --data-dir .\data resume TASK_ID 'Continue using the saved decision'
```

Set `SENIOR_DE_CODEX_BIN` to a standalone native Codex executable to override discovery.
The official Python SDK is pinned to 0.147.0; standalone CLI 0.153.3 was probed locally.
Ray strips inherited cloud/bot credentials from model and validation subprocesses,
disables model network tools and denies SDK permission escalation. Its reviewer uses
a fresh read-only thread. Host validation and review determine completion.
On Windows, fresh isolated profiles explicitly select the non-admin native sandbox;
an existing `[windows] sandbox = "elevated"` selection in the isolated profile is
preserved. Ray excludes Store PowerShell aliases from its own subprocess PATH and
uses its installed Python environment for local commands. `doctor` checks an actual
sandboxed shell command separately from authentication. See
[the runtime repair and verification record](docs/runtime-authoring-repair.md).

Project IDs bind immutably to the full configuration and resolved paths. After a
policy, model or repository-path change, use a new `project_id` and a new reviewed
task. This prevents old conversations or approvals from inheriting changed targets.

## Stop, memory and recovery

`stop` does not wait for the active project lock. It cancels pending plans/decisions
and interrupts model or validation work. It cannot cancel a job already submitted
to Fabric. `unpause` permits new work without replaying anything; `resume` explicitly
continues the selected task. Interrupted external writes remain uncertain until
reconciled. A failed job reported by Fabric is recorded as failed.

```powershell
& $ray --project $project --data-dir .\data stop
& $ray --project $project --data-dir .\data recover
& $ray --project $project --data-dir .\data unpause
& $ray --project $project --data-dir .\data memory add 'Inventory key' --context 'IDs have leading zeros' --decision 'Keep IDs as strings' --consequences 'No numeric coercion'
& $ray --project $project --data-dir .\data memory search 'inventory key'
& $ray --project $project --data-dir .\data backup .\backups\ray-001.zip
& $ray --project $project --data-dir .\data verify-backup .\backups\ray-001.zip
& $ray --project $project --data-dir .\data evaluation .\reports\evaluation.html
```

The backup contains the complete SQLite registry plus the selected project's config,
context and decisions. It excludes credential profiles and Codex conversation rollouts.
Restoring only this ZIP does not restore a live Codex conversation. See the
[operations runbook](docs/operations.md) for recovery and evidence limitations.

## Telegram setup and workspace enrollment in chat

Ordinary Telegram messages are conversations with Ray: greetings, everyday chat,
brainstorming, and support do not start engineering tasks or read Fabric. Ray uses
a separate conversational persona and the most recent 40 messages for that private
Telegram actor, including across restarts. Responses have no task/status wrapper.
`/forget` clears the history used for future conversation; it does not delete Telegram
messages, operational delivery records, or project task records.

When you ask for project work, Ray can offer a **Work on this** button. Clicking it
starts the normal governed workflow using your original message. You can also use
`/work <request>` directly. Chat text and model suggestions never approve cloud
operations. `/resume` continues an existing engineering task. Manual gateways can
set `conversational: false` to retain the earlier task-only behavior.

If you already have a bot token from Telegram's **@BotFather**, run:

```powershell
.\scripts\setup-telegram.ps1
```

Paste the token at the hidden local prompt, then send the displayed `/pair` command
to your bot in a private Telegram chat. This pairs the exact user/chat IDs without
using a third-party ID bot. The token is protected with Windows current-user DPAPI.
Setup checks the bot identity and refuses bots with an existing webhook. Keep the
pairing code private. See [Telegram's bot tutorial](https://core.telegram.org/bots/tutorial).

Ray starts in that terminal after pairing. Keep it open. Re-running the same script
starts the saved configuration. The default location is `%LOCALAPPDATA%\Ray`, with
separate `state`, `lobby`, and `workspaces` directories; use `-Root` to choose another
location. Setup does not register a startup task.

Before your first model conversation, sign in to Ray's isolated lobby profile:

```powershell
.\scripts\login-ray-chat.ps1
```

Follow the local device sign-in instructions. A workspace and Fabric authentication
are not needed to talk with Ray. Conversation uses the first project on your access
list for Codex authentication, so changing the selected workspace does not interrupt
personal chat. Engineering turns still use the selected project's own profile.

Send these commands to Ray:

```text
/start
/connect DEV https://app.fabric.microsoft.com/groups/YOUR-WORKSPACE-UUID/list
/workspace
```

`/connect` accepts a workspace URL or UUID and an explicit DEV, TEST, or PROD
environment. Ray saves and selects a separate **read-only** project for that
workspace and Telegram actor. It survives restarts. Repeating the same connection
selects the existing project with a fresh task; other workspace/environment choices
receive separate project identities. `/project` lists the projects you can access.
Enrollment does not change existing project bindings, action policies, or approvals.
Only private, allow-listed accounts with `allow_workspace_setup: true` can enroll.
Ordinary model output, forwarded messages and documents cannot enroll workspaces.

Ray returns two local PowerShell sign-in commands, for Fabric and Codex. Run these
on the Ray computer to authenticate that project's isolated profiles, then send
`/workspace check` to verify workspace metadata and item inventory access. Use
`/workspace login` to retrieve the sign-in commands again. Passwords, client secrets
and access tokens belong in local authentication, never Telegram. Saving workspace
details alone does not establish or verify a live connection.

After sign-in, send engineering questions in plain language and use the offered
work button, or send `/work` followed by the request. For example:
`List the items in this workspace and explain what you can infer about the pipelines.`
Newly enrolled projects have empty local repositories and all write permissions
disabled. Connecting a workspace does not download source code or enable cloud writes.

To enable authoring in an enrolled DEV or TEST workspace, send `/workspace enable-write`.
This explicit host command creates and selects a new project in write mode with local
source validation, workspace-wide item authoring and creation enabled. It preserves
the previous project's binding and task history, and reuses only the same owner's
same-workspace authentication files. Reconnecting selects the enabled project.
DEV actions run after source validation and independent review; each TEST action
still needs its exact approval. `/mode read` selects read-only tasks when wanted.

## Manual Telegram configuration and Windows startup

The private long-polling gateway supports `/project`, `/mode read|write`, `/new`,
`/resume [task-id]`, `/status`, `/stop`, `/doctor`, `/memory`, `/remember`, `/actions`
and `/rate 0-4`. Project access is checked against the exact Telegram user/chat pair.
Clarification and approval buttons survive restart, belong to one actor/project/task,
expire, and cannot be replayed. Telegram messages are redacted and chunked within
UTF-16 message limits. Raw model/HTTP logs are never sent.

1. Copy `gateway.example.yaml` and add your explicit private user/chat IDs.
2. Store the token with `ray --project PROJECT --data-dir DATA secret-set`. Input is
   hidden and stored using Windows current-user DPAPI. Keep tokens out of YAML/chat.
   `RAY_TELEGRAM_BOT_TOKEN` is an alternative process environment input.
3. Run `scripts/start-ray.ps1 -Config YOUR_CONFIG -DataDir YOUR_DATA` interactively.
4. Preview startup configuration with `scripts/register-startup.ps1 -Config YOUR_CONFIG
   -DataDir YOUR_DATA`. Adding `-Install` registers it for the current user's next logon.
   Registration does not start it immediately and refuses to overwrite an existing task.

Startup runs without elevation in a hidden PowerShell window and retries a failed
process up to three times. It runs after user logon, not before logon; DPAPI and
user authentication require the same Windows account. Reboot/logon operation has not
been exercised in this local-only delivery. SQLite records channel inbox/outbox state,
task events, action receipts and the latest polling health.

## Governed Fabric operations — opt in later

Reads allow only configured workspace metadata, item inventory, item metadata and
Lakehouse table inventory. `fabric snapshot` creates a project-bound input for the
model. It never enumerates the whole tenant or follows response-provided URLs.
Use `login fabric` to authenticate the isolated project profile when live work is wanted.

Writes support generic Fabric item creation, metadata updates, definition updates,
and supported jobs. Notebook, DataPipeline and SparkJobDefinition job types are
resolved automatically; explicit targets can configure other job types. They require:

- An explicit workspace/environment, with either `workspace_write: true` or an item-ID allow-list.
- `create_items: true` to create items through Fabric's Items API.
- `fabric_dev_write: true` for DEV, or `fabric_test_write: approval` for TEST promotion.
- `allow_definition_export: true` for preflight and rollback capture.
- A definition JSON file inside the reviewed repository, passing source validation,
  a passing independent review, and unchanged source/policy digests.
- Remote metadata/definition verification and completed job receipts for workspace
  authoring. Configured `post_validation_commands` run as additional checks; legacy
  item-allow-list configurations still require them. Data jobs should include meaningful
  runtime assertions for their business outcome.

PROD writes, deletion, permission changes and capacity administration remain disabled.
The service determines which item types, definitions and jobs the account/capacity
supports. Direct host OneLake upload and binary definition parts are not implemented;
reviewed notebooks can generate Excel files and load lakehouse tables. Examples remain local-only. See
[cloud setup and action flow](docs/cloud-actions.md) for the exact YAML and commands.

DEV proposals can execute after host gates pass. TEST proposals pause for a human
approval tied to the exact action digest. Generic chat text never approves a plan.
Ambiguous outcomes are never automatically resubmitted. The transport uses pinned
Fabric CLI authentication and a single HTTP request per host call, avoiding CLI
implicit retry, pagination and polling. See [the integration decision](docs/fabric-cicd-decision.md).

Ray can return source files as structured artifacts when model file tools are unavailable.
Local definition parts may reference a repository file with `payloadType: SourceFile`
and `source`; the host converts it to Fabric's InlineBase64 before validation and review.
Dependent stages use `continue_work: true`: the host returns successful creation receipts
before another model turn plans the next stage. Up to eight stages run per invocation;
pending approvals, errors or uncertain writes stop continuation. Every stage receives
fresh source validation and independent review. Lost creation responses without an
item/operation receipt require inspection; Ray never adopts an item solely by its name.

## Evaluation and boundaries

`evaluations/scenarios.json` contains 12 repeatable engineering and operational
scenarios. `ray rate TASK_ID 0-4` records optional human usefulness scores;
`ray evaluation OUTPUT.html` produces a local evidence report. No user ratings or
live success rates have been invented. `monitor-once` is an optional read-only
inventory check: first observation establishes a baseline, unchanged results stay
quiet, and changes or new failures set `notify: true`. It does not send notifications
or install a schedule by itself.

The application provides logical project separation. Trusted local validation
commands and processes run as the current OS user; this is not an isolation boundary
against hostile code or another user with access to the same files. Validate the
Windows sandbox and use separate OS accounts/VMs for mutually untrusted projects
before relying on confidentiality. Other Fabric authors can change an item between
preflight and write; API requests here are not a distributed transaction.

Teams, WhatsApp, voice, email intake, GitHub integration and broader Power BI
operations remain the blueprint's explicitly deferred backlog.
