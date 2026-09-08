"""Private allow-listed Telegram long-polling gateway. No public listener."""

from __future__ import annotations
import argparse, asyncio, json, time, urllib.request, urllib.error
from pathlib import Path
import yaml
from pydantic import Field, model_validator
from .config import StrictModel, UniqueLoader, load_project
from .state import StateStore, project_lock
from .control import Control, TaskStopped
from .memory import redact, save_decision, recall
from .codex_client import CodexRunner
from .cloud import CloudActions
from .service import TaskService
from .secrets import load_token
from .onboarding import WorkspaceRegistry
from .conversation import ConversationService
from .errors import describe_error, error_text, record_failure
from .telegram_format import formatted_chunks, task_message, task_details, friendly_error
from .documents import check_file, read_document, ReadError, MAX_BYTES
from .github_reading import NoRedirect, github_urls


class Access(StrictModel):
    user_id: int = Field(gt=0)
    chat_id: int = Field(gt=0)
    projects: list[str] = Field(min_length=1)
    allow_workspace_setup: bool = False


class GatewayConfig(StrictModel):
    projects: dict[str, str]
    access: list[Access]
    workspace_root: str | None = None
    conversational: bool = True

    @model_validator(mode="after")
    def valid_access(self):
        pairs = [(a.user_id, a.chat_id) for a in self.access]
        if len(set(pairs)) != len(pairs):
            raise ValueError("Duplicate Telegram access entries")
        if any(set(a.projects) - self.projects.keys() for a in self.access):
            raise ValueError("Access references an unknown project")
        if any(a.allow_workspace_setup for a in self.access) and not self.workspace_root:
            raise ValueError("Workspace setup requires workspace_root")
        return self


def chunks(text, limit=3500):
    current = []
    units = 0
    for char in text:
        size = len(char.encode("utf-16-le")) // 2
        if units + size > limit:
            yield "".join(current)
            current = []
            units = 0
        current.append(char)
        units += size
    if current:
        yield "".join(current)


class TelegramAPI:
    def __init__(self, token):
        self._token = token

    def _request(self, method, payload):
        if method not in {"getUpdates", "sendMessage", "answerCallbackQuery", "getMe", "getWebhookInfo", "getFile"}:
            raise ValueError("Unsupported Telegram method")
        request = urllib.request.Request(
            "https://api.telegram.org/bot" + self._token + "/" + method,
            data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=35) as response:
                body = json.load(response)
            if not body.get("ok"):
                raise RuntimeError("Telegram rejected the request")
            return body["result"]
        except Exception:
            raise RuntimeError(
                "Telegram request failed; credentials and request URL are withheld"
            ) from None

    async def call(self, method, payload):
        return await asyncio.to_thread(self._request, method, payload)

    def download_document(self, document):
        import re
        from urllib.parse import quote
        check_file(document.get("file_name", ""), document.get("file_size", 0))
        file_id = document.get("file_id")
        if not isinstance(file_id, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,512}", file_id):
            raise ReadError("Telegram did not provide a valid file. Please attach it again.")
        try:
            info = self._request("getFile", {"file_id": file_id})
            check_file(document["file_name"], info.get("file_size", 0))
            path = info.get("file_path", "")
            if not re.fullmatch(r"[A-Za-z0-9_./-]{1,512}", path) or path.startswith("/") or any(p in {"", ".", ".."} for p in path.split("/")):
                raise ReadError("Telegram did not provide a valid file location. Please attach it again.")
            request = urllib.request.Request("https://api.telegram.org/file/bot" + self._token + "/" + quote(path, safe="/"))
            with urllib.request.build_opener(NoRedirect()).open(request, timeout=25) as response:
                if int(response.headers.get("Content-Length", "0")) > MAX_BYTES:
                    raise ReadError("This file is too large. Send a file smaller than 20 MB.")
                data = response.read(MAX_BYTES + 1)
            check_file(document["file_name"], len(data))
            return data
        except ReadError:
            raise
        except Exception:
            raise ReadError("I couldn't download this file from Telegram. Please attach it again.") from None


class Gateway:
    def __init__(self, config, config_path, store, data_dir, api, *, service=None, conversation=None):
        self.config, self.store, self.data_dir, self.api = (
            config,
            store,
            Path(data_dir),
            api,
        )
        self.control = Control(store)
        self.projects = {}
        self.jobs = {}
        for id, path in config.projects.items():
            project = load_project(
                (Path(config_path).resolve().parent / path).resolve()
            )
            if project.id != id:
                raise ValueError("Gateway mapping does not match project_id")
            if self.data_dir.resolve().is_relative_to(
                project.repo
            ) or project.repo.is_relative_to(self.data_dir.resolve()):
                raise ValueError("State and project repository must be separate")
            for other in self.projects.values():
                if any(
                    a.is_relative_to(b) or b.is_relative_to(a)
                    for a, b in (
                        (project.repo, other.repo),
                        (project.directory, other.directory),
                    )
                ):
                    raise ValueError(
                        "Gateway project repositories and context directories must not overlap"
                    )
            self.projects[id] = project
            store.bind(project)
        self.registry = None
        if config.workspace_root:
            root = (Path(config_path).resolve().parent / config.workspace_root).resolve()
            boundaries = [self.data_dir.resolve()]
            boundaries.extend(p.directory for p in self.projects.values())
            boundaries.extend(p.repo for p in self.projects.values())
            if any(root.is_relative_to(p) or p.is_relative_to(root) for p in boundaries):
                raise ValueError("Workspace root must be separate from state and existing projects")
            self.registry = WorkspaceRegistry(store, root)
            for access in config.access:
                if access.allow_workspace_setup:
                    actor = f"telegram:{access.user_id}:{access.chat_id}"
                    for project in self.registry.load(actor):
                        self.projects[project.id] = project
        with store.connect() as db:
            db.execute(
                "CREATE TABLE IF NOT EXISTS channel_tasks(actor TEXT NOT NULL,project_id TEXT NOT NULL,task_id TEXT NOT NULL,PRIMARY KEY(actor,project_id,task_id))"
            )
        self.service = service or TaskService(
            store, CodexRunner(self.data_dir), self.data_dir
        )
        self.conversation = conversation or ConversationService(store, CodexRunner(self.data_dir))

    def identify(self, update):
        callback = update.get("callback_query")
        message = callback.get("message", {}) if callback else update.get("message", {})
        user = callback.get("from", {}) if callback else message.get("from", {})
        chat = message.get("chat", {})
        if (
            user.get("is_bot")
            or chat.get("type") != "private"
            or message.get("forward_origin")
        ):
            return None
        match = next(
            (
                a
                for a in self.config.access
                if a.user_id == user.get("id") and a.chat_id == chat.get("id")
            ),
            None,
        )
        if not match:
            return None
        return f"telegram:{match.user_id}:{match.chat_id}", match, message, callback

    async def send(self, actor, text, keyboard=None, *, formatted=False):
        chat = int(actor.split(":")[-1])
        safe = redact(text)
        pieces = list(formatted_chunks(safe) if formatted else ((piece, []) for piece in chunks(safe))) or [("No message", [])]
        for index, (piece, entities) in enumerate(pieces):
            payload = {"chat_id": chat, "text": piece}
            if entities:
                payload["entities"] = entities
            if keyboard and index == len(pieces) - 1:
                payload["reply_markup"] = {"inline_keyboard": keyboard}
            with self.store.connect() as db:
                id = db.execute(
                    "INSERT INTO outbox(actor,payload) VALUES (?,?)",
                    (actor, json.dumps(payload)),
                ).lastrowid
            await self.deliver(id)

    async def deliver(self, id):
        with self.store.connect() as db:
            row = db.execute(
                "SELECT * FROM outbox WHERE id=? AND state='PENDING'", (id,)
            ).fetchone()
            if not row:
                return
            db.execute("UPDATE outbox SET state='SENDING' WHERE id=?", (id,))
        try:
            await self.api.call("sendMessage", json.loads(row["payload"]))
            state = "SENT"
        except Exception:
            state = "UNCERTAIN"
        with self.store.connect() as db:
            db.execute("UPDATE outbox SET state=? WHERE id=?", (state, id))

    def session(self, actor, access):
        session = self.control.session(actor)
        if not session or session["project_id"] not in self.available_projects(actor, access):
            self.control.set_session(actor, access.projects[0])
            session = self.control.session(actor)
        return session

    def available_projects(self, actor, access):
        extra = self.registry.owned(actor) if self.registry and access.allow_workspace_setup else []
        return access.projects + extra

    def login_instructions(self, project, *, sql=False):
        # Display-only PowerShell command, using literal single-quote escaping.
        quote = lambda value: "'" + str(value).replace("'", "''") + "'"
        import sys
        command = (f"& {quote(sys.executable)} -m ray_de.cli --project "
                   f"{quote(project.config_path)} --data-dir {quote(self.data_dir.resolve())} login ")
        if sql:
            return ("Authorize SQL access on the Ray computer using PowerShell:\n" + command + "sql\n"
                    "Complete Microsoft sign-in with the same workspace account, then /resume the failed task. "
                    "The workspace check tests Fabric metadata; SQL uses separate authorization. "
                    "If the Windows broker fails, append --browser to this SQL login command.")
        return ("Sign in on the Ray computer using PowerShell:\n" + command + "fabric\n"
                + command + "codex\nThen send /workspace check here.")

    def busy(self, actor):
        return actor in self.jobs and not self.jobs[actor].done()

    def owns(self, actor, project_id, task_id):
        with self.store.connect() as db:
            return bool(
                db.execute(
                    "SELECT 1 FROM channel_tasks WHERE actor=? AND project_id=? AND task_id=?",
                    (actor, project_id, task_id),
                ).fetchone()
            )

    async def handle(self, update):
        found = self.identify(update)
        if not found:
            return
        actor, access, message, callback = found
        if type(update.get("update_id")) is not int or not self.control.receive(
            update["update_id"], actor
        ):
            return
        try:
            session = self.session(actor, access)
            project = self.projects[session["project_id"]]
            if callback:
                await self.callback(actor, session, project, callback)
                return
            text = (message.get("text") or message.get("caption") or "").strip()
            if message.get("document"):
                if self.busy(actor):
                    await self.send(actor, "Give me a moment to finish. Then send the file again, or use /stop first.")
                    return
                if len(text) > 16000 or redact(text) != text:
                    raise ReadError("The caption is too long or appears to contain a credential. Send a shorter caption without secrets.")
                document = message["document"]
                check_file(document.get("file_name", ""), document.get("file_size", 0))
                self.launch_conversation(actor, session, project, self.projects[access.projects[0]],
                                         text or "Read this attached file and give me a short, clear summary.", document=document)
                await self.send(actor, "I'm reading your file. You can use /stop to pause.")
                return
            if not text:
                await self.send(
                    actor, "Send a message, a GitHub repository link, or an MD, DOCX, PDF, XLSX or XLS file. Voice intake is not enabled."
                )
                return
            if len(text) > 16000:
                await self.send(actor, "Please shorten this task to 16,000 characters.")
                return
            if redact(text) != text:
                await self.send(actor, "That message appears to contain a credential. Ray has not saved it or sent it to the model. Enter credentials only in the local sign-in prompt.")
                return
            command, _, rest = text.partition(" ")
            command = command.split("@")[0]
            if command == "/stop":
                self.conversation.stop(actor)
                self.control.stop(project.id)
                if session["task_id"]:
                    self.store.update(project.id, session["task_id"], "PAUSED")
                await self.send(
                    actor,
                    "Stopped "
                    + project.id
                    + ". No new write will start. Already submitted Fabric jobs are not cancelled.",
                )
                return
            if command == "/details":
                if rest not in {"", "technical"}:
                    raise ValueError("Use /details for a short update, or /details technical for full logs.")
                if not session["task_id"]:
                    await self.send(actor, "There is no current task. Send a request with /work first.")
                    return
                task = self.store.task(project.id, session["task_id"])
                report = json.loads(task["result"]) if task.get("result") else {}
                report["cloud_plans"] = CloudActions(project, self.store, self.data_dir).receipts(task["id"])
                await self.send(actor, task_details(project.id, task, report, technical=rest == "technical"), formatted=True)
                return
            if command == "/status":
                task = (
                    self.store.task(project.id, session["task_id"])
                    if session["task_id"]
                    else None
                )
                if task:
                    report = json.loads(task["result"] or "{}")
                    report["cloud_plans"] = CloudActions(project, self.store, self.data_dir).receipts(task["id"])
                    await self.send(actor, task_details(project.id, task, report), formatted=True)
                else:
                    await self.send(actor, "No task is selected. Use /work followed by what you'd like done.")
                for row in self.control.pending(actor, project.id):
                    await self.show_decision(row)
                return
            if command == "/doctor":
                from .cli import doctor

                result = await asyncio.to_thread(doctor, project, self.data_dir)
                await self.send(actor, json.dumps(result, indent=2))
                return
            if command == "/start":
                await self.send(actor, "Hey, I'm Ray. We can talk about your day, think something through, or work on a project together. What's on your mind?")
                return
            if command == "/help":
                await self.send(
                    actor,
                    "Talk normally, attach an MD, DOCX, PDF or Excel file, or paste a public GitHub repository link. Add what you'd like checked. Files can be up to 20 MB; scanned PDFs need OCR. To start project work, use /work <request>. /status or /details gives a short update; /details technical shows full logs. /stop pauses; /resume continues. /forget clears recent chat and file text. Other controls: /connect DEV|TEST|PROD <workspace URL or UUID>, /workspace [check|login], /project, /mode read|write, /new, /memory [query], /remember title | context | decision | consequences, /actions, /rate 0-4, /doctor.",
                )
                return
            if self.busy(actor):
                await self.send(
                    actor,
                    "Give me a moment to finish this reply. You can use /stop if you want me to pause.",
                )
                return
            if command == "/forget":
                self.conversation.clear(actor)
                await self.send(actor, "I've cleared the recent chat and file text I use for our conversations. Project task records and Telegram message history are still there.")
                return
            if command == "/project":
                if not rest:
                    await self.send(
                        actor, "Available projects: " + ", ".join(self.available_projects(actor, access))
                    )
                    return
                if rest not in self.available_projects(actor, access):
                    raise ValueError("Project is not available to this user")
                self.control.set_session(actor, rest)
                await self.send(actor, "Selected " + rest)
                return
            if command == "/connect":
                if not self.registry or not access.allow_workspace_setup:
                    raise ValueError("Workspace setup is not enabled for this Telegram account")
                if not rest:
                    await self.send(actor, "Send /connect DEV|TEST|PROD <workspace URL or UUID>. I will save a separate read-only project and select it. No passwords or tokens are needed in chat.")
                    return
                with project_lock(self.data_dir / "locks", "workspace-enrollment"):
                    connected = self.registry.enroll(actor, rest)
                self.projects[connected.id] = connected
                self.control.set_session(actor, connected.id, mode="write" if connected.config.fabric.workspace_write else "read")
                ws = connected.config.fabric.workspaces[0]
                capability = "workspace authoring" if connected.config.fabric.workspace_write else "read-only (use /workspace enable-write to enable authoring)"
                await self.send(actor, f"Saved and selected {connected.id}\nWorkspace: {ws.id}\nEnvironment: {ws.environment}\nAccess: {capability}. Live connection has not yet been checked.\n" + self.login_instructions(connected))
                return
            if command == "/workspace":
                if rest not in {"", "check", "login", "login-sql", "enable-write"}:
                    raise ValueError("Use /workspace [check|login|login-sql|enable-write]")
                if not project.workspaces:
                    await self.send(actor, "No Fabric workspace configured. Send /connect DEV|TEST|PROD <workspace URL or UUID>.")
                elif rest == "enable-write":
                    if not self.registry or not access.allow_workspace_setup:
                        raise ValueError("Workspace setup is not enabled for this account")
                    with project_lock(self.data_dir / "locks", "workspace-enrollment"):
                        enabled = self.registry.enable_authoring(actor, project, self.data_dir)
                    self.projects[enabled.id] = enabled
                    self.control.set_session(actor, enabled.id, mode="write")
                    await self.send(actor, "Authoring enabled and selected. I can now create Fabric items, update metadata and definitions, and run supported jobs in this workspace. I validate and review each source stage; TEST actions require exact approval. Send your work request normally. Authentication and service permissions still apply.")
                elif rest == "login":
                    await self.send(actor, self.login_instructions(project))
                elif rest == "login-sql":
                    await self.send(actor, self.login_instructions(project, sql=True))
                elif rest == "check":
                    self.check_workspace(actor, project)
                    await self.send(actor, "Checking Fabric workspace access...")
                else:
                    await self.send(actor, "Selected project: " + project.id + "\n" + "\n".join(
                        f"{w.environment}: {w.id}" for w in project.workspaces
                    ) + "\nUse /workspace check to test live access, or /workspace login for local sign-in commands.")
                return
            if command == "/mode":
                if rest not in {"read", "write"}:
                    raise ValueError("Use /mode read or /mode write")
                if rest == "write" and not project.config.policy.local_write:
                    raise ValueError("This project does not allow local writes")
                self.control.set_session(actor, project.id, mode=rest)
                await self.send(actor, "Mode set to " + rest + " for the next task.")
                return
            if command == "/new":
                self.control.set_session(actor, project.id, mode=session["mode"])
                await self.send(actor, "Ready for a new task.")
                return
            if command == "/memory":
                records = recall(project, rest)
                await self.send(
                    actor,
                    json.dumps(records, ensure_ascii=False, indent=2)
                    if records
                    else "No matching recorded decisions.",
                )
                return
            if command == "/remember":
                parts = [p.strip() for p in rest.split("|")]
                if len(parts) != 4:
                    raise ValueError(
                        "Use /remember title | context | decision | consequences"
                    )
                with project_lock(self.data_dir / "locks", project.id):
                    record = save_decision(project, *parts, actor)
                await self.send(actor, "Recorded decision " + record["id"])
                return
            if command == "/actions":
                with self.store.connect() as db:
                    rows = [
                        dict(r)
                        for r in db.execute(
                            "SELECT id FROM plans WHERE project_id=? AND actor=? AND state='PENDING_APPROVAL'",
                            (project.id, actor),
                        )
                    ]
                for row in rows:
                    await self.approval_button(actor, project, row["id"])
                if not rows:
                    await self.send(actor, "No pending approvals.")
                return
            if command == "/rate":
                from .evaluation import record_rating

                if not session["task_id"]:
                    raise ValueError("No selected task")
                record_rating(
                    self.store,
                    project.id,
                    session["task_id"],
                    int(rest),
                    "Telegram usefulness rating",
                )
                await self.send(actor, "Rating recorded.")
                return
            if command == "/resume":
                task_id = rest or session["task_id"]
                if not task_id or not self.owns(actor, project.id, task_id):
                    raise ValueError("Task does not belong to this conversation")
                task = self.store.task(project.id, task_id)
                self.control.set_session(actor, project.id, task_id, task["mode"])
                session = self.control.session(actor)
                self.control.resume(project.id)
                text = ("Continue the original user request below using current evidence and the saved task context. "
                        "Earlier runtime errors are historical diagnostics, not a request to repair Ray. "
                        "Keep policy limits and unresolved action receipts in force. If the original request is only a vague status question, explain the known status or ask what work was intended.\n\n"
                        "Original user request:\n" + task["objective"])
            elif command == "/work":
                if not rest.strip():
                    raise ValueError("Tell me what you'd like done: /work <request>")
                text = rest.strip()
                self.control.set_session(actor, project.id, mode=session["mode"])
                session = self.control.session(actor)
            elif command.startswith("/"):
                raise ValueError("Unknown command; use /help")
            elif await self.confirm_work_handoff(actor, session, project, text):
                return
            elif self.config.conversational or github_urls(text):
                self.launch_conversation(actor, session, project, self.projects[access.projects[0]], text)
                if github_urls(text):
                    await self.send(actor, "I'm opening the GitHub source and checking the relevant files.")
                return
            self.launch(actor, session, project, text)
            await self.send(
                actor,
                "I’m working on your request. Use /stop to pause.",
            )
        except (ValueError, RuntimeError) as exc:
            await self.send(actor, "Cannot continue: " + redact(str(exc))[:1000])
        except Exception:
            await self.send(
                actor,
                "The request failed. Check /status; no automatic action retry was started.",
            )
        finally:
            self.control.received_done(update["update_id"])

    async def confirm_work_handoff(self, actor, session, project, text):
        # Only a direct, unambiguous confirmation of the selected pending task.
        # This never consumes a cloud approval or a clarification decision.
        confirmation = " ".join(text.lower().strip(" .!,").split())
        if confirmation not in {"yes", "yes please", "ok", "okay", "go ahead", "ok go ahead",
                                "okay go ahead", "ok sure go ahead", "sure go ahead", "please proceed", "proceed"}:
            return False
        task_id = session["task_id"]
        if not task_id or not self.owns(actor, project.id, task_id):
            return False
        pending = [row for row in self.control.pending(actor, project.id) if row["task_id"] == task_id]
        if len(pending) != 1 or pending[0]["kind"] != "work_handoff":
            return False
        if self.store.task(project.id, task_id)["status"] != "WAITING":
            return False
        self.control.answer(pending[0]["id"], actor, project.id, 0)
        await self.start_handoff(actor, session, project)
        return True

    async def start_handoff(self, actor, session, project):
        task = self.store.task(project.id, session["task_id"])
        self.control.resume(project.id)
        self.launch(actor, session, project, task["objective"])
        await self.send(actor, "I'll take a look. You can use /stop to pause me.")

    def check_workspace(self, actor, project):
        from .fabric import FabricGateway

        async def run():
            try:
                await asyncio.to_thread(FabricGateway(
                    project, self.store, self.data_dir, actor=actor
                ).snapshot)
                await self.send(actor, "Fabric workspace metadata and item inventory are reachable for " + project.id + ". Read-only check passed. This check does not test SQL authentication or table queries.")
            except Exception as exc:
                await self.send(actor, "Fabric check failed for " + project.id + ".\n" + error_text(describe_error(exc, "fabric_read")))
        self.jobs[actor] = asyncio.create_task(run())

    def launch_conversation(self, actor, session, project, auth_project, text, *, document=None):
        token = self.conversation.begin(actor)
        async def run():
            try:
                documents = []
                if document:
                    if token:
                        token.check()
                    data = await asyncio.to_thread(self.api.download_document, document)
                    documents.append(await asyncio.to_thread(read_document, document["file_name"], data, cancel=token))
                    del data
                result = await asyncio.to_thread(self.conversation.reply, auth_project, project, actor, text, cancel=token, mode=session["mode"], documents=documents)
                if token:
                    token.check()
                keyboard = None
                if result.offer_work:
                    # Only the original direct user message can become the objective.
                    # Model text can offer a button, never authorize an operation.
                    task = self.store.create(project.id, text, session["mode"])
                    from .task_context import capture_handoff
                    capture_handoff(self.store, project.id, task["id"], actor)
                    with self.store.connect() as db:
                        db.execute("INSERT INTO channel_tasks VALUES (?,?,?)", (actor, project.id, task["id"]))
                    self.control.set_session(actor, project.id, task["id"], session["mode"])
                    id = self.control.decide(actor, project.id, task["id"], "work_handoff", {
                        "question": "Work on your request in " + project.config.name + "?",
                        "recommendation": "Start the selected project's governed workflow.",
                        "options": ["Work on this", "Just chatting"],
                    })
                    keyboard = [[{"text": "Work on this", "callback_data": id + ":0"},
                                 {"text": "Just chatting", "callback_data": id + ":1"}]]
                await self.send(actor, result.message, keyboard, formatted=True)
            except TaskStopped:
                await self.send(actor, "Okay, I've paused. You can talk to me again whenever you like.")
            except ReadError as exc:
                await self.send(actor, str(exc))
            except Exception as exc:
                await self.send(actor, "I couldn't finish my reply.\n" + friendly_error(describe_error(exc, "conversation")))
        self.jobs[actor] = asyncio.create_task(run())

    def launch(self, actor, session, project, text):
        token = self.control.token(project.id)
        task_id = session["task_id"]
        if not task_id:
            task = self.store.create(project.id, text, session["mode"])
            from .task_context import capture_handoff
            capture_handoff(self.store, project.id, task["id"], actor)
            task_id = task["id"]
            with self.store.connect() as db:
                db.execute(
                    "INSERT INTO channel_tasks VALUES (?,?,?)",
                    (actor, project.id, task_id),
                )
            self.control.set_session(actor, project.id, task_id, session["mode"])
        with self.store.connect() as db:
            db.execute(
                "UPDATE decisions SET status='SUPERSEDED' WHERE actor=? AND project_id=? AND task_id=? AND kind='clarification' AND status='PENDING'",
                (actor, project.id, task_id),
            )

        def work():
            with project_lock(self.data_dir / "locks", project.id):
                token.check()
                return self.service.run(project, task_id, text, actor, cancel=token)

        async def run():
            report = None
            try:
                report = await asyncio.to_thread(work)
                await self.send(
                    actor,
                    task_message(report), formatted=True,
                )
                if report["status"] == "blocked" and report.get("question"):
                    await self.send(
                        actor,
                        "**Next step**\n" + report["question"] + "\n\n**Recommendation**\n" + str(report.get("recommendation") or ""),
                        formatted=True,
                    )
                if report["status"] == "clarifying":
                    choices = report.get("options") or [
                        "Proceed with recommendation",
                        "Explain trade-offs",
                    ]
                    id = self.control.decide(
                        actor,
                        project.id,
                        task_id,
                        "clarification",
                        {
                            "question": report.get("question"),
                            "recommendation": report.get("recommendation"),
                            "options": choices,
                        },
                    )
                    row = next(
                        r
                        for r in self.control.pending(actor, project.id)
                        if r["id"] == id
                    )
                    await self.show_decision(row)
                for plan in report.get("cloud_plans", []):
                    if plan["state"] == "PENDING_APPROVAL":
                        await self.approval_button(actor, project, plan["id"])
            except TaskStopped:
                self.store.update(project.id, task_id, "PAUSED")
                await self.send(actor, "⏸ Paused. Use /resume to continue or /details to inspect the task.")
            except Exception as exc:
                if report is None:
                    failed = self.store.task(project.id, task_id)
                    saved = json.loads(failed["result"]) if failed.get("result") else {}
                    error = saved.get("error") if failed["status"] in {"ERROR", "PAUSED", "BLOCKED"} else None
                    if not error:
                        error = record_failure(self.store, project.id, task_id, exc, status="PAUSED" if isinstance(exc, TimeoutError) else "ERROR")
                else:
                    error = describe_error(exc)
                await self.send(
                    actor,
                    "**⚠️ Couldn’t finish**\n\n" + friendly_error(error)
                    + "\n\nNothing was retried automatically.\n/details technical — full error details",
                    formatted=True,
                )

        self.jobs[actor] = asyncio.create_task(run())

    async def show_decision(self, row):
        data = json.loads(row["payload"])
        keyboard = [
            [{"text": choice[:80], "callback_data": row["id"] + ":" + str(i)}]
            for i, choice in enumerate(data["options"][:8])
        ]
        await self.send(
            row["actor"],
            "**Your decision**\n" + str(data.get("question", ""))
            + "\n\n**Recommendation**\n"
            + str(data.get("recommendation", "")),
            keyboard, formatted=True,
        )

    async def approval_button(self, actor, project, id):
        plan = CloudActions(project, self.store, self.data_dir).get(id)
        if plan["actor"] != actor or plan["state"] != "PENDING_APPROVAL":
            raise ValueError("Plan is not pending for this actor")
        payload = {
            "question": plan["body"]["summary"] + "\nAction hash: " + plan["digest"],
            "recommendation": "Approve only if this exact reviewed target/content is intended.",
            "options": ["Approve exact action", "Reject"],
            "plan_id": id,
            "digest": plan["digest"],
        }
        decision = self.control.decide(
            actor,
            project.id,
            plan["task_id"],
            "approval:" + id,
            payload,
            ttl=max(1, plan["expires"] - time.time()),
        )
        row = next(
            r for r in self.control.pending(actor, project.id) if r["id"] == decision
        )
        await self.show_decision(row)

    async def callback(self, actor, session, project, callback):
        if self.busy(actor):
            raise ValueError("Wait for the running task to stop or finish")
        id, choice = callback.get("data", "").rsplit(":", 1)
        pending = next(
            (r for r in self.control.pending(actor, project.id) if r["id"] == id), None
        )
        if not pending or pending["task_id"] != session["task_id"]:
            raise ValueError("Decision belongs to a different task")
        row, answer = self.control.answer(id, actor, project.id, int(choice))
        try:
            await self.api.call(
                "answerCallbackQuery", {"callback_query_id": callback["id"]}
            )
        except Exception:
            pass
        if row["kind"] == "clarification":
            self.launch(actor, session, project, "Decision: " + answer)
            return
        if row["kind"] == "work_handoff":
            if int(choice) == 1:
                self.store.update(project.id, row["task_id"], "PAUSED")
                self.control.set_session(actor, project.id, mode=session["mode"])
                await self.send(actor, "Sure, we can just talk.")
            else:
                await self.start_handoff(actor, session, project)
            return
        data = json.loads(row["payload"])
        cloud = CloudActions(project, self.store, self.data_dir)
        if int(choice) == 1:
            with self.store.connect() as db:
                db.execute(
                    "UPDATE plans SET state='CANCELLED' WHERE id=? AND actor=? AND state='PENDING_APPROVAL'",
                    (data["plan_id"], actor),
                )
            cloud._sync_task(row["task_id"])
            await self.send(actor, "Action rejected.")
            return

        def work():
            with project_lock(self.data_dir / "locks", project.id):
                cloud.approve(data["plan_id"], actor, data["digest"])
                return cloud.execute(data["plan_id"], actor)

        async def run():
            try:
                result = await asyncio.to_thread(work)
                await self.send(
                    actor, "Action " + result["id"] + ": " + result["state"]
                )
                task = self.store.task(project.id, row["task_id"])
                report = json.loads(task["result"] or "{}")
                if result["state"] == "SUCCEEDED" and task["status"] == "WAITING" and report.get("continue_work"):
                    self.launch(actor, session, project,
                        "Continue the original objective using the successful action receipts and any remaining proposed actions. "
                        "Do not repeat completed actions. Review the next dependent stage before proposing it.\n\n"
                        + task["objective"])
            except Exception as exc:
                await self.send(
                    actor,
                    "I couldn't finish the action.\n" + error_text(describe_error(exc, "cloud_action"))
                    + "\nCheck /status before retrying; an external action may need reconciliation.",
                )

        self.jobs[actor] = asyncio.create_task(run())

    async def serve(self):
        if not self.config.access:
            raise ValueError("Configure an explicit Telegram user/chat allow-list")
        with project_lock(self.data_dir / "locks", "telegram-gateway"):
            self.control.recover()
            for project in self.projects.values():
                with project_lock(self.data_dir / "locks", project.id):
                    self.control.recover(project.id)
                    self.store.recover(project.id)
            backoff = 1
            try:
                while True:
                    try:
                        updates = await self.api.call(
                            "getUpdates",
                            {
                                "offset": self.control.setting("telegram_offset", 0),
                                "timeout": 25,
                                "limit": 50,
                                "allowed_updates": ["message", "callback_query"],
                            },
                        )
                        backoff = 1
                        self.control.set_setting(
                            "telegram_health", {"state": "polling", "time": time.time()}
                        )
                    except Exception:
                        self.control.set_setting(
                            "telegram_health",
                            {"state": "connection_error", "time": time.time()},
                        )
                        await asyncio.sleep(backoff)
                        backoff = min(backoff * 2, 30)
                        continue
                    for update in updates:
                        await self.handle(update)
                        self.control.set_setting(
                            "telegram_offset", update["update_id"] + 1
                        )
            finally:
                for actor in self.jobs:
                    self.conversation.stop(actor)
                for project in self.projects.values():
                    self.control.interrupt_running(project.id)
                if self.jobs:
                    await asyncio.gather(*self.jobs.values(), return_exceptions=True)


def main(argv=None):
    parser = argparse.ArgumentParser(description="Ray Telegram long polling")
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--data-dir", type=Path, default=Path("data"))
    args = parser.parse_args(argv)
    data_dir = args.data_dir.resolve()
    data_dir.mkdir(parents=True, exist_ok=True)
    config = GatewayConfig.model_validate(
        yaml.load(args.config.read_text(encoding="utf-8"), Loader=UniqueLoader)
    )
    gateway = Gateway(
        config,
        args.config,
        StateStore(data_dir / "ray.db"),
        data_dir,
        TelegramAPI(load_token(data_dir)),
    )
    try:
        asyncio.run(gateway.serve())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
