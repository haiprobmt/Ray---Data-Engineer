"""Personal conversation, separated from task execution and cloud operations."""

import hashlib
import json

from pydantic import Field

from .config import StrictModel
from .control import Control
from .memory import redact, safe_text
from .errors import error_text


class ConversationResult(StrictModel):
    message: str = Field(min_length=1, max_length=12000)
    offer_work: bool


class ConversationService:
    def __init__(self, store, runner):
        self.store, self.runner = store, runner
        self.control = Control(store)
        with store.connect() as db:
            db.execute("CREATE TABLE IF NOT EXISTS conversations (id INTEGER PRIMARY KEY, actor TEXT NOT NULL, role TEXT NOT NULL, text TEXT NOT NULL)")

    @staticmethod
    def control_id(actor):
        return "conversation-" + hashlib.sha256(actor.encode()).hexdigest()[:24]

    def stop(self, actor):
        self.control.stop(self.control_id(actor))

    def begin(self, actor):
        control_id = self.control_id(actor)
        self.control.resume(control_id)
        return self.control.token(control_id)

    def clear(self, actor):
        self.stop(actor)
        with self.store.connect() as db:
            db.execute("DELETE FROM conversations WHERE actor=?", (actor,))

    def project_facts(self, project, actor, mode="read"):
        policy = project.config.policy
        tasks = []
        with self.store.connect() as db:
            if db.execute("SELECT 1 FROM sqlite_master WHERE name='channel_tasks'").fetchone():
                rows = db.execute(
                    "SELECT t.id,t.objective,t.status,t.result FROM tasks t JOIN channel_tasks c ON c.task_id=t.id AND c.project_id=t.project_id WHERE c.actor=? AND t.project_id=? ORDER BY t.updated_at DESC LIMIT 3",
                    (actor, project.id),
                ).fetchall()
                for row in rows:
                    result = json.loads(row["result"]) if row["result"] else {}
                    tasks.append({"id": row["id"], "original_request": redact(row["objective"]),
                                  "state": row["status"],
                                  "cloud_plans": result.get("cloud_plans", []),
                                  "reported_result": redact(result.get("message", ""))[:3000],
                                  "error": error_text(result["error"]) if result.get("error") else None})
        observation = self.control.setting("fabric_observation:" + project.id)
        if observation and observation.get("binding") != project.binding:
            observation = None
        return {
            "id": project.id, "name": project.config.name,
            "workspace_configured": bool(project.config.fabric.workspaces), "mode": mode,
            "policy": policy.model_dump(),
            "can_edit_local_files": mode == "write" and policy.local_write and bool(project.config.validation_commands),
            "write_target_count": len(project.config.fabric.write_targets),
            "workspace_write": project.config.fabric.workspace_write,
            "create_items": project.config.fabric.create_items,
            "supported_cloud_writes": "Create items through the Fabric Items API when create_items is enabled; update metadata/definitions and run supported item jobs across configured workspaces when workspace_write is enabled, otherwise only explicit targets. Host validates and independently reviews source, executes DEV actions, and requires exact TEST approval. A notebook job can generate Excel files and load lakehouse tables. Dependent creation stages use returned item IDs.",
            "unavailable_operations": ["Direct host upload of local files to OneLake", "PROD writes, deletion and administration"],
            "latest_workspace_observation": observation,
            "recent_tasks": tasks,
        }

    def reply(self, auth_project, project, actor, text, *, cancel=None, mode="read"):
        safe_text(text, limit=16000)
        token = cancel or self.begin(actor)
        token.check()
        with self.store.connect() as db:
            history = [dict(row) for row in db.execute(
                "SELECT role,text FROM (SELECT id,role,text FROM conversations WHERE actor=? ORDER BY id DESC LIMIT 40) ORDER BY id", (actor,)
            )]
        prompt = json.dumps({
            "recent_conversation": history,
            "selected_project": self.project_facts(project, actor, mode),
            "current_user_message": text,
        }, ensure_ascii=False)
        result = ConversationResult.model_validate(self.runner.run(
            auth_project, prompt, ConversationResult.model_json_schema(),
            read_only=True, conversation=True, cancel=token,
        ))
        token.check()
        result = result.model_copy(update={"message": redact(result.message)})
        with self.store.connect() as db:
            db.executemany("INSERT INTO conversations(actor,role,text) VALUES (?,?,?)", [
                (actor, "user", text), (actor, "assistant", result.message),
            ])
            db.execute("DELETE FROM conversations WHERE actor=? AND id NOT IN (SELECT id FROM conversations WHERE actor=? ORDER BY id DESC LIMIT 40)", (actor, actor))
        return result
