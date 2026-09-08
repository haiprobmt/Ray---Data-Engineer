from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


class StateStore:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        with self.connect() as db:
            db.executescript("""
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS projects (
                    id TEXT PRIMARY KEY, binding TEXT NOT NULL, repo TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS tasks (
                    id TEXT PRIMARY KEY, project_id TEXT NOT NULL REFERENCES projects(id),
                    objective TEXT NOT NULL, mode TEXT NOT NULL, status TEXT NOT NULL,
                    thread_id TEXT, reviewer_thread_id TEXT, result TEXT,
                    created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY, task_id TEXT NOT NULL REFERENCES tasks(id),
                    status TEXT NOT NULL, time TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS actions (
                    id TEXT PRIMARY KEY, project_id TEXT NOT NULL, task_id TEXT,
                    actor TEXT NOT NULL, operation TEXT NOT NULL, target TEXT NOT NULL,
                    environment TEXT, status TEXT NOT NULL, error_code TEXT,
                    created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS source_checkpoints (
                    task_id TEXT PRIMARY KEY REFERENCES tasks(id), manifest TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS tenant_resources (
                    project_id TEXT NOT NULL, grant_key TEXT NOT NULL, kind TEXT NOT NULL,
                    resource TEXT NOT NULL, plan_id TEXT NOT NULL,
                    PRIMARY KEY(project_id, grant_key));
            """)

    @contextmanager
    def connect(self):
        db = sqlite3.connect(self.path, timeout=10)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys=ON")
        try:
            with db:
                yield db
        finally:
            db.close()

    def bind(self, project):
        project.state_store = self
        with self.connect() as db:
            old = db.execute(
                "SELECT binding FROM projects WHERE id=?", (project.id,)
            ).fetchone()
            if old and old["binding"] != project.binding:
                raise ValueError(
                    "Project binding changed; use a new project_id for the changed target or policy"
                )
            db.execute(
                "INSERT OR IGNORE INTO projects VALUES (?,?,?)",
                (project.id, project.binding, str(project.repo)),
            )

    def tenant_resources(self, project):
        from .cloud import digest
        with self.connect() as db:
            if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='plans'").fetchone():
                return {}
            rows = db.execute("SELECT r.*, p.body, p.digest, p.state FROM tenant_resources r JOIN plans p ON p.id=r.plan_id AND p.project_id=r.project_id WHERE r.project_id=?", (project.id,)).fetchall()
        result = {}
        for row in rows:
            body = json.loads(row["body"])
            if row["state"] != "SUCCEEDED" or body.get("binding") != project.binding or digest(body) != row["digest"]:
                continue
            if body.get("tenant_grant") != row["grant_key"]:
                raise ValueError("Tenant resource receipt does not match its capability")
            result[row["grant_key"]] = dict(json.loads(row["resource"]), kind=row["kind"])
        return result

    def create(self, project_id: str, objective: str, mode: str):
        from .memory import safe_text
        safe_text(objective, limit=16000)
        if mode not in {"read", "write"}:
            raise ValueError("Task mode must be read or write")
        task_id = str(uuid4())
        with self.connect() as db:
            db.execute(
                "INSERT INTO tasks VALUES (?,?,?,?,?,?,?,?,?,?)",
                (
                    task_id,
                    project_id,
                    objective,
                    mode,
                    "WAITING",
                    None,
                    None,
                    None,
                    now(),
                    now(),
                ),
            )
        return self.task(project_id, task_id)

    def task(self, project_id: str, task_id: str):
        with self.connect() as db:
            row = db.execute(
                "SELECT * FROM tasks WHERE id=? AND project_id=?", (task_id, project_id)
            ).fetchone()
        if row is None:
            raise ValueError("Task does not belong to this project")
        return dict(row)

    def list_tasks(self, project_id: str):
        with self.connect() as db:
            return [
                dict(r)
                for r in db.execute(
                    "SELECT * FROM tasks WHERE project_id=? ORDER BY created_at DESC",
                    (project_id,),
                )
            ]

    def update(self, project_id: str, task_id: str, status: str, **fields):
        if set(fields) - {"thread_id", "reviewer_thread_id", "result"}:
            raise ValueError("Unsupported task fields")
        if status not in {
            "WAITING",
            "WORKING",
            "VALIDATING",
            "REVIEWING",
            "CLARIFYING",
            "APPROVAL_REQUIRED",
            "BLOCKED",
            "PAUSED",
            "COMPLETED",
            "ERROR",
        }:
            raise ValueError("Unknown task state")
        if status == "PAUSED":
            previous = self.task(project_id, task_id)
            report = dict(fields.get("result") or json.loads(previous["result"] or "{}"))
            report.update(status="paused", cloud_eligible=False)
            fields["result"] = report
        if "result" in fields:
            from .memory import redact_data
            fields["result"] = json.dumps(redact_data(fields["result"]), ensure_ascii=False)
        fields.update(status=status, updated_at=now())
        with self.connect() as db:
            cur = db.execute(
                "UPDATE tasks SET "
                + ",".join(k + "=?" for k in fields)
                + " WHERE id=? AND project_id=?",
                (*fields.values(), task_id, project_id),
            )
            if cur.rowcount != 1:
                raise ValueError("Task does not belong to this project")
            db.execute(
                "INSERT INTO events(task_id,status,time) VALUES (?,?,?)",
                (task_id, status, now()),
            )

    def checkpoint_source(self, project_id, task_id, baseline):
        self.task(project_id, task_id)
        with self.connect() as db:
            # Keep the earliest uncompleted source baseline across crashes/resumes.
            db.execute("INSERT OR IGNORE INTO source_checkpoints VALUES (?,?)", (task_id, json.dumps(baseline)))

    def pending_source(self, project_id, task_id):
        self.task(project_id, task_id)
        with self.connect() as db:
            row = db.execute(
                "SELECT c.manifest,p.repo FROM source_checkpoints c JOIN tasks t ON t.id=c.task_id "
                "JOIN projects p ON p.id=t.project_id WHERE t.id=? AND t.project_id=?",
                (task_id, project_id),
            ).fetchone()
        if not row:
            return []
        from .context import changed, manifest
        return changed(json.loads(row["manifest"]), manifest(Path(row["repo"])))

    def clear_source_checkpoint(self, project_id, task_id):
        self.task(project_id, task_id)
        with self.connect() as db:
            db.execute("DELETE FROM source_checkpoints WHERE task_id=?", (task_id,))

    def preserve_source(self, project_id, task_id):
        task = self.task(project_id, task_id)
        report = json.loads(task["result"] or "{}")
        try:
            paths = self.pending_source(project_id, task_id)
            report["review_paths"] = sorted(set(report.get("review_paths", report.get("changed_files", []))) | set(paths))
            report.pop("source_review_incomplete", None)
        except (OSError, ValueError):
            # Preserve the baseline for the next recovery if the tree is unreadable.
            report["source_review_incomplete"] = True
        report["cloud_eligible"] = False
        self.update(project_id, task_id, task["status"], result=report)

    def recover(self, project_id):
        # Call only while holding the project process lock. Never repeat work.
        count = 0
        for task in self.list_tasks(project_id):
            report = json.loads(task.get("result") or "{}")
            interrupted_wait = task["status"] == "WAITING" and report.get("phase") in {"reading_fabric", "source_ready", "preparing_fabric", "applying_changes", "running_job", "preparing_next_step"}
            if task["status"] in {"WORKING", "VALIDATING", "REVIEWING"} or interrupted_wait:
                self.update(project_id, task["id"], "PAUSED")
                self.preserve_source(project_id, task["id"])
                count += 1
        return count

    def audit_start(self, project_id, task_id, actor, operation, target, environment):
        action_id = str(uuid4())
        with self.connect() as db:
            db.execute(
                "INSERT INTO actions VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (
                    action_id,
                    project_id,
                    task_id,
                    actor,
                    operation,
                    target,
                    environment,
                    "STARTED",
                    None,
                    now(),
                    now(),
                ),
            )
        return action_id

    def audit_finish(self, action_id, status, error_code=None):
        with self.connect() as db:
            db.execute(
                "UPDATE actions SET status=?,error_code=?,updated_at=? WHERE id=?",
                (status, error_code, now(), action_id),
            )


@contextmanager
def project_lock(directory: Path, project_id: str):
    """OS lock releases on process death, unlike a persistent 'busy' flag."""
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / (project_id + ".lock")
    with path.open("a+b") as handle:
        handle.seek(0, 2)
        if handle.tell() == 0:
            handle.write(b"0")
            handle.flush()
        handle.seek(0)
        try:
            if __import__("os").name == "nt":
                import msvcrt

                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            raise RuntimeError("Another Ray process is using this project") from exc
        try:
            yield
        finally:
            handle.seek(0)
            if __import__("os").name == "nt":
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(handle, fcntl.LOCK_UN)
