"""Durable channel, decision, approval and interruption state."""

from __future__ import annotations
import json, secrets, time
from .state import now


class TaskStopped(RuntimeError):
    pass


class Control:
    def __init__(self, store):
        self.store = store
        with store.connect() as db:
            db.executescript("""
            CREATE TABLE IF NOT EXISTS control(project_id TEXT PRIMARY KEY, stopped INTEGER NOT NULL DEFAULT 0, generation INTEGER NOT NULL DEFAULT 0);
            CREATE TABLE IF NOT EXISTS sessions(actor TEXT PRIMARY KEY, project_id TEXT NOT NULL, task_id TEXT, mode TEXT NOT NULL DEFAULT 'read');
            CREATE TABLE IF NOT EXISTS decisions(id TEXT PRIMARY KEY, actor TEXT NOT NULL, project_id TEXT NOT NULL, task_id TEXT NOT NULL, kind TEXT NOT NULL, payload TEXT NOT NULL, expires REAL NOT NULL, status TEXT NOT NULL DEFAULT 'PENDING', answer TEXT);
            CREATE TABLE IF NOT EXISTS plans(id TEXT PRIMARY KEY, project_id TEXT NOT NULL, task_id TEXT NOT NULL, actor TEXT NOT NULL, digest TEXT NOT NULL, body TEXT NOT NULL, state TEXT NOT NULL, expires REAL NOT NULL, approved_by TEXT, remote TEXT, result TEXT);
            CREATE TABLE IF NOT EXISTS inbox(update_id INTEGER PRIMARY KEY, status TEXT NOT NULL, actor TEXT, received_at TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS outbox(id INTEGER PRIMARY KEY, actor TEXT NOT NULL, payload TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'PENDING');
            CREATE TABLE IF NOT EXISTS ratings(task_id TEXT PRIMARY KEY, project_id TEXT NOT NULL, score INTEGER NOT NULL, notes TEXT NOT NULL, recorded_at TEXT NOT NULL);
            """)

    def token(self, project_id):
        with self.store.connect() as db:
            db.execute(
                "INSERT OR IGNORE INTO control(project_id) VALUES (?)", (project_id,)
            )
            row = db.execute(
                "SELECT * FROM control WHERE project_id=?", (project_id,)
            ).fetchone()
        if row["stopped"]:
            raise TaskStopped("Project is stopped; explicitly resume before continuing")
        return Token(self, project_id, row["generation"])

    def stop(self, project_id):
        with self.store.connect() as db:
            db.execute(
                "INSERT OR IGNORE INTO control(project_id) VALUES (?)", (project_id,)
            )
            db.execute(
                "UPDATE control SET stopped=1,generation=generation+1 WHERE project_id=?",
                (project_id,),
            )
            db.execute(
                "UPDATE plans SET state='CANCELLED' WHERE project_id=? AND state IN ('READY','PENDING_APPROVAL','APPROVED')",
                (project_id,),
            )
            db.execute(
                "UPDATE decisions SET status='CANCELLED' WHERE project_id=? AND status='PENDING'",
                (project_id,),
            )
        for task in self.store.list_tasks(project_id):
            if task["status"] in {
                "WAITING",
                "WORKING",
                "VALIDATING",
                "REVIEWING",
                "CLARIFYING",
                "APPROVAL_REQUIRED",
            }:
                self.store.update(project_id, task["id"], "PAUSED")

    def interrupt_running(self, project_id):
        """Service shutdown interrupts workers while preserving pending user decisions."""
        with self.store.connect() as db:
            db.execute(
                "INSERT OR IGNORE INTO control(project_id) VALUES (?)", (project_id,)
            )
            db.execute(
                "UPDATE control SET generation=generation+1 WHERE project_id=?",
                (project_id,),
            )

    def resume(self, project_id):
        with self.store.connect() as db:
            db.execute(
                "INSERT OR IGNORE INTO control(project_id) VALUES (?)", (project_id,)
            )
            db.execute("UPDATE control SET stopped=0 WHERE project_id=?", (project_id,))

    def session(self, actor):
        with self.store.connect() as db:
            row = db.execute(
                "SELECT * FROM sessions WHERE actor=?", (actor,)
            ).fetchone()
        return dict(row) if row else None

    def set_session(self, actor, project_id, task_id=None, mode="read"):
        with self.store.connect() as db:
            db.execute(
                "INSERT OR REPLACE INTO sessions VALUES (?,?,?,?)",
                (actor, project_id, task_id, mode),
            )

    def decide(self, actor, project_id, task_id, kind, payload, ttl=3600):
        self.store.task(project_id, task_id)
        id = secrets.token_urlsafe(16)
        with self.store.connect() as db:
            db.execute(
                "UPDATE decisions SET status='SUPERSEDED' WHERE actor=? AND project_id=? AND task_id=? AND kind=? AND status='PENDING'",
                (actor, project_id, task_id, kind),
            )
            db.execute(
                "INSERT INTO decisions(id,actor,project_id,task_id,kind,payload,expires) VALUES (?,?,?,?,?,?,?)",
                (
                    id,
                    actor,
                    project_id,
                    task_id,
                    kind,
                    json.dumps(payload),
                    time.time() + ttl,
                ),
            )
        return id

    def answer(self, id, actor, project_id, answer):
        with self.store.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                "SELECT * FROM decisions WHERE id=? AND actor=? AND project_id=? AND status='PENDING'",
                (id, actor, project_id),
            ).fetchone()
            if not row or row["expires"] < time.time():
                raise ValueError(
                    "Decision is expired, used or belongs to another conversation"
                )
            payload = json.loads(row["payload"])
            if (
                type(answer) is not int
                or answer < 0
                or answer >= len(payload["options"])
            ):
                raise ValueError("Invalid decision choice")
            db.execute(
                "UPDATE decisions SET status='ANSWERED',answer=? WHERE id=?",
                (str(answer), id),
            )
        return dict(row), payload["options"][answer]

    def pending(self, actor, project_id):
        with self.store.connect() as db:
            return [
                dict(r)
                for r in db.execute(
                    "SELECT * FROM decisions WHERE actor=? AND project_id=? AND status='PENDING' AND expires>?",
                    (actor, project_id, time.time()),
                )
            ]

    def setting(self, key, default=None):
        with self.store.connect() as db:
            row = db.execute(
                "SELECT value FROM settings WHERE key=?", (key,)
            ).fetchone()
        return json.loads(row[0]) if row else default

    def set_setting(self, key, value):
        with self.store.connect() as db:
            db.execute(
                "INSERT OR REPLACE INTO settings VALUES (?,?)", (key, json.dumps(value))
            )

    def receive(self, id, actor):
        with self.store.connect() as db:
            return (
                db.execute(
                    "INSERT OR IGNORE INTO inbox VALUES (?,'RECEIVED',?,?)",
                    (id, actor, now()),
                ).rowcount
                == 1
            )

    def received_done(self, id):
        with self.store.connect() as db:
            db.execute("UPDATE inbox SET status='HANDLED' WHERE update_id=?", (id,))

    def recover(self, project_id=None):
        with self.store.connect() as db:
            if project_id is not None:
                db.execute(
                    "UPDATE plans SET state='UNCERTAIN' WHERE state='EXECUTING' AND project_id=?",
                    (project_id,),
                )
                ids = [
                    r[0]
                    for r in db.execute(
                        "SELECT DISTINCT task_id FROM plans WHERE project_id=? AND state='UNCERTAIN'",
                        (project_id,),
                    )
                ]
                for task_id in ids:
                    db.execute(
                        "UPDATE tasks SET status='BLOCKED',updated_at=? WHERE id=? AND project_id=?",
                        (now(), task_id, project_id),
                    )
                    db.execute(
                        "INSERT INTO events(task_id,status,time) VALUES (?,'BLOCKED',?)",
                        (task_id, now()),
                    )
                return
            db.execute("UPDATE inbox SET status='INTERRUPTED' WHERE status='RECEIVED'")
            db.execute("UPDATE outbox SET state='UNCERTAIN' WHERE state='SENDING'")


class Token:
    def __init__(self, control, project_id, generation):
        self.control, self.project_id, self.generation = control, project_id, generation

    def check(self):
        with self.control.store.connect() as db:
            row = db.execute(
                "SELECT * FROM control WHERE project_id=?", (self.project_id,)
            ).fetchone()
        if not row or row["stopped"] or row["generation"] != self.generation:
            raise TaskStopped("Task stopped; no new actions will be started")

    def wait(self, seconds):
        self.check()
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            self.check()
            time.sleep(min(0.1, max(0, deadline - time.monotonic())))
