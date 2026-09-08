"""Actor/project-scoped reference material, separate from user instructions."""
import json

from .github_reading import GitHubReader, github_urls
from .documents import ReadError
from .memory import redact_data


class ReadingStore:
    def __init__(self, store):
        self.store = store
        with store.connect() as db:
            db.execute("CREATE TABLE IF NOT EXISTS reading_material (id INTEGER PRIMARY KEY, actor TEXT NOT NULL, project_id TEXT NOT NULL, binding TEXT NOT NULL, name TEXT NOT NULL, body TEXT NOT NULL)")

    def save(self, actor, project, evidence):
        evidence = redact_data(evidence)
        # Bound whole structured records without cutting JSON or hiding omissions.
        for key in ("tree", "files", "sections"):
            while len(json.dumps(evidence, ensure_ascii=False)) > 160000 and evidence.get(key):
                evidence[key] = evidence[key][:int(len(evidence[key]) * 0.75)]
                evidence["truncated"] = True
                if key == "tree":
                    evidence["tree_truncated"] = True
        if evidence.get("truncated"):
            evidence.setdefault("warnings", []).append("Only part of the reference material is available in this conversation.")
        name = evidence.get("url") or evidence["name"]
        with self.store.connect() as db:
            db.execute("DELETE FROM reading_material WHERE actor=? AND project_id=? AND name=?", (actor, project.id, name))
            db.execute("INSERT INTO reading_material(actor,project_id,binding,name,body) VALUES (?,?,?,?,?)", (actor, project.id, project.binding, name, json.dumps(evidence, ensure_ascii=False)))
            db.execute("DELETE FROM reading_material WHERE actor=? AND project_id=? AND id NOT IN (SELECT id FROM reading_material WHERE actor=? AND project_id=? ORDER BY id DESC LIMIT 6)", (actor, project.id, actor, project.id))
        return evidence

    def load(self, actor, project):
        result, size = [], 0
        with self.store.connect() as db:
            rows = db.execute("SELECT body FROM reading_material WHERE actor=? AND project_id=? AND binding=? ORDER BY id DESC LIMIT 6", (actor, project.id, project.binding)).fetchall()
        for row in rows:
            if size + len(row["body"]) > 190000:
                result.append({"source": "host_read_limit", "warning": "Older reference material was omitted from this turn. Send that file or link again if needed."})
                break
            result.append(json.loads(row["body"]))
            size += len(row["body"])
        return result

    def clear(self, actor):
        with self.store.connect() as db:
            db.execute("DELETE FROM reading_material WHERE actor=?", (actor,))

    def read_links(self, actor, project, text, *, cancel=None, reader=None):
        urls = github_urls(text)
        if len(urls) > 2:
            raise ReadError("Send up to two GitHub repository links at a time.")
        reader = reader or GitHubReader(cancel=cancel)
        for url in urls:
            self.save(actor, project, reader.read(url))
        return self.load(actor, project)
