"""Local operational evidence and user usefulness ratings, without invented scores."""

import html, json
from pathlib import Path
from .control import Control
from .memory import safe_text, redact
from .state import now


def record_rating(store, project_id, task_id, score, notes=""):
    store.task(project_id, task_id)
    Control(store)
    safe_text(notes)
    if type(score) is not int or not 0 <= score <= 4:
        raise ValueError(
            "Use a rating from 0 (harmful/wrong) to 4 (mostly handled independently)"
        )
    with store.connect() as db:
        db.execute(
            "INSERT OR REPLACE INTO ratings VALUES (?,?,?,?,?)",
            (task_id, project_id, score, notes, now()),
        )


def summarize(store, project_id):
    Control(store)
    tasks = store.list_tasks(project_id)
    with store.connect() as db:
        actions = [
            dict(r)
            for r in db.execute(
                "SELECT operation,status,error_code FROM actions WHERE project_id=?",
                (project_id,),
            )
        ]
        plans = [
            dict(r)
            for r in db.execute(
                "SELECT id,state,digest FROM plans WHERE project_id=?", (project_id,)
            )
        ]
        ratings = [
            dict(r)
            for r in db.execute(
                "SELECT task_id,score,notes FROM ratings WHERE project_id=?",
                (project_id,),
            )
        ]
    from .task_context import read
    intelligence = []
    for task in tasks:
        context = read(store, project_id, task["id"])
        result = json.loads(task["result"] or "{}")
        intelligence.append({"task_id": task["id"], "execution": context.get("execution"),
                             "model_runs": context.get("model_runs", []),
                             "repair_attempts": len(result.get("repair_history", [])),
                             "repair_stop": result.get("repair_stop"),
                             "has_handoff_context": bool(context.get("brief")),
                             "plan": context.get("plan")})
    return {
        "intelligence": intelligence,
        "project_id": project_id,
        "generated_at": now(),
        "task_count": len(tasks),
        "completed": sum(t["status"] == "COMPLETED" for t in tasks),
        "tasks": [
            {"id": t["id"], "status": t["status"], "mode": t["mode"]} for t in tasks
        ],
        "actions": actions,
        "plans": plans,
        "ratings": ratings,
        "average_usefulness": sum(r["score"] for r in ratings) / len(ratings)
        if ratings
        else None,
        "interpretation": "Observed local records only. Test/simulation records do not establish live performance or unauthorized-write rates.",
    }


def export(store, project_id, output):
    output = Path(output).resolve()
    if output.exists():
        raise ValueError("Evaluation output already exists")
    report = summarize(store, project_id)
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.suffix.lower() == ".html":
        body = (
            '<html lang="en"><meta charset="utf-8"><title>Ray evaluation</title><style>body{font:16px system-ui;max-width:1000px;margin:40px auto;padding:20px;background:#f5f7fb;color:#172a40}pre{white-space:pre-wrap;background:white;padding:24px;border-radius:12px}h1{color:#146b65}</style><h1>Ray operational evaluation</h1><p>Local evidence, with live acceptance tracked separately.</p><pre>'
            + html.escape(redact(json.dumps(report, indent=2)))
            + "</pre></html>"
        )
    else:
        body = json.dumps(report, indent=2)
    output.write_text(body, encoding="utf-8")
    return {"output": str(output), "task_count": len(report["tasks"])}
