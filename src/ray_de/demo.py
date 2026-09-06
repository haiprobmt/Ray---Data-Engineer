"""Offline demonstration: real host logic, synthetic model and Fabric responses.
Never calls Codex, Telegram, Fabric or authentication endpoints.
"""

import argparse, base64, copy, json
from pathlib import Path
import yaml
from .config import load_project
from .state import StateStore
from .orchestrator import Orchestrator
from .cloud import CloudActions
from .control import Control
from .memory import save_decision, recall, backup, verify_backup
from .evaluation import export

DEV = "11111111-1111-1111-1111-111111111111"
TEST = "33333333-3333-3333-3333-333333333333"
ITEM = "22222222-2222-2222-2222-222222222222"
JOB = "44444444-4444-4444-4444-444444444444"
OP = "55555555-5555-5555-5555-555555555555"
ACTOR = "local:offline-demo"


def definition(code):
    return {
        "definition": {
            "format": "fabricGitSource",
            "parts": [
                {
                    "path": "notebook-content.py",
                    "payloadType": "InlineBase64",
                    "payload": base64.b64encode(code.encode()).decode(),
                }
            ],
        }
    }


class SyntheticRunner:
    def run(self, project, prompt, schema, **kwargs):
        review = "verdict" in schema["properties"]
        kwargs["on_thread"]("synthetic-review" if review else "synthetic-primary")
        if review:
            return {
                "verdict": "PASS",
                "summary": "Synthetic reviewer response; no model quality claim",
                "findings": [],
                "evidence": ["Local regression executed by host"],
            }
        if not kwargs["read_only"]:
            (project.repo / "main.py").write_text("answer = 2\n", encoding="utf-8")
        return {
            "status": "completed",
            "message": "Offline fixture handled",
            "recommendation": None,
            "question": None,
            "options": [],
            "evidence": ["Synthetic model response"],
            "skills_used": [],
            "cloud_actions": [],
        }


class SyntheticFabric:
    def __init__(self):
        self.definitions = {
            DEV: definition("# Fabric notebook source\nprint(1)\n"),
            TEST: definition("# Fabric notebook source\nprint(1)\n"),
        }
        self.calls = []
        self.lose_response = False
        self.async_update = False
        self.job_status = "Completed"

    def request(self, method, endpoint, payload=None):
        self.calls.append((method, endpoint, copy.deepcopy(payload)))
        path = endpoint.split("?")[0]
        parts = path.split("/")
        body = {}
        headers = {}
        status = 200
        if path.startswith("operations/"):
            body = {} if path.endswith("/result") else {"status": "Succeeded"}
        elif path.endswith("getDefinition"):
            body = copy.deepcopy(self.definitions[parts[1]])
        elif path.endswith("updateDefinition"):
            self.definitions[parts[1]] = copy.deepcopy(payload)
            if self.lose_response:
                self.lose_response = False
                raise TimeoutError("Synthetic lost response after commit")
            if self.async_update:
                status = 202
                headers = {"x-ms-operation-id": OP, "Retry-After": "0"}
        elif path.endswith("/jobs/RunNotebook/instances"):
            status = 202
            headers = {
                "Location": "https://api.fabric.microsoft.com/v1/"
                + path.replace("/jobs/RunNotebook/instances", "/jobs/instances/")
                + JOB,
                "Retry-After": "0",
            }
        elif "/jobs/instances/" in path:
            body = {"status": self.job_status}
        else:
            body = {"id": ITEM, "type": "Notebook"}
        return {"status_code": status, "text": body, "headers": headers}

    @property
    def mutations(self):
        return [
            c
            for c in self.calls
            if c[1]
            .split("?")[0]
            .endswith(("updateDefinition", "RunNotebook/instances"))
        ]


def fixture(root):
    root = Path(root)
    directory = root / "project"
    repo = directory / "repo"
    repo.mkdir(parents=True)
    (repo / "main.py").write_text("answer = 1\n", encoding="utf-8")
    (repo / "definition.json").write_text(
        json.dumps(definition("# Fabric notebook source\nprint(2)\n")), encoding="utf-8"
    )
    config = {
        "project_id": "offline-demo",
        "name": "Offline simulation",
        "repo_path": "repo",
        "policy": {
            "local_write": True,
            "fabric_dev_write": True,
            "fabric_test_write": "approval",
        },
        "fabric": {
            "workspaces": [
                {"id": DEV, "environment": "DEV"},
                {"id": TEST, "environment": "TEST"},
            ],
            "allow_definition_export": True,
            "write_targets": [
                {
                    "workspace_id": ws,
                    "item_id": ITEM,
                    "item_type": "Notebook",
                    "job_type": "RunNotebook",
                }
                for ws in (DEV, TEST)
            ],
        },
        "validation_commands": [
            ["{python}", "-c", "import main; assert main.answer == 2"]
        ],
        "post_validation_commands": [
            ["{python}", "-c", "import main; assert main.answer == 2"]
        ],
    }
    (directory / "config.yaml").write_text(yaml.safe_dump(config), encoding="utf-8")
    (directory / "CONTEXT.md").write_text(
        "Offline synthetic project. No live cloud claims.", encoding="utf-8"
    )
    project = load_project(directory / "config.yaml")
    store = StateStore(root / "state" / "ray.db")
    store.bind(project)
    task = store.create(project.id, "Improve a notebook", "write")
    Orchestrator(store, SyntheticRunner()).run(
        project, task["id"], "Improve the fixture"
    )
    transport = SyntheticFabric()
    cloud = CloudActions(project, store, root / "state", transport=transport)
    return project, store, task, transport, cloud


def run_demo(output):
    output = Path(output).resolve()
    if output.exists():
        raise ValueError("Choose a new demo directory")
    project, store, task, transport, cloud = fixture(output)
    checks = []

    def record(name, ok):
        if not ok:
            raise AssertionError(name)
        checks.append({"check": name, "passed": True})

    record(
        "Local source validation and synthetic independent review",
        json.loads(store.task(project.id, task["id"])["result"])["cloud_eligible"],
    )
    save_decision(
        project,
        "Notebook validation",
        "Need repeatability",
        "Validate before promotion",
        "Requires test evidence",
        ACTOR,
    )
    record("Durable project memory", bool(recall(project, "repeatability")))
    plan = cloud.prepare(
        task["id"], "update_definition", DEV, ITEM, "definition.json", ACTOR
    )
    record(
        "DEV definition update and post-validation",
        cloud.execute(plan["id"], ACTOR)["state"] == "SUCCEEDED",
    )
    plan = cloud.prepare(task["id"], "run_job", DEV, ITEM, "definition.json", ACTOR)
    record(
        "DEV job submission and completion polling",
        cloud.execute(plan["id"], ACTOR)["state"] == "SUCCEEDED",
    )
    plan = cloud.prepare(
        task["id"], "deploy_to_test", TEST, ITEM, "definition.json", ACTOR
    )
    record("TEST waits for exact approval", plan["state"] == "PENDING_APPROVAL")
    cloud.approve(plan["id"], ACTOR, plan["digest"])
    record(
        "Approved TEST promotion",
        cloud.execute(plan["id"], ACTOR)["state"] == "SUCCEEDED",
    )
    try:
        cloud.execute(plan["id"], ACTOR)
        replay = False
    except ValueError:
        replay = True
    record("Approval/action replay rejected", replay)
    control = Control(store)
    token = control.token(project.id)
    control.stop(project.id)
    control.resume(project.id)
    try:
        token.check()
        stopped = False
    except RuntimeError:
        stopped = True
    record("Old work remains stopped after resume", stopped)
    # Rollback is a new reviewed action, never an automatic second mutation.
    (project.repo / "definition.json").write_text(
        json.dumps(plan["body"]["rollback"]), encoding="utf-8"
    )
    rollback_task = store.create(
        project.id, "Restore reviewed prior definition", "write"
    )
    Orchestrator(store, SyntheticRunner()).run(
        project, rollback_task["id"], "Review the explicit rollback source"
    )
    rollback_plan = cloud.prepare(
        rollback_task["id"], "update_definition", DEV, ITEM, "definition.json", ACTOR
    )
    record(
        "Rollback uses a fresh reviewed plan",
        cloud.execute(rollback_plan["id"], ACTOR)["state"] == "SUCCEEDED",
    )
    backup(store, [project], output / "backup.zip")
    record(
        "Backup hash verification",
        verify_backup(output / "backup.zip")["verified_files"] >= 3,
    )
    export(store, project.id, output / "evaluation.html")
    report = {
        "mode": "OFFLINE SIMULATION",
        "live_services_contacted": False,
        "model_responses": "synthetic",
        "fabric_responses": "synthetic",
        "local_validation": "real subprocesses, SQLite and file operations",
        "checks": checks,
        "synthetic_mutations": len(transport.mutations),
        "live_acceptance": "Not run: user requested local-only.",
    }
    (output / "report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(run_demo(args.output), indent=2))


if __name__ == "__main__":
    main()
