"""Run the supplied scenarios against the real Ray SDK workflow, without its answer key.

Results are observations, not automatic acceptance verdicts. Evaluate separately.
"""
import argparse
import hashlib
import json
import shutil
from pathlib import Path

import yaml

from ray_de.codex_client import CodexRunner
from ray_de.config import load_project
from ray_de.errors import describe_error
from ray_de.service import TaskService
from ray_de.state import StateStore, project_lock


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pack", type=Path, required=True)
    parser.add_argument("--run-root", type=Path, required=True)
    parser.add_argument("--auth-home", type=Path, required=True)
    parser.add_argument("--scenario", nargs="*", default=[])
    parser.add_argument("--followup", help="Continue a single existing scenario with this message")
    args = parser.parse_args()
    pack, root = args.pack.resolve(), args.run_root.resolve()
    if root.is_relative_to(pack) or pack.is_relative_to(root):
        parser.error("Run directory must be separate from the input/evaluator pack")
    if args.followup and len(args.scenario) != 1:
        parser.error("A follow-up requires exactly one scenario")
    prompts = json.loads((pack / "TEST_PROMPTS.json").read_text(encoding="utf-8"))
    selected = {k: v for k, v in prompts.items() if not args.scenario or k.split("_")[0] in args.scenario}
    if not selected:
        parser.error("No matching scenarios")
    state = root / "state"
    store = StateStore(state / "ray.db")
    runner = CodexRunner(state)
    for name, prompt in selected.items():
        directory = root / "projects" / name
        repo = directory / "repo"
        config = directory / "config.yaml"
        if not config.exists():
            repo.mkdir(parents=True, exist_ok=True)
            hashes = {}
            for source in sorted((pack / "ray_input").iterdir()):
                if not source.is_file() or source.is_symlink():
                    raise ValueError("Input pack must contain only regular files")
                shutil.copyfile(source, repo / source.name)
                hashes[source.name] = hashlib.sha256(source.read_bytes()).hexdigest()
            (directory / "input-manifest.json").write_text(json.dumps(hashes, indent=2))
            cfg = {"project_id": "scenario-" + name.lower(), "name": "Fabric local engineering lab",
                   "repo_path": "repo", "policy": {"local_write": True},
                   "fabric": {"workspaces": []},
                   "validation_commands": [["{python}", "-m", "ray_de.artifacts"]],
                   "timeout_seconds": 600}
            config.write_text(yaml.safe_dump(cfg), encoding="utf-8")
            (directory / "CONTEXT.md").write_text(
                "This is a local engineering lab. The repository contains the supplied source files. "
                "Inspect them before deciding what to ask or implement. Use business_rules.json for "
                "the documented conventions. Produce local source, analysis and validation evidence. "
                "No cloud workspace is configured; do not perform or claim cloud execution.\n", encoding="utf-8")
        project = load_project(config)
        store.bind(project)
        auth_file = runner.home(project) / "auth.json"
        if not auth_file.exists():
            # Same user's authentication only; never copy configuration or rollouts.
            shutil.copyfile(args.auth_home / "auth.json", auth_file)
        tasks = store.list_tasks(project.id)
        if tasks and not args.followup:
            print(json.dumps({"scenario": name, "skipped_existing_task": tasks[0]["id"]}), flush=True)
            continue
        task = tasks[0] if tasks else store.create(project.id, prompt, "read" if name.startswith("T05") else "write")
        message = args.followup or prompt
        print(json.dumps({"scenario": name, "task_id": task["id"], "state": "running"}), flush=True)
        try:
            with project_lock(state / "locks", project.id):
                result = TaskService(store, runner, state).run(project, task["id"], message, "local-scenario-evaluator")
        except Exception as exc:
            result = {"status": "error", "error": describe_error(exc), "task_id": task["id"]}
        record = {"scenario": name, "prompt": message, "result": result,
                  "runtime": "real Codex SDK and Ray task service; local-only project"}
        rounds = list(directory.glob("round-*.json"))
        output = directory / f"round-{len(rounds)+1:02d}.json"
        output.write_text(json.dumps(record, indent=2), encoding="utf-8")
        print(json.dumps({"scenario": name, "status": result["status"], "report": str(output),
                          "message": result.get("message", ""), "error": result.get("error")}), flush=True)


if __name__ == "__main__":
    main()
