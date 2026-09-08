from __future__ import annotations

import argparse
import getpass
import json
import os
import re
import subprocess
import sys
from pathlib import Path

from pydantic import ValidationError

from .codex_client import CodexRunner
from .config import load_project
from .fabric import FabricGateway, fabric_env
from .service import TaskService
from .control import Control
from .cloud import CloudActions
from .runtime import clean_env, probe, resolve_binary, versions
from .state import StateStore, project_lock


def emit(value):
    print(json.dumps(value, ensure_ascii=False, indent=2))


def parser():
    p = argparse.ArgumentParser(
        prog="ray",
        description="Ray local engineering, memory and governed Fabric operations",
    )
    p.add_argument("--project", type=Path, required=True, help="Project config.yaml")
    p.add_argument(
        "--data-dir", type=Path, default=Path(os.environ.get("RAY_DATA_DIR", "data"))
    )
    sub = p.add_subparsers(dest="command", required=True)
    sub.add_parser("doctor")
    sub.add_parser("status")
    tenant = sub.add_parser("tenant", help="Tenant enrollment, local repository and read-only discovery")
    tenant.add_argument("operation", choices=["capabilities", "secret-set", "read", "import", "allocate"])
    tenant.add_argument("--kind", default="resources")
    tenant.add_argument("--workspace", default="")
    tenant.add_argument("--id")
    tenant.add_argument("--name")
    tenant.add_argument("--output", type=Path)
    tenant.add_argument("--allocation", type=Path)
    tenant.add_argument("--apply", action="store_true")
    document = sub.add_parser("read-file", help="Read an MD, Word, PDF or Excel file without running code")
    document.add_argument("path", type=Path)
    github = sub.add_parser("github", help="Read public GitHub repository source without executing it")
    github.add_argument("url")
    sub.add_parser("recover", help="Pause interrupted tasks without rerunning them")
    login = sub.add_parser(
        "login", help="Authenticate this project's isolated local profile"
    )
    login.add_argument("service", choices=["codex", "fabric", "sql"])
    login.add_argument("--browser", action="store_true", help="Use browser SQL sign-in when the Windows broker fails")
    login.add_argument("--service-principal-env", type=Path, help="Import a local SP dotenv file into project-bound Windows protected storage")
    start = sub.add_parser("run")
    start.add_argument("message")
    start.add_argument("--mode", choices=["read", "write"], default="read")
    start.add_argument("--snapshot", type=Path)
    resume = sub.add_parser("resume")
    resume.add_argument("task_id")
    resume.add_argument("message")
    resume.add_argument("--snapshot", type=Path)
    fab = sub.add_parser("fabric")
    fab.add_argument(
        "operation",
        choices=["workspaces", "snapshot", "items", "item", "lakehouse-tables", "sql-database", "environment"],
    )
    fab.add_argument("--workspace")
    fab.add_argument("--item")
    fab.add_argument("--output", type=Path)
    sub.add_parser("skills", help="List pinned Microsoft Fabric skills")
    sub.add_parser(
        "stop", help="Durably interrupt this project without waiting for its lock"
    )
    sub.add_parser("unpause", help="Allow new work after a stop; never reruns a task")
    sub.add_parser(
        "secret-set",
        help="Prompt locally for the Telegram token and store with Windows DPAPI",
    )
    mem = sub.add_parser("memory")
    mem.add_argument("operation", choices=["search", "add"])
    mem.add_argument("text", nargs="?", default="")
    mem.add_argument("--context", default="")
    mem.add_argument("--decision", default="")
    mem.add_argument("--consequences", default="")
    for name in ("backup", "verify-backup", "evaluation"):
        command = sub.add_parser(name)
        command.add_argument("path", type=Path)
    rating = sub.add_parser("rate")
    rating.add_argument("task_id")
    rating.add_argument("score", type=int)
    rating.add_argument("--notes", default="")
    sub.add_parser("monitor-once")
    actions = sub.add_parser("actions")
    actions.add_argument(
        "operation",
        choices=[
            "list",
            "show",
            "plan",
            "approve",
            "execute",
            "reconcile",
            "rollback-export",
        ],
    )
    actions.add_argument("--id")
    actions.add_argument("--task")
    actions.add_argument(
        "--action", choices=["create_item", "update_item", "update_definition", "run_job", "deploy_to_test", "publish_environment", "tenant_action"]
    )
    actions.add_argument("--workspace")
    actions.add_argument("--item")
    actions.add_argument("--definition")
    actions.add_argument("--digest")
    actions.add_argument("--output", type=Path)
    return p


def doctor(project, data_dir):
    report = {
        "versions": versions(),
        "project_id": project.id,
        "repo": str(project.repo),
        "cloud_writes": project.config.policy.model_dump(),
        "local_write": project.config.policy.local_write,
        "allow_listed_workspaces": len(project.workspaces),
        "telegram": "available; opt-in configuration and token required",
        "checks": {},
    }
    for kind in ("codex", "fab"):
        try:
            binary = resolve_binary(kind)
            env = clean_env()
            if kind == "codex":
                env["CODEX_HOME"] = str(CodexRunner(data_dir).home(project))
                auth_args = ["login", "status"]
            else:
                env = fabric_env(data_dir, project.id)
                auth_args = ["auth", "status"]
            version = probe(binary, ["--version"], env=env)
            auth = probe(binary, auth_args, env=env)
            # Do not return auth stdout: Fabric can include names/tenant/account details.
            report["checks"][kind] = {
                "binary": binary,
                "version": version,
                "authenticated": (
                    auth.get("ok", False)
                    and (
                        kind == "codex"
                        or bool(
                            re.search(r'"logged_in"\s*:\s*true', auth.get("output", ""))
                        )
                    )
                ),
            }
        except (OSError, ValueError, ImportError) as exc:
            report["checks"][kind] = {"available": False, "error": type(exc).__name__}
    report["ready_for_local_turn"] = bool(
        report["checks"].get("codex", {}).get("authenticated")
    )
    try:
        report["checks"]["command_execution"] = CodexRunner(data_dir).probe(project)
    except (OSError, ValueError, RuntimeError, ImportError, TimeoutError):
        report["checks"]["command_execution"] = {"shell_available": False, "error_code": "SANDBOX_FAILED"}
    report["ready_for_command_execution"] = report["checks"]["command_execution"].get("shell_available", False)
    report["ready_for_fabric"] = bool(
        report["checks"].get("fab", {}).get("authenticated")
        and project.workspaces
    )
    return report


def main(argv=None):
    args = parser().parse_args(argv)
    try:
        project = load_project(args.project)
        data_dir = args.data_dir.resolve()
        if data_dir.is_relative_to(project.repo) or project.repo.is_relative_to(
            data_dir
        ):
            raise ValueError("Ray state and repository roots must be separate")
        data_dir.mkdir(parents=True, exist_ok=True)
        store = StateStore(data_dir / "ray.db")
        store.bind(project)
        control = Control(store)
        actor = "local:" + getpass.getuser()
        if args.command in {"read-file", "github"}:
            from .reading import ReadingStore
            if args.command == "read-file":
                from .documents import check_file, read_document
                path = args.path.resolve(strict=True)
                check_file(path.name, path.stat().st_size)
                with path.open("rb") as stream:
                    from .documents import MAX_BYTES
                    evidence = read_document(path.name, stream.read(MAX_BYTES + 1))
            else:
                from .github_reading import GitHubReader
                evidence = GitHubReader().read(args.url)
            emit(ReadingStore(store).save(actor, project, evidence))
            return 0
        if args.command in {"stop", "unpause"}:
            (control.stop if args.command == "stop" else control.resume)(project.id)
            emit(
                {
                    "project_id": project.id,
                    "state": "stopped" if args.command == "stop" else "ready",
                    "automatic_retries": 0,
                }
            )
            return 0
        if args.command == "secret-set":
            from .secrets import save_token

            save_token(data_dir, getpass.getpass("Telegram token (hidden): "))
            emit({"stored": "Windows current-user DPAPI"})
            return 0
        if args.command == "status":
            emit(store.list_tasks(project.id))
            return 0
        if args.command == "doctor":
            report = doctor(project, data_dir)
            emit(report)
            return 0 if report["ready_for_local_turn"] else 2
        if args.command == "skills":
            from .skills import catalogue

            emit(catalogue())
            return 0
        with project_lock(data_dir / "locks", project.id):
            if args.command == "recover":
                control.recover(project.id)
                emit(
                    {"paused_tasks": store.recover(project.id), "automatic_retries": 0}
                )
            elif args.command == "memory":
                from .memory import recall, save_decision

                emit(
                    recall(project, args.text)
                    if args.operation == "search"
                    else save_decision(
                        project,
                        args.text,
                        args.context,
                        args.decision,
                        args.consequences,
                        actor,
                    )
                )
            elif args.command in {"backup", "verify-backup"}:
                from .memory import backup, verify_backup

                emit(
                    backup(store, [project], args.path)
                    if args.command == "backup"
                    else verify_backup(args.path)
                )
            elif args.command == "evaluation":
                from .evaluation import export

                emit(export(store, project.id, args.path))
            elif args.command == "rate":
                from .evaluation import record_rating

                record_rating(store, project.id, args.task_id, args.score, args.notes)
                emit({"rating": args.score})
            elif args.command == "monitor-once":
                from .monitor import check_once

                emit(check_once(project, store, data_dir))
            elif args.command == "tenant":
                from .tenant import settings, github_settings
                from .fabric import TenantGateway
                tenant = settings(project)
                if args.operation == "capabilities":
                    emit({"tenant_id": tenant.tenant_id, "client_id": tenant.client_id,
                          "grants": [g.model_dump() for g in tenant.grants], "resources": store.tenant_resources(project)})
                elif args.operation == "secret-set":
                    if not args.name:
                        raise ValueError("--name is required for a local credential reference")
                    from .tenant_credentials import save
                    profile = Path(fabric_env(data_dir, project.id)["USERPROFILE"]) / ".config" / "fab"
                    value = getpass.getpass("Credential value (hidden, local only): ")
                    save(profile, tenant.tenant_id, tenant.client_id, args.name, value)
                    value = None
                    emit({"credential_ref": args.name, "storage": "Windows DPAPI; tenant/client/profile bound"})
                elif args.operation == "allocate":
                    from .tenant_repository import allocate
                    github = github_settings(project)
                    binding = next((w for w in github.workspaces if w.workspace == args.workspace), None)
                    if not binding or not args.allocation:
                        raise ValueError("An enrolled --workspace and --allocation JSON file are required")
                    if args.apply and not project.config.policy.local_write:
                        raise ValueError("Local writes are disabled")
                    emit(allocate(project.repo, binding.directory, json.loads(args.allocation.read_text(encoding="utf-8")), apply=args.apply))
                elif args.operation == "import":
                    if not project.config.policy.local_write or not args.id:
                        raise ValueError("Local writes and an exact --id Git commit are required for import")
                    from .tenant_repository import import_workspace
                    emit(import_workspace(project, TenantGateway(project, store, data_dir, actor=actor), args.workspace, args.id))
                else:
                    value = TenantGateway(project, store, data_dir, actor=actor).read(args.kind, args.workspace, {"id": args.id} if args.id else {})
                    if args.output:
                        output = (project.repo / args.output).resolve()
                        if output.exists() or not output.is_relative_to(project.repo) or not project.config.policy.local_write:
                            raise ValueError("Read output requires a new file within the writable project repository")
                        output.parent.mkdir(parents=True, exist_ok=True)
                        output.write_text(json.dumps(value["data"], indent=2), encoding="utf-8")
                        emit({"output": str(output), "source": value["source"]})
                    else:
                        emit(value)
            elif args.command == "actions":
                cloud = CloudActions(project, store, data_dir)
                if args.operation == "list":
                    with store.connect() as db:
                        emit(
                            [
                                dict(r)
                                for r in db.execute(
                                    "SELECT id,task_id,state,digest,expires FROM plans WHERE project_id=? AND actor=?",
                                    (project.id, actor),
                                )
                            ]
                        )
                elif args.operation == "plan":
                    if not all(
                        [
                            args.task,
                            args.action,
                            args.workspace or args.action == "tenant_action",
                            args.item or args.action == "create_item",
                            args.definition,
                        ]
                    ):
                        raise ValueError(
                            "Planning requires --task, --action, --workspace, --item and --definition"
                        )
                    plan = cloud.prepare(
                        args.task,
                        args.action,
                        args.workspace or "",
                        args.item or "",
                        args.definition,
                        actor,
                    )
                    emit(
                        {
                            k: v
                            for k, v in plan.items()
                            if k not in {"body", "remote", "result"}
                        }
                        | {"summary": plan["body"]["summary"]}
                    )
                else:
                    if not args.id:
                        raise ValueError("--id is required")
                    plan = cloud.get(args.id)
                    if plan["actor"] != actor:
                        raise ValueError("Plan belongs to a different actor")
                    if args.operation == "approve":
                        if not args.digest:
                            raise ValueError(
                                "Inspect the plan and pass its exact --digest"
                            )
                        plan = cloud.approve(args.id, actor, args.digest)
                    elif args.operation == "execute":
                        plan = cloud.execute(args.id, actor)
                    elif args.operation == "reconcile":
                        plan = cloud.reconcile(args.id, actor)
                    elif args.operation == "rollback-export":
                        if not args.output or args.output.exists():
                            raise ValueError("Choose a new --output path")
                        args.output.parent.mkdir(parents=True, exist_ok=True)
                        args.output.write_text(
                            json.dumps(plan["body"]["rollback"], indent=2),
                            encoding="utf-8",
                        )
                    emit(
                        {k: v for k, v in plan.items() if k != "body"}
                        | {
                            "summary": plan["body"]["summary"],
                            "source_digest": plan["body"]["repo_digest"],
                            "definition_path": plan["body"]["definition_path"],
                        }
                    )
            elif args.command == "login":
                if args.service_principal_env and (args.service != "fabric" or args.browser):
                    raise ValueError("--service-principal-env requires login fabric without --browser")
                if args.browser and args.service != "sql":
                    raise ValueError("--browser is supported only for SQL login")
                env = clean_env()
                if args.service_principal_env:
                    env = fabric_env(data_dir, project.id)
                    command = [sys.executable, "-m", "ray_de.fabric_auth", str(args.service_principal_env.resolve(strict=True))]
                elif args.service == "codex":
                    env["CODEX_HOME"] = str(CodexRunner(data_dir).home(project))
                    command = [resolve_binary("codex"), "login", "--device-auth"]
                elif args.service == "sql":
                    env = fabric_env(data_dir, project.id)
                    command = [sys.executable, "-m", "ray_de.sql_worker", "--login"]
                    if args.browser:
                        command.append("--browser")
                else:
                    env = fabric_env(data_dir, project.id)
                    command = [resolve_binary("fab"), "auth", "login"]
                return subprocess.call(command, env=env, shell=False)
            elif args.command == "fabric":
                gateway = FabricGateway(project, store, data_dir)
                if args.operation == "workspaces":
                    result = gateway.list_workspaces()
                elif args.operation == "snapshot":
                    result = gateway.snapshot()
                else:
                    if not args.workspace:
                        raise ValueError("--workspace is required")
                    operation = {
                        "items": "list_items",
                        "item": "get_item",
                        "lakehouse-tables": "list_lakehouse_tables",
                        "sql-database": "get_sql_database",
                        "environment": "get_environment",
                    }[args.operation]
                    result = gateway.call(operation, args.workspace, args.item)
                if args.output:
                    output = args.output.resolve()
                    if output.exists():
                        raise ValueError("Output already exists; choose a new filename")
                    output.parent.mkdir(parents=True, exist_ok=True)
                    output.write_text(
                        json.dumps(result, indent=2, ensure_ascii=False),
                        encoding="utf-8",
                    )
                    emit({"output": str(output)})
                else:
                    emit(result)
            else:
                if args.command == "run":
                    if args.mode == "write" and not project.config.policy.local_write:
                        raise ValueError("Local writes are disabled for this project")
                    task = store.create(project.id, args.message, args.mode)
                else:
                    task = store.task(project.id, args.task_id)
                    control.resume(project.id)
                snapshot = (
                    json.loads(args.snapshot.read_text(encoding="utf-8"))
                    if args.snapshot
                    else None
                )
                result = TaskService(store, CodexRunner(data_dir), data_dir).run(
                    project, task["id"], args.message, actor, snapshot=snapshot
                )
                emit(result)
                return 0 if result["status"] not in {"blocked", "error"} else 1
        return 0
    except ValidationError as exc:
        emit(
            {
                "error": "Invalid structured data",
                "fields": [
                    ".".join(map(str, e["loc"]))
                    for e in exc.errors(include_input=False)
                ],
            }
        )
        return 1
    except KeyboardInterrupt:
        print("Task paused.", file=sys.stderr)
        return 130
    except (ValueError, OSError, RuntimeError, TimeoutError) as exc:
        # Known validation errors are actionable. Runtime subprocess errors are
        # deliberately generic; no raw model/Fabric log or secret is printed.
        emit(
            {
                "error": type(exc).__name__,
                "message": str(exc)
                if not isinstance(exc, OSError)
                else "Local file or process unavailable",
            }
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
