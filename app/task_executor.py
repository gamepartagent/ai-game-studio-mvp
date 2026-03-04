from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import re
import shutil
import subprocess
import time
from typing import Any, Dict

from .store import Store


@dataclass
class ExecutionResult:
    ok: bool
    summary: str
    artifact_id: str | None = None
    details: Dict[str, Any] | None = None


class BaseTaskExecutor:
    name = "base"

    def run(self, store: Store, task_id: str, actor_id: str, config: Dict[str, Any] | None = None) -> ExecutionResult:
        raise NotImplementedError


class DevDryRunExecutor(BaseTaskExecutor):
    name = "dev_dryrun"

    def run(self, store: Store, task_id: str, actor_id: str, config: Dict[str, Any] | None = None) -> ExecutionResult:
        if task_id not in store.tasks:
            return ExecutionResult(ok=False, summary=f"task not found: {task_id}")
        task = store.tasks[task_id]
        artifact = store.create_artifact(
            title=f"Dry-run output for {task.id}",
            created_by=actor_id,
            task_id=task_id,
            content={
                "task": store.task_to_dict(task),
                "executor": self.name,
                "note": "Placeholder output. Replace with real build/test executor.",
            },
        )
        return ExecutionResult(
            ok=True,
            summary=f"executor {self.name} produced artifact {artifact.id}",
            artifact_id=artifact.id,
            details={"task_id": task_id, "executor": self.name},
        )


class _CommandExecutor(BaseTaskExecutor):
    def _workdir(self) -> Path:
        raw = os.getenv("STUDIO_WORKDIR", ".")
        p = Path(raw).resolve()
        if not p.exists():
            p.mkdir(parents=True, exist_ok=True)
        return p

    def _run_cmd(self, command: str, cwd: Path, timeout_sec: int = 180) -> Dict[str, Any]:
        started = time.time()
        proc = subprocess.run(
            command,
            cwd=str(cwd),
            shell=True,
            capture_output=True,
            text=True,
            timeout=max(1, int(timeout_sec)),
        )
        return {
            "command": command,
            "returncode": int(proc.returncode),
            "stdout": (proc.stdout or "")[-12000:],
            "stderr": (proc.stderr or "")[-12000:],
            "duration_sec": round(time.time() - started, 3),
        }


class DevTestBuildExecutor(_CommandExecutor):
    name = "dev_test_build"

    def run(self, store: Store, task_id: str, actor_id: str, config: Dict[str, Any] | None = None) -> ExecutionResult:
        if task_id not in store.tasks:
            return ExecutionResult(ok=False, summary=f"task not found: {task_id}")
        cfg = config or {}
        commands = cfg.get("commands")
        if not isinstance(commands, list) or not commands:
            commands = [
                os.getenv("STUDIO_TEST_CMD", "python -m pytest -q"),
                os.getenv("STUDIO_BUILD_CMD", "python -m compileall -q app"),
            ]
        timeout_sec = int(cfg.get("timeout_sec", 300))
        cwd = self._workdir()
        runs = [self._run_cmd(str(cmd), cwd=cwd, timeout_sec=timeout_sec) for cmd in commands]
        ok = all(r["returncode"] == 0 for r in runs)
        task = store.tasks[task_id]
        artifact = store.create_artifact(
            title=f"Test/Build report for {task.id}",
            created_by=actor_id,
            task_id=task_id,
            content={
                "task": store.task_to_dict(task),
                "executor": self.name,
                "workdir": str(cwd),
                "runs": runs,
            },
        )
        status = "succeeded" if ok else "failed"
        return ExecutionResult(
            ok=ok,
            summary=f"executor {self.name} {status}: artifact {artifact.id}",
            artifact_id=artifact.id,
            details={"task_id": task_id, "executor": self.name, "runs": len(runs)},
        )


class DevGitOpsExecutor(_CommandExecutor):
    name = "dev_git_ops"

    def run(self, store: Store, task_id: str, actor_id: str, config: Dict[str, Any] | None = None) -> ExecutionResult:
        if task_id not in store.tasks:
            return ExecutionResult(ok=False, summary=f"task not found: {task_id}")
        if shutil.which("git") is None:
            return ExecutionResult(ok=False, summary="git not available in runtime environment")

        cfg = config or {}
        operation = str(cfg.get("operation", "status")).strip().lower()
        timeout_sec = int(cfg.get("timeout_sec", 180))
        cwd = self._workdir()

        commands: list[str]
        if operation == "status":
            commands = ["git status --short"]
        elif operation == "diff":
            commands = ["git diff --stat"]
        elif operation == "branch":
            branch = str(cfg.get("branch", "")).strip()
            if not branch:
                return ExecutionResult(ok=False, summary="branch operation requires config.branch")
            commands = [f"git checkout -b {branch}"]
        elif operation == "commit":
            message = str(cfg.get("message", "")).strip()
            if not message:
                return ExecutionResult(ok=False, summary="commit operation requires config.message")
            files = cfg.get("files")
            if isinstance(files, list) and files:
                add_target = " ".join(str(x) for x in files)
            else:
                add_target = "-A"
            commands = [f"git add {add_target}", f'git commit -m "{message}" --no-verify']
        else:
            return ExecutionResult(ok=False, summary=f"unknown git operation: {operation}")

        runs = [self._run_cmd(cmd, cwd=cwd, timeout_sec=timeout_sec) for cmd in commands]
        ok = all(r["returncode"] == 0 for r in runs)
        task = store.tasks[task_id]
        artifact = store.create_artifact(
            title=f"Git ops report for {task.id}",
            created_by=actor_id,
            task_id=task_id,
            content={
                "task": store.task_to_dict(task),
                "executor": self.name,
                "operation": operation,
                "workdir": str(cwd),
                "runs": runs,
            },
        )
        status = "succeeded" if ok else "failed"
        return ExecutionResult(
            ok=ok,
            summary=f"executor {self.name} {operation} {status}: artifact {artifact.id}",
            artifact_id=artifact.id,
            details={"task_id": task_id, "executor": self.name, "operation": operation},
        )


class ProjectAutoUpgradeExecutor(BaseTaskExecutor):
    name = "project_autoupgrade"

    def _extract_project_id(self, store: Store, task_id: str, config: Dict[str, Any]) -> str:
        pid = str(config.get("project_id", "")).strip().upper()
        if pid:
            return pid
        task = store.tasks.get(task_id)
        if not task:
            return ""
        text = f"{task.title} {task.description}"
        m = re.search(r"(GAM-\d{5})", text.upper())
        return m.group(1) if m else ""

    def run(self, store: Store, task_id: str, actor_id: str, config: Dict[str, Any] | None = None) -> ExecutionResult:
        if task_id not in store.tasks:
            return ExecutionResult(ok=False, summary=f"task not found: {task_id}")
        cfg = config or {}
        project_id = self._extract_project_id(store, task_id, cfg)
        if not project_id:
            return ExecutionResult(ok=False, summary="project_autoupgrade requires project_id")
        if project_id not in store.game_projects:
            return ExecutionResult(ok=False, summary=f"project not found: {project_id}")
        before = int(store.game_projects[project_id].demo_build_count or 0)
        try:
            out = store.auto_upgrade_project(project_id, actor_id=actor_id, reason="task_executor")
        except Exception as exc:
            return ExecutionResult(ok=False, summary=f"project_autoupgrade blocked: {str(exc)}")
        after = int(store.game_projects[project_id].demo_build_count or 0)
        artifact = store.create_artifact(
            title=f"Auto-upgrade report for {project_id}",
            created_by=actor_id,
            task_id=task_id,
            content={
                "executor": self.name,
                "project_id": project_id,
                "build_count_before": before,
                "build_count_after": after,
                "demo_url": out.get("demo_url"),
            },
        )
        return ExecutionResult(
            ok=True,
            summary=f"executor {self.name} upgraded {project_id} ({before} -> {after})",
            artifact_id=artifact.id,
            details={"project_id": project_id, "build_count_before": before, "build_count_after": after},
        )


class TaskExecutorRegistry:
    def __init__(self) -> None:
        self._executors: Dict[str, BaseTaskExecutor] = {}
        self.register(DevDryRunExecutor())
        self.register(DevTestBuildExecutor())
        self.register(DevGitOpsExecutor())
        self.register(ProjectAutoUpgradeExecutor())

    def register(self, executor: BaseTaskExecutor) -> None:
        self._executors[executor.name] = executor

    def names(self) -> list[str]:
        return sorted(self._executors.keys())

    def run(
        self,
        name: str,
        store: Store,
        task_id: str,
        actor_id: str,
        config: Dict[str, Any] | None = None,
    ) -> ExecutionResult:
        ex = self._executors.get(name)
        if not ex:
            return ExecutionResult(ok=False, summary=f"unknown executor: {name}", details={"available": self.names()})
        return ex.run(store=store, task_id=task_id, actor_id=actor_id, config=config)
