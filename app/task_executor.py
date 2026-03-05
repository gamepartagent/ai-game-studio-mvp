from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import time
from typing import Any, Dict
from urllib import error as urlerror
from urllib import request as urlrequest

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


class DevGitHubPRExecutor(_CommandExecutor):
    name = "dev_github_pr"

    def _extract_project_id(self, store: Store, task_id: str, config: Dict[str, Any]) -> str:
        pid = str(config.get("project_id", "")).strip().upper()
        if pid:
            return pid
        task = store.tasks.get(task_id)
        if not task:
            return ""
        text = f"{task.title} {task.description}"
        m = re.search(r"(GAM-\d{5})", text.upper())
        return m.group(1) if m else "AUTO"

    def run(self, store: Store, task_id: str, actor_id: str, config: Dict[str, Any] | None = None) -> ExecutionResult:
        if task_id not in store.tasks:
            return ExecutionResult(ok=False, summary=f"task not found: {task_id}")
        if shutil.which("git") is None:
            return ExecutionResult(ok=False, summary="git not available in runtime environment")

        cfg = config or {}
        cwd = self._workdir()
        timeout_sec = int(cfg.get("timeout_sec", 240))
        project_id = self._extract_project_id(store, task_id, cfg)
        base_branch = str(cfg.get("base_branch", os.getenv("STUDIO_GIT_BASE_BRANCH", "main"))).strip() or "main"
        branch_prefix = str(cfg.get("branch_prefix", os.getenv("STUDIO_GIT_BRANCH_PREFIX", "agent/upgrade"))).strip()
        branch = f"{branch_prefix}-{project_id.lower()}-{int(time.time())}"
        files = cfg.get("files")
        add_target = " ".join(str(x) for x in files) if isinstance(files, list) and files else "-A"
        title = str(cfg.get("title", f"[AUTO] {project_id} gameplay upgrade pass")).strip()
        body = str(
            cfg.get(
                "body",
                (
                    f"Automated upgrade proposal for {project_id}.\n\n"
                    "- Generated by dev_github_pr executor\n"
                    "- Includes latest gameplay/demo changes\n"
                    "- Please review QA checklist before merge"
                ),
            )
        ).strip()
        commit_message = str(cfg.get("message", f"auto: upgrade {project_id} gameplay pass")).strip()
        gh_available = shutil.which("gh") is not None
        gh_repo = str(cfg.get("repo", os.getenv("STUDIO_GITHUB_REPO", os.getenv("GITHUB_REPOSITORY", "")))).strip()
        gh_token = str(cfg.get("token", os.getenv("GITHUB_TOKEN", ""))).strip()
        if not gh_available and (not gh_repo or not gh_token):
            return ExecutionResult(
                ok=False,
                summary="cannot create PR: neither gh cli nor GITHUB_TOKEN+repo configured",
            )

        runs: list[Dict[str, Any]] = []
        runs.append(self._run_cmd("git status --porcelain", cwd=cwd, timeout_sec=timeout_sec))
        if runs[-1]["returncode"] != 0:
            return ExecutionResult(ok=False, summary="git status failed", details={"runs": runs})
        if not str(runs[-1].get("stdout", "")).strip():
            return ExecutionResult(ok=False, summary="no changes to commit for auto PR", details={"runs": runs})

        for cmd in [
            f"git checkout -B {branch}",
            f"git add {add_target}",
            f'git commit -m "{commit_message}" --no-verify',
            f"git push -u origin {branch}",
        ]:
            runs.append(self._run_cmd(cmd, cwd=cwd, timeout_sec=timeout_sec))
            if runs[-1]["returncode"] != 0:
                break

        if all(r["returncode"] == 0 for r in runs[1:]) and gh_available:
            pr_cmd = f'gh pr create --base {base_branch} --head {branch} --title "{title}" --body "{body}"'
            runs.append(self._run_cmd(pr_cmd, cwd=cwd, timeout_sec=timeout_sec))

        api_pr_url = ""
        if all(r["returncode"] == 0 for r in runs[1:]) and not gh_available:
            try:
                payload = {
                    "title": title,
                    "head": branch,
                    "base": base_branch,
                    "body": body,
                }
                req = urlrequest.Request(
                    url=f"https://api.github.com/repos/{gh_repo}/pulls",
                    data=json.dumps(payload).encode("utf-8"),
                    headers={
                        "Authorization": f"Bearer {gh_token}",
                        "Accept": "application/vnd.github+json",
                        "Content-Type": "application/json",
                    },
                    method="POST",
                )
                with urlrequest.urlopen(req, timeout=max(10, timeout_sec)) as resp:
                    raw = resp.read().decode("utf-8", errors="replace")
                    parsed = json.loads(raw or "{}")
                    api_pr_url = str(parsed.get("html_url", "")).strip()
                    runs.append(
                        {
                            "command": "github_api_create_pr",
                            "returncode": 0 if api_pr_url else 1,
                            "stdout": raw[-4000:],
                            "stderr": "",
                            "duration_sec": 0.0,
                        }
                    )
            except urlerror.HTTPError as exc:
                runs.append(
                    {
                        "command": "github_api_create_pr",
                        "returncode": int(getattr(exc, "code", 1) or 1),
                        "stdout": "",
                        "stderr": str(exc),
                        "duration_sec": 0.0,
                    }
                )
            except Exception as exc:
                runs.append(
                    {
                        "command": "github_api_create_pr",
                        "returncode": 1,
                        "stdout": "",
                        "stderr": str(exc),
                        "duration_sec": 0.0,
                    }
                )

        ok = all(r["returncode"] == 0 for r in runs[1:])
        pr_url = ""
        for r in runs:
            out = str(r.get("stdout", "") or "")
            for tok in out.split():
                if tok.startswith("http://") or tok.startswith("https://"):
                    pr_url = tok
        if not pr_url and api_pr_url:
            pr_url = api_pr_url
        task = store.tasks[task_id]
        artifact = store.create_artifact(
            title=f"GitHub PR report for {task.id}",
            created_by=actor_id,
            task_id=task_id,
            content={
                "task": store.task_to_dict(task),
                "executor": self.name,
                "project_id": project_id,
                "branch": branch,
                "base_branch": base_branch,
                "pull_request_url": pr_url,
                "workdir": str(cwd),
                "runs": runs,
            },
        )
        status = "succeeded" if ok else "failed"
        summary = f"executor {self.name} {status}: artifact {artifact.id}"
        if pr_url:
            summary += f" (PR: {pr_url})"
        return ExecutionResult(
            ok=ok,
            summary=summary,
            artifact_id=artifact.id,
            details={
                "task_id": task_id,
                "executor": self.name,
                "project_id": project_id,
                "branch": branch,
                "pull_request_url": pr_url,
            },
        )


class DevGitHubMergeExecutor(BaseTaskExecutor):
    name = "dev_github_merge"

    def run(self, store: Store, task_id: str, actor_id: str, config: Dict[str, Any] | None = None) -> ExecutionResult:
        if task_id not in store.tasks:
            return ExecutionResult(ok=False, summary=f"task not found: {task_id}")
        cfg = config or {}
        gh_available = shutil.which("gh") is not None
        gh_repo = str(cfg.get("repo", os.getenv("STUDIO_GITHUB_REPO", os.getenv("GITHUB_REPOSITORY", "")))).strip()
        gh_token = str(cfg.get("token", os.getenv("GITHUB_TOKEN", ""))).strip()
        timeout_sec = int(cfg.get("timeout_sec", 240))
        if not gh_available and (not gh_repo or not gh_token):
            return ExecutionResult(
                ok=False,
                summary="cannot merge PR: neither gh cli nor GITHUB_TOKEN+repo configured",
            )

        pr_url = str(cfg.get("pull_request_url", "")).strip()
        pr_number = int(cfg.get("pr_number", 0) or 0)
        if pr_number <= 0 and pr_url:
            m = re.search(r"/pull/(\d+)", pr_url)
            if m:
                pr_number = int(m.group(1))
        if pr_number <= 0:
            return ExecutionResult(ok=False, summary="dev_github_merge requires pr_number or pull_request_url")

        merge_method = str(cfg.get("merge_method", "squash")).strip() or "squash"
        title = str(cfg.get("title", f"[AUTO] Merge PR #{pr_number}")).strip()
        body = str(cfg.get("body", "Auto-merged by agent orchestration after QA+CEO gate")).strip()
        runs: list[Dict[str, Any]] = []
        cmd_runner = _CommandExecutor()
        cwd = cmd_runner._workdir()

        if gh_available:
            if pr_url:
                cmd = f"gh pr merge {pr_url} --{merge_method} --delete-branch"
            elif gh_repo:
                cmd = f"gh pr merge {pr_number} --repo {gh_repo} --{merge_method} --delete-branch"
            else:
                cmd = f"gh pr merge {pr_number} --{merge_method} --delete-branch"
            runs.append(cmd_runner._run_cmd(command=cmd, cwd=cwd, timeout_sec=timeout_sec))
            if runs[-1]["returncode"] == 0:
                task = store.tasks[task_id]
                artifact = store.create_artifact(
                    title=f"GitHub merge report for {task.id}",
                    created_by=actor_id,
                    task_id=task_id,
                    content={
                        "executor": self.name,
                        "pull_request_url": pr_url,
                        "pr_number": pr_number,
                        "repo": gh_repo,
                        "merged": True,
                        "merge_method": merge_method,
                        "runs": runs,
                    },
                )
                return ExecutionResult(
                    ok=True,
                    summary=f"executor {self.name} merged PR #{pr_number}: artifact {artifact.id}",
                    artifact_id=artifact.id,
                    details={"pr_number": pr_number, "pull_request_url": pr_url, "repo": gh_repo},
                )

        if not gh_repo or not gh_token:
            task = store.tasks[task_id]
            artifact = store.create_artifact(
                title=f"GitHub merge report for {task.id}",
                created_by=actor_id,
                task_id=task_id,
                content={
                    "executor": self.name,
                    "pull_request_url": pr_url,
                    "pr_number": pr_number,
                    "repo": gh_repo,
                    "merged": False,
                    "merge_method": merge_method,
                    "runs": runs,
                },
            )
            return ExecutionResult(
                ok=False,
                summary=f"executor {self.name} failed: missing repo/token for API merge",
                artifact_id=artifact.id,
                details={"pr_number": pr_number},
            )

        api_raw = ""
        api_ok = False
        api_err = ""
        try:
            payload = {
                "commit_title": title,
                "commit_message": body,
                "merge_method": merge_method,
            }
            req = urlrequest.Request(
                url=f"https://api.github.com/repos/{gh_repo}/pulls/{pr_number}/merge",
                data=json.dumps(payload).encode("utf-8"),
                headers={
                    "Authorization": f"Bearer {gh_token}",
                    "Accept": "application/vnd.github+json",
                    "Content-Type": "application/json",
                },
                method="PUT",
            )
            with urlrequest.urlopen(req, timeout=max(10, timeout_sec)) as resp:
                api_raw = resp.read().decode("utf-8", errors="replace")
                parsed = json.loads(api_raw or "{}")
                api_ok = bool(parsed.get("merged", False))
        except urlerror.HTTPError as exc:
            api_err = str(exc)
        except Exception as exc:
            api_err = str(exc)

        task = store.tasks[task_id]
        artifact = store.create_artifact(
            title=f"GitHub merge report for {task.id}",
            created_by=actor_id,
            task_id=task_id,
            content={
                "executor": self.name,
                "pull_request_url": pr_url,
                "pr_number": pr_number,
                "repo": gh_repo,
                "merged": api_ok,
                "merge_method": merge_method,
                "api_response": api_raw[-4000:],
                "api_error": api_err,
                "runs": runs,
            },
        )
        return ExecutionResult(
            ok=api_ok,
            summary=(
                f"executor {self.name} merged PR #{pr_number}: artifact {artifact.id}"
                if api_ok
                else f"executor {self.name} merge failed for PR #{pr_number}: artifact {artifact.id}"
            ),
            artifact_id=artifact.id,
            details={"pr_number": pr_number, "pull_request_url": pr_url, "repo": gh_repo},
        )


class DevGameplaySmokeExecutor(BaseTaskExecutor):
    name = "dev_gameplay_smoke"

    def _extract_project_id(self, store: Store, task_id: str, config: Dict[str, Any]) -> str:
        pid = str(config.get("project_id", "")).strip().upper()
        if pid:
            return pid
        task = store.tasks.get(task_id)
        if not task:
            return ""
        m = re.search(r"(GAM-\d{5})", f"{task.title} {task.description}".upper())
        return m.group(1) if m else ""

    def run(self, store: Store, task_id: str, actor_id: str, config: Dict[str, Any] | None = None) -> ExecutionResult:
        if task_id not in store.tasks:
            return ExecutionResult(ok=False, summary=f"task not found: {task_id}")
        cfg = config or {}
        project_id = self._extract_project_id(store, task_id, cfg)
        if not project_id:
            return ExecutionResult(ok=False, summary="dev_gameplay_smoke requires project_id")
        if project_id not in store.game_projects:
            return ExecutionResult(ok=False, summary=f"project not found: {project_id}")
        gp = store.game_projects[project_id]

        if not gp.demo_url:
            try:
                store.generate_project_demo(project_id, actor_id=actor_id)
                gp = store.game_projects[project_id]
            except Exception as exc:
                return ExecutionResult(ok=False, summary=f"failed to generate demo before smoke test: {str(exc)}")

        demo_url = str(gp.demo_url or "")
        static_root = Path(os.getenv("STUDIO_STATIC_DIR", "static")).resolve()
        rel = demo_url.replace("/static/", "", 1).lstrip("/")
        index_path = (static_root / rel).resolve()
        game_js_path = index_path.parent / "game.js"
        mode = str((gp.game_blueprint or {}).get("mode_base") or (gp.game_blueprint or {}).get("mode") or "").strip().lower()

        checks: list[Dict[str, Any]] = []

        def add_check(name: str, passed: bool, detail: str = "") -> None:
            checks.append({"name": name, "passed": bool(passed), "detail": detail})

        add_check("index_exists", index_path.exists(), str(index_path))
        add_check("game_js_exists", game_js_path.exists(), str(game_js_path))

        index_src = ""
        js_src = ""
        if index_path.exists():
            index_src = index_path.read_text(encoding="utf-8", errors="replace")
            add_check("index_min_size", len(index_src) >= 1000, f"size={len(index_src)}")
            add_check("index_has_canvas", "id=\"game\"" in index_src, "canvas id check")
        if game_js_path.exists():
            js_src = game_js_path.read_text(encoding="utf-8", errors="replace")
            add_check("game_js_min_size", len(js_src) >= 3000, f"size={len(js_src)}")
            add_check("game_js_has_step_loop", ("function step()" in js_src) or ("requestAnimationFrame(step)" in js_src), "loop check")
            add_check("game_js_has_score_binding", "scoreEl" in js_src, "score HUD check")

        mode_signatures = {
            "aim": ["headshots", "bots", "hitZone"],
            "rhythm": ["judgePress", "beatSec", "PERFECT"],
            "runner": ["spawnObstacle", "dash", "stage"],
            "dodge": ["tailTarget", "spawnBall", "Worm Dodge End"],
            "memory": ["cardN", "sequence", "Memory End"],
            "clicker": ["combo", "coins", "Clicker End"],
        }
        sigs = mode_signatures.get(mode, [])
        if sigs and js_src:
            hit = sum(1 for s in sigs if s in js_src)
            add_check("mode_signature", hit >= max(1, len(sigs) - 1), f"mode={mode} hit={hit}/{len(sigs)}")
        else:
            add_check("mode_signature", bool(js_src), f"mode={mode or '-'}")

        passed = sum(1 for c in checks if c["passed"])
        total = max(1, len(checks))
        score = round((passed / total) * 100.0, 1)
        ok = score >= float(cfg.get("pass_score", 75.0))

        task = store.tasks[task_id]
        artifact = store.create_artifact(
            title=f"Gameplay smoke report for {task.id}",
            created_by=actor_id,
            task_id=task_id,
            content={
                "executor": self.name,
                "project_id": project_id,
                "project_title": gp.title,
                "mode": mode,
                "demo_url": demo_url,
                "score": score,
                "passed": ok,
                "checks": checks,
            },
        )
        return ExecutionResult(
            ok=ok,
            summary=f"executor {self.name} {'passed' if ok else 'failed'} ({score:.1f}): artifact {artifact.id}",
            artifact_id=artifact.id,
            details={"project_id": project_id, "mode": mode, "score": score},
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
        self.register(DevGitHubPRExecutor())
        self.register(DevGitHubMergeExecutor())
        self.register(DevGameplaySmokeExecutor())
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
