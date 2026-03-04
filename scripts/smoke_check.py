from __future__ import annotations

import asyncio
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.orchestrator import ToolExecutor
from app.store import Store


async def _run() -> None:
    os.environ["STUDIO_DB_PATH"] = "data/smoke_studio.db"
    os.environ["ARTIFACT_ROOT_DIR"] = "data/smoke_artifacts"
    db_path = Path(os.environ["STUDIO_DB_PATH"])
    artifact_root = Path(os.environ["ARTIFACT_ROOT_DIR"])
    if db_path.exists():
        db_path.unlink()
    if artifact_root.exists():
        for p in sorted(artifact_root.rglob("*"), reverse=True):
            if p.is_file():
                p.unlink()
            elif p.is_dir():
                p.rmdir()

    store = Store()

    async def emit(_: dict) -> None:
        return

    tools = ToolExecutor(store, emit)

    # 1) regular task action executes immediately
    await tools.execute_many(
        [
            {
                "tool": "create_task",
                "args": {
                    "title": "Smoke task",
                    "description": "verify create_task",
                    "type": "DEV",
                    "priority": "P2",
                },
            }
        ],
        default_agent_id="ops",
        source="smoke",
    )
    assert any(t.title == "Smoke task" for t in store.tasks.values()), "create_task failed"
    smoke_task_id = next(t.id for t in store.tasks.values() if t.title == "Smoke task")

    # 1.5) task executor plugin creates artifact and marks done
    await tools.execute_many(
        [
            {
                "tool": "run_task_executor",
                "args": {"task_id": smoke_task_id, "executor": "dev_dryrun", "actor_id": "ops"},
            }
        ],
        default_agent_id="ops",
        source="smoke",
    )
    assert store.tasks[smoke_task_id].status == "Done", "executor should mark task done"

    # 2) high-risk action is gated
    await tools.execute_many(
        [{"tool": "create_artifact", "args": {"title": "Build artifact", "actor_id": "ops"}}],
        default_agent_id="ops",
        source="smoke",
    )
    gates = [a for a in store.approvals.values() if a.kind == "action_gate" and a.status == "Pending"]
    assert gates, "expected pending action_gate approval"

    # 3) approved gate executes
    gate = gates[0]
    store.decide_approval(gate.id, "approve", "ceo")
    await tools.execute_approved_action_gates()
    executed_evt = [e for e in store.events if e.type == "action_gate.executed"]
    assert executed_evt, "approved gate was not executed"
    assert store.artifacts, "artifact record not created"
    art = list(store.artifacts.values())[0]
    assert art.latest_version == 1, "artifact version mismatch"
    assert art.versions and Path(art.versions[0]["file_path"]).exists(), "artifact file not saved"

    # 4) persistence restore
    reloaded = Store()
    assert any(t.title == "Smoke task" for t in reloaded.tasks.values()), "snapshot restore failed"
    assert art.id in reloaded.artifacts, "artifact restore failed"

    print("SMOKE_OK")
    print(
        f"tasks={len(reloaded.tasks)} approvals={len(reloaded.approvals)} "
        f"artifacts={len(reloaded.artifacts)} events={len(reloaded.events)}"
    )


if __name__ == "__main__":
    asyncio.run(_run())
