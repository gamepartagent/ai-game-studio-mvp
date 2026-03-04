from __future__ import annotations

import asyncio
import random
from typing import Optional, Callable, Awaitable, Dict, Any, List

from .store import Store


# Simple role-to-task-type mapping
ROLE_TYPES = {
    "CEO": ["CEO"],
    "MKT": ["MKT"],
    "DEV": ["DEV"],
    "QA": ["QA"],
    "OPS": ["OPS"],
}


TASK_TEMPLATES = [
    # DEV
    ("DEV", "P0", "Fix crash in battle scene (random null ref)", "Investigate logs, reproduce, patch, add test."),
    ("DEV", "P1", "Implement settings menu (audio sliders)", "Add UI + persist settings."),
    ("DEV", "P1", "Optimize loading time (asset prefetch)", "Profile and reduce cold start."),
    ("DEV", "P2", "Add small UX polish (button feedback)", "Haptics + audio + animation timing."),
    # QA
    ("QA", "P1", "Run smoke tests (new build)", "Onboarding, core loop, shop, exit/relaunch."),
    ("QA", "P2", "Write repro steps for reported bug", "Turn user report into deterministic repro."),
    # MKT
    ("MKT", "P2", "Draft promo post (feature highlight)", "Short hook + bullet benefits + CTA."),
    ("MKT", "P2", "Prepare patch notes summary", "User-facing summary; keep it concise."),
    # OPS
    ("OPS", "P2", "Daily report: progress + blockers", "Summarize events and current sprint."),
    ("OPS", "P2", "Organize backlog tags", "Normalize task types/priority and add notes."),
    # CEO
    ("CEO", "P1", "Review sprint plan and approve release", "Check approvals queue and decide."),
]


def pick_template(types: List[str]) -> Optional[tuple]:
    candidates = [t for t in TASK_TEMPLATES if t[0] in types]
    return random.choice(candidates) if candidates else None


async def run_simulator(store: Store, emit: Callable[[Dict[str, Any]], Awaitable[None]]) -> None:
    """
    Background loop:
    - Assigns Todo tasks to idle agents
    - Moves work forward with a countdown
    - Occasionally requests approvals (release/post)
    """
    tick = 1.0
    while True:
        await asyncio.sleep(tick)

        if not store.auto_run:
            continue

        speed = max(0.2, min(store.speed, 5.0))
        dt = tick * speed

        # 1) Decrement active work; complete tasks when countdown ends
        for agent in store.agents.values():
            if agent.status in ("Working", "Testing", "Drafting") and agent.work_remaining > 0:
                agent.work_remaining -= dt
                if agent.work_remaining <= 0:
                    # finish activity
                    # normal task completion
                    task_id = agent.current_task_id
                    if task_id and task_id in store.tasks:
                        store.update_task(task_id, status="Done")
                        await emit({"type": "event", "data": store.event_to_dict(store.events[0])})

                        # Sometimes create an approval request after finishing relevant tasks
                        if store.tasks[task_id].type == "MKT" and random.random() < 0.35:
                            apr = store.create_approval(
                                kind="post",
                                title="Approve community post draft",
                                requested_by=agent.id,
                                payload={"task_id": task_id},
                            )
                            await emit({"type": "event", "data": store.event_to_dict(store.events[0])})

                        if store.tasks[task_id].type in ("DEV", "QA") and random.random() < 0.2:
                            apr = store.create_approval(
                                kind="release",
                                title="Approve release candidate build",
                                requested_by="qa",
                                payload={"related_task": task_id},
                            )
                            await emit({"type": "event", "data": store.event_to_dict(store.events[0])})

                    # reset agent
                    agent.current_task_id = None
                    prev = agent.status
                    agent.status = "Idle"
                    store.add_event(
                        type="agent.status_changed",
                        actor_id=agent.id,
                        summary=f"{agent.name} finished work and is now idle",
                        refs={"task_id": task_id},
                        payload={"status": [prev, "Idle"]},
                    )
                    await emit({"type": "event", "data": store.event_to_dict(store.events[0])})

        # 2) Assign work to idle agents
        for agent in store.agents.values():
            if agent.status != "Idle":
                continue

            allowed_types = ROLE_TYPES.get(agent.role, [])
            todo = store.find_todo_tasks(allowed_types=allowed_types)
            if not todo:
                # create new task sometimes
                if random.random() < 0.25:
                    tpl = pick_template(allowed_types or ["OPS"])
                    if tpl:
                        ttype, pr, title, desc = tpl
                        task = store.create_task(title=title, description=desc, type=ttype, priority=pr, assignee_id=None)
                        await emit({"type": "event", "data": store.event_to_dict(store.events[0])})
                continue

            task = todo[0]
            # claim task
            store.update_task(task.id, status="Doing", assignee_id=agent.id)
            await emit({"type": "event", "data": store.event_to_dict(store.events[0])})

            # set agent status by role
            prev = agent.status
            if agent.role == "DEV":
                agent.status = "Working"
            elif agent.role == "QA":
                agent.status = "Testing"
            elif agent.role == "MKT":
                agent.status = "Drafting"
            elif agent.role == "OPS":
                agent.status = "Working"
            else:
                agent.status = "Working"

            agent.current_task_id = task.id
            agent.work_remaining = random.uniform(5.0, 14.0)

            store.add_event(
                type="agent.status_changed",
                actor_id=agent.id,
                summary=f"{agent.name} started {agent.status.lower()} on {task.id}",
                refs={"task_id": task.id},
                payload={"status": [prev, agent.status]},
            )
            await emit({"type": "event", "data": store.event_to_dict(store.events[0])})
