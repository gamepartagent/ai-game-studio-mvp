from __future__ import annotations

import asyncio
import random
from typing import Any, Awaitable, Callable, Dict

from .store import Store


TOPICS = [
    ("LiveOps planning sync", "Decide this week mission cadence and reward pacing."),
    ("Economy balancing review", "Review sinks/sources and early progression friction."),
    ("Release checkpoint", "Confirm blockers and ship/no-ship criteria."),
]

NOTES = [
    "Retention dip appears at level transition 2->3.",
    "Need clearer value communication for starter offer.",
    "One crash repro still appears on resume.",
    "QA requests one more regression pass before release.",
]

DECISIONS = [
    "Proceed with staggered rollout.",
    "Block release until crash fix verification.",
    "Run A/B test on onboarding prompt.",
]

ACTIONS = [
    "Create task for retention funnel instrumentation",
    "Create task for resume crash hotfix validation",
    "Create task for pricing copy variant B",
]


async def run_meeting_bot(store: Store, emit: Callable[[Dict[str, Any]], Awaitable[None]]) -> None:
    tick = 3.0
    store.add_event(
        type="meeting_bot.started",
        actor_id="ops",
        summary="Meeting bot started",
        refs={},
        payload={"tick_seconds": tick},
        source="orchestrator",
    )
    await emit({"type": "event", "data": store.event_to_dict(store.events[0])})
    while True:
        await asyncio.sleep(tick)
        if not store.auto_run:
            continue

        scheduled = [m for m in store.meetings.values() if m.status == "Scheduled"]
        ongoing = [m for m in store.meetings.values() if m.status == "Ongoing"]

        if not scheduled and not ongoing and random.random() < 0.35:
            topic, agenda = random.choice(TOPICS)
            participants = random.sample(list(store.agents.keys()), k=min(3, len(store.agents)))
            creator = participants[0] if participants else "ops"
            store.create_meeting(topic, agenda, participants, created_by=creator, source="orchestrator")
            await emit({"type": "event", "data": store.event_to_dict(store.events[0])})
            continue

        if scheduled and not ongoing:
            m = scheduled[0]
            actor = m.participant_ids[0] if m.participant_ids else "ops"
            store.start_meeting(m.id, actor_id=actor, source="orchestrator")
            await emit({"type": "event", "data": store.event_to_dict(store.events[0])})
            continue

        for m in ongoing:
            actor = random.choice(m.participant_ids) if m.participant_ids else "ops"
            decision = random.choice(DECISIONS) if random.random() < 0.4 else None
            action = random.choice(ACTIONS) if random.random() < 0.6 else None
            store.add_meeting_note(
                m.id,
                note=random.choice(NOTES),
                author_id=actor,
                decision=decision,
                action_item={"text": action, "created_by": actor} if action else None,
                source="orchestrator",
            )
            await emit({"type": "event", "data": store.event_to_dict(store.events[0])})

            if len(m.notes) >= 3:
                store.close_meeting(m.id, actor_id=actor, source="orchestrator")
                await emit({"type": "event", "data": store.event_to_dict(store.events[0])})
