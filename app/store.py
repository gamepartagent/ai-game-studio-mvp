from __future__ import annotations

from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Deque
from collections import deque
import os
import random
from pathlib import Path

from .artifact_repo import ArtifactRepo
from .persistence import SnapshotSQLite
from .role_policy import profile_for_agent

KST = timezone(timedelta(hours=9))


def now_iso() -> str:
    return datetime.now(KST).isoformat(timespec="seconds")


def _task_id_gen():
    i = 1
    while True:
        yield f"TASK-{i:04d}"
        i += 1


def _event_id_gen():
    i = 1
    while True:
        yield f"EVT-{i:06d}"
        i += 1


def _approval_id_gen():
    i = 1
    while True:
        yield f"APR-{i:05d}"
        i += 1


def _artifact_id_gen():
    i = 1
    while True:
        yield f"ART-{i:05d}"
        i += 1


def _meeting_id_gen():
    i = 1
    while True:
        yield f"MTG-{i:05d}"
        i += 1


def _kpi_event_id_gen():
    i = 1
    while True:
        yield f"KPI-{i:06d}"
        i += 1


def _experiment_id_gen():
    i = 1
    while True:
        yield f"EXP-{i:05d}"
        i += 1


def _release_id_gen():
    i = 1
    while True:
        yield f"REL-{i:05d}"
        i += 1


def _trend_id_gen():
    i = 1
    while True:
        yield f"TRD-{i:05d}"
        i += 1


def _game_project_id_gen():
    i = 1
    while True:
        yield f"GAM-{i:05d}"
        i += 1


TASK_ID = _task_id_gen()
EVENT_ID = _event_id_gen()
APPROVAL_ID = _approval_id_gen()
ARTIFACT_ID = _artifact_id_gen()
MEETING_ID = _meeting_id_gen()
KPI_EVENT_ID = _kpi_event_id_gen()
EXPERIMENT_ID = _experiment_id_gen()
RELEASE_ID = _release_id_gen()
TREND_ID = _trend_id_gen()
GAME_PROJECT_ID = _game_project_id_gen()


def _extract_id_num(value: str) -> int:
    try:
        return int(value.split("-")[-1])
    except Exception:
        return 0


def _reset_generators(
    task_next: int,
    event_next: int,
    approval_next: int,
    artifact_next: int,
    meeting_next: int,
    kpi_next: int,
    experiment_next: int,
    release_next: int,
    trend_next: int,
    game_next: int,
) -> None:
    global TASK_ID, EVENT_ID, APPROVAL_ID, ARTIFACT_ID, MEETING_ID, KPI_EVENT_ID, EXPERIMENT_ID, RELEASE_ID
    global TREND_ID, GAME_PROJECT_ID

    def _task_gen(start: int):
        i = start
        while True:
            yield f"TASK-{i:04d}"
            i += 1

    def _event_gen(start: int):
        i = start
        while True:
            yield f"EVT-{i:06d}"
            i += 1

    def _approval_gen(start: int):
        i = start
        while True:
            yield f"APR-{i:05d}"
            i += 1

    def _artifact_gen(start: int):
        i = start
        while True:
            yield f"ART-{i:05d}"
            i += 1

    def _meeting_gen(start: int):
        i = start
        while True:
            yield f"MTG-{i:05d}"
            i += 1

    def _kpi_gen(start: int):
        i = start
        while True:
            yield f"KPI-{i:06d}"
            i += 1

    def _experiment_gen(start: int):
        i = start
        while True:
            yield f"EXP-{i:05d}"
            i += 1

    def _release_gen(start: int):
        i = start
        while True:
            yield f"REL-{i:05d}"
            i += 1

    def _trend_gen(start: int):
        i = start
        while True:
            yield f"TRD-{i:05d}"
            i += 1

    def _game_gen(start: int):
        i = start
        while True:
            yield f"GAM-{i:05d}"
            i += 1

    TASK_ID = _task_gen(task_next)
    EVENT_ID = _event_gen(event_next)
    APPROVAL_ID = _approval_gen(approval_next)
    ARTIFACT_ID = _artifact_gen(artifact_next)
    MEETING_ID = _meeting_gen(meeting_next)
    KPI_EVENT_ID = _kpi_gen(kpi_next)
    EXPERIMENT_ID = _experiment_gen(experiment_next)
    RELEASE_ID = _release_gen(release_next)
    TREND_ID = _trend_gen(trend_next)
    GAME_PROJECT_ID = _game_gen(game_next)


@dataclass
class Agent:
    id: str
    name: str
    role: str  # CEO, MKT, DEV, QA, OPS
    seat: Dict[str, int]  # {x,y} grid location for office map
    color: str  # hex
    title: str = ""
    level: str = ""
    department: str = ""
    status: str = "Idle"
    current_task_id: Optional[str] = None
    work_remaining: float = 0.0  # seconds


@dataclass
class Task:
    id: str
    title: str
    description: str
    type: str  # DEV/QA/MKT/OPS/CEO
    priority: str  # P0/P1/P2
    status: str = "Todo"  # Todo/Doing/Done/Blocked
    assignee_id: Optional[str] = None
    created_at: str = field(default_factory=now_iso)
    updated_at: str = field(default_factory=now_iso)
    checklist: List[Dict[str, Any]] = field(default_factory=list)
    comments: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class Approval:
    id: str
    kind: str  # release / post / deploy etc.
    title: str
    requested_by: str  # agent_id
    status: str = "Pending"  # Pending/Approved/Rejected
    created_at: str = field(default_factory=now_iso)
    decided_at: Optional[str] = None
    decision_by: Optional[str] = None
    payload: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Event:
    id: str
    ts: str
    type: str
    source: str
    actor: Dict[str, Any]
    refs: Dict[str, Optional[str]]
    summary: str
    payload: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Artifact:
    id: str
    title: str
    task_id: Optional[str]
    created_by: str
    created_at: str = field(default_factory=now_iso)
    updated_at: str = field(default_factory=now_iso)
    latest_version: int = 0
    versions: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class Meeting:
    id: str
    title: str
    agenda: str
    participant_ids: List[str]
    status: str = "Scheduled"  # Scheduled/Ongoing/Done
    created_at: str = field(default_factory=now_iso)
    started_at: Optional[str] = None
    ended_at: Optional[str] = None
    notes: List[Dict[str, Any]] = field(default_factory=list)
    decisions: List[str] = field(default_factory=list)
    action_items: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class KPIEvent:
    id: str
    ts: str
    event_type: str
    user_id: str
    value: float = 0.0
    meta: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Experiment:
    id: str
    name: str
    hypothesis: str
    primary_metric: str
    variants: List[str]
    project_id: str = ""
    status: str = "Running"  # Running/Completed
    created_at: str = field(default_factory=now_iso)
    ended_at: Optional[str] = None
    exposures: Dict[str, int] = field(default_factory=dict)
    conversions: Dict[str, float] = field(default_factory=dict)
    winner_variant: Optional[str] = None


@dataclass
class Release:
    id: str
    version: str
    title: str
    task_id: Optional[str]
    requested_by: str
    approval_id: str
    status: str = "PendingApproval"  # PendingApproval/Approved/Rejected/Deployed
    created_at: str = field(default_factory=now_iso)
    deployed_at: Optional[str] = None
    final_confirmed: bool = False
    final_confirmed_at: Optional[str] = None
    final_confirmed_by: Optional[str] = None
    notes: str = ""
    artifact_id: Optional[str] = None
    rollout_stage: str = "PreDeploy"  # PreDeploy/Canary/Stage50/Full/RolledBack
    rollout_percent: int = 0
    rollout_blocked: bool = False
    rollback_reason: str = ""


@dataclass
class TrendSignal:
    id: str
    topic: str
    genre: str
    score: float
    source: str
    summary: str
    created_at: str = field(default_factory=now_iso)


@dataclass
class GameProject:
    id: str
    title: str
    genre: str
    concept: str
    status: str = "Ideation"  # Ideation/Prototype/QA/ReadyToRelease/SubmittedForHuman/Released/Archived
    trend_ids: List[str] = field(default_factory=list)
    meeting_ids: List[str] = field(default_factory=list)
    task_ids: List[str] = field(default_factory=list)
    release_id: Optional[str] = None
    release_version: Optional[str] = None
    demo_url: Optional[str] = None
    demo_build_count: int = 0
    game_blueprint: Dict[str, Any] = field(default_factory=dict)
    quality_score: float = 0.0
    submission_reason: str = ""
    submitted_for_human: bool = False
    submitted_at: Optional[str] = None
    review_checklist: Dict[str, bool] = field(
        default_factory=lambda: {
            "no_personal_data": False,
            "no_third_party_ip": False,
            "license_checked": False,
            "policy_checked": False,
        }
    )
    review_notes: str = ""
    reviewed_by: Optional[str] = None
    reviewed_at: Optional[str] = None
    created_at: str = field(default_factory=now_iso)
    updated_at: str = field(default_factory=now_iso)


class Store:
    """
    In-memory store for MVP.
    Swap this with SQLite/Postgres later.
    """
    def __init__(self) -> None:
        self.agents: Dict[str, Agent] = {}
        self.tasks: Dict[str, Task] = {}
        self.approvals: Dict[str, Approval] = {}
        self.artifacts: Dict[str, Artifact] = {}
        self.meetings: Dict[str, Meeting] = {}
        self.kpi_events: Deque[KPIEvent] = deque(maxlen=5000)
        self.experiments: Dict[str, Experiment] = {}
        self.releases: Dict[str, Release] = {}
        self.trend_signals: Deque[TrendSignal] = deque(maxlen=1200)
        self.game_projects: Dict[str, GameProject] = {}
        self.mode_extensions: List[Dict[str, Any]] = []
        self.learning_memory: Dict[str, Any] = self._default_learning_memory()
        self.events: Deque[Event] = deque(maxlen=300)
        self._bootstrapping: bool = True
        db_path = os.getenv("STUDIO_DB_PATH", "data/studio.db")
        artifact_dir = os.getenv("ARTIFACT_ROOT_DIR", "data/artifacts")
        self._persistence = SnapshotSQLite(db_path=db_path)
        self._artifact_repo = ArtifactRepo(root_dir=artifact_dir)

        # control knobs
        self.auto_run: bool = True
        self.speed: float = 1.0  # 0.5x ~ 5x

        loaded = self._persistence.load()
        if loaded:
            self._load_snapshot(loaded)
        else:
            self._seed_default_data()
            self._bootstrapping = False
            self.persist()
        self._bootstrapping = False

    def _seed_default_data(self) -> None:
        # Seats on a 3x2 grid
        agent_specs = [
            ("ceo", "CEO AI", "CEO", {"x": 0, "y": 0}, "#FF6B6B"),
            ("mkt", "Marketing AI", "MKT", {"x": 1, "y": 0}, "#4D96FF"),
            ("dev_a", "Dev AI A", "DEV", {"x": 2, "y": 0}, "#FFD166"),
            ("dev_b", "Dev AI B", "DEV", {"x": 0, "y": 1}, "#A27BFF"),
            ("qa", "QA / Release AI", "QA", {"x": 1, "y": 1}, "#06D6A0"),
            ("ops", "Ops AI", "OPS", {"x": 2, "y": 1}, "#FFA559"),
        ]
        for _id, name, role, seat, color in agent_specs:
            profile = profile_for_agent(_id, role)
            self.agents[_id] = Agent(
                id=_id,
                name=name,
                role=role,
                seat=seat,
                color=color,
                title=profile.title,
                level=profile.level,
                department=profile.department,
            )

        # initial tasks
        self.create_task(
            title="Fix crash on launch (#203)",
            description="Repro on some devices. Investigate stack trace and patch.",
            type="DEV",
            priority="P0",
            assignee_id=None,
        )
        self.create_task(
            title="Update tutorial flow (reduce drop-off)",
            description="Add guidance steps and UI hints for first session.",
            type="DEV",
            priority="P1",
            assignee_id=None,
        )
        self.create_task(
            title="Run regression test for v0.1 build",
            description="Smoke + core loops + onboarding.",
            type="QA",
            priority="P1",
            assignee_id=None,
        )
        self.create_task(
            title="Draft patch notes (v0.1.1)",
            description="Summarize fixes + improvements for community post.",
            type="MKT",
            priority="P2",
            assignee_id=None,
        )
        self.create_task(
            title="Daily report: yesterday summary",
            description="Summarize progress, blockers, and next actions.",
            type="OPS",
            priority="P2",
            assignee_id=None,
        )

        self.add_event(
            type="system.boot",
            actor_id="ops",
            summary="Studio booted. Simulator started.",
            refs={},
            payload={},
        )
        self._rebuild_id_generators()

    def _default_learning_memory(self) -> Dict[str, Any]:
        return {
            "updated_at": now_iso(),
            "mode_bias": {
                "aim": 0.0,
                "runner": 0.0,
                "dodge": 0.0,
                "clicker": 0.0,
                "memory": 0.0,
                "rhythm": 0.0,
            },
            "variant_bias": {},
            "project_outcomes": [],
        }

    def _normalize_learning_memory(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        base = self._default_learning_memory()
        row = dict(raw or {})
        mode_bias = dict(row.get("mode_bias", {}) or {})
        for k in list(base["mode_bias"].keys()):
            try:
                base["mode_bias"][k] = float(mode_bias.get(k, base["mode_bias"][k]))
            except Exception:
                pass
        vb = {}
        for k, v in dict(row.get("variant_bias", {}) or {}).items():
            key = str(k or "").strip().lower()
            if not key:
                continue
            try:
                vb[key] = float(v)
            except Exception:
                continue
        outcomes: List[Dict[str, Any]] = []
        for x in list(row.get("project_outcomes", []) or [])[:300]:
            item = dict(x or {})
            pid = str(item.get("project_id", "")).strip()
            if not pid:
                continue
            outcomes.append(
                {
                    "project_id": pid,
                    "mode": str(item.get("mode", "")).strip().lower(),
                    "variant": str(item.get("variant", "")).strip().lower(),
                    "quality": float(item.get("quality", 0.0) or 0.0),
                    "kpi": float(item.get("kpi", 0.0) or 0.0),
                    "outcome": float(item.get("outcome", 0.0) or 0.0),
                    "reason": str(item.get("reason", "")).strip(),
                    "ts": str(item.get("ts", now_iso())).strip() or now_iso(),
                }
            )
        base["variant_bias"] = vb
        base["project_outcomes"] = outcomes
        base["updated_at"] = str(row.get("updated_at", now_iso())).strip() or now_iso()
        return base

    def _load_snapshot(self, snap: Dict[str, Any]) -> None:
        self.auto_run = bool(snap.get("control", {}).get("auto_run", True))
        self.speed = float(snap.get("control", {}).get("speed", 1.0))

        self.agents.clear()
        for raw in snap.get("agents", []):
            raw = dict(raw)
            p = profile_for_agent(str(raw.get("id", "")), str(raw.get("role", "OPS")))
            raw["title"] = p.title
            raw["level"] = p.level
            raw["department"] = p.department
            a = Agent(**raw)
            self.agents[a.id] = a

        self.tasks.clear()
        for raw in snap.get("tasks", []):
            t = Task(**raw)
            self.tasks[t.id] = t

        self.approvals.clear()
        for raw in snap.get("approvals", []):
            a = Approval(**raw)
            self.approvals[a.id] = a

        self.artifacts.clear()
        for raw in snap.get("artifacts", []):
            a = Artifact(**raw)
            self.artifacts[a.id] = a

        self.meetings.clear()
        for raw in snap.get("meetings", []):
            m = Meeting(**raw)
            self.meetings[m.id] = m

        self.kpi_events.clear()
        for raw in snap.get("kpi_events", []):
            self.kpi_events.append(KPIEvent(**raw))

        self.experiments.clear()
        for raw in snap.get("experiments", []):
            e = Experiment(**raw)
            self.experiments[e.id] = e

        self.releases.clear()
        for raw in snap.get("releases", []):
            r = Release(**raw)
            self.releases[r.id] = r

        self.trend_signals.clear()
        for raw in snap.get("trend_signals", []):
            self.trend_signals.append(TrendSignal(**raw))

        self.game_projects.clear()
        for raw in snap.get("game_projects", []):
            g = GameProject(**raw)
            self.game_projects[g.id] = g

        self.mode_extensions = []
        for raw in snap.get("mode_extensions", []):
            row = dict(raw or {})
            row["mode_id"] = str(row.get("mode_id", "")).strip().lower()
            row["name"] = str(row.get("name", "")).strip()
            row["base_mode"] = str(row.get("base_mode", "")).strip().lower()
            row["keywords"] = [str(x).strip().lower() for x in (row.get("keywords", []) or []) if str(x).strip()]
            row["created_at"] = str(row.get("created_at", "")).strip() or now_iso()
            row["source"] = str(row.get("source", "trend")).strip() or "trend"
            row["reason"] = str(row.get("reason", "")).strip()
            if row["mode_id"] and row["name"] and row["base_mode"] in {"aim", "runner", "dodge", "clicker", "memory", "rhythm"}:
                self.mode_extensions.append(row)

        self.learning_memory = self._normalize_learning_memory(dict(snap.get("learning_memory", {}) or {}))

        self.events.clear()
        for raw in snap.get("events", []):
            raw = dict(raw)
            raw.setdefault("source", "runtime")
            self.events.append(Event(**raw))

        # fallback if snapshot had no seeded agents
        if not self.agents:
            self._seed_default_data()

        self._rebuild_id_generators()

    def _rebuild_id_generators(self) -> None:
        max_task = max([_extract_id_num(tid) for tid in self.tasks.keys()] or [0])
        max_event = max([_extract_id_num(eid) for eid in [e.id for e in self.events]] or [0])
        max_apr = max([_extract_id_num(aid) for aid in self.approvals.keys()] or [0])
        max_art = max([_extract_id_num(aid) for aid in self.artifacts.keys()] or [0])
        max_mtg = max([_extract_id_num(mid) for mid in self.meetings.keys()] or [0])
        max_kpi = max([_extract_id_num(k.id) for k in self.kpi_events] or [0])
        max_exp = max([_extract_id_num(eid) for eid in self.experiments.keys()] or [0])
        max_rel = max([_extract_id_num(rid) for rid in self.releases.keys()] or [0])
        max_trd = max([_extract_id_num(t.id) for t in self.trend_signals] or [0])
        max_game = max([_extract_id_num(gid) for gid in self.game_projects.keys()] or [0])
        _reset_generators(
            max_task + 1,
            max_event + 1,
            max_apr + 1,
            max_art + 1,
            max_mtg + 1,
            max_kpi + 1,
            max_exp + 1,
            max_rel + 1,
            max_trd + 1,
            max_game + 1,
        )

    # ---------- CRUD ----------
    def create_task(self, title: str, description: str, type: str, priority: str, assignee_id: Optional[str]) -> Task:
        task = Task(
            id=next(TASK_ID),
            title=title,
            description=description,
            type=type,
            priority=priority,
            status="Todo",
            assignee_id=assignee_id,
        )
        self.tasks[task.id] = task
        self.add_event(
            type="task.created",
            actor_id=assignee_id or "ceo",
            summary=f"{task.id} created: {task.title}",
            refs={"task_id": task.id},
            payload={"task": self.task_to_dict(task)},
        )
        return task

    def update_task(self, task_id: str, **changes: Any) -> Task:
        task = self.tasks[task_id]
        before = self.task_to_dict(task)
        for k, v in changes.items():
            if hasattr(task, k):
                setattr(task, k, v)
        task.updated_at = now_iso()
        after = self.task_to_dict(task)

        diff = {}
        for k in after.keys():
            if before.get(k) != after.get(k):
                diff[k] = [before.get(k), after.get(k)]

        self.add_event(
            type="task.updated",
            actor_id=changes.get("assignee_id") or task.assignee_id or "ops",
            summary=f"{task.id} updated",
            refs={"task_id": task.id},
            payload={"changes": diff},
        )
        return task

    def add_task_checklist_item(self, task_id: str, text: str, actor_id: str = "ops") -> Task:
        task = self.tasks[task_id]
        task.checklist.append({"text": text, "done": False, "created_at": now_iso()})
        task.updated_at = now_iso()
        self.add_event(
            type="task.checklist_item_added",
            actor_id=actor_id,
            summary=f"{task.id} checklist item added",
            refs={"task_id": task.id},
            payload={"text": text},
        )
        return task

    def toggle_task_checklist_item(self, task_id: str, index: int, actor_id: str = "ops") -> Task:
        task = self.tasks[task_id]
        item = task.checklist[index]
        item["done"] = not bool(item.get("done"))
        task.updated_at = now_iso()
        self.add_event(
            type="task.checklist_item_toggled",
            actor_id=actor_id,
            summary=f"{task.id} checklist item toggled",
            refs={"task_id": task.id},
            payload={"index": index, "done": item["done"]},
        )
        return task

    def add_task_comment(self, task_id: str, text: str, actor_id: str = "ops") -> Task:
        task = self.tasks[task_id]
        task.comments.append(
            {
                "id": f"CMT-{len(task.comments) + 1:03d}",
                "author_id": actor_id,
                "text": text,
                "created_at": now_iso(),
            }
        )
        task.updated_at = now_iso()
        self.add_event(
            type="task.comment_added",
            actor_id=actor_id,
            summary=f"{task.id} comment added",
            refs={"task_id": task.id},
            payload={"text": text},
        )
        return task

    def create_approval(self, kind: str, title: str, requested_by: str, payload: Optional[Dict[str, Any]] = None) -> Approval:
        apr = Approval(
            id=next(APPROVAL_ID),
            kind=kind,
            title=title,
            requested_by=requested_by,
            status="Pending",
            payload=payload or {},
        )
        self.approvals[apr.id] = apr
        self.add_event(
            type="approval.requested",
            actor_id=requested_by,
            summary=f"{apr.id} approval requested: {apr.title}",
            refs={"approval_id": apr.id},
            payload={"approval": self.approval_to_dict(apr)},
        )
        return apr

    def decide_approval(self, approval_id: str, decision: str, decision_by: str) -> Approval:
        apr = self.approvals[approval_id]
        apr.status = "Approved" if decision.lower() == "approve" else "Rejected"
        apr.decided_at = now_iso()
        apr.decision_by = decision_by
        self.add_event(
            type="approval.decided",
            actor_id=decision_by,
            summary=f"{apr.id} {apr.status.lower()}",
            refs={"approval_id": apr.id},
            payload={"approval": self.approval_to_dict(apr)},
        )
        if apr.kind == "release_gate":
            for rel in self.releases.values():
                if rel.approval_id == approval_id and rel.status == "PendingApproval":
                    rel.status = "Approved" if apr.status == "Approved" else "Rejected"
                    for g in self.game_projects.values():
                        if g.release_id == rel.id:
                            g.status = "ReadyToRelease" if rel.status == "Approved" else "Prototype"
                            g.updated_at = now_iso()
                            break
                    self.add_event(
                        type="release.approval_updated",
                        actor_id=decision_by,
                        summary=f"{rel.id} marked {rel.status}",
                        refs={"approval_id": apr.id},
                        payload={"release": self.release_to_dict(rel)},
                    )
                    break
        return apr

    def create_artifact(
        self,
        title: str,
        created_by: str,
        task_id: Optional[str],
        content: Optional[Dict[str, Any]] = None,
        artifact_id: Optional[str] = None,
    ) -> Artifact:
        if artifact_id and artifact_id in self.artifacts:
            art = self.artifacts[artifact_id]
        else:
            art = Artifact(
                id=artifact_id or next(ARTIFACT_ID),
                title=title,
                task_id=task_id,
                created_by=created_by,
                latest_version=0,
                versions=[],
            )
            self.artifacts[art.id] = art

        art.latest_version += 1
        art.updated_at = now_iso()
        payload = {
            "artifact_id": art.id,
            "title": title,
            "task_id": task_id,
            "created_by": created_by,
            "version": art.latest_version,
            "content": content or {},
            "created_at": art.updated_at,
        }
        file_path = self._artifact_repo.save_version(art.id, art.latest_version, payload)
        art.versions.append(
            {
                "version": art.latest_version,
                "file_path": file_path,
                "created_at": art.updated_at,
                "meta": {"title": title},
            }
        )
        self.add_event(
            type="artifact.created",
            actor_id=created_by,
            summary=f"{art.id} v{art.latest_version:03d} created: {title}",
            refs={"artifact_id": art.id, "task_id": task_id},
            payload={"artifact": self.artifact_to_dict(art), "saved_path": file_path},
            source="orchestrator",
        )
        return art

    def create_meeting(
        self,
        title: str,
        agenda: str,
        participant_ids: List[str],
        created_by: str = "ops",
        source: str = "ui",
    ) -> Meeting:
        m = Meeting(
            id=next(MEETING_ID),
            title=title,
            agenda=agenda,
            participant_ids=participant_ids,
            status="Scheduled",
        )
        self.meetings[m.id] = m
        self.add_event(
            type="meeting.created",
            actor_id=created_by,
            summary=f"{m.id} scheduled: {m.title}",
            refs={},
            payload={"meeting": self.meeting_to_dict(m)},
            source=source,
        )
        return m

    def start_meeting(self, meeting_id: str, actor_id: str = "ops", source: str = "ui") -> Meeting:
        m = self.meetings[meeting_id]
        if m.status == "Scheduled":
            m.status = "Ongoing"
            m.started_at = now_iso()
            self.add_event(
                type="meeting.started",
                actor_id=actor_id,
                summary=f"{m.id} started",
                refs={},
                payload={"meeting_id": m.id},
                source=source,
            )
        return m

    def add_meeting_note(
        self,
        meeting_id: str,
        note: str,
        author_id: str = "ops",
        decision: Optional[str] = None,
        action_item: Optional[Dict[str, Any]] = None,
        source: str = "ui",
    ) -> Meeting:
        m = self.meetings[meeting_id]
        m.notes.append({"author_id": author_id, "text": note, "created_at": now_iso()})
        if decision:
            m.decisions.append(decision)
        if action_item:
            action_item.setdefault("created_at", now_iso())
            m.action_items.append(action_item)
        self.add_event(
            type="meeting.note_added",
            actor_id=author_id,
            summary=f"{m.id} note added",
            refs={},
            payload={"meeting_id": m.id, "note": note},
            source=source,
        )
        return m

    def close_meeting(self, meeting_id: str, actor_id: str = "ops", source: str = "ui") -> Meeting:
        m = self.meetings[meeting_id]
        if m.status != "Done":
            m.status = "Done"
            m.ended_at = now_iso()
            created_task_ids: List[str] = []
            created_project_ids: List[str] = []
            for item in m.action_items:
                text = str(item.get("text", "")).strip()
                if not text:
                    continue
                if item.get("created_task_id"):
                    continue
                t = self.create_task(
                    title=f"[From {m.id}] {text}",
                    description=f"Auto-created from meeting action item in {m.title}",
                    type="OPS",
                    priority="P2",
                    assignee_id=None,
                )
                item["created_task_id"] = t.id
                created_task_ids.append(t.id)

            if m.decisions:
                existing_meeting_linked = {
                    mid
                    for g in self.game_projects.values()
                    for mid in g.meeting_ids
                }
                if m.id not in existing_meeting_linked:
                    top_trends = sorted(list(self.trend_signals)[:10], key=lambda x: x.score, reverse=True)
                    trend_ids = [t.id for t in top_trends[:3]]
                    genre, concept = self._plan_game_from_meeting(m, top_trends)
                    title = f"{genre} Sprint {len(self.game_projects) + 1}"
                    gp = self.create_game_project(
                        title=title,
                        genre=genre,
                        concept=concept,
                        trend_ids=trend_ids,
                        meeting_ids=[m.id],
                        created_by=actor_id,
                    )
                    created_project_ids.append(gp.id)
            # Persist structured outcome so meetings become executable governance artifacts.
            self.create_artifact(
                title=f"Meeting execution plan {m.id}",
                created_by=actor_id,
                task_id=created_task_ids[0] if created_task_ids else None,
                content={
                    "meeting_id": m.id,
                    "title": m.title,
                    "agenda": m.agenda,
                    "participants": list(m.participant_ids),
                    "decisions": list(m.decisions),
                    "action_items": list(m.action_items),
                    "notes_count": len(m.notes),
                    "created_task_ids": created_task_ids,
                    "created_project_ids": created_project_ids,
                },
            )
            self.add_event(
                type="meeting.closed",
                actor_id=actor_id,
                summary=f"{m.id} closed",
                refs={},
                payload={
                    "meeting_id": m.id,
                    "decisions": m.decisions,
                    "action_items": m.action_items,
                    "created_task_ids": created_task_ids,
                    "created_project_ids": created_project_ids,
                },
                source=source,
            )
        return m

    # ---------- KPI / Experiments ----------
    def add_kpi_event(
        self,
        event_type: str,
        user_id: str,
        value: float = 0.0,
        meta: Optional[Dict[str, Any]] = None,
        source: str = "api",
    ) -> KPIEvent:
        e = KPIEvent(
            id=next(KPI_EVENT_ID),
            ts=now_iso(),
            event_type=event_type,
            user_id=user_id,
            value=float(value),
            meta=meta or {},
        )
        self.kpi_events.appendleft(e)
        self.add_event(
            type="kpi.event_recorded",
            actor_id="ops",
            summary=f"KPI {e.event_type} recorded ({e.id})",
            refs={},
            payload={"kpi_event": self.kpi_event_to_dict(e)},
            source=source,
        )
        return e

    def kpi_summary(self, since_minutes: Optional[int] = None) -> Dict[str, Any]:
        rows = list(self.kpi_events)
        if since_minutes is not None:
            cutoff = datetime.now(KST) - timedelta(minutes=since_minutes)
            filtered: List[KPIEvent] = []
            for e in rows:
                try:
                    ts = datetime.fromisoformat(e.ts)
                except Exception:
                    continue
                if ts >= cutoff:
                    filtered.append(e)
            rows = filtered

        by_type: Dict[str, int] = {}
        revenue_total = 0.0
        for e in rows:
            by_type[e.event_type] = by_type.get(e.event_type, 0) + 1
            if e.event_type in {"revenue", "purchase", "iap_revenue"}:
                revenue_total += float(e.value)

        return {
            "event_count": len(rows),
            "by_type": by_type,
            "revenue_total": round(revenue_total, 2),
            "since_minutes": since_minutes,
        }

    def project_kpi_summary(self, project_id: str, since_minutes: Optional[int] = None) -> Dict[str, Any]:
        pid = str(project_id or "").strip()
        rows = list(self.kpi_events)
        if since_minutes is not None:
            cutoff = datetime.now(KST) - timedelta(minutes=since_minutes)
            filtered: List[KPIEvent] = []
            for e in rows:
                try:
                    ts = datetime.fromisoformat(e.ts)
                except Exception:
                    continue
                if ts >= cutoff:
                    filtered.append(e)
            rows = filtered
        rows = [e for e in rows if str((e.meta or {}).get("project_id", "")).strip() == pid]

        by_type: Dict[str, int] = {}
        revenue_total = 0.0
        for e in rows:
            by_type[e.event_type] = by_type.get(e.event_type, 0) + 1
            if e.event_type in {"revenue", "purchase", "iap_revenue"}:
                revenue_total += float(e.value)
        installs = int(by_type.get("acquisition.install", 0))
        sessions = int(by_type.get("engagement.session_start", 0))
        spi = (sessions / installs) if installs > 0 else 0.0

        return {
            "project_id": pid,
            "event_count": len(rows),
            "by_type": by_type,
            "revenue_total": round(revenue_total, 2),
            "installs": installs,
            "sessions": sessions,
            "session_per_install": round(spi, 2),
            "since_minutes": since_minutes,
        }

    def release_kpi_gate(self, since_minutes: int = 180, project_id: Optional[str] = None) -> Dict[str, Any]:
        rows = list(self.kpi_events)
        cutoff = datetime.now(KST) - timedelta(minutes=since_minutes)
        filtered: List[KPIEvent] = []
        for e in rows:
            try:
                ts = datetime.fromisoformat(e.ts)
            except Exception:
                continue
            if ts < cutoff:
                continue
            if project_id and str((e.meta or {}).get("project_id", "")).strip() != project_id:
                continue
            filtered.append(e)

        installs = 0
        sessions = 0
        revenue = 0.0
        for e in filtered:
            if e.event_type == "acquisition.install":
                installs += 1
            elif e.event_type == "engagement.session_start":
                sessions += 1
            elif e.event_type in {"revenue", "purchase", "iap_revenue"}:
                revenue += float(e.value)
        session_per_install = (sessions / installs) if installs > 0 else 0.0
        score = 0.0
        score += min(38.0, installs * 0.75)
        score += min(42.0, session_per_install * 21.0)
        score += min(20.0, revenue * 3.0)
        passed = (installs >= 8 and sessions >= 14 and revenue >= 2.0 and session_per_install >= 1.1)
        return {
            "passed": bool(passed),
            "score": round(min(100.0, score), 1),
            "since_minutes": since_minutes,
            "project_id": project_id or "",
            "installs": installs,
            "sessions": sessions,
            "revenue_total": round(revenue, 2),
            "session_per_install": round(session_per_install, 2),
        }

    def create_experiment(
        self,
        name: str,
        hypothesis: str,
        primary_metric: str,
        variants: List[str],
        project_id: str = "",
        created_by: str = "ops",
    ) -> Experiment:
        vs = [v.strip() for v in variants if v and v.strip()]
        if len(vs) < 2:
            raise ValueError("experiment requires at least 2 variants")
        exp = Experiment(
            id=next(EXPERIMENT_ID),
            name=name.strip(),
            hypothesis=hypothesis.strip(),
            primary_metric=primary_metric.strip(),
            variants=vs,
            project_id=str(project_id or "").strip(),
            exposures={v: 0 for v in vs},
            conversions={v: 0.0 for v in vs},
        )
        self.experiments[exp.id] = exp
        self.add_event(
            type="experiment.created",
            actor_id=created_by,
            summary=f"{exp.id} created: {exp.name}",
            refs={},
            payload={"experiment": self.experiment_to_dict(exp)},
            source="orchestrator",
        )
        return exp

    def record_experiment_exposure(self, experiment_id: str, variant: str, user_id: str = "sim") -> Experiment:
        exp = self.experiments[experiment_id]
        if exp.status != "Running":
            return exp
        if variant not in exp.exposures:
            raise ValueError(f"variant not found: {variant}")
        exp.exposures[variant] = int(exp.exposures.get(variant, 0)) + 1
        self.add_kpi_event(
            event_type=f"exp.exposure.{exp.id}.{variant}",
            user_id=user_id,
            value=1,
            meta={"experiment_id": exp.id, "variant": variant, "project_id": exp.project_id},
            source="orchestrator",
        )
        return exp

    def record_experiment_conversion(
        self,
        experiment_id: str,
        variant: str,
        value: float = 1.0,
        user_id: str = "sim",
    ) -> Experiment:
        exp = self.experiments[experiment_id]
        if exp.status != "Running":
            return exp
        if variant not in exp.conversions:
            raise ValueError(f"variant not found: {variant}")
        exp.conversions[variant] = float(exp.conversions.get(variant, 0.0)) + float(value)
        self.add_kpi_event(
            event_type=f"exp.conversion.{exp.id}.{variant}",
            user_id=user_id,
            value=float(value),
            meta={"experiment_id": exp.id, "variant": variant, "project_id": exp.project_id},
            source="orchestrator",
        )
        return exp

    def close_experiment(self, experiment_id: str, winner_variant: str, actor_id: str = "ceo") -> Experiment:
        exp = self.experiments[experiment_id]
        if winner_variant not in exp.variants:
            raise ValueError(f"winner variant not found: {winner_variant}")
        exp.status = "Completed"
        exp.ended_at = now_iso()
        exp.winner_variant = winner_variant
        # Experiment winner nudges variant policy for the linked project/mode.
        if exp.project_id and exp.project_id in self.game_projects:
            gp = self.game_projects[exp.project_id]
            bp = dict(gp.game_blueprint or {})
            mode = str(bp.get("mode_base") or bp.get("mode") or "").strip().lower()
            variant = str(bp.get("variant", "")).strip().lower()
            if mode and variant:
                key = f"{mode}:{variant}"
                sign = 1.0 if str(winner_variant) == "B" else -0.4
                lm = self.learning_memory or self._default_learning_memory()
                vb = dict(lm.get("variant_bias", {}) or {})
                prev = float(vb.get(key, 0.0))
                vb[key] = round(prev * 0.9 + sign * 0.1, 4)
                lm["variant_bias"] = vb
                lm["updated_at"] = now_iso()
                self.learning_memory = lm
        self.add_event(
            type="experiment.closed",
            actor_id=actor_id,
            summary=f"{exp.id} closed. winner={winner_variant}",
            refs={},
            payload={"experiment": self.experiment_to_dict(exp)},
            source="orchestrator",
        )
        return exp

    # ---------- Release workflow ----------
    def create_release_candidate(
        self,
        version: str,
        title: str,
        task_id: Optional[str],
        requested_by: str,
        notes: str = "",
    ) -> Release:
        rid = next(RELEASE_ID)
        apr = self.create_approval(
            kind="release_gate",
            title=f"Release approval {version}: {title}",
            requested_by=requested_by,
            payload={"release_id": rid, "version": version, "title": title},
        )
        rel = Release(
            id=rid,
            version=version.strip(),
            title=title.strip(),
            task_id=task_id,
            requested_by=requested_by,
            approval_id=apr.id,
            notes=notes.strip(),
        )
        self.releases[rel.id] = rel
        self.add_event(
            type="release.requested",
            actor_id=requested_by,
            summary=f"{rel.id} requested ({rel.version})",
            refs={"approval_id": rel.approval_id, "task_id": task_id},
            payload={"release": self.release_to_dict(rel)},
            source="orchestrator",
        )
        return rel

    def promote_release(self, release_id: str, actor_id: str = "ops") -> Release:
        rel = self.releases[release_id]
        apr = self.approvals.get(rel.approval_id)
        if not apr or apr.status != "Approved":
            raise ValueError(f"release {rel.id} requires approved gate")
        if not rel.final_confirmed:
            raise ValueError(f"release {rel.id} requires final confirmation")
        if rel.rollout_stage == "Full" and rel.status == "Deployed":
            return rel
        rel.status = "Deployed"
        rel.rollout_stage = "Full"
        rel.rollout_percent = 100
        rel.rollout_blocked = False
        rel.deployed_at = now_iso()
        artifact = self.create_artifact(
            title=f"Release manifest {rel.version}",
            created_by=actor_id,
            task_id=rel.task_id,
            content={"release": self.release_to_dict(rel)},
        )
        rel.artifact_id = artifact.id
        self.add_event(
            type="release.deployed",
            actor_id=actor_id,
            summary=f"{rel.id} deployed",
            refs={"approval_id": rel.approval_id, "artifact_id": rel.artifact_id, "task_id": rel.task_id},
            payload={"release": self.release_to_dict(rel)},
            source="orchestrator",
        )
        for g in self.game_projects.values():
            if g.release_id == rel.id:
                g.status = "Released"
                g.updated_at = now_iso()
                break
        return rel

    def start_release_rollout(self, release_id: str, actor_id: str = "ops") -> Release:
        rel = self.releases[release_id]
        apr = self.approvals.get(rel.approval_id)
        if not apr or apr.status != "Approved":
            raise ValueError(f"release {rel.id} requires approved gate")
        if not rel.final_confirmed:
            raise ValueError(f"release {rel.id} requires final confirmation")
        rel.status = "Deployed"
        rel.rollout_stage = "Canary"
        rel.rollout_percent = 10
        rel.rollout_blocked = False
        if not rel.deployed_at:
            rel.deployed_at = now_iso()
        self.add_event(
            type="release.rollout_started",
            actor_id=actor_id,
            summary=f"{rel.id} canary rollout started (10%)",
            refs={"approval_id": rel.approval_id},
            payload={"release": self.release_to_dict(rel)},
            source="orchestrator",
        )
        return rel

    def project_has_meeting_alignment(self, project_id: str) -> bool:
        if project_id not in self.game_projects:
            return False
        gp = self.game_projects[project_id]
        mids = [mid for mid in (gp.meeting_ids or []) if mid in self.meetings]
        # Auto-heal dangling meeting references from old snapshots.
        if len(mids) != len(gp.meeting_ids or []):
            gp.meeting_ids = mids
            gp.updated_at = now_iso()
        if not mids:
            return False
        # latest linked meeting must be closed with at least one decision and one action item
        mids.sort(key=lambda mid: self.meetings[mid].created_at, reverse=True)
        m = self.meetings[mids[0]]
        if m.status != "Done":
            return False
        if len(m.decisions) < 1 or len(m.action_items) < 1:
            return False
        return True

    def ensure_alignment_meeting_for_project(self, project_id: str, created_by: str = "ceo") -> Optional[Meeting]:
        if project_id not in self.game_projects:
            return None
        gp = self.game_projects[project_id]
        # already aligned
        if self.project_has_meeting_alignment(project_id):
            return None
        # avoid duplicates if active linked meeting already exists
        for mid in gp.meeting_ids:
            m = self.meetings.get(mid)
            if not m:
                continue
            if m.status in {"Scheduled", "Ongoing"}:
                return m
        topic = f"{gp.id} 업그레이드 전략 회의"
        agenda = f"{gp.title} 품질 향상/출시 기준 합의 (모드 {gp.game_blueprint.get('mode_base') if gp.game_blueprint else '-'})"
        participants = ["ceo", "mkt", "dev_a", "qa"]
        m = self.create_meeting(
            title=topic,
            agenda=agenda,
            participant_ids=participants,
            created_by=created_by,
            source="orchestrator",
        )
        gp.meeting_ids.append(m.id)
        gp.updated_at = now_iso()
        self.add_event(
            type="project.meeting_alignment_requested",
            actor_id=created_by,
            summary=f"{gp.id} requires meeting alignment before execution",
            refs={},
            payload={"project_id": gp.id, "meeting_id": m.id},
            source="orchestrator",
        )
        return m

    def advance_release_rollout(self, release_id: str, actor_id: str = "ops") -> Release:
        rel = self.releases[release_id]
        if rel.status != "Deployed":
            raise ValueError(f"release {rel.id} is not deployed")
        if rel.rollout_blocked:
            raise ValueError(f"release {rel.id} rollout is blocked")
        prev_stage = rel.rollout_stage
        if rel.rollout_stage in {"PreDeploy", ""}:
            rel.rollout_stage = "Canary"
            rel.rollout_percent = 10
        elif rel.rollout_stage == "Canary":
            rel.rollout_stage = "Stage50"
            rel.rollout_percent = 50
        elif rel.rollout_stage == "Stage50":
            rel.rollout_stage = "Full"
            rel.rollout_percent = 100
        else:
            return rel
        self.add_event(
            type="release.rollout_advanced",
            actor_id=actor_id,
            summary=f"{rel.id} rollout {prev_stage} -> {rel.rollout_stage}",
            refs={"approval_id": rel.approval_id},
            payload={"release": self.release_to_dict(rel)},
            source="orchestrator",
        )
        return rel

    def rollback_release(self, release_id: str, reason: str, actor_id: str = "ops") -> Release:
        rel = self.releases[release_id]
        rel.rollout_blocked = True
        rel.rollout_stage = "RolledBack"
        rel.rollout_percent = 0
        rel.rollback_reason = str(reason or "").strip() or "kpi gate failure"
        self.add_event(
            type="release.rolled_back",
            actor_id=actor_id,
            summary=f"{rel.id} rolled back",
            refs={"approval_id": rel.approval_id},
            payload={"release": self.release_to_dict(rel), "reason": rel.rollback_reason},
            source="orchestrator",
        )
        return rel

    def confirm_project_release(
        self,
        project_id: str,
        confirmer_id: str = "human_ceo",
        comment: str = "",
    ) -> Release:
        if project_id not in self.game_projects:
            raise ValueError(f"project not found: {project_id}")
        gp = self.game_projects[project_id]
        if not self.can_confirm_project_release(project_id):
            raise ValueError(f"project {gp.id} review checklist is incomplete")
        if not gp.release_id:
            rel = self.try_prepare_project_release(project_id, requested_by="qa")
            if rel is None:
                raise ValueError(f"project {gp.id} is not ready for release request")
        rel = self.releases[gp.release_id]
        apr = self.approvals.get(rel.approval_id)
        if not apr:
            raise ValueError(f"release gate not found for {rel.id}")
        if apr.status == "Pending":
            self.decide_approval(apr.id, "approve", decision_by=confirmer_id)
        elif apr.status != "Approved":
            raise ValueError(f"release {rel.id} gate status is {apr.status}")

        rel.final_confirmed = True
        rel.final_confirmed_at = now_iso()
        rel.final_confirmed_by = confirmer_id
        kpi_gate = self.release_kpi_gate(since_minutes=180)
        decision_artifact = self.create_artifact(
            title=f"Final release decision log {gp.id}",
            created_by=confirmer_id,
            task_id=gp.task_ids[0] if gp.task_ids else None,
            content={
                "project_id": gp.id,
                "release_id": rel.id,
                "release_version": rel.version,
                "submission_reason": gp.submission_reason,
                "quality_score": gp.quality_score,
                "review_checklist": dict(gp.review_checklist),
                "kpi_gate": kpi_gate,
                "final_confirmed_at": rel.final_confirmed_at,
                "final_confirmed_by": rel.final_confirmed_by,
                "human_comment": comment.strip(),
            },
        )
        self.add_event(
            type="release.final_confirmed",
            actor_id=confirmer_id,
            summary=f"{rel.id} final confirmation completed",
            refs={"approval_id": rel.approval_id, "artifact_id": decision_artifact.id},
            payload={
                "release": self.release_to_dict(rel),
                "project_id": gp.id,
                "human_comment": comment.strip(),
            },
            source="ui",
        )
        deployed = self.start_release_rollout(rel.id, actor_id=confirmer_id)
        gp.status = "Released"
        gp.updated_at = now_iso()
        self.learn_from_project_outcome(gp.id, reason="final_release")
        return deployed

    # ---------- Trend / Game pipeline ----------
    def _normalize_genre(self, raw: str) -> str:
        s = str(raw or "").strip().lower()
        if not s:
            return "Arcade"
        mapping = [
            (["arcade", "?꾩??대뱶"], "Arcade"),
            (["runner", "?щ꼫", "platform"], "Runner"),
            (["aim", "precision", "trainer", "?먯엫"], "Skill Trainer"),
            (["puzzle", "merge", "match", "?쇱쫹"], "Puzzle"),
            (["dodge", "survival", "?앹〈"], "Survival"),
            (["idle", "clicker", "諛⑹튂"], "Idle"),
            (["rhythm", "music", "由щ벉"], "Rhythm"),
            (["strategy", "sim", "simulation", "?꾨왂"], "Strategy"),
        ]
        for keys, out in mapping:
            if any(k in s for k in keys):
                return out
        if isinstance(raw, str) and raw.strip():
            return raw.strip()[:28]
        return "Arcade"

    def _plan_game_from_meeting(self, meeting: Meeting, top_trends: List[TrendSignal]) -> tuple[str, str]:
        decisions = [str(x).strip() for x in (meeting.decisions or []) if str(x).strip()]
        base_concept = decisions[0] if decisions else (meeting.agenda or meeting.title or "Trend-inspired prototype")
        clean_trends = [t for t in top_trends if str(t.topic).strip()]
        if clean_trends:
            sampled = clean_trends[: min(3, len(clean_trends))]
            trend_hint = " + ".join([str(t.topic).strip() for t in sampled])
            concept = f"{base_concept} | Trend mix: {trend_hint}"
            genre_candidates = [self._normalize_genre(t.genre) for t in sampled]
            genre_candidates = [g for g in genre_candidates if g]
            genre = random.choice(genre_candidates) if genre_candidates else "Arcade"
            return genre, concept
        return "Arcade", base_concept

    def add_trend_signal(
        self,
        topic: str,
        genre: str,
        score: float,
        source: str,
        summary: str,
    ) -> TrendSignal:
        trd = TrendSignal(
            id=next(TREND_ID),
            topic=topic.strip(),
            genre=self._normalize_genre(genre),
            score=max(0.0, min(1.0, float(score))),
            source=source.strip() or "internal",
            summary=summary.strip(),
        )
        self.trend_signals.appendleft(trd)
        self.add_event(
            type="trend.signal_added",
            actor_id="mkt",
            summary=f"{trd.id} trend scored {trd.score:.2f}: {trd.topic}",
            refs={},
            payload={"trend": self.trend_to_dict(trd)},
            source="orchestrator",
        )
        return trd

    def create_game_project(
        self,
        title: str,
        genre: str,
        concept: str,
        trend_ids: Optional[List[str]] = None,
        meeting_ids: Optional[List[str]] = None,
        created_by: str = "ceo",
    ) -> GameProject:
        gp = GameProject(
            id=next(GAME_PROJECT_ID),
            title=title.strip(),
            genre=self._normalize_genre(genre),
            concept=concept.strip(),
            trend_ids=[x for x in (trend_ids or []) if x],
            meeting_ids=[x for x in (meeting_ids or []) if x],
        )
        self.game_projects[gp.id] = gp
        self.add_event(
            type="game_project.created",
            actor_id=created_by,
            summary=f"{gp.id} created: {gp.title}",
            refs={},
            payload={"project": self.game_project_to_dict(gp)},
            source="orchestrator",
        )
        return gp

    def update_game_project_status(self, project_id: str, status: str, actor_id: str = "ops") -> GameProject:
        gp = self.game_projects[project_id]
        prev = gp.status
        gp.status = status
        gp.updated_at = now_iso()
        self.add_event(
            type="game_project.status_updated",
            actor_id=actor_id,
            summary=f"{gp.id} {prev} -> {gp.status}",
            refs={},
            payload={"project_id": gp.id, "status": [prev, gp.status]},
            source="orchestrator",
        )
        return gp

    def evaluate_project_quality(self, project_id: str) -> float:
        gp = self.game_projects[project_id]
        health = self.project_artifact_health(project_id)
        # simple deterministic quality rubric for autonomous filtering
        score = 0.0
        score += min(35.0, float(gp.demo_build_count) * 11.0)
        if gp.task_ids:
            done = [tid for tid in gp.task_ids if tid in self.tasks and self.tasks[tid].status == "Done"]
            score += min(30.0, (len(done) / max(1, len(gp.task_ids))) * 30.0)
        if gp.demo_url:
            score += 10.0
        if gp.game_blueprint and gp.game_blueprint.get("mode"):
            score += 10.0
        if health["test_build_reports"] > 0:
            score += 8.0
        if health["git_reports"] > 0:
            score += 7.0
        if self.can_confirm_project_release(project_id):
            score += 15.0
        gp.quality_score = round(min(100.0, score), 1)
        gp.updated_at = now_iso()
        return gp.quality_score

    def project_artifact_health(self, project_id: str) -> Dict[str, int]:
        gp = self.game_projects[project_id]
        task_ids = set(gp.task_ids or [])
        health = {
            "project_artifacts": 0,
            "demo_artifacts": 0,
            "test_build_reports": 0,
            "git_reports": 0,
        }
        if not task_ids:
            return health
        for art in self.artifacts.values():
            if art.task_id not in task_ids:
                continue
            title = str(art.title or "").lower()
            health["project_artifacts"] += 1
            if "playable demo for" in title:
                health["demo_artifacts"] += 1
            if "test/build report for" in title:
                health["test_build_reports"] += 1
            if "git ops report for" in title:
                health["git_reports"] += 1
        return health

    def completion_report(self) -> Dict[str, Any]:
        def _estimated_quality(p: GameProject) -> float:
            if float(p.quality_score or 0.0) > 0.0:
                return float(p.quality_score)
            score = 0.0
            score += min(35.0, float(p.demo_build_count or 0) * 10.0)
            if p.task_ids:
                done = [tid for tid in p.task_ids if tid in self.tasks and self.tasks[tid].status == "Done"]
                score += min(30.0, (len(done) / max(1, len(p.task_ids))) * 30.0)
            if p.demo_url:
                score += 10.0
            if p.game_blueprint and p.game_blueprint.get("mode"):
                score += 10.0
            if self.can_confirm_project_release(p.id):
                score += 15.0
            return min(100.0, score)

        role_coverage = len({a.role for a in self.agents.values()} & {"CEO", "MKT", "DEV", "QA", "OPS"}) / 5.0
        projects = list(self.game_projects.values())
        total_projects = len(projects)
        active_statuses = {"Prototype", "QA", "ReadyToRelease", "SubmittedForHuman", "Released"}
        active_projects = [p for p in projects if p.status in active_statuses]
        submitted = [p for p in projects if p.submitted_for_human]
        released = [p for p in projects if p.status == "Released"]
        release_ready = [p for p in projects if p.release_id and self.can_confirm_project_release(p.id)]
        avg_quality = (sum(_estimated_quality(p) for p in projects) / float(total_projects) if total_projects else 0.0)

        # Infra score: orchestration quality for an autonomous studio pipeline.
        infra_score = (
            role_coverage * 20.0
            + (min(1.0, len(self.approvals) / 25.0) * 20.0)
            + (min(1.0, len(self.meetings) / 18.0) * 15.0)
            + (min(1.0, len(self.experiments) / 10.0) * 10.0)
            + (min(1.0, len(self.releases) / 12.0) * 15.0)
            + ((len(active_projects) / max(1, total_projects)) * 20.0)
        )

        # Business-autonomy score: quality-filtered submissions and releases are weighted higher.
        business_score = (
            (len(submitted) / max(1, total_projects)) * 22.0
            + (len(released) / max(1, total_projects)) * 28.0
            + (len(release_ready) / max(1, total_projects)) * 14.0
            + (min(1.0, avg_quality / 85.0) * 20.0)
            + (min(1.0, len([e for e in self.kpi_events if e.event_type == "revenue"]) / 220.0) * 16.0)
        )

        return {
            "infra_percent": round(min(100.0, infra_score), 1),
            "business_percent": round(min(100.0, business_score), 1),
            "details": {
                "projects_total": total_projects,
                "projects_active": len(active_projects),
                "projects_submitted_for_human": len(submitted),
                "projects_released": len(released),
                "projects_release_ready": len(release_ready),
                "releases_total": len(self.releases),
                "approvals_total": len(self.approvals),
                "meetings_total": len(self.meetings),
                "experiments_total": len(self.experiments),
                "kpi_events_total": len(self.kpi_events),
                "average_quality": round(avg_quality, 1),
            },
        }

    def learning_status(self) -> Dict[str, Any]:
        lm = self.learning_memory or self._default_learning_memory()
        outcomes = list(lm.get("project_outcomes", []) or [])
        return {
            "updated_at": lm.get("updated_at"),
            "mode_bias": dict(lm.get("mode_bias", {}) or {}),
            "variant_bias_top": sorted(
                [(k, float(v)) for k, v in dict(lm.get("variant_bias", {}) or {}).items()],
                key=lambda x: x[1],
                reverse=True,
            )[:20],
            "outcomes_count": len(outcomes),
            "recent_outcomes": outcomes[-8:],
        }

    def learn_from_project_outcome(self, project_id: str, reason: str = "release") -> Optional[Dict[str, Any]]:
        if project_id not in self.game_projects:
            return None
        gp = self.game_projects[project_id]
        bp = dict(gp.game_blueprint or {})
        mode = str(bp.get("mode_base") or bp.get("mode") or "").strip().lower()
        variant = str(bp.get("variant", "")).strip().lower()
        if mode not in {"aim", "runner", "dodge", "clicker", "memory", "rhythm"}:
            return None

        quality = float(self.evaluate_project_quality(project_id))
        kpi = float(self.release_kpi_gate(since_minutes=180, project_id=project_id).get("score", 0.0))
        # Outcome scale: [-1, +1]
        outcome = ((quality / 100.0) * 0.55 + (kpi / 100.0) * 0.45) * 2.0 - 1.0

        lm = self.learning_memory or self._default_learning_memory()
        mode_bias = dict(lm.get("mode_bias", {}) or {})
        variant_bias = dict(lm.get("variant_bias", {}) or {})
        prev_mode = float(mode_bias.get(mode, 0.0))
        mode_bias[mode] = round(prev_mode * 0.86 + outcome * 0.14, 4)
        if variant:
            key = f"{mode}:{variant}"
            prev_v = float(variant_bias.get(key, 0.0))
            variant_bias[key] = round(prev_v * 0.82 + outcome * 0.18, 4)

        outcomes = list(lm.get("project_outcomes", []) or [])
        outcomes.append(
            {
                "project_id": project_id,
                "mode": mode,
                "variant": variant,
                "quality": round(quality, 1),
                "kpi": round(kpi, 1),
                "outcome": round(outcome, 4),
                "reason": str(reason or "").strip() or "runtime",
                "ts": now_iso(),
            }
        )
        lm["mode_bias"] = mode_bias
        lm["variant_bias"] = variant_bias
        lm["project_outcomes"] = outcomes[-300:]
        lm["updated_at"] = now_iso()
        self.learning_memory = lm

        self.add_event(
            type="learning.policy_updated",
            actor_id="ceo",
            summary=f"Learning updated from {project_id} ({reason})",
            refs={},
            payload={
                "project_id": project_id,
                "mode": mode,
                "variant": variant,
                "outcome": round(outcome, 4),
                "mode_bias": mode_bias.get(mode, 0.0),
                "variant_bias": variant_bias.get(f'{mode}:{variant}', 0.0) if variant else 0.0,
            },
            source="orchestrator",
        )
        return {
            "project_id": project_id,
            "mode": mode,
            "variant": variant,
            "outcome": round(outcome, 4),
        }

    def submit_project_for_human_approval(self, project_id: str, reason: str, actor_id: str = "ceo") -> GameProject:
        gp = self.game_projects[project_id]
        if gp.submitted_for_human:
            return gp
        gp.submitted_for_human = True
        gp.submitted_at = now_iso()
        gp.submission_reason = reason.strip()
        gp.status = "SubmittedForHuman"
        gp.updated_at = now_iso()
        self.add_event(
            type="game_project.submitted_for_human",
            actor_id=actor_id,
            summary=f"{gp.id} submitted for final human approval",
            refs={},
            payload={
                "project_id": gp.id,
                "quality_score": gp.quality_score,
                "reason": gp.submission_reason,
            },
            source="orchestrator",
        )
        return gp

    def update_project_review(
        self,
        project_id: str,
        *,
        checklist_updates: Optional[Dict[str, bool]] = None,
        notes: Optional[str] = None,
        reviewer_id: str = "human_ceo",
    ) -> GameProject:
        gp = self.game_projects[project_id]
        allowed = {"no_personal_data", "no_third_party_ip", "license_checked", "policy_checked"}
        if checklist_updates:
            for key, value in checklist_updates.items():
                if key in allowed:
                    gp.review_checklist[key] = bool(value)
        if notes is not None:
            gp.review_notes = notes.strip()
        gp.reviewed_by = reviewer_id
        gp.reviewed_at = now_iso()
        gp.updated_at = now_iso()
        self.add_event(
            type="game_project.review_updated",
            actor_id=reviewer_id,
            summary=f"{gp.id} review checklist updated",
            refs={},
            payload={"project_id": gp.id, "review_checklist": dict(gp.review_checklist)},
            source="ui",
        )
        return gp

    def can_confirm_project_release(self, project_id: str) -> bool:
        gp = self.game_projects[project_id]
        required = ["no_personal_data", "no_third_party_ip", "license_checked", "policy_checked"]
        return all(bool(gp.review_checklist.get(k)) for k in required)

    def ensure_project_tasks(self, project_id: str) -> List[str]:
        gp = self.game_projects[project_id]
        valid_task_ids = [tid for tid in (gp.task_ids or []) if tid in self.tasks]
        repaired = len(valid_task_ids) != len(gp.task_ids or [])
        gp.task_ids = list(valid_task_ids)

        existing_types = {self.tasks[tid].type for tid in gp.task_ids if tid in self.tasks}
        specs = [
            ("DEV", "P1", f"[{gp.id}] Build playable core loop"),
            ("QA", "P1", f"[{gp.id}] QA smoke for prototype"),
            ("MKT", "P2", f"[{gp.id}] Draft launch copy and hooks"),
        ]
        created = 0
        for ttype, pri, title in specs:
            if ttype in existing_types:
                continue
            task = self.create_task(
                title=title,
                description=f"Project {gp.title} ({gp.genre})",
                type=ttype,
                priority=pri,
                assignee_id=None,
            )
            gp.task_ids.append(task.id)
            created += 1
        if created == 0 and not repaired:
            return list(gp.task_ids)
        gp.updated_at = now_iso()
        self.add_event(
            type="game_project.tasks_generated",
            actor_id="ops",
            summary=f"{gp.id} project tasks ensured (+{created}, repaired={repaired})",
            refs={},
            payload={"project_id": gp.id, "task_ids": list(gp.task_ids), "created_count": created, "repaired": repaired},
            source="orchestrator",
        )
        return list(gp.task_ids)

    def try_prepare_project_release(self, project_id: str, requested_by: str = "qa") -> Optional[Release]:
        gp = self.game_projects[project_id]
        if not self.project_has_meeting_alignment(project_id):
            return None
        if gp.release_id:
            return self.releases.get(gp.release_id)
        if not gp.task_ids:
            return None
        done = [tid for tid in gp.task_ids if tid in self.tasks and self.tasks[tid].status == "Done"]
        if len(done) < max(2, len(gp.task_ids) - 1):
            return None
        version = f"1.0.{_extract_id_num(gp.id):02d}"
        rel = self.create_release_candidate(
            version=version,
            title=f"{gp.title} web launch",
            task_id=done[0],
            requested_by=requested_by,
            notes=f"Automated release for project {gp.id}",
        )
        gp.release_id = rel.id
        gp.release_version = rel.version
        gp.status = "ReadyToRelease"
        gp.updated_at = now_iso()
        self.add_event(
            type="game_project.release_requested",
            actor_id=requested_by,
            summary=f"{gp.id} requested release {rel.id}",
            refs={"approval_id": rel.approval_id},
            payload={"project": self.game_project_to_dict(gp), "release": self.release_to_dict(rel)},
            source="orchestrator",
        )
        return rel

    def register_mode_extension(
        self,
        name: str,
        base_mode: str,
        keywords: List[str],
        reason: str,
        source: str = "trend",
        created_by: str = "mkt",
    ) -> Optional[Dict[str, Any]]:
        base = str(base_mode or "").strip().lower()
        if base not in {"aim", "runner", "dodge", "clicker", "memory", "rhythm"}:
            return None
        clean_name = str(name or "").strip()
        if not clean_name:
            return None
        mode_id = clean_name.lower().replace(" ", "_").replace("-", "_")
        mode_id = "".join(ch for ch in mode_id if ch.isalnum() or ch == "_").strip("_")
        if not mode_id:
            return None
        if mode_id in {"aim", "runner", "dodge", "clicker", "memory", "rhythm"}:
            return None
        if any(str(x.get("mode_id", "")).lower() == mode_id for x in self.mode_extensions):
            return None
        keyset = sorted({str(k).strip().lower() for k in (keywords or []) if str(k).strip()})
        if not keyset:
            return None
        row = {
            "mode_id": mode_id,
            "name": clean_name,
            "base_mode": base,
            "keywords": keyset,
            "reason": str(reason or "").strip(),
            "source": str(source or "trend").strip() or "trend",
            "created_at": now_iso(),
        }
        self.mode_extensions.append(row)
        self.add_event(
            type="mode_extension.registered",
            actor_id=created_by,
            summary=f"New mode extension registered: {clean_name} -> {base}",
            refs={},
            payload={"mode_extension": row},
            source="orchestrator",
        )
        return row

    def refresh_mode_extensions_from_trends(self, max_new: int = 1) -> int:
        if max_new <= 0:
            return 0
        added = 0
        recent = sorted(list(self.trend_signals)[:24], key=lambda t: t.score, reverse=True)
        rules = [
            ("Survival Arena", "dodge", ["survival", "horde", "roguelike", "bullet", "생존", "로그라이크", "탄막"]),
            ("Speedrun Parkour", "runner", ["speedrun", "parkour", "runner", "파쿠르", "스피드런", "러너"]),
            ("Sniper Precision", "aim", ["sniper", "precision", "aim", "에임", "저격", "정밀"]),
            ("Idle Tycoon", "clicker", ["idle", "tycoon", "incremental", "방치", "타이쿤"]),
            ("Card Tactics", "memory", ["card", "deck", "match", "memory", "카드", "덱", "기억"]),
            ("Beat Rush", "rhythm", ["rhythm", "beat", "music", "리듬", "비트", "음악"]),
        ]
        for tr in recent:
            if added >= max_new:
                break
            text = f"{tr.topic} {tr.genre} {tr.summary}".lower()
            for name, base_mode, keys in rules:
                if added >= max_new:
                    break
                if not any(k in text for k in keys):
                    continue
                row = self.register_mode_extension(
                    name=name,
                    base_mode=base_mode,
                    keywords=keys + [tr.topic.lower(), tr.genre.lower()],
                    reason=f"trend={tr.id} score={tr.score:.2f}",
                    source=tr.source,
                    created_by="mkt",
                )
                if row is not None:
                    added += 1
        return added

    def _resolve_mode_info(self, mode: str) -> Dict[str, Any]:
        raw = str(mode or "").strip().lower()
        base_labels = {
            "aim": "AIM DRILL",
            "runner": "RUNNER",
            "dodge": "DODGE",
            "clicker": "CLICKER",
            "memory": "MEMORY MATCH",
            "rhythm": "RHYTHM TAP",
        }
        if raw in base_labels:
            return {"mode": raw, "mode_base": raw, "mode_label": base_labels[raw], "mode_extension": ""}
        for ext in self.mode_extensions:
            if str(ext.get("mode_id", "")).lower() == raw:
                base = str(ext.get("base_mode", "aim")).lower()
                name = str(ext.get("name", raw)).strip() or raw
                return {"mode": raw, "mode_base": base, "mode_label": name, "mode_extension": raw}
        return {"mode": "aim", "mode_base": "aim", "mode_label": base_labels["aim"], "mode_extension": ""}

    def _infer_demo_mode(self, gp: GameProject) -> str:
        text = f"{gp.genre} {gp.title} {gp.concept}".lower()
        for ext in self.mode_extensions:
            keys = [str(k).strip().lower() for k in (ext.get("keywords", []) or []) if str(k).strip()]
            if keys and any(k in text for k in keys):
                return str(ext.get("mode_id", "aim")).strip().lower() or "aim"
        if any(k in text for k in ["aim", "precision", "trainer", "에임"]):
            return "aim"
        if any(k in text for k in ["runner", "platform", "timing", "러너"]):
            return "runner"
        if any(k in text for k in ["merge", "puzzle", "block", "tile", "퍼즐"]):
            return "memory"
        if any(k in text for k in ["idle", "clicker", "incremental", "방치"]):
            return "clicker"
        if any(k in text for k in ["memory", "card", "match", "기억"]):
            return "memory"
        if any(k in text for k in ["rhythm", "beat", "music", "리듬"]):
            return "rhythm"
        n = _extract_id_num(gp.id)
        return ["aim", "runner", "dodge", "clicker", "memory", "rhythm"][n % 6]

    def _build_game_blueprint(self, gp: GameProject) -> Dict[str, Any]:
        mode = self._infer_demo_mode(gp)
        mode_info = self._resolve_mode_info(mode)
        n = max(1, _extract_id_num(gp.id))
        duration = 30 + (n % 3) * 5
        difficulty = 1 + (n % 4)
        lm = self.learning_memory or self._default_learning_memory()
        mode_bias = float(dict(lm.get("mode_bias", {}) or {}).get(str(mode_info["mode_base"]), 0.0))
        difficulty = max(1, min(5, difficulty + (1 if mode_bias > 0.35 else (-1 if mode_bias < -0.45 else 0))))
        tier = max(1, min(4, int(gp.demo_build_count or 0) + 1 + (1 if mode_bias > 0.55 else 0)))
        themes = [
            {"id": "neon", "bg1": "#08142a", "bg2": "#101f3f", "panel": "#12274a", "line": "#2f5fa8", "accent": "#7af0ff"},
            {"id": "sunset", "bg1": "#2a1222", "bg2": "#3e1c30", "panel": "#4a2140", "line": "#8a3e66", "accent": "#ffb36b"},
            {"id": "mint", "bg1": "#0e1f24", "bg2": "#15323a", "panel": "#1a3f48", "line": "#2c7f8f", "accent": "#7effd4"},
            {"id": "retro", "bg1": "#1b1731", "bg2": "#2f2750", "panel": "#3a3166", "line": "#6757a9", "accent": "#ffe57a"},
        ]
        asset_packs = [
            {"id": "pack_neon_ops", "character_style": "pilot", "sfx": "neon_arcade", "bgm": "pulse_120"},
            {"id": "pack_retro_pixel", "character_style": "pixel_runner", "sfx": "retro_chip", "bgm": "chip_8bit"},
            {"id": "pack_mint_clean", "character_style": "minimal_orb", "sfx": "clean_soft", "bgm": "ambient_100"},
            {"id": "pack_sunset_pop", "character_style": "hero_glow", "sfx": "pop_bright", "bgm": "groove_110"},
        ]
        base_mode = mode_info["mode_base"]
        variants = {
            "aim": ["focus_shot", "multi_target"],
            "runner": ["classic_dash", "orb_hunt"],
            "dodge": ["rain_field", "spiral_wave"],
            "clicker": ["precision_click", "chain_click"],
            "memory": ["flash_match", "sequence_match"],
            "rhythm": ["lane_tap", "burst_tap"],
        }.get(base_mode, ["default"])
        variant_bias = dict(lm.get("variant_bias", {}) or {})
        weighted_variants: List[str] = []
        weighted_scores: List[float] = []
        for v in variants:
            key = f"{base_mode}:{v}"
            weighted_variants.append(v)
            weighted_scores.append(max(0.2, 1.0 + float(variant_bias.get(key, 0.0))))
        pick = n + int(gp.demo_build_count or 0) + len(gp.title or "") + len(gp.concept or "")
        theme = themes[pick % len(themes)]
        asset_pack = asset_packs[pick % len(asset_packs)]
        variant = random.choices(weighted_variants, weights=weighted_scores, k=1)[0]
        return {
            "mode": mode_info["mode"],
            "mode_base": mode_info["mode_base"],
            "mode_label": mode_info["mode_label"],
            "mode_extension": mode_info["mode_extension"],
            "variant": variant,
            "tier": tier,
            "theme": theme,
            "asset_pack": asset_pack,
            "duration_sec": duration,
            "difficulty": difficulty,
            "title": gp.title,
            "genre": gp.genre,
            "concept": gp.concept,
        }

    def _render_game_html(self, gp: GameProject, blueprint: Dict[str, Any]) -> str:
        title = gp.title or f"{gp.genre} Prototype"
        mode_name = str(blueprint.get("mode_label") or "ARCADE")
        base_mode = str(blueprint.get("mode_base") or blueprint.get("mode") or "aim").lower()
        asset_pack = dict(blueprint.get("asset_pack", {}) or {})
        asset_id = str(asset_pack.get("id", "pack_default"))
        sfx_name = str(asset_pack.get("sfx", "default"))
        bgm_name = str(asset_pack.get("bgm", "default"))
        control_hint = {
            "aim": "Click targets. Keep combo alive.",
            "runner": "Space/Up to jump. Avoid blocks and collect orbs.",
            "dodge": "Arrow keys move, Shift dash, survive waves and collect power-ups.",
            "clicker": "Fast clicks build combo multiplier.",
            "memory": "Memorize flashes and type matching keys.",
            "rhythm": "Hit lane keys on beat timing.",
        }.get(base_mode, "Play with keyboard/mouse.")
        objective = {
            "aim": "High precision score race",
            "runner": "Endless lane survival run",
            "dodge": "Arena survival with escalating waves",
            "clicker": "Combo-driven score burst",
            "memory": "Pattern recall challenge",
            "rhythm": "Beat sync timing challenge",
        }.get(base_mode, "Arcade challenge")
        theme = blueprint.get("theme", {}) or {}
        bg1 = str(theme.get("bg1", "#0e1320"))
        bg2 = str(theme.get("bg2", "#17233e"))
        panel = str(theme.get("panel", "#151c2f"))
        line = str(theme.get("line", "#2a385b"))
        accent = str(theme.get("accent", "#7aa2ff"))
        return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>{title}</title>
  <style>
    body {{
      margin: 0;
      background: linear-gradient(135deg, {bg1}, {bg2});
      color: #e8ecf2;
      font-family: 'Segoe UI', Tahoma, sans-serif;
      display: grid;
      place-items: center;
      min-height: 100vh;
    }}
    .wrap {{
      width: min(92vw, 860px);
      background: {panel};
      border: 1px solid {line};
      border-radius: 14px;
      padding: 14px;
      box-shadow: 0 10px 30px rgba(0, 0, 0, 0.28);
    }}
    .top {{
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 8px;
      font-size: 14px;
      gap: 10px;
    }}
    canvas {{
      width: 100%;
      height: auto;
      background: radial-gradient(circle at 20% 20%, #1f2a46, #0f1526);
      border-radius: 10px;
      border: 1px solid {line};
    }}
    .hint {{ color: #c6d6f4; font-size: 12px; margin-top: 8px; }}
    .sub {{
      margin: 8px 0 10px;
      display: flex;
      justify-content: space-between;
      gap: 10px;
      color: #d6e5ff;
      font-size: 12px;
    }}
    .sub2 {{
      margin: 0 0 10px;
      display: flex;
      justify-content: space-between;
      gap: 10px;
      color: #9fc2ff;
      font-size: 11px;
    }}
    .audio-btn {{
      border: 1px solid {line};
      background: rgba(10,18,34,0.5);
      color: #d8e7ff;
      border-radius: 8px;
      padding: 4px 8px;
      cursor: pointer;
      font-size: 11px;
    }}
    .tag {{
      display: inline-block;
      margin-left: 8px;
      padding: 2px 8px;
      border-radius: 999px;
      border: 1px solid {line};
      color: {accent};
      font-size: 11px;
      font-weight: 700;
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="top">
      <div><b>{title}</b> ({gp.id}) <span class="tag">Tier {blueprint.get("tier", 1)} / {str(blueprint.get("variant", "default"))}</span></div>
      <div>Mode <b>{mode_name}</b> | Score <span id="score">0</span> | Time <span id="time">{blueprint["duration_sec"]}</span>s</div>
    </div>
    <div class="sub">
      <div>Objective: <b>{objective}</b></div>
      <div>Controls: {control_hint}</div>
    </div>
    <div class="sub2">
      <div>Asset Pack: <b>{asset_id}</b> · SFX: {sfx_name}</div>
      <div>BGM: {bgm_name} <button id="audioToggle" class="audio-btn">Audio ON/OFF</button></div>
    </div>
    <canvas id="game" width="820" height="440"></canvas>
    <div class="hint">{gp.concept or "Auto-generated playable prototype by AI studio."}</div>
  </div>
  <script>
  (() => {{
    const state = {{
      audioCtx: null,
      audioEnabled: false,
      bgmTimer: null,
      hud: {{ wave: 1, hp: 3, combo: 0, note: "" }},
      particles: [],
    }};

    function ensureCtx() {{
      if (!state.audioCtx) {{
        const C = window.AudioContext || window.webkitAudioContext;
        if (!C) return null;
        state.audioCtx = new C();
      }}
      return state.audioCtx;
    }}

    function tone(freq=440, len=0.05, gain=0.03, type='sine') {{
      const a = ensureCtx();
      if (!a || !state.audioEnabled) return;
      const o = a.createOscillator();
      const g = a.createGain();
      o.type = type;
      o.frequency.value = freq;
      g.gain.value = gain;
      o.connect(g);
      g.connect(a.destination);
      o.start();
      o.stop(a.currentTime + len);
    }}

    function startBgm() {{
      if (state.bgmTimer) return;
      const seq = [262, 330, 392, 523, 659];
      let i = 0;
      state.bgmTimer = setInterval(() => {{
        if (!state.audioEnabled) return;
        const f = seq[i % seq.length];
        tone(f, 0.045, 0.018, 'triangle');
        if (i % 3 === 0) tone(f / 2, 0.035, 0.012, 'sine');
        i++;
      }}, 260);
    }}

    function stopBgm() {{
      if (state.bgmTimer) clearInterval(state.bgmTimer);
      state.bgmTimer = null;
    }}

    function addParticle(x, y, color='#7af0ff', power=1) {{
      for (let i = 0; i < Math.max(4, Math.floor(10 * power)); i++) {{
        state.particles.push({{
          x, y,
          vx: (-1 + Math.random() * 2) * (1.2 + Math.random() * 1.8) * power,
          vy: (-1 + Math.random() * 2) * (1.2 + Math.random() * 1.8) * power,
          life: 22 + Math.floor(Math.random() * 18),
          color,
        }});
      }}
      if (state.particles.length > 220) {{
        state.particles.splice(0, state.particles.length - 220);
      }}
    }}

    function drawOverlay(ctx, w, h) {{
      ctx.save();
      if (state.particles.length) {{
        for (let i = state.particles.length - 1; i >= 0; i--) {{
          const p = state.particles[i];
          p.x += p.vx;
          p.y += p.vy;
          p.vy += 0.01;
          p.life -= 1;
          if (p.life <= 0) {{
            state.particles.splice(i, 1);
            continue;
          }}
          ctx.globalAlpha = Math.max(0, p.life / 40);
          ctx.fillStyle = p.color;
          ctx.fillRect(p.x, p.y, 2, 2);
        }}
      }}

      ctx.globalAlpha = 1;
      ctx.fillStyle = 'rgba(8,14,26,0.42)';
      ctx.fillRect(8, 8, 260, 58);
      ctx.strokeStyle = 'rgba(130,170,255,0.55)';
      ctx.strokeRect(8, 8, 260, 58);
      ctx.fillStyle = '#dbe8ff';
      ctx.font = '13px Segoe UI';
      ctx.fillText('Wave ' + (state.hud.wave || 1), 18, 28);
      ctx.fillText('HP ' + (state.hud.hp || 0), 92, 28);
      ctx.fillText('Combo ' + (state.hud.combo || 0), 156, 28);
      if (state.hud.note) {{
        ctx.fillStyle = '#9ad3ff';
        ctx.fillText(String(state.hud.note).slice(0, 30), 18, 49);
      }}
      ctx.restore();
    }}

    window.__studioEngine = {{
      audio: {{
        toggle() {{
          state.audioEnabled = !state.audioEnabled;
          if (state.audioEnabled) startBgm();
          else stopBgm();
          return state.audioEnabled;
        }},
        hit() {{ tone(760, 0.03, 0.028, 'square'); }},
        alert() {{ tone(190, 0.08, 0.03, 'sawtooth'); }},
        success() {{ tone(520, 0.04, 0.028, 'triangle'); tone(700, 0.03, 0.02, 'sine'); }},
      }},
      hud: {{
        set(next) {{
          state.hud = Object.assign({{}}, state.hud, next || {{}});
        }},
      }},
      vfx: {{
        burst(x, y, color='#7af0ff', power=1) {{
          addParticle(x, y, color, power);
        }},
      }},
      drawOverlay,
    }};

    const btn = document.getElementById('audioToggle');
    if (btn) btn.addEventListener('click', () => {{
      const on = window.__studioEngine.audio.toggle();
      btn.textContent = on ? 'Audio ON' : 'Audio OFF';
    }});
  }})();
  </script>
  <script src="./game.js"></script>
</body>
</html>
"""

    def _render_game_js(self, blueprint: Dict[str, Any]) -> str:
        mode = str(blueprint.get("mode_base") or blueprint["mode"]).lower()
        duration = int(blueprint["duration_sec"])
        difficulty = int(blueprint["difficulty"])
        tier = int(blueprint.get("tier", 1))
        variant = str(blueprint.get("variant", "default")).lower()
        if mode == "clicker":
            return f"""const cvs=document.getElementById('game');const ctx=cvs.getContext('2d');
const scoreEl=document.getElementById('score');const timeEl=document.getElementById('time');
let score=0,t={duration},running=true,multi=1,target={{x:410,y:220,r:44,vx:0,vy:0}},pulse=0,combo=0;
function spawn(){{target.r=24+Math.random()*30;target.x=target.r+Math.random()*(cvs.width-target.r*2);target.y=target.r+Math.random()*(cvs.height-target.r*2);
target.vx=({tier}>=3?(-1+Math.random()*2)*(1+Math.random()*1.6):0);target.vy=({tier}>=3?(-1+Math.random()*2)*(1+Math.random()*1.6):0);}}
function draw(){{ctx.clearRect(0,0,cvs.width,cvs.height);ctx.fillStyle='#101a31';ctx.fillRect(0,0,cvs.width,cvs.height);
ctx.beginPath();ctx.arc(target.x,target.y,target.r+pulse,0,Math.PI*2);ctx.fillStyle='#ff7b7b';ctx.fill();
ctx.beginPath();ctx.arc(target.x,target.y,target.r*0.58,0,Math.PI*2);ctx.fillStyle='#ffe08a';ctx.fill();
ctx.fillStyle='#cde3ff';ctx.font='16px Segoe UI';ctx.fillText('x'+multi.toFixed(1)+' combo '+combo,14,26);pulse=Math.max(0,pulse-0.4);}}
cvs.addEventListener('click',e=>{{if(!running)return;const r=cvs.getBoundingClientRect();const x=(e.clientX-r.left)*(cvs.width/r.width);const y=(e.clientY-r.top)*(cvs.height/r.height);
const hit=Math.hypot(x-target.x,y-target.y)<=target.r;if(hit){{combo+=1;score+=Math.round((55-target.r)*multi)+(combo*{max(1, tier-1)});multi=Math.min(5,multi+0.08*{difficulty});pulse=6;spawn();}}
else{{combo=0;multi=Math.max(1,multi-0.15);}}scoreEl.textContent=String(score);}});
spawn();(function loop(){{if({tier}>=3){{target.x+=target.vx;target.y+=target.vy;if(target.x<target.r||target.x>cvs.width-target.r)target.vx*=-1;if(target.y<target.r||target.y>cvs.height-target.r)target.vy*=-1;}}
draw();if(running)requestAnimationFrame(loop);}})();
const timer=setInterval(()=>{{t-=1;timeEl.textContent=String(Math.max(0,t));if(t<=0){{running=false;clearInterval(timer);
ctx.fillStyle='rgba(8,12,24,0.74)';ctx.fillRect(0,0,cvs.width,cvs.height);ctx.fillStyle='#fff';ctx.font='bold 36px Segoe UI';
ctx.fillText('Clicker End',cvs.width/2-105,cvs.height/2-8);ctx.font='22px Segoe UI';ctx.fillText('Score: '+score,cvs.width/2-48,cvs.height/2+28);}}}},1000);"""
        if mode == "memory":
            return f"""const cvs=document.getElementById('game');const ctx=cvs.getContext('2d');
const scoreEl=document.getElementById('score');const timeEl=document.getElementById('time');
let running=true,t={duration},score=0,shown=-1;const cardN=({tier}>=3?6:4);const seq=[...Array(cardN).keys()].sort(()=>Math.random()-0.5);
function draw(){{ctx.clearRect(0,0,cvs.width,cvs.height);ctx.fillStyle='#111b31';ctx.fillRect(0,0,cvs.width,cvs.height);
for(let i=0;i<cardN;i++){{const x=80+i*115,y=170,w=96,h=90;ctx.fillStyle=(shown===i)?'#ffd166':'#344e7b';ctx.fillRect(x,y,w,h);ctx.strokeStyle='#8ea6c9';ctx.strokeRect(x,y,w,h);ctx.fillStyle='#cde3ff';ctx.fillText(String(i+1),x+42,y+54);}}
ctx.fillStyle='#cde3ff';ctx.font='18px Segoe UI';ctx.fillText('순서 카드 입력 1~'+cardN,280,90);}}
window.addEventListener('keydown',e=>{{if(!running)return;const n=parseInt(e.key,10)-1;if(Number.isNaN(n)||n<0||n>=cardN)return;
if(n===seq[0]){{score+=10;seq.push(seq.shift());}}else{{score=Math.max(0,score-4);}}scoreEl.textContent=String(score);}});
(function loop(){{draw();if(running)requestAnimationFrame(loop);}})();
const flash=setInterval(()=>{{shown=Math.floor(Math.random()*cardN);setTimeout(()=>{{shown=-1;}},420);}},(variant==='sequence_match'?1050:1400));
const timer=setInterval(()=>{{t-=1;timeEl.textContent=String(Math.max(0,t));if(t<=0){{running=false;clearInterval(timer);clearInterval(flash);
ctx.fillStyle='rgba(8,12,24,0.74)';ctx.fillRect(0,0,cvs.width,cvs.height);ctx.fillStyle='#fff';ctx.font='bold 34px Segoe UI';
ctx.fillText('Memory End',cvs.width/2-98,cvs.height/2-8);ctx.font='22px Segoe UI';ctx.fillText('Score: '+score,cvs.width/2-48,cvs.height/2+28);}}}},1000);"""
        if mode == "rhythm":
            return f"""const cvs=document.getElementById('game');const ctx=cvs.getContext('2d');
const scoreEl=document.getElementById('score');const timeEl=document.getElementById('time');
let running=true,t={duration},score=0;const laneX=[160,290,420,550,680],use5=({tier}>=3&&'{variant}'==='burst_tap'),notes=[];let beat=0;
function spawn(){{const lane=Math.floor(Math.random()*4);notes.push({{lane,y:-20,speed:3+Math.random()*{1+difficulty*0.4}}});}}
function draw(){{ctx.clearRect(0,0,cvs.width,cvs.height);ctx.fillStyle='#0f1730';ctx.fillRect(0,0,cvs.width,cvs.height);
const lanes=(use5?laneX:laneX.slice(0,4));for(const x of lanes){{ctx.fillStyle='#213354';ctx.fillRect(x-28,30,56,340);}}ctx.fillStyle='#7aa2ff';ctx.fillRect(120,360,580,10);
for(const n of notes){{ctx.beginPath();ctx.arc(laneX[n.lane],n.y,14,0,Math.PI*2);ctx.fillStyle='#ff9f7a';ctx.fill();}}}}
window.addEventListener('keydown',e=>{{if(!running)return;const map=(use5?{{'a':0,'s':1,'d':2,'f':3,'g':4}}:{{'a':0,'s':1,'d':2,'f':3}});const lane=map[(e.key||'').toLowerCase()];if(lane===undefined)return;
const cand=notes.filter(n=>n.lane===lane);if(!cand.length)return;const n=cand.reduce((p,c)=>Math.abs(c.y-360)<Math.abs(p.y-360)?c:p);
const dist=Math.abs(n.y-360);if(dist<26){{score+=Math.max(3,20-dist);notes.splice(notes.indexOf(n),1);}}else score=Math.max(0,score-4);scoreEl.textContent=String(score);}});
function step(){{if(!running)return;beat++;if(beat%(use5?14:18)===0){{const laneMax=(use5?5:4);const lane=Math.floor(Math.random()*laneMax);notes.push({{lane,y:-20,speed:3+Math.random()*{1+difficulty*0.45}}});}}
for(const n of notes)n.y+=n.speed;for(const n of notes){{if(n.y>390)score=Math.max(0,score-2);}}
for(let i=notes.length-1;i>=0;i--)if(notes[i].y>430)notes.splice(i,1);scoreEl.textContent=String(score);draw();requestAnimationFrame(step);}}
step();const timer=setInterval(()=>{{t-=1;timeEl.textContent=String(Math.max(0,t));if(t<=0){{running=false;clearInterval(timer);
ctx.fillStyle='rgba(8,12,24,0.74)';ctx.fillRect(0,0,cvs.width,cvs.height);ctx.fillStyle='#fff';ctx.font='bold 34px Segoe UI';
ctx.fillText('Rhythm End',cvs.width/2-92,cvs.height/2-8);ctx.font='22px Segoe UI';ctx.fillText('Score: '+score,cvs.width/2-48,cvs.height/2+28);}}}},1000);"""
        if mode == "runner":
            return f"""const cvs=document.getElementById('game');const ctx=cvs.getContext('2d');
const scoreEl=document.getElementById('score');const timeEl=document.getElementById('time');
const E=window.__studioEngine||{{audio:{{hit(){{}},alert(){{}},success(){{}}}},hud:{{set(){{}}}},vfx:{{burst(){{}}}},drawOverlay(){{}}}};
const metaKey='studio_meta_runner_v2';
const meta=(()=>{{try{{return JSON.parse(localStorage.getItem(metaKey)||'{{}}')}}catch(_e){{return {{}};}}}})();
const metaLvl=Math.max(0,Number(meta.level||0));const speedBonus=Math.min(2.5,metaLvl*0.12);
let running=true,t={duration},score=0,vy=0,y=332,onGround=true,speed={3 + difficulty}+speedBonus,tick=0,combo=0,bestCombo=0,stage=1;
let stamina=100,dashCd=0;const px=92,pw=30,ph=46;let boss=null;
let obs=[],orbs=[],bgStars=Array.from({{length:44}},()=>({{x:Math.random()*cvs.width,y:Math.random()*cvs.height,s:1+Math.random()*2}}));
function spawnObstacle(mult=1){{const tall=Math.random()<0.2;obs.push({{x:cvs.width+30,w:30+Math.random()*40,h:(tall?95:36)+Math.random()*72*mult,fly:tall?0:(Math.random()<0.24?1:0)}});}}
function spawnOrb(){{orbs.push({{x:cvs.width+40,y:170+Math.random()*180,r:9+Math.random()*5}});}}
function ensureBoss(elapsed){{if(elapsed<{duration}*0.65||boss)return;boss={{x:cvs.width+40,y:290,w:74,h:98,hp:5+Math.floor({difficulty}/2),phase:0,tick:0}};}}
function drawParallax(){{ctx.fillStyle='#0f1a34';ctx.fillRect(0,0,cvs.width,cvs.height);for(const s of bgStars){{s.x-=s.s*0.36;s.y+=Math.sin((tick+s.x)*0.002)*0.12;if(s.x<0)s.x=cvs.width;ctx.fillStyle='rgba(170,210,255,0.33)';ctx.fillRect(s.x,s.y,s.s,s.s);}}
ctx.fillStyle='#1c2f53';ctx.fillRect(0,330,cvs.width,110);ctx.fillStyle='#223a66';ctx.fillRect(0,378,cvs.width,62);}}
function draw(){{ctx.clearRect(0,0,cvs.width,cvs.height);drawParallax();
ctx.fillStyle='#7dd3ff';ctx.fillRect(px,y,pw,ph);ctx.fillStyle='#9af0c2';ctx.fillRect(px+6,y+10,18,12);
ctx.fillStyle='#ff7f8e';for(const o of obs){{const top=o.fly?260-o.h:380-o.h;ctx.fillRect(o.x,top,o.w,o.h);}}
ctx.fillStyle='#ffd56c';for(const o of orbs){{ctx.beginPath();ctx.arc(o.x,o.y,o.r,0,Math.PI*2);ctx.fill();}}
if(boss){{ctx.fillStyle='#ff4d7b';ctx.fillRect(boss.x,boss.y-boss.h,boss.w,boss.h);ctx.fillStyle='#fff';ctx.fillRect(boss.x+16,boss.y-boss.h+22,8,8);ctx.fillRect(boss.x+48,boss.y-boss.h+22,8,8);
ctx.fillStyle='rgba(8,14,26,0.54)';ctx.fillRect(590,16,210,12);ctx.fillStyle='#ff7f9f';ctx.fillRect(590,16,210*Math.max(0,boss.hp)/(5+Math.floor({difficulty}/2)),12);ctx.strokeStyle='rgba(255,180,200,0.8)';ctx.strokeRect(590,16,210,12);}}
ctx.fillStyle='rgba(8,14,26,0.5)';ctx.fillRect(12,398,220,20);ctx.fillStyle='#6be0ff';ctx.fillRect(12,398,Math.max(0,Math.min(220,stamina*2.2)),20);ctx.strokeStyle='rgba(130,170,255,0.75)';ctx.strokeRect(12,398,220,20);
E.drawOverlay(ctx,cvs.width,cvs.height);}}
function step(){{if(!running)return;tick++;const elapsed={duration}-t;stage=Math.min(3,1+Math.floor(elapsed/Math.max(1,{duration}/3)));
vy+=0.88;y+=vy;if(y>=332){{y=332;vy=0;onGround=true;}}if(dashCd>0)dashCd--;stamina=Math.min(100,stamina+0.14);
let s=speed+stage*0.35+(dashCd>0?2.5:0);for(const o of obs)o.x-=s;for(const o of orbs)o.x-=s+1.2;
if(Math.random()<(0.022+stage*0.01)*{difficulty})spawnObstacle(1+stage*0.12);if(Math.random()<(('{variant}'==='orb_hunt')?0.03:0.015))spawnOrb();
obs=obs.filter(o=>o.x+o.w>-20);orbs=orbs.filter(o=>o.x+o.r>-20);
for(let i=orbs.length-1;i>=0;i--){{if(Math.abs(orbs[i].x-(px+pw/2))<20&&Math.abs(orbs[i].y-(y+20))<24){{score+=12+Math.floor(combo*0.4);combo++;bestCombo=Math.max(bestCombo,combo);E.audio.success();E.vfx.burst(orbs[i].x,orbs[i].y,'#ffe07f',1.2);orbs.splice(i,1);}}}}
for(const o of obs){{const top=o.fly?260-o.h:380-o.h;if(px<o.x+o.w&&px+pw>o.x&&y+ph>top&&y<top+o.h){{if(dashCd>0){{score+=4;E.vfx.burst(px+15,y+22,'#7af0ff',1.1);}}else{{running=false;E.audio.alert();}}}}
if(o.x+o.w<px&&o.x+o.w>px-8){{combo++;bestCombo=Math.max(bestCombo,combo);score+=3;}}}}
ensureBoss(elapsed);
if(boss){{boss.tick++;boss.x=Math.max(520,boss.x-1.3-stage*0.2);if(boss.tick%120===0)boss.phase=(boss.phase+1)%2;if(boss.phase===0&&boss.tick%32===0)spawnObstacle(1.25);if(boss.phase===1&&boss.tick%24===0)spawnOrb();
const hitBoss=(dashCd>0&&px+pw>boss.x&&px<boss.x+boss.w&&y+ph>boss.y-boss.h&&y<boss.y);if(hitBoss){{boss.hp--;score+=22;E.audio.success();E.vfx.burst(boss.x+boss.w*0.5,boss.y-boss.h*0.5,'#ff7f9f',1.6);dashCd=0;}}
if(boss.hp<=0){{score+=160;boss=null;E.audio.success();for(let n=0;n<4;n++)E.vfx.burst(650+Math.random()*80,220+Math.random()*80,'#ffd56c',1.8);}}}}
if(running)score+=1+Math.floor(combo/6)+stage;else combo=0;if(tick%45===0&&combo>0)combo--;scoreEl.textContent=String(score);
E.hud.set({{wave:stage,hp:running?1:0,combo:bestCombo,note:boss?('BOSS HP '+boss.hp):('DashCD '+Math.max(0,Math.ceil(dashCd/6)))}});draw();requestAnimationFrame(step);}}
window.addEventListener('keydown',e=>{{if((e.key===' '||e.key==='ArrowUp')&&onGround){{vy=-14.8;onGround=false;E.audio.hit();}}
if((e.key||'').toLowerCase()==='shift'&&dashCd<=0&&stamina>=26){{dashCd=30;stamina-=26;E.audio.success();}}}});
cvs.addEventListener('click',()=>{{if(onGround){{vy=-14.8;onGround=false;E.audio.hit();}}}});
step();const timer=setInterval(()=>{{t-=1;timeEl.textContent=String(Math.max(0,t));if(t<=0||!running){{running=false;clearInterval(timer);
const prevBest=Math.max(0,Number(meta.best||0));meta.best=Math.max(prevBest,score);if(score>=Math.max(120,prevBest*0.9))meta.level=metaLvl+1;localStorage.setItem(metaKey,JSON.stringify(meta));
ctx.fillStyle='rgba(8,12,24,0.74)';ctx.fillRect(0,0,cvs.width,cvs.height);ctx.fillStyle='#fff';ctx.font='bold 34px Segoe UI';
ctx.fillText('Run End',cvs.width/2-86,cvs.height/2-16);ctx.font='18px Segoe UI';ctx.fillText('Score: '+score+' | Stage: '+stage+' | Meta Lv: '+(meta.level||0),cvs.width/2-170,cvs.height/2+16);}}}},1000);"""
        if mode == "dodge":
            return f"""const cvs=document.getElementById('game');const ctx=cvs.getContext('2d');
const scoreEl=document.getElementById('score');const timeEl=document.getElementById('time');
const E=window.__studioEngine||{{audio:{{hit(){{}},alert(){{}},success(){{}}}},hud:{{set(){{}}}},vfx:{{burst(){{}}}},drawOverlay(){{}}}};
const metaKey='studio_meta_dodge_v2';
const meta=(()=>{{try{{return JSON.parse(localStorage.getItem(metaKey)||'{{}}')}}catch(_e){{return {{}};}}}})();
const metaLvl=Math.max(0,Number(meta.level||0));const bonusShield=Math.min(180,metaLvl*12);
let running=true,t={duration},score=0,tick=0,wave=1,lastHit=0,stage=1;
const p={{x:410,y:220,vx:0,vy:0,r:14,hp:4,shield:bonusShield,dashCd:0}};
let balls=[],pickups=[],rings=[];let slowmo=0;const keys={{}};let boss=null;
function spawnBall(mult=1){{const spd=2.1+Math.random()*({2+difficulty})+wave*0.15*mult;
if('{variant}'==='spiral_wave'&&Math.random()<0.74){{const a=tick*0.18+Math.random()*1.2;balls.push({{x:410+Math.cos(a)*390,y:220+Math.sin(a)*215,r:8+Math.random()*12,vx:-Math.cos(a)*(1.2+Math.random()*2.2),vy:-Math.sin(a)*(1.0+Math.random()*2.0),kind:'spiral'}});return;}}
const edge=Math.floor(Math.random()*4);let x=0,y=0;if(edge===0){{x=Math.random()*cvs.width;y=-20;}}if(edge===1){{x=cvs.width+20;y=Math.random()*cvs.height;}}if(edge===2){{x=Math.random()*cvs.width;y=cvs.height+20;}}if(edge===3){{x=-20;y=Math.random()*cvs.height;}}
const homing=Math.random()<0.3;const dx=p.x-x,dy=p.y-y,d=Math.max(1,Math.hypot(dx,dy));balls.push({{x,y,r:8+Math.random()*13,vx:(dx/d)*spd*(homing?0.6:1),vy:(dy/d)*spd*(homing?0.6:1),kind:homing?'homing':'normal'}});}}
function spawnPickup(){{const kind=Math.random()<0.52?'shield':'slow';pickups.push({{x:40+Math.random()*(cvs.width-80),y:40+Math.random()*(cvs.height-80),r:11,kind,ttl:430}});}}
function ensureBoss(elapsed){{if(elapsed<{duration}*0.62||boss)return;boss={{x:410,y:56,r:34,hp:8+Math.floor({difficulty}/2),tick:0}};}}
function drawBg(){{const g=ctx.createLinearGradient(0,0,0,cvs.height);g.addColorStop(0,'#101a31');g.addColorStop(1,'#0a1226');ctx.fillStyle=g;ctx.fillRect(0,0,cvs.width,cvs.height);
for(let i=0;i<22;i++){{const y=(i*27+(tick*0.38)%27)%cvs.height;ctx.fillStyle='rgba(96,132,190,0.12)';ctx.fillRect(0,y,cvs.width,1);}}
for(const r of rings){{ctx.beginPath();ctx.arc(r.x,r.y,r.radius,0,Math.PI*2);ctx.strokeStyle='rgba(122,240,255,'+(r.life/40)+')';ctx.lineWidth=2;ctx.stroke();r.radius+=1.7;r.life-=1;}}rings=rings.filter(r=>r.life>0);}}
function draw(){{ctx.clearRect(0,0,cvs.width,cvs.height);drawBg();for(const b of balls){{ctx.beginPath();ctx.arc(b.x,b.y,b.r,0,Math.PI*2);ctx.fillStyle=b.kind==='homing'?'#ff5f7a':'#ff8b8b';ctx.fill();}}
for(const it of pickups){{ctx.beginPath();ctx.arc(it.x,it.y,it.r,0,Math.PI*2);ctx.fillStyle=it.kind==='shield'?'#6bc3ff':'#ffe07f';ctx.fill();}}
if(boss){{ctx.beginPath();ctx.arc(boss.x,boss.y,boss.r,0,Math.PI*2);ctx.fillStyle='#ff4d7b';ctx.fill();ctx.fillStyle='#fff';ctx.fillRect(boss.x-10,boss.y-4,6,6);ctx.fillRect(boss.x+4,boss.y-4,6,6);
ctx.fillStyle='rgba(8,14,26,0.54)';ctx.fillRect(590,16,210,12);ctx.fillStyle='#ff7f9f';ctx.fillRect(590,16,210*Math.max(0,boss.hp)/(8+Math.floor({difficulty}/2)),12);ctx.strokeStyle='rgba(255,180,200,0.8)';ctx.strokeRect(590,16,210,12);}}
ctx.beginPath();ctx.arc(p.x,p.y,p.r,0,Math.PI*2);ctx.fillStyle=p.shield>0?'#8fe7ff':'#75d0a2';ctx.fill();if(p.shield>0){{ctx.beginPath();ctx.arc(p.x,p.y,p.r+7,0,Math.PI*2);ctx.strokeStyle='rgba(120,220,255,0.75)';ctx.lineWidth=2;ctx.stroke();}}
E.drawOverlay(ctx,cvs.width,cvs.height);}}
window.addEventListener('keydown',e=>{{keys[e.key]=true;if((e.key==='Shift'||e.key==='ShiftLeft')&&p.dashCd<=0){{const dx=(keys['ArrowRight']?1:0)-(keys['ArrowLeft']?1:0);const dy=(keys['ArrowDown']?1:0)-(keys['ArrowUp']?1:0);p.x+=dx*84;p.y+=dy*84;p.dashCd=84;E.audio.success();rings.push({{x:p.x,y:p.y,radius:8,life:30}});}}}});
window.addEventListener('keyup',e=>{{keys[e.key]=false;}});
cvs.addEventListener('mousemove',e=>{{const r=cvs.getBoundingClientRect();p.x=(e.clientX-r.left)*(cvs.width/r.width);p.y=(e.clientY-r.top)*(cvs.height/r.height);}});
function step(){{if(!running)return;tick++;const elapsed={duration}-t;stage=Math.min(3,1+Math.floor(elapsed/Math.max(1,{duration}/3)));if(tick%240===0)wave++;const mv=4.5;
p.vx=((keys['ArrowRight']?1:0)-(keys['ArrowLeft']?1:0))*mv;p.vy=((keys['ArrowDown']?1:0)-(keys['ArrowUp']?1:0))*mv;p.x=Math.max(p.r,Math.min(cvs.width-p.r,p.x+p.vx));p.y=Math.max(p.r,Math.min(cvs.height-p.r,p.y+p.vy));if(p.dashCd>0)p.dashCd--;
if(Math.random()<(0.025+stage*0.01)*{difficulty})spawnBall(1+stage*0.2);if(Math.random()<0.0038&&pickups.length<2)spawnPickup();ensureBoss(elapsed);
const slowFactor=slowmo>0?0.45:1.0;if(slowmo>0)slowmo--;
for(const b of balls){{if(b.kind==='homing'){{const dx=p.x-b.x,dy=p.y-b.y,d=Math.max(1,Math.hypot(dx,dy));b.vx+=(dx/d)*0.047;b.vy+=(dy/d)*0.047;}}b.x+=(b.vx||0)*slowFactor;b.y+=(b.vy||0)*slowFactor;}}
balls=balls.filter(b=>b.y<cvs.height+40&&b.x>-50&&b.x<cvs.width+50);
if(boss){{boss.tick++;boss.x=410+Math.sin(boss.tick*0.03)*220;if(boss.tick%18===0)balls.push({{x:boss.x,y:boss.y+8,r:9,vx:Math.sin(boss.tick*0.1)*1.8,vy:2.9+stage*0.5,kind:'boss'}});}}
for(let i=pickups.length-1;i>=0;i--){{const it=pickups[i];it.ttl--;if(it.ttl<=0){{pickups.splice(i,1);continue;}}
if(Math.hypot(p.x-it.x,p.y-it.y)<p.r+it.r){{if(it.kind==='shield')p.shield=Math.max(p.shield,240+bonusShield);if(it.kind==='slow')slowmo=190;pickups.splice(i,1);score+=16;E.audio.success();E.vfx.burst(p.x,p.y,'#7af0ff',1.3);}}}}
for(const b of balls){{const dist=Math.hypot(p.x-b.x,p.y-b.y);if(dist<b.r+p.r){{if(p.shield>0){{p.shield=Math.max(0,p.shield-120);score+=5;E.audio.hit();E.vfx.burst(p.x,p.y,'#8fe7ff',1.0);}}
else if(tick-lastHit>24){{p.hp--;lastHit=tick;score=Math.max(0,score-18);E.audio.alert();rings.push({{x:p.x,y:p.y,radius:6,life:34}});}}}} else if(dist<b.r+p.r+16){{score+=1;}}}}
if(boss&&p.dashCd>0&&Math.hypot(p.x-boss.x,p.y-boss.y)<p.r+boss.r+10){{boss.hp--;score+=30;E.audio.success();E.vfx.burst(boss.x,boss.y,'#ff7f9f',1.6);p.dashCd=0;if(boss.hp<=0){{score+=190;boss=null;for(let n=0;n<6;n++)E.vfx.burst(410+Math.random()*120-60,110+Math.random()*80,'#ffd56c',1.9);}}}}
if(p.shield>0)p.shield--;if(p.hp<=0)running=false;if(running)score+=1+Math.floor(wave*0.25)+stage;scoreEl.textContent=String(score);
E.hud.set({{wave:wave,hp:p.hp,combo:Math.max(0,Math.floor(score/45)),note:boss?('BOSS HP '+boss.hp):(slowmo>0?'SLOW-MO':'Dash '+Math.max(0,Math.ceil(p.dashCd/6)))}});draw();requestAnimationFrame(step);}}
step();const timer=setInterval(()=>{{t-=1;timeEl.textContent=String(Math.max(0,t));if(t<=0||!running){{running=false;clearInterval(timer);
const prevBest=Math.max(0,Number(meta.best||0));meta.best=Math.max(prevBest,score);if(score>=Math.max(130,prevBest*0.9))meta.level=metaLvl+1;localStorage.setItem(metaKey,JSON.stringify(meta));
ctx.fillStyle='rgba(8,12,24,0.72)';ctx.fillRect(0,0,cvs.width,cvs.height);ctx.fillStyle='#fff';ctx.font='bold 36px Segoe UI';
ctx.fillText('Arena End',cvs.width/2-88,cvs.height/2-16);ctx.font='18px Segoe UI';ctx.fillText('Score: '+score+' | Wave: '+wave+' | Meta Lv: '+(meta.level||0),cvs.width/2-158,cvs.height/2+18);}}}},1000);
window.addEventListener('keydown',e=>{{if((e.key||'').toLowerCase()==='r')location.reload();}});"""
        # default aim
        return f"""const cvs=document.getElementById('game');const ctx=cvs.getContext('2d');
const scoreEl=document.getElementById('score');const timeEl=document.getElementById('time');
let score=0,t={duration},target={{x:180,y:150,r:24,vx:0,vy:0}},target2={{x:640,y:180,r:20,vx:0,vy:0}},running=true,combo=0;
function spawn(main=true){{const r=14+Math.random()*({12 + difficulty * 2});const tx={{x:r+Math.random()*(cvs.width-r*2),y:r+Math.random()*(cvs.height-r*2),r,vx:(-1+Math.random()*2)*(0.6+0.2*{tier}),vy:(-1+Math.random()*2)*(0.6+0.2*{tier})}};
if(main)target=tx;else target2=tx;}}
function draw(){{ctx.clearRect(0,0,cvs.width,cvs.height);const g=ctx.createLinearGradient(0,0,0,cvs.height);g.addColorStop(0,'#1d2b4b');g.addColorStop(1,'#0f172b');
ctx.fillStyle=g;ctx.fillRect(0,0,cvs.width,cvs.height);ctx.beginPath();ctx.arc(target.x,target.y,target.r,0,Math.PI*2);ctx.fillStyle='#ff6b6b';ctx.fill();
ctx.beginPath();ctx.arc(target.x,target.y,target.r*0.55,0,Math.PI*2);ctx.fillStyle='#ffd166';ctx.fill();ctx.beginPath();ctx.arc(target.x,target.y,target.r*0.2,0,Math.PI*2);ctx.fillStyle='#fff';ctx.fill();
if('{variant}'==='multi_target'||{tier}>=3){{ctx.beginPath();ctx.arc(target2.x,target2.y,target2.r,0,Math.PI*2);ctx.fillStyle='#7ac7ff';ctx.fill();}}
ctx.fillStyle='#d9eaff';ctx.fillText('combo '+combo,14,24);}}
function loop(){{draw();if(running)requestAnimationFrame(loop);}}
cvs.addEventListener('click',e=>{{if(!running)return;const r=cvs.getBoundingClientRect();const x=(e.clientX-r.left)*(cvs.width/r.width);const y=(e.clientY-r.top)*(cvs.height/r.height);
let hit=false;if(Math.hypot(x-target.x,y-target.y)<=target.r){{score+=Math.max(1,Math.round(48-target.r))+combo;combo+=1;spawn(true);hit=true;}}
if((('{variant}'==='multi_target'||{tier}>=3))&&Math.hypot(x-target2.x,y-target2.y)<=target2.r){{score+=Math.max(1,Math.round(38-target2.r))+combo;combo+=1;spawn(false);hit=true;}}
if(!hit)combo=0;scoreEl.textContent=String(score);}});
spawn(true);spawn(false);loop();const timer=setInterval(()=>{{t-=1;timeEl.textContent=String(Math.max(0,t));
target.x+=target.vx;target.y+=target.vy;if(target.x<target.r||target.x>cvs.width-target.r)target.vx*=-1;if(target.y<target.r||target.y>cvs.height-target.r)target.vy*=-1;
target2.x+=target2.vx;target2.y+=target2.vy;if(target2.x<target2.r||target2.x>cvs.width-target2.r)target2.vx*=-1;if(target2.y<target2.r||target2.y>cvs.height-target2.r)target2.vy*=-1;
if(t<=0){{running=false;clearInterval(timer);
ctx.fillStyle='rgba(8,12,24,0.74)';ctx.fillRect(0,0,cvs.width,cvs.height);ctx.fillStyle='#fff';ctx.font='bold 38px Segoe UI';
ctx.fillText('Time Up',cvs.width/2-90,cvs.height/2-8);ctx.font='22px Segoe UI';ctx.fillText('Score: '+score,cvs.width/2-48,cvs.height/2+28);}}}},1000);"""

    def generate_project_demo(self, project_id: str, actor_id: str = "dev_a") -> Dict[str, Any]:
        gp = self.game_projects[project_id]
        safe_id = gp.id.lower().replace(" ", "_")
        static_root = Path(os.getenv("STUDIO_STATIC_DIR", "static"))
        out_dir = static_root / "generated" / safe_id
        out_dir.mkdir(parents=True, exist_ok=True)

        blueprint = self._build_game_blueprint(gp)
        index_html = self._render_game_html(gp, blueprint)
        game_js = self._render_game_js(blueprint)
        (out_dir / "index.html").write_text(index_html, encoding="utf-8")
        (out_dir / "game.js").write_text(game_js, encoding="utf-8")

        gp.demo_build_count = int(gp.demo_build_count) + 1
        gp.demo_url = f"/static/generated/{safe_id}/index.html"
        gp.game_blueprint = blueprint
        gp.updated_at = now_iso()

        artifact = self.create_artifact(
            title=f"Playable demo for {gp.id}",
            created_by=actor_id,
            task_id=gp.task_ids[0] if gp.task_ids else None,
            content={
                "project_id": gp.id,
                "title": gp.title,
                "genre": gp.genre,
                "demo_url": gp.demo_url,
                "build_count": gp.demo_build_count,
                "blueprint": blueprint,
                "files": ["index.html", "game.js"],
            },
        )
        self.add_event(
            type="game_project.demo_generated",
            actor_id=actor_id,
            summary=f"{gp.id} playable demo generated",
            refs={"artifact_id": artifact.id, "task_id": gp.task_ids[0] if gp.task_ids else None},
            payload={
                "project_id": gp.id,
                "demo_url": gp.demo_url,
                "artifact_id": artifact.id,
                "build_count": gp.demo_build_count,
                "mode": blueprint.get("mode"),
            },
            source="orchestrator",
        )
        return {
            "project": self.game_project_to_dict(gp),
            "artifact": self.artifact_to_dict(artifact),
            "demo_url": gp.demo_url,
            "output_dir": str(out_dir),
        }

    def auto_upgrade_project(
        self,
        project_id: str,
        actor_id: str = "dev_a",
        reason: str = "agent_autoupgrade",
    ) -> Dict[str, Any]:
        if project_id not in self.game_projects:
            raise ValueError(f"project not found: {project_id}")
        if not self.project_has_meeting_alignment(project_id):
            raise ValueError(f"project {project_id} requires meeting alignment")
        gp = self.game_projects[project_id]
        bp = dict(gp.game_blueprint or {})
        base_mode = str(bp.get("mode_base") or bp.get("mode") or "").strip().lower()
        if not base_mode:
            # Ensure initial blueprint exists.
            self.generate_project_demo(project_id, actor_id=actor_id)
            bp = dict(self.game_projects[project_id].game_blueprint or {})
            base_mode = str(bp.get("mode_base") or bp.get("mode") or "aim").strip().lower()

        upgrade_menu = {
            "aim": ["moving targets", "combo bonus", "critical zone", "target pattern shuffle"],
            "runner": ["orb route", "lane hazard mix", "jump timing smoothing", "pace escalation"],
            "dodge": ["wave escalation", "power-up spawn", "dash tuning", "homing balance"],
            "clicker": ["combo scaler", "moving target", "multi-target burst", "tempo boost"],
            "memory": ["sequence depth", "flash cadence", "mistake penalty tuning", "board variation"],
            "rhythm": ["lane burst", "timing window tuning", "note density ramp", "beat accent"],
        }
        picks = upgrade_menu.get(base_mode, ["polish pass", "difficulty tuning", "feedback enhancement"])
        pick = random.choice(picks)
        pass_no = int(gp.demo_build_count or 0) + 1
        if gp.concept:
            gp.concept = f"{gp.concept} | Upgrade pass {pass_no}: {pick}"
        else:
            gp.concept = f"Upgrade pass {pass_no}: {pick}"
        gp.updated_at = now_iso()

        generated = self.generate_project_demo(project_id, actor_id=actor_id)
        self.add_event(
            type="game_project.auto_upgraded",
            actor_id=actor_id,
            summary=f"{gp.id} auto-upgraded by agent ({pick})",
            refs={},
            payload={
                "project_id": gp.id,
                "mode_base": base_mode,
                "upgrade_pick": pick,
                "build_count": gp.demo_build_count,
                "reason": reason,
            },
            source="orchestrator",
        )
        return generated

    # ---------- Events ----------
    def add_event(
        self,
        type: str,
        actor_id: str,
        summary: str,
        refs: Dict[str, Optional[str]],
        payload: Dict[str, Any],
        source: str = "runtime",
    ) -> Event:
        actor = self.agents.get(actor_id)
        event = Event(
            id=next(EVENT_ID),
            ts=now_iso(),
            type=type,
            source=source,
            actor={
                "agent_id": actor_id,
                "name": actor.name if actor else actor_id,
                "role": actor.role if actor else "SYSTEM",
            },
            refs={
                "task_id": refs.get("task_id"),
                "approval_id": refs.get("approval_id"),
                "artifact_id": refs.get("artifact_id"),
            },
            summary=summary,
            payload=payload or {},
        )
        self.events.appendleft(event)
        if not self._bootstrapping:
            self.persist()
        return event

    # ---------- Serialization ----------
    def agent_to_dict(self, a: Agent) -> Dict[str, Any]:
        return asdict(a)

    def task_to_dict(self, t: Task) -> Dict[str, Any]:
        return asdict(t)

    def approval_to_dict(self, a: Approval) -> Dict[str, Any]:
        return asdict(a)

    def event_to_dict(self, e: Event) -> Dict[str, Any]:
        return asdict(e)

    def artifact_to_dict(self, a: Artifact) -> Dict[str, Any]:
        return asdict(a)

    def artifact_to_snapshot_dict(self, a: Artifact) -> Dict[str, Any]:
        # Keep /api/state lightweight: omit full artifact content payload.
        return {
            "id": a.id,
            "title": a.title,
            "created_by": a.created_by,
            "task_id": a.task_id,
            "created_at": a.created_at,
            "updated_at": a.updated_at,
        }

    def meeting_to_dict(self, m: Meeting) -> Dict[str, Any]:
        return asdict(m)

    def kpi_event_to_dict(self, e: KPIEvent) -> Dict[str, Any]:
        return asdict(e)

    def experiment_to_dict(self, e: Experiment) -> Dict[str, Any]:
        return asdict(e)

    def release_to_dict(self, r: Release) -> Dict[str, Any]:
        return asdict(r)

    def trend_to_dict(self, t: TrendSignal) -> Dict[str, Any]:
        return asdict(t)

    def game_project_to_dict(self, g: GameProject) -> Dict[str, Any]:
        return asdict(g)

    def snapshot(self) -> Dict[str, Any]:
        return {
            "control": {"auto_run": self.auto_run, "speed": self.speed},
            "agents": [self.agent_to_dict(a) for a in self.agents.values()],
            "tasks": [self.task_to_dict(t) for t in self.tasks.values()],
            "approvals": [self.approval_to_dict(a) for a in self.approvals.values()],
            "artifacts": [self.artifact_to_snapshot_dict(a) for a in self.artifacts.values()],
            "meetings": [self.meeting_to_dict(m) for m in self.meetings.values()],
            "kpi_events": [self.kpi_event_to_dict(e) for e in list(self.kpi_events)[:1000]],
            "experiments": [self.experiment_to_dict(e) for e in self.experiments.values()],
            "releases": [self.release_to_dict(r) for r in self.releases.values()],
            "trend_signals": [self.trend_to_dict(t) for t in list(self.trend_signals)[:500]],
            "game_projects": [self.game_project_to_dict(g) for g in self.game_projects.values()],
            "mode_extensions": list(self.mode_extensions),
            "learning_memory": dict(self.learning_memory),
            "events": [self.event_to_dict(e) for e in list(self.events)[:120]],
        }

    def persist(self) -> None:
        if self._bootstrapping:
            return
        self._persistence.save(self.snapshot())

    # ---------- Helpers ----------
    def find_todo_tasks(self, allowed_types: Optional[List[str]] = None) -> List[Task]:
        tasks = [t for t in self.tasks.values() if t.status == "Todo"]
        if allowed_types:
            tasks = [t for t in tasks if t.type in allowed_types]
        # priority sort: P0 first
        pr_rank = {"P0": 0, "P1": 1, "P2": 2}
        tasks.sort(key=lambda t: (pr_rank.get(t.priority, 9), t.created_at))
        return tasks
