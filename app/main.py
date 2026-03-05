from __future__ import annotations

import asyncio
from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import os
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .risk_policy import RiskPolicy
from .role_policy import ROLE_BASE_SKILLS, ROLE_PROFILES, profile_for_agent
from .store import Store
from .orchestrator import run_orchestrator
from .meeting_bot import run_meeting_bot
from .task_executor import TaskExecutorRegistry


app = FastAPI(title="AI Game Studio MVP", version="0.1.0")
store = Store()
executor_registry = TaskExecutorRegistry()


class TaskCreate(BaseModel):
    title: str
    description: str = ""
    type: str = Field(default="DEV", description="DEV/QA/MKT/OPS/CEO")
    priority: str = Field(default="P2", description="P0/P1/P2")


class ControlUpdate(BaseModel):
    auto_run: Optional[bool] = None
    speed: Optional[float] = None


class ApprovalDecision(BaseModel):
    decision: str = Field(description="approve/reject")


class ChecklistCreate(BaseModel):
    text: str


class CommentCreate(BaseModel):
    text: str
    author_id: str = "ops"


class MeetingCreate(BaseModel):
    title: str
    agenda: str = ""
    participant_ids: List[str] = []
    created_by: str = "ops"


class MeetingNoteCreate(BaseModel):
    note: str
    author_id: str = "ops"
    decision: Optional[str] = None
    action_item: Optional[str] = None


class TaskExecutorRun(BaseModel):
    executor: str
    actor_id: str = "ops"
    config: Dict[str, Any] = Field(default_factory=dict)


class KPIEventCreate(BaseModel):
    event_type: str
    user_id: str = "anon"
    value: float = 0.0
    meta: Dict[str, Any] = Field(default_factory=dict)


class PortalAdEventCreate(BaseModel):
    event_type: str = Field(description="impression/click/revenue")
    slot: str = Field(default="left", description="left/right/top/bottom")
    value: float = 0.0
    user_id: str = "anon"


class ExperimentCreate(BaseModel):
    name: str
    hypothesis: str = ""
    primary_metric: str = "retention_d1"
    variants: List[str] = Field(default_factory=lambda: ["A", "B"])
    project_id: str = ""
    created_by: str = "ops"


class ExperimentVariantEvent(BaseModel):
    variant: str
    user_id: str = "sim"
    value: float = 1.0


class ExperimentClose(BaseModel):
    winner_variant: str
    actor_id: str = "ceo"


class ReleaseRequestCreate(BaseModel):
    version: str
    title: str
    task_id: Optional[str] = None
    requested_by: str = "qa"
    notes: str = ""


class ProjectReviewUpdate(BaseModel):
    checklist: Dict[str, bool] = Field(default_factory=dict)
    notes: str = ""
    reviewer_id: str = "human_ceo"


class ProjectConfirmDeploy(BaseModel):
    confirmer_id: str = "human_ceo"
    comment: str = ""


class ConnectionManager:
    def __init__(self) -> None:
        self.active: List[WebSocket] = []
        self._lock = asyncio.Lock()

    async def connect(self, ws: WebSocket) -> None:
        await ws.accept()
        async with self._lock:
            self.active.append(ws)

    async def disconnect(self, ws: WebSocket) -> None:
        async with self._lock:
            if ws in self.active:
                self.active.remove(ws)

    async def broadcast(self, message: Dict[str, Any]) -> None:
        async with self._lock:
            conns = list(self.active)

        dead: List[WebSocket] = []
        for ws in conns:
            try:
                await ws.send_json(message)
            except Exception:
                dead.append(ws)

        for ws in dead:
            await self.disconnect(ws)


manager = ConnectionManager()


@app.on_event("startup")
async def _startup() -> None:
    async def emit(message: Dict[str, Any]) -> None:
        await manager.broadcast(message)

    # kick off orchestrator (rule-based now, LLM-ready action pipeline)
    asyncio.create_task(run_orchestrator(store, emit))
    # dedicated meeting automation loop
    asyncio.create_task(run_meeting_bot(store, emit))


# --- Static UI ---
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/", response_class=HTMLResponse)
async def index() -> Any:
    return FileResponse("static/index.html")


# --- API ---
@app.get("/api/state")
async def get_state() -> Dict[str, Any]:
    return store.snapshot()


@app.get("/api/completion")
async def get_completion() -> Dict[str, Any]:
    return {"completion": store.completion_report()}


@app.get("/api/learning/status")
async def get_learning_status() -> Dict[str, Any]:
    return {"learning": store.learning_status()}


@app.get("/api/org/roles")
async def get_org_roles() -> Dict[str, Any]:
    agents = []
    for a in store.agents.values():
        p = profile_for_agent(a.id, a.role)
        agents.append(
            {
                "id": a.id,
                "name": a.name,
                "role": a.role,
                "title": a.title or p.title,
                "level": a.level or p.level,
                "department": a.department or p.department,
                "allowed_tools": sorted(list(p.allowed_tools)),
                "can_create_task_types": sorted(list(p.can_create_task_types)),
                "can_request_approval_kinds": sorted(list(p.can_request_approval_kinds)),
                "can_execute_executors": sorted(list(p.can_execute_executors)),
                "responsibilities": list(p.responsibilities),
                "skills": dict(a.skills or {}),
            }
        )
    return {
        "roles": {
            role: {
                "title": rp.title,
                "level": rp.level,
                "department": rp.department,
                "responsibilities": rp.responsibilities,
                "allowed_tools": sorted(list(rp.allowed_tools)),
                "can_create_task_types": sorted(list(rp.can_create_task_types)),
                "can_request_approval_kinds": sorted(list(rp.can_request_approval_kinds)),
                "can_execute_executors": sorted(list(rp.can_execute_executors)),
                "default_skills": dict(ROLE_BASE_SKILLS.get(role, {})),
            }
            for role, rp in ROLE_PROFILES.items()
        },
        "agents": agents,
    }


@app.get("/api/artifacts")
async def list_artifacts() -> Dict[str, Any]:
    items = sorted(store.artifacts.values(), key=lambda a: a.updated_at, reverse=True)
    return {"artifacts": [store.artifact_to_dict(a) for a in items]}


@app.get("/api/artifacts/{artifact_id}")
async def get_artifact(artifact_id: str) -> Dict[str, Any]:
    if artifact_id not in store.artifacts:
        return {"error": f"artifact not found: {artifact_id}"}
    return {"artifact": store.artifact_to_dict(store.artifacts[artifact_id])}


@app.get("/api/trends")
async def list_trends(limit: int = Query(default=50, ge=1, le=300)) -> Dict[str, Any]:
    items = list(store.trend_signals)[:limit]
    return {"trends": [store.trend_to_dict(t) for t in items]}


@app.get("/api/projects")
async def list_projects() -> Dict[str, Any]:
    items = sorted(store.game_projects.values(), key=lambda g: g.created_at, reverse=True)
    return {
        "projects": [
            {
                **store.game_project_to_dict(g),
                "can_confirm": store.can_confirm_project_release(g.id),
            }
            for g in items
        ]
    }


@app.post("/api/projects/{project_id}/generate_demo")
async def generate_project_demo(project_id: str) -> Dict[str, Any]:
    if project_id not in store.game_projects:
        return {"error": f"project not found: {project_id}"}
    data = store.generate_project_demo(project_id, actor_id="dev_a")
    await manager.broadcast({"type": "event", "data": store.event_to_dict(store.events[0])})
    await manager.broadcast({"type": "snapshot", "data": store.snapshot()})
    return data


@app.post("/api/projects/{project_id}/review")
async def update_project_review(project_id: str, body: ProjectReviewUpdate) -> Dict[str, Any]:
    if project_id not in store.game_projects:
        return {"error": f"project not found: {project_id}"}
    project = store.update_project_review(
        project_id,
        checklist_updates=body.checklist,
        notes=body.notes,
        reviewer_id=body.reviewer_id.strip() or "human_ceo",
    )
    await manager.broadcast({"type": "event", "data": store.event_to_dict(store.events[0])})
    await manager.broadcast({"type": "snapshot", "data": store.snapshot()})
    return {"project": store.game_project_to_dict(project), "can_confirm": store.can_confirm_project_release(project_id)}


@app.post("/api/projects/{project_id}/confirm_deploy")
async def confirm_project_deploy(project_id: str, body: ProjectConfirmDeploy) -> Dict[str, Any]:
    if project_id not in store.game_projects:
        return {"error": f"project not found: {project_id}"}
    try:
        rel = store.confirm_project_release(
            project_id,
            confirmer_id=body.confirmer_id.strip() or "human_ceo",
            comment=body.comment,
        )
    except ValueError as exc:
        return {"error": str(exc)}
    await manager.broadcast({"type": "event", "data": store.event_to_dict(store.events[0])})
    await manager.broadcast({"type": "snapshot", "data": store.snapshot()})
    return {"release": store.release_to_dict(rel), "project": store.game_project_to_dict(store.game_projects[project_id])}


@app.get("/api/projects/{project_id}/kpi_gate")
async def get_project_kpi_gate(project_id: str, since_minutes: int = Query(default=180, ge=10, le=10080)) -> Dict[str, Any]:
    if project_id not in store.game_projects:
        return {"error": f"project not found: {project_id}"}
    return {"kpi_gate": store.release_kpi_gate(since_minutes=since_minutes, project_id=project_id)}


@app.get("/api/kpi/summary")
async def get_kpi_summary(since_minutes: Optional[int] = Query(default=None, ge=1, le=10080)) -> Dict[str, Any]:
    return {"summary": store.kpi_summary(since_minutes=since_minutes)}


@app.get("/api/projects/{project_id}/kpi_summary")
async def get_project_kpi_summary(
    project_id: str,
    since_minutes: Optional[int] = Query(default=180, ge=1, le=10080),
) -> Dict[str, Any]:
    if project_id not in store.game_projects:
        return {"error": f"project not found: {project_id}"}
    return {"summary": store.project_kpi_summary(project_id=project_id, since_minutes=since_minutes)}


@app.get("/api/portal/catalog")
async def get_portal_catalog() -> Dict[str, Any]:
    return store.portal_catalog()


@app.get("/api/monetization/summary")
async def get_monetization_summary(
    since_minutes: int = Query(default=1440, ge=1, le=10080),
    project_id: str = Query(default=""),
) -> Dict[str, Any]:
    return {"summary": store.monetization_summary(since_minutes=since_minutes, project_id=project_id)}


@app.post("/api/portal/{project_id}/ad_event")
async def create_portal_ad_event(project_id: str, body: PortalAdEventCreate) -> Dict[str, Any]:
    if project_id not in store.game_projects:
        return {"error": f"project not found: {project_id}"}
    gp = store.game_projects[project_id]
    if gp.status != "Released" or not gp.release_id or gp.release_id not in store.releases:
        return {"error": f"project {project_id} is not publicly released"}
    rel = store.releases[gp.release_id]
    if not rel.final_confirmed:
        return {"error": f"project {project_id} is not final-confirmed"}

    et_raw = str(body.event_type or "").strip().lower()
    mapping = {
        "impression": "ad.impression",
        "click": "ad.click",
        "revenue": "ad.revenue",
    }
    if et_raw not in mapping:
        return {"error": "event_type must be impression/click/revenue"}
    et = mapping[et_raw]
    value = float(body.value or 0.0)
    if et != "ad.revenue":
        value = 0.0
    elif value <= 0:
        return {"error": "revenue event requires value > 0"}

    slot = str(body.slot or "left").strip().lower()
    user_id = str(body.user_id or "anon").strip() or "anon"
    e = store.add_kpi_event(
        event_type=et,
        user_id=user_id,
        value=value,
        meta={"project_id": project_id, "slot": slot, "release_id": rel.id},
        source="portal",
    )
    await manager.broadcast({"type": "event", "data": store.event_to_dict(store.events[0])})
    await manager.broadcast({"type": "snapshot", "data": store.snapshot()})
    return {
        "kpi_event": store.kpi_event_to_dict(e),
        "summary": store.monetization_summary(since_minutes=1440, project_id=project_id),
    }


@app.post("/api/kpi/events")
async def create_kpi_event(body: KPIEventCreate) -> Dict[str, Any]:
    event_type = body.event_type.strip()
    if not event_type:
        return {"error": "event_type is required"}
    e = store.add_kpi_event(
        event_type=event_type,
        user_id=body.user_id.strip() or "anon",
        value=body.value,
        meta=body.meta,
        source="api",
    )
    await manager.broadcast({"type": "event", "data": store.event_to_dict(store.events[0])})
    return {"kpi_event": store.kpi_event_to_dict(e), "summary": store.kpi_summary()}


@app.get("/api/experiments")
async def list_experiments(project_id: str = Query(default="")) -> Dict[str, Any]:
    items = sorted(store.experiments.values(), key=lambda e: e.created_at, reverse=True)
    pid = project_id.strip()
    if pid:
        items = [e for e in items if str(getattr(e, "project_id", "")).strip() == pid]
    return {"experiments": [store.experiment_to_dict(e) for e in items]}


def _artifact_latest_content(artifact_id: str) -> Dict[str, Any]:
    art = store.artifacts.get(artifact_id)
    if not art or not art.versions:
        return {}
    path = str((art.versions[-1] or {}).get("file_path", "")).strip()
    if not path:
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        return dict(payload.get("content", {}) or {})
    except Exception:
        return {}


@app.get("/api/devops/pr_pipeline")
async def get_pr_pipeline(limit: int = Query(default=40, ge=1, le=200)) -> Dict[str, Any]:
    records: List[Dict[str, Any]] = []
    approvals = list(store.approvals.values())
    arts = sorted(store.artifacts.values(), key=lambda a: a.updated_at, reverse=True)
    merge_contents: List[Dict[str, Any]] = []
    for a in arts:
        c = _artifact_latest_content(a.id)
        if str(c.get("executor", "")) == "dev_github_merge":
            merge_contents.append(c)

    for a in arts:
        c = _artifact_latest_content(a.id)
        if str(c.get("executor", "")) != "dev_github_pr":
            continue
        project_id = str(c.get("project_id", "")).strip().upper()
        pr_url = str(c.get("pull_request_url", "")).strip()
        if not project_id:
            continue

        gp = store.game_projects.get(project_id)
        qa_done = False
        if gp:
            qa_done = any(
                tid in store.tasks and store.tasks[tid].type == "QA" and store.tasks[tid].status == "Done"
                for tid in (gp.task_ids or [])
            )

        related_approval = None
        for apr in approvals:
            payload = apr.payload or {}
            if str(payload.get("artifact_id", "")).strip() == a.id:
                related_approval = apr
                break

        related_merge = None
        for mc in merge_contents:
            if pr_url and str(mc.get("pull_request_url", "")).strip() == pr_url:
                related_merge = mc
                break

        status = "Drafted"
        if related_merge and bool(related_merge.get("merged", False)):
            status = "Merged"
        elif related_merge and not bool(related_merge.get("merged", True)):
            status = "MergeFailed"
        elif related_approval and related_approval.status == "Rejected":
            status = "RejectedByHuman"
        elif related_approval and related_approval.status == "Approved":
            status = "ApprovedWaitingMerge"
        elif related_approval and related_approval.status == "Pending":
            status = "PendingHumanApproval"

        records.append(
            {
                "artifact_id": a.id,
                "project_id": project_id,
                "project_title": gp.title if gp else "",
                "pr_url": pr_url,
                "branch": str(c.get("branch", "")).strip(),
                "created_at": a.created_at,
                "updated_at": a.updated_at,
                "qa_done": qa_done,
                "approval_id": related_approval.id if related_approval else "",
                "approval_status": related_approval.status if related_approval else "",
                "status": status,
            }
        )
        if len(records) >= int(limit):
            break

    counts = Counter([r.get("status", "Unknown") for r in records])
    return {"items": records, "summary": dict(counts)}


@app.post("/api/experiments")
async def create_experiment(body: ExperimentCreate) -> Dict[str, Any]:
    name = body.name.strip()
    if not name:
        return {"error": "name is required"}
    try:
        exp = store.create_experiment(
            name=name,
            hypothesis=body.hypothesis,
            primary_metric=body.primary_metric,
            variants=body.variants,
            project_id=body.project_id,
            created_by=body.created_by.strip() or "ops",
        )
    except ValueError as exc:
        return {"error": str(exc)}
    await manager.broadcast({"type": "event", "data": store.event_to_dict(store.events[0])})
    await manager.broadcast({"type": "snapshot", "data": store.snapshot()})
    return {"experiment": store.experiment_to_dict(exp)}


@app.post("/api/experiments/{experiment_id}/exposure")
async def add_experiment_exposure(experiment_id: str, body: ExperimentVariantEvent) -> Dict[str, Any]:
    if experiment_id not in store.experiments:
        return {"error": f"experiment not found: {experiment_id}"}
    try:
        exp = store.record_experiment_exposure(
            experiment_id=experiment_id,
            variant=body.variant.strip(),
            user_id=body.user_id.strip() or "sim",
        )
    except ValueError as exc:
        return {"error": str(exc)}
    await manager.broadcast({"type": "event", "data": store.event_to_dict(store.events[0])})
    return {"experiment": store.experiment_to_dict(exp)}


@app.post("/api/experiments/{experiment_id}/conversion")
async def add_experiment_conversion(experiment_id: str, body: ExperimentVariantEvent) -> Dict[str, Any]:
    if experiment_id not in store.experiments:
        return {"error": f"experiment not found: {experiment_id}"}
    try:
        exp = store.record_experiment_conversion(
            experiment_id=experiment_id,
            variant=body.variant.strip(),
            value=body.value,
            user_id=body.user_id.strip() or "sim",
        )
    except ValueError as exc:
        return {"error": str(exc)}
    await manager.broadcast({"type": "event", "data": store.event_to_dict(store.events[0])})
    return {"experiment": store.experiment_to_dict(exp)}


@app.post("/api/experiments/{experiment_id}/close")
async def close_experiment(experiment_id: str, body: ExperimentClose) -> Dict[str, Any]:
    if experiment_id not in store.experiments:
        return {"error": f"experiment not found: {experiment_id}"}
    try:
        exp = store.close_experiment(
            experiment_id=experiment_id,
            winner_variant=body.winner_variant.strip(),
            actor_id=body.actor_id.strip() or "ceo",
        )
    except ValueError as exc:
        return {"error": str(exc)}
    await manager.broadcast({"type": "event", "data": store.event_to_dict(store.events[0])})
    await manager.broadcast({"type": "snapshot", "data": store.snapshot()})
    return {"experiment": store.experiment_to_dict(exp)}


@app.get("/api/releases")
async def list_releases() -> Dict[str, Any]:
    items = sorted(store.releases.values(), key=lambda r: r.created_at, reverse=True)
    return {"releases": [store.release_to_dict(r) for r in items]}


@app.post("/api/releases/request")
async def request_release(body: ReleaseRequestCreate) -> Dict[str, Any]:
    version = body.version.strip()
    title = body.title.strip()
    if not version:
        return {"error": "version is required"}
    if not title:
        return {"error": "title is required"}
    if body.task_id and body.task_id not in store.tasks:
        return {"error": f"task not found: {body.task_id}"}
    rel = store.create_release_candidate(
        version=version,
        title=title,
        task_id=body.task_id,
        requested_by=body.requested_by.strip() or "qa",
        notes=body.notes,
    )
    await manager.broadcast({"type": "event", "data": store.event_to_dict(store.events[0])})
    await manager.broadcast({"type": "snapshot", "data": store.snapshot()})
    return {"release": store.release_to_dict(rel)}


@app.post("/api/releases/{release_id}/promote")
async def promote_release(release_id: str) -> Dict[str, Any]:
    if release_id not in store.releases:
        return {"error": f"release not found: {release_id}"}
    try:
        rel = store.promote_release(release_id, actor_id="ops")
    except ValueError as exc:
        return {"error": str(exc)}
    await manager.broadcast({"type": "event", "data": store.event_to_dict(store.events[0])})
    await manager.broadcast({"type": "snapshot", "data": store.snapshot()})
    return {"release": store.release_to_dict(rel)}


@app.get("/api/meetings")
async def list_meetings() -> Dict[str, Any]:
    items = sorted(store.meetings.values(), key=lambda m: m.created_at, reverse=True)
    return {"meetings": [store.meeting_to_dict(m) for m in items]}


@app.post("/api/meetings")
async def create_meeting(body: MeetingCreate) -> Dict[str, Any]:
    title = body.title.strip()
    if not title:
        return {"error": "title is required"}
    participants = [p.strip() for p in body.participant_ids if p and p.strip()]
    m = store.create_meeting(
        title=title,
        agenda=body.agenda.strip(),
        participant_ids=participants,
        created_by=body.created_by.strip() or "ops",
    )
    await manager.broadcast({"type": "event", "data": store.event_to_dict(store.events[0])})
    await manager.broadcast({"type": "snapshot", "data": store.snapshot()})
    return {"meeting": store.meeting_to_dict(m)}


@app.post("/api/meetings/{meeting_id}/start")
async def start_meeting(meeting_id: str) -> Dict[str, Any]:
    if meeting_id not in store.meetings:
        return {"error": f"meeting not found: {meeting_id}"}
    m = store.start_meeting(meeting_id, actor_id="ops")
    await manager.broadcast({"type": "event", "data": store.event_to_dict(store.events[0])})
    await manager.broadcast({"type": "snapshot", "data": store.snapshot()})
    return {"meeting": store.meeting_to_dict(m)}


@app.post("/api/meetings/{meeting_id}/note")
async def add_meeting_note(meeting_id: str, body: MeetingNoteCreate) -> Dict[str, Any]:
    if meeting_id not in store.meetings:
        return {"error": f"meeting not found: {meeting_id}"}
    note = body.note.strip()
    if not note:
        return {"error": "note is required"}
    item = body.action_item.strip() if body.action_item else None
    action_item = {"text": item, "created_by": body.author_id, "created_at": ""} if item else None
    m = store.add_meeting_note(
        meeting_id,
        note=note,
        author_id=body.author_id.strip() or "ops",
        decision=body.decision.strip() if body.decision else None,
        action_item=action_item,
    )
    await manager.broadcast({"type": "event", "data": store.event_to_dict(store.events[0])})
    await manager.broadcast({"type": "snapshot", "data": store.snapshot()})
    return {"meeting": store.meeting_to_dict(m)}


@app.post("/api/meetings/{meeting_id}/close")
async def close_meeting(meeting_id: str) -> Dict[str, Any]:
    if meeting_id not in store.meetings:
        return {"error": f"meeting not found: {meeting_id}"}
    m = store.close_meeting(meeting_id, actor_id="ops")
    await manager.broadcast({"type": "event", "data": store.event_to_dict(store.events[0])})
    await manager.broadcast({"type": "snapshot", "data": store.snapshot()})
    return {"meeting": store.meeting_to_dict(m)}


@app.get("/api/audit/summary")
async def get_audit_summary(
    source: Optional[str] = Query(default=None),
    event_type: Optional[str] = Query(default=None),
    since_minutes: Optional[int] = Query(default=None, ge=1, le=10080),
    recent_limit: int = Query(default=20, ge=1, le=200),
) -> Dict[str, Any]:
    tasks = list(store.tasks.values())
    approvals = list(store.approvals.values())
    events_all = list(store.events)
    events = events_all
    if source:
        events = [e for e in events if getattr(e, "source", "runtime") == source]
    if event_type:
        events = [e for e in events if e.type == event_type]
    if since_minutes is not None:
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=since_minutes)
        filtered: List[Any] = []
        for e in events:
            try:
                ts = datetime.fromisoformat(e.ts).astimezone(timezone.utc)
            except Exception:
                continue
            if ts >= cutoff:
                filtered.append(e)
        events = filtered

    task_status = Counter(t.status for t in tasks)
    task_type = Counter(t.type for t in tasks)
    task_priority = Counter(t.priority for t in tasks)
    approval_kind = Counter(a.kind for a in approvals)
    approval_status = Counter(a.status for a in approvals)
    action_gate_status = Counter(
        a.status for a in approvals if a.kind == "action_gate"
    )
    event_type_counts = Counter(e.type for e in events)
    event_source = Counter(getattr(e, "source", "runtime") for e in events)

    return {
        "control": {"auto_run": store.auto_run, "speed": store.speed},
        "counts": {
            "tasks_total": len(tasks),
            "approvals_total": len(approvals),
            "events_total": len(events),
            "events_total_unfiltered": len(events_all),
            "agents_total": len(store.agents),
            "artifacts_total": len(store.artifacts),
            "experiments_total": len(store.experiments),
            "releases_total": len(store.releases),
            "kpi_events_total": len(store.kpi_events),
            "trends_total": len(store.trend_signals),
            "projects_total": len(store.game_projects),
        },
        "tasks": {
            "by_status": dict(task_status),
            "by_type": dict(task_type),
            "by_priority": dict(task_priority),
        },
        "approvals": {
            "by_kind": dict(approval_kind),
            "by_status": dict(approval_status),
            "pending_action_gates": len([a for a in approvals if a.kind == "action_gate" and a.status == "Pending"]),
            "action_gate_by_status": dict(action_gate_status),
        },
        "events": {
            "by_type_top10": dict(event_type_counts.most_common(10)),
            "by_source": dict(event_source),
        },
        "filters": {
            "source": source,
            "event_type": event_type,
            "since_minutes": since_minutes,
            "recent_limit": recent_limit,
        },
        "recent": [
            {
                "id": e.id,
                "type": e.type,
                "source": getattr(e, "source", "runtime"),
                "summary": e.summary,
                "ts": e.ts,
            }
            for e in events[:recent_limit]
        ],
    }


@app.get("/api/runtime/config")
async def get_runtime_config() -> Dict[str, Any]:
    policy = RiskPolicy.from_env()
    return {
        "db_path": os.getenv("STUDIO_DB_PATH", "data/studio.db"),
        "llm_enabled": bool(os.getenv("OPENAI_API_KEY", "").strip()),
        "llm_model": os.getenv("OPENAI_MODEL", "gpt-4.1-mini"),
        "risk_policy": {
            "gate_create_artifact": policy.gate_create_artifact,
            "gate_release_request": policy.gate_release_request,
            "action_gate_ttl_seconds": int(os.getenv("ACTION_GATE_TTL_SECONDS", "900")),
        },
    }


@app.get("/api/executors")
async def list_executors() -> Dict[str, Any]:
    return {"executors": executor_registry.names()}


@app.get("/healthz")
async def healthz() -> Dict[str, Any]:
    return {"status": "ok", "service": "ai-game-studio-mvp"}


@app.get("/readyz")
async def readyz() -> Dict[str, Any]:
    return {
        "ready": True,
        "agents": len(store.agents),
        "tasks": len(store.tasks),
        "approvals": len(store.approvals),
    }


@app.post("/api/tasks")
async def create_task(body: TaskCreate) -> Dict[str, Any]:
    t = store.create_task(
        title=body.title,
        description=body.description,
        type=body.type,
        priority=body.priority,
        assignee_id=None,
    )
    # broadcast event
    await manager.broadcast({"type": "event", "data": store.event_to_dict(store.events[0])})
    return {"task": store.task_to_dict(t)}


@app.post("/api/tasks/{task_id}/execute")
async def execute_task(task_id: str, body: TaskExecutorRun) -> Dict[str, Any]:
    if task_id not in store.tasks:
        return {"error": f"task not found: {task_id}"}
    result = executor_registry.run(
        name=body.executor.strip(),
        store=store,
        task_id=task_id,
        actor_id=body.actor_id.strip() or "ops",
        config=body.config,
    )
    evt_type = "task.executor_succeeded" if result.ok else "task.executor_failed"
    store.add_event(
        type=evt_type,
        actor_id=body.actor_id.strip() or "ops",
        summary=result.summary,
        refs={"task_id": task_id, "artifact_id": result.artifact_id},
        payload={"executor": body.executor, "result": result.details or {}},
        source="api",
    )
    await manager.broadcast({"type": "event", "data": store.event_to_dict(store.events[0])})
    await manager.broadcast({"type": "snapshot", "data": store.snapshot()})
    return {
        "ok": result.ok,
        "summary": result.summary,
        "artifact_id": result.artifact_id,
        "details": result.details or {},
    }


@app.post("/api/tasks/{task_id}/checklist")
async def add_checklist_item(task_id: str, body: ChecklistCreate) -> Dict[str, Any]:
    if task_id not in store.tasks:
        return {"error": f"task not found: {task_id}"}
    text = body.text.strip()
    if not text:
        return {"error": "text is required"}

    task = store.add_task_checklist_item(task_id, text=text, actor_id="ops")
    await manager.broadcast({"type": "event", "data": store.event_to_dict(store.events[0])})
    await manager.broadcast({"type": "snapshot", "data": store.snapshot()})
    return {"task": store.task_to_dict(task)}


@app.post("/api/tasks/{task_id}/checklist/{item_index}/toggle")
async def toggle_checklist_item(task_id: str, item_index: int) -> Dict[str, Any]:
    if task_id not in store.tasks:
        return {"error": f"task not found: {task_id}"}
    task = store.tasks[task_id]
    if item_index < 0 or item_index >= len(task.checklist):
        return {"error": f"invalid checklist index: {item_index}"}

    updated = store.toggle_task_checklist_item(task_id, index=item_index, actor_id="ops")
    await manager.broadcast({"type": "event", "data": store.event_to_dict(store.events[0])})
    await manager.broadcast({"type": "snapshot", "data": store.snapshot()})
    return {"task": store.task_to_dict(updated)}


@app.post("/api/tasks/{task_id}/comments")
async def add_task_comment(task_id: str, body: CommentCreate) -> Dict[str, Any]:
    if task_id not in store.tasks:
        return {"error": f"task not found: {task_id}"}
    text = body.text.strip()
    if not text:
        return {"error": "text is required"}

    author_id = body.author_id.strip() or "ops"
    task = store.add_task_comment(task_id, text=text, actor_id=author_id)
    await manager.broadcast({"type": "event", "data": store.event_to_dict(store.events[0])})
    await manager.broadcast({"type": "snapshot", "data": store.snapshot()})
    return {"task": store.task_to_dict(task)}


@app.post("/api/control")
async def update_control(body: ControlUpdate) -> Dict[str, Any]:
    if body.auto_run is not None:
        store.auto_run = bool(body.auto_run)
        store.add_event(
            type="control.updated",
            actor_id="ops",
            summary=f"Auto-run set to {store.auto_run}",
            refs={},
            payload={"auto_run": store.auto_run},
            source="ui",
        )
        await manager.broadcast({"type": "event", "data": store.event_to_dict(store.events[0])})

    if body.speed is not None:
        store.speed = float(body.speed)
        store.add_event(
            type="control.updated",
            actor_id="ops",
            summary=f"Speed set to {store.speed:.1f}x",
            refs={},
            payload={"speed": store.speed},
            source="ui",
        )
        await manager.broadcast({"type": "event", "data": store.event_to_dict(store.events[0])})

    # send new snapshot too (simplify UI sync)
    await manager.broadcast({"type": "snapshot", "data": store.snapshot()})
    return {"control": {"auto_run": store.auto_run, "speed": store.speed}}


@app.post("/api/approvals/{approval_id}/decision")
async def decide_approval(approval_id: str, body: ApprovalDecision) -> Dict[str, Any]:
    if approval_id not in store.approvals:
        return {"error": f"approval not found: {approval_id}"}

    apr = store.approvals[approval_id]
    if apr.status != "Pending":
        return {"error": f"approval already decided: {approval_id}"}

    decision = body.decision.lower().strip()
    if decision not in ("approve", "reject"):
        return {"error": "decision must be approve or reject"}

    updated = store.decide_approval(approval_id, decision, decision_by="human_ceo")
    await manager.broadcast({"type": "event", "data": store.event_to_dict(store.events[0])})
    await manager.broadcast({"type": "snapshot", "data": store.snapshot()})
    return {"approval": store.approval_to_dict(updated)}


# --- WebSocket ---
@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket) -> None:
    await manager.connect(ws)
    try:
        # send snapshot on connect
        await ws.send_json({"type": "snapshot", "data": store.snapshot()})

        while True:
            # client messages optional (e.g. ping)
            msg = await ws.receive_text()
            if msg == "ping":
                await ws.send_text("pong")
    except WebSocketDisconnect:
        pass
    finally:
        await manager.disconnect(ws)
