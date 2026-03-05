from __future__ import annotations

import asyncio
import os
import random
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, List

from .action_schema import sanitize_actions
from .risk_policy import RiskPolicy
from .role_policy import profile_for_agent
from .store import Store
from .task_executor import TaskExecutorRegistry
from .llm_adapter import PlannerRouter

MEETING_TOPICS = [
    ("코어 루프 개선 동기화", "첫 10분 유지율을 개선하고 다음 스프린트를 정렬합니다."),
    ("수익화 실험 리뷰", "스타터팩 가격과 보상 밸런스를 논의합니다."),
    ("릴리즈 준비 스탠드업", "QA 결과, 블로커 수정, 릴리즈 게이트를 점검합니다."),
    ("콘텐츠 주기 기획", "다음 주 이벤트 미션과 운영 일정을 계획합니다."),
]

MEETING_NOTES = [
    "레벨 1 온보딩 안내 신호를 더 명확히 해야 합니다.",
    "A/B 테스트는 최소 48시간 운영해야 합니다.",
    "재실행 이후 크래시 재현 경로 1건을 QA가 확인했습니다.",
    "마케팅팀은 패치노트 CTA 강화를 요청했습니다.",
]

MEETING_DECISIONS = [
    "스타터 번들 A/B 테스트 롤아웃을 승인합니다.",
    "다음 패치에 온보딩 툴팁 개선을 반영합니다.",
    "크래시 수정이 메인 반영될 때까지 릴리즈를 차단합니다.",
    "QA 사인오프 이후 커뮤니티 게시를 진행합니다.",
]

MEETING_ACTION_ITEMS = [
    "D1 유지율 변화 KPI 추적 업무 생성",
    "크래시 수정 검증 체크리스트 준비",
    "가격 카피 B안 검토용 초안 작성",
    "릴리즈 노트에 알려진 이슈 섹션 추가",
]

EXPERIMENT_HYPOTHESES = [
    ("스타터 번들 가격 테스트", "스타터 번들 가격 인하가 ARPU 훼손 없이 전환율을 개선한다."),
    ("온보딩 CTA 카피 테스트", "더 명확한 CTA가 튜토리얼 완료율과 D1 유지율을 높인다."),
    ("리워드 광고 쿨다운 테스트", "약간 긴 쿨다운이 이탈을 줄이고 세션 품질을 높인다."),
]

TREND_SIGNALS = [
    ("짧고 강한 생존 세션", "아케이드", 0.81, "public-trendboard", "짧지만 강한 세션 선호가 증가 중입니다."),
    ("정밀 트레이닝 챌린지", "스킬 트레이너", 0.78, "community-forum", "경쟁 유저들이 에임 훈련 콘텐츠를 공유합니다."),
    ("합치기 성장 루프", "퍼즐", 0.73, "store-topcharts", "저마찰 합치기 루프가 안정적 유지율을 보입니다."),
    ("원버튼 타이밍 플랫폼", "러너", 0.69, "creator-social", "반응 속도 중심 클립 확산이 빠릅니다."),
    ("코지 방치형 자동화", "방치형", 0.66, "genre-report", "느린 성장형 장르의 유입 범위가 넓습니다."),
]


class ToolExecutor:
    """
    Tool layer for orchestrator actions.
    LLM output contract:
    {
      "actions": [
        {"tool": "<tool_name>", "args": {...}}
      ]
    }
    """

    def __init__(self, store: Store, emit: Callable[[Dict[str, Any]], Awaitable[None]]) -> None:
        self.store = store
        self.emit = emit
        self.risk_policy = RiskPolicy.from_env()
        self.action_gate_ttl_seconds = int(os.getenv("ACTION_GATE_TTL_SECONDS", "900"))
        self.executors = TaskExecutorRegistry()
        self._meeting_tick = 0
        self._experiment_tick = 0
        self._kpi_tick = 0
        self._trend_tick = 0
        self._project_tick = 0
        self._release_tick = 0
        self._learning_tick = 0

    def _is_high_risk(self, action: Dict[str, Any]) -> bool:
        return self.risk_policy.is_high_risk(action)

    def _is_authorized(self, agent_id: str, action: Dict[str, Any]) -> tuple[bool, str]:
        agent = self.store.agents.get(agent_id)
        if not agent:
            return False, f"unknown agent: {agent_id}"
        profile = profile_for_agent(agent_id, agent.role)
        tool = str(action.get("tool", "")).strip()
        args = action.get("args", {}) or {}

        if tool not in profile.allowed_tools:
            return False, f"{agent.role} cannot use tool={tool}"

        if tool == "create_task":
            ttype = str(args.get("type", "DEV")).strip().upper()
            if ttype not in profile.can_create_task_types:
                return False, f"{agent.role} cannot create task type={ttype}"

        if tool == "request_approval":
            kind = str(args.get("kind", "")).strip()
            if kind and kind not in profile.can_request_approval_kinds:
                return False, f"{agent.role} cannot request approval kind={kind}"

        if tool == "run_task_executor":
            ex = str(args.get("executor", "")).strip()
            if ex not in profile.can_execute_executors:
                return False, f"{agent.role} cannot run executor={ex}"

        if tool == "set_agent_state":
            target = str(args.get("agent_id", agent_id)).strip() or agent_id
            if target != agent_id and agent.role not in {"CEO", "OPS"}:
                return False, f"{agent.role} cannot set other agent state"

        if tool == "update_task":
            task_id = str(args.get("task_id", "")).strip()
            if task_id and task_id in self.store.tasks:
                t = self.store.tasks[task_id]
                if agent.role not in {"CEO", "OPS"}:
                    if t.assignee_id not in {None, agent_id}:
                        return False, f"{agent.role} cannot update task owned by {t.assignee_id}"
                changes = args.get("changes", {}) or {}
                new_status = str(changes.get("status", "")).strip()
                # Company rule: only QA/CEO/OPS can confirm Done.
                if new_status == "Done" and agent.role not in {"QA", "CEO", "OPS"}:
                    return False, f"{agent.role} cannot mark task Done"

        return True, "ok"

    async def _queue_action_gate(self, action: Dict[str, Any], requested_by: str, source: str) -> None:
        tool = action.get("tool")
        args = action.get("args", {})
        title = f"Approve high-risk action: {tool}"
        if tool == "create_artifact":
            title = f"Approve artifact creation: {args.get('title', 'Untitled')}"
        elif tool == "request_approval":
            title = f"Approve release approval request: {args.get('title', 'Release')}"

        self.store.create_approval(
            kind="action_gate",
            title=title,
            requested_by=requested_by or "ops",
            payload={
                "action": action,
                "source": source,
                "gate_status": "queued",
                "executed": False,
                "ttl_seconds": self.action_gate_ttl_seconds,
            },
        )
        await self._emit_latest_event()

    async def execute_many(
        self,
        actions: List[Dict[str, Any]],
        *,
        default_agent_id: str | None = None,
        source: str = "orchestrator",
    ) -> None:
        safe_actions, errors = sanitize_actions(actions, default_agent_id=default_agent_id, max_actions=8)
        queued_count = 0
        executed_count = 0
        if errors:
            self.store.add_event(
                type="orchestrator.actions_rejected",
                actor_id="ops",
                summary=f"{len(errors)} invalid action(s) rejected",
                refs={},
                payload={"errors": errors[:4], "source": source, "agent_id": default_agent_id},
                source=source,
            )
            await self._emit_latest_event()

        for action in safe_actions:
            if default_agent_id:
                ok_auth, reason = self._is_authorized(default_agent_id, action)
                if not ok_auth:
                    self.store.add_event(
                        type="orchestrator.action_blocked_by_role",
                        actor_id=default_agent_id,
                        summary=f"Action blocked by role policy: {action.get('tool')}",
                        refs={},
                        payload={"reason": reason, "action": action, "source": source},
                        source="orchestrator",
                    )
                    await self._emit_latest_event()
                    continue
            if self._is_high_risk(action):
                await self._queue_action_gate(action, requested_by=default_agent_id or "ops", source=source)
                queued_count += 1
                continue
            ok = await self._execute_with_retry(action, source=source, actor_id=default_agent_id or "ops")
            if ok:
                executed_count += 1

        if safe_actions:
            self.store.add_event(
                type="orchestrator.actions_processed",
                actor_id=default_agent_id or "ops",
                summary=f"Processed actions from {source}: executed={executed_count}, queued={queued_count}",
                refs={},
                payload={
                    "source": source,
                    "agent_id": default_agent_id,
                    "executed_count": executed_count,
                    "queued_count": queued_count,
                    "total_valid_actions": len(safe_actions),
                },
                source=source,
            )
            await self._emit_latest_event()

    async def _execute_with_retry(self, action: Dict[str, Any], *, source: str, actor_id: str) -> bool:
        attempts = 3
        delay = 0.2
        for i in range(attempts):
            try:
                await self.execute(action)
                return True
            except Exception as exc:
                if i < attempts - 1:
                    self.store.add_event(
                        type="orchestrator.action_retry",
                        actor_id=actor_id,
                        summary=f"Retrying action {action.get('tool')} ({i + 1}/{attempts - 1})",
                        refs={},
                        payload={"error": str(exc), "source": source, "action": action},
                        source="orchestrator",
                    )
                    await self._emit_latest_event()
                    await asyncio.sleep(delay)
                    delay *= 2
                    continue

                self.store.add_event(
                    type="orchestrator.action_failed",
                    actor_id=actor_id,
                    summary=f"Action failed: {action.get('tool')}",
                    refs={},
                    payload={"error": str(exc), "source": source, "action": action},
                    source="orchestrator",
                )
                await self._emit_latest_event()
                return False
        return False

    async def execute(self, action: Dict[str, Any]) -> None:
        tool = action.get("tool")
        args = action.get("args", {})
        if tool == "create_task":
            task = self.store.create_task(
                title=args["title"],
                description=args.get("description", ""),
                type=args.get("type", "DEV"),
                priority=args.get("priority", "P2"),
                assignee_id=args.get("assignee_id"),
            )
            await self._emit_latest_event()
            if args.get("set_doing") and args.get("assignee_id"):
                self.store.update_task(task.id, status="Doing", assignee_id=args["assignee_id"])
                await self._emit_latest_event()
            return

        if tool == "update_task":
            self.store.update_task(args["task_id"], **args.get("changes", {}))
            await self._emit_latest_event()
            return

        if tool == "request_approval":
            self.store.create_approval(
                kind=args.get("kind", "release"),
                title=args["title"],
                requested_by=args.get("requested_by", "ops"),
                payload=args.get("payload", {}),
            )
            await self._emit_latest_event()
            return

        if tool == "create_artifact":
            self.store.create_artifact(
                artifact_id=args.get("artifact_id"),
                title=args.get("title", "Untitled artifact"),
                created_by=args.get("actor_id", "ops"),
                task_id=args.get("task_id"),
                content=args.get("content", {}),
            )
            await self._emit_latest_event()
            return

        if tool == "set_agent_state":
            agent = self.store.agents[args["agent_id"]]
            prev = agent.status
            agent.status = args.get("status", agent.status)
            agent.current_task_id = args.get("current_task_id", agent.current_task_id)
            if "work_remaining" in args:
                agent.work_remaining = float(args["work_remaining"])
            self.store.add_event(
                type="agent.status_changed",
                actor_id=agent.id,
                summary=args.get("summary", f"{agent.name} status changed"),
                refs={"task_id": agent.current_task_id},
                payload={"status": [prev, agent.status]},
            )
            await self._emit_latest_event()
            return

        if tool == "run_task_executor":
            task_id = args["task_id"]
            executor_name = args["executor"]
            actor_id = args.get("actor_id", "ops")
            # Run executor off the event loop; subprocess-heavy tasks can otherwise stall API responses.
            result = await asyncio.to_thread(
                self.executors.run,
                name=executor_name,
                store=self.store,
                task_id=task_id,
                actor_id=actor_id,
                config=args.get("config", {}),
            )
            evt_type = "task.executor_succeeded" if result.ok else "task.executor_failed"
            self.store.add_event(
                type=evt_type,
                actor_id=actor_id,
                summary=result.summary,
                refs={"task_id": task_id, "artifact_id": result.artifact_id},
                payload={"executor": executor_name, "result": result.details or {}},
                source="orchestrator",
            )
            await self._emit_latest_event()
            if result.ok and task_id in self.store.tasks:
                self.store.update_task(task_id, status="Done")
                await self._emit_latest_event()
            return

    async def _emit_latest_event(self) -> None:
        await self.emit({"type": "event", "data": self.store.event_to_dict(self.store.events[0])})

    async def execute_approved_action_gates(self) -> None:
        approved = [
            a
            for a in self.store.approvals.values()
            if a.kind == "action_gate" and a.status == "Approved" and not bool((a.payload or {}).get("executed"))
        ]
        for apr in approved:
            payload = apr.payload or {}
            raw_action = payload.get("action")
            safe_actions, errors = sanitize_actions([raw_action], default_agent_id=apr.requested_by, max_actions=1)
            if errors or not safe_actions:
                payload["executed"] = True
                payload["gate_status"] = "invalid_action"
                payload["execution_error"] = "; ".join(errors[:2]) if errors else "invalid action"
                self.store.add_event(
                    type="action_gate.execution_failed",
                    actor_id="ops",
                    summary=f"{apr.id} rejected action could not execute",
                    refs={"approval_id": apr.id},
                    payload={"errors": errors[:2]},
                    source="orchestrator",
                )
                await self._emit_latest_event()
                continue

            payload["executed"] = True
            payload["gate_status"] = "executing"
            ok = await self._execute_with_retry(
                safe_actions[0], source="action_gate", actor_id=apr.decision_by or "ceo"
            )
            payload["gate_status"] = "executed" if ok else "execution_failed"
            evt_type = "action_gate.executed" if ok else "action_gate.execution_failed"
            evt_summary = (
                f"{apr.id} approved action executed" if ok else f"{apr.id} approved action failed during execution"
            )
            self.store.add_event(
                type=evt_type,
                actor_id=apr.decision_by or "ceo",
                summary=evt_summary,
                refs={"approval_id": apr.id},
                payload={"action": safe_actions[0]},
                source="orchestrator",
            )
            await self._emit_latest_event()

    async def process_action_gate_timeouts(self) -> None:
        pending = [a for a in self.store.approvals.values() if a.kind == "action_gate" and a.status == "Pending"]
        now = datetime.now(timezone.utc)
        for apr in pending:
            try:
                created = datetime.fromisoformat(apr.created_at)
            except Exception:
                continue
            age_sec = (now - created.astimezone(timezone.utc)).total_seconds()
            if age_sec < self.action_gate_ttl_seconds:
                continue

            apr.payload = apr.payload or {}
            apr.payload["gate_status"] = "stale_timeout"
            apr.payload["executed"] = False
            apr.payload["expired_after_seconds"] = self.action_gate_ttl_seconds
            self.store.decide_approval(apr.id, "reject", "ops")
            await self._emit_latest_event()
            self.store.add_event(
                type="action_gate.stale",
                actor_id="ops",
                summary=f"{apr.id} expired after {self.action_gate_ttl_seconds}s",
                refs={"approval_id": apr.id},
                payload={"age_seconds": round(age_sec, 2)},
                source="orchestrator",
            )
            await self._emit_latest_event()

    async def auto_manage_meetings(self) -> None:
        self._meeting_tick += 1
        scheduled = [m for m in self.store.meetings.values() if m.status == "Scheduled"]
        ongoing = [m for m in self.store.meetings.values() if m.status == "Ongoing"]

        # 1) create meeting sometimes when there is none active
        if not scheduled and not ongoing and random.random() < 0.08:
            topic, agenda = random.choice(MEETING_TOPICS)
            participants = random.sample(list(self.store.agents.keys()), k=min(3, len(self.store.agents)))
            created_by = random.choice(participants) if participants else "ops"
            self.store.create_meeting(
                title=topic,
                agenda=agenda,
                participant_ids=participants,
                created_by=created_by,
                source="orchestrator",
            )
            await self._emit_latest_event()
            scheduled = [m for m in self.store.meetings.values() if m.status == "Scheduled"]

        # 2) start one scheduled meeting
        if scheduled and not ongoing and random.random() < 0.35:
            m = scheduled[0]
            actor = m.participant_ids[0] if m.participant_ids else "ops"
            self.store.start_meeting(m.id, actor_id=actor, source="orchestrator")
            await self._emit_latest_event()
            ongoing = [x for x in self.store.meetings.values() if x.status == "Ongoing"]

        # 3) ongoing meetings add notes and then close
        for m in ongoing:
            actor = random.choice(m.participant_ids) if m.participant_ids else "ops"
            # deterministic cadence so users can visibly verify meeting automation
            if self._meeting_tick % 3 == 0:
                note = random.choice(MEETING_NOTES)
                decision = random.choice(MEETING_DECISIONS) if random.random() < 0.45 else None
                action = random.choice(MEETING_ACTION_ITEMS) if random.random() < 0.6 else None
                self.store.add_meeting_note(
                    m.id,
                    note=note,
                    author_id=actor,
                    decision=decision,
                    action_item={"text": action, "created_by": actor} if action else None,
                    source="orchestrator",
                )
                await self._emit_latest_event()

            # close after enough notes
            if len(m.notes) >= 3 and self._meeting_tick % 5 == 0:
                self.store.close_meeting(m.id, actor_id=actor, source="orchestrator")
                await self._emit_latest_event()

    async def auto_optimize_experiments(self) -> None:
        self._experiment_tick += 1
        if self._experiment_tick % 6 != 0:
            return

        running = [e for e in self.store.experiments.values() if e.status == "Running"]
        if not running:
            name, hypo = random.choice(EXPERIMENT_HYPOTHESES)
            released = [p for p in self.store.game_projects.values() if p.status == "Released"]
            project_id = ""
            if released:
                released.sort(key=lambda x: x.updated_at, reverse=True)
                project_id = released[0].id
            self.store.create_experiment(
                name=name,
                hypothesis=hypo,
                primary_metric="retention_d1",
                variants=["A", "B"],
                project_id=project_id,
                created_by="ceo",
            )
            await self._emit_latest_event()
            return

        exp = running[0]
        exp_meta = {"experiment_id": exp.id, "variant": "", "project_id": exp.project_id}
        for variant in exp.variants:
            exposures = random.randint(7, 18)
            for _ in range(exposures):
                self.store.record_experiment_exposure(exp.id, variant=variant, user_id=f"sim_{random.randint(1, 9999)}")
                if exp.project_id:
                    self.store.add_kpi_event(
                        "engagement.session_start",
                        user_id=f"sim_{random.randint(1, 9999)}",
                        value=1,
                        meta={**exp_meta, "variant": variant},
                        source="orchestrator",
                    )
            conv_rate = 0.12 if variant == "A" else 0.16
            conversions = int(exposures * conv_rate)
            for _ in range(conversions):
                self.store.record_experiment_conversion(
                    exp.id,
                    variant=variant,
                    value=1.0,
                    user_id=f"sim_{random.randint(1, 9999)}",
                )
                if exp.project_id:
                    self.store.add_kpi_event(
                        "revenue",
                        user_id=f"sim_{random.randint(1, 9999)}",
                        value=round(random.uniform(0.69, 2.99), 2),
                        meta={**exp_meta, "variant": variant},
                        source="orchestrator",
                    )

        exposure_total = sum(exp.exposures.values())
        if exposure_total < 140:
            return

        best_variant = None
        best_score = -1.0
        for variant in exp.variants:
            ex = max(1, int(exp.exposures.get(variant, 0)))
            cv = float(exp.conversions.get(variant, 0.0))
            score = cv / ex
            if score > best_score:
                best_score = score
                best_variant = variant
        if not best_variant:
            return

        self.store.close_experiment(exp.id, winner_variant=best_variant, actor_id="ceo")
        await self._emit_latest_event()
        self.store.create_task(
            title=f"Roll out experiment winner {exp.id}/{best_variant}",
            description=f"Ship winning variant from {exp.name} (score={best_score:.3f}).",
            type="OPS",
            priority="P1",
            assignee_id=None,
        )
        if exp.project_id:
            self.store.create_task(
                title=f"[{exp.project_id}] Apply winner {exp.id}/{best_variant}",
                description=f"Project-linked experiment rollout with winner score={best_score:.3f}",
                type="DEV",
                priority="P1",
                assignee_id=None,
            )
            self.store.add_event(
                type="experiment.project_rollout_planned",
                actor_id="ceo",
                summary=f"{exp.id} winner rollout planned for {exp.project_id}",
                refs={},
                payload={"experiment_id": exp.id, "project_id": exp.project_id, "winner": best_variant},
                source="orchestrator",
            )
        await self._emit_latest_event()

    async def auto_generate_kpi_events(self) -> None:
        self._kpi_tick += 1
        if self._kpi_tick % 2 != 0:
            return
        released = [p for p in self.store.game_projects.values() if p.status == "Released"]
        target_project_id = ""
        if released:
            released.sort(key=lambda x: x.updated_at, reverse=True)
            target_project_id = released[0].id
        meta = {"project_id": target_project_id} if target_project_id else {}
        installs = random.randint(12, 40)
        sessions = random.randint(installs, installs * 3)
        purchases = random.randint(0, max(1, installs // 4))
        revenue = round(purchases * random.uniform(0.99, 5.49), 2)

        for _ in range(installs):
            self.store.add_kpi_event(
                "acquisition.install",
                user_id=f"u_{random.randint(1, 500000)}",
                value=1,
                meta=meta,
                source="orchestrator",
            )
        for _ in range(sessions):
            self.store.add_kpi_event(
                "engagement.session_start",
                user_id=f"u_{random.randint(1, 500000)}",
                value=1,
                meta=meta,
                source="orchestrator",
            )
        if purchases > 0:
            each = round(revenue / purchases, 2)
            for _ in range(purchases):
                self.store.add_kpi_event(
                    "revenue",
                    user_id=f"u_{random.randint(1, 500000)}",
                    value=each,
                    meta=meta,
                    source="orchestrator",
                )
        await self._emit_latest_event()

    async def auto_manage_releases(self) -> None:
        # 0) Internal automation only: CEO agent auto-decides pending approvals.
        # Human handles only the final launch approval in UI.
        pending_approvals = [a for a in self.store.approvals.values() if a.status == "Pending"]
        for apr in pending_approvals:
            self.store.decide_approval(apr.id, "approve", decision_by="ceo")
            await self._emit_latest_event()

        # 1) Auto-fill review checklist so project can become final-approval ready.
        for gp in list(self.store.game_projects.values()):
            if not gp.release_id:
                continue
            rel = self.store.releases.get(gp.release_id)
            if not rel:
                continue
            apr = self.store.approvals.get(rel.approval_id)
            if not apr:
                continue
            if not self.store.project_has_meeting_alignment(gp.id):
                self.store.ensure_alignment_meeting_for_project(gp.id, created_by="ceo")
                self.store.add_event(
                    type="release.gate_waiting_meeting_alignment",
                    actor_id="ceo",
                    summary=f"{gp.id} release gate paused: meeting alignment required",
                    refs={"approval_id": apr.id},
                    payload={"project_id": gp.id, "release_id": rel.id},
                    source="orchestrator",
                )
                await self._emit_latest_event()
                continue

            if not self.store.can_confirm_project_release(gp.id):
                self.store.update_project_review(
                    gp.id,
                    checklist_updates={
                        "no_personal_data": True,
                        "no_third_party_ip": True,
                        "license_checked": True,
                        "policy_checked": True,
                    },
                    notes="자동 검토: 기본 정책 체크리스트 통과",
                    reviewer_id="ceo",
                )
                await self._emit_latest_event()

            if apr.status == "Approved" and not rel.final_confirmed and self.store.can_confirm_project_release(gp.id):
                quality = self.store.evaluate_project_quality(gp.id)
                originality = self.store.evaluate_project_originality(gp.id)
                kpi_gate = self.store.release_kpi_gate(since_minutes=180, project_id=gp.id)
                target_builds = 4
                originality_ok = (
                    float(originality.get("originality_score", 0.0)) >= 58.0
                    and float(originality.get("imitation_risk", 100.0)) <= 55.0
                )
                low_traffic = int(kpi_gate.get("sessions", 0) or 0) < 90 and int(kpi_gate.get("installs", 0) or 0) < 25
                kpi_ok = bool(kpi_gate.get("passed")) or (low_traffic and quality >= 82.0 and gp.demo_build_count >= 5)
                if gp.demo_build_count >= target_builds and quality >= 76.0 and kpi_ok and originality_ok:
                    self.store.submit_project_for_human_approval(
                        gp.id,
                        reason=(
                            f"Auto QA+KPI pass complete (quality={quality:.1f}, "
                            f"builds={gp.demo_build_count}, kpi={kpi_gate['score']:.1f}, "
                            f"orig={originality.get('originality_score', 0.0):.1f})"
                        ),
                        actor_id="ceo",
                    )
                    await self._emit_latest_event()
                elif gp.demo_build_count >= target_builds and quality >= 78.0 and not originality_ok:
                    self.store.create_task(
                        title=f"[{gp.id}] Differentiation polish sprint",
                        description="유사도 위험을 낮추도록 핵심 메커닉/비주얼 피드백을 차별화합니다.",
                        type="DEV",
                        priority="P1",
                        assignee_id=None,
                    )
                    self.store.add_event(
                        type="game_project.submit_blocked_originality",
                        actor_id="ceo",
                        summary=f"{gp.id} submission blocked by originality gate",
                        refs={"approval_id": apr.id},
                        payload={"project_id": gp.id, "originality": originality},
                        source="orchestrator",
                    )
                    await self._emit_latest_event()
                elif gp.demo_build_count >= target_builds and quality >= 76.0:
                    self.store.create_task(
                        title=f"[{gp.id}] KPI hypothesis sprint",
                        description="온보딩/난이도/보상 루프 가설 1개를 선택해 KPI 개선 실험을 수행합니다.",
                        type="OPS",
                        priority="P1",
                        assignee_id=None,
                    )
                    self.store.add_event(
                        type="game_project.submit_blocked",
                        actor_id="ceo",
                        summary=f"{gp.id} submission blocked by KPI gate",
                        refs={"approval_id": apr.id},
                        payload={"project_id": gp.id, "kpi_gate": kpi_gate},
                        source="orchestrator",
                    )
                    await self._emit_latest_event()
                else:
                    self.store.add_event(
                        type="game_project.submit_deferred",
                        actor_id="ceo",
                        summary=f"{gp.id} submission deferred (quality/build not enough)",
                        refs={"approval_id": apr.id},
                        payload={
                            "project_id": gp.id,
                            "quality": quality,
                            "builds": gp.demo_build_count,
                            "required_quality": 78.0,
                            "required_builds": target_builds,
                        },
                        source="orchestrator",
                    )
                    await self._emit_latest_event()

        # 1.5) rollout train automation: Canary -> 50% -> 100%, rollback on poor KPI.
        for gp in list(self.store.game_projects.values()):
            if not gp.release_id:
                continue
            rel = self.store.releases.get(gp.release_id)
            if not rel or not rel.final_confirmed or rel.status != "Deployed":
                continue
            if rel.rollout_stage in {"Full", "RolledBack"}:
                continue
            gate = self.store.release_kpi_gate(since_minutes=180, project_id=gp.id)
            if not gate["passed"] and rel.rollout_stage in {"Canary", "Stage50"}:
                self.store.rollback_release(rel.id, reason=f"KPI gate failed score={gate['score']:.1f}", actor_id="qa")
                self.store.create_task(
                    title=f"[{gp.id}] Rollback analysis and hotfix",
                    description=f"Release {rel.id} rolled back due to KPI score {gate['score']:.1f}. Analyze cause and patch.",
                    type="DEV",
                    priority="P0",
                    assignee_id=None,
                )
                await self._emit_latest_event()
                continue
            # advance gradually when KPI is healthy
            if gate["passed"] and random.random() < 0.55:
                self.store.advance_release_rollout(rel.id, actor_id="ops")
                await self._emit_latest_event()

        # 2) Stop creating new releases while one is pending internal gate or waiting human final approval.
        if any(r.status in {"PendingApproval", "Approved"} and not bool(r.final_confirmed) for r in self.store.releases.values()):
            return

        # 3) Build a release candidate from recent completed QA/DEV work.
        done_candidates = [
            t for t in self.store.tasks.values()
            if t.status == "Done" and t.type in {"QA", "DEV"}
        ]
        if not done_candidates:
            return
        done_candidates.sort(key=lambda t: t.updated_at, reverse=True)
        base = done_candidates[0]
        version = f"0.1.{random.randint(2, 99)}"
        self.store.create_release_candidate(
            version=version,
            title=f"Auto release from {base.id}",
            task_id=base.id,
            requested_by="qa",
            notes="Automated release proposal from orchestrator",
        )
        await self._emit_latest_event()

    async def auto_post_release_improvements(self) -> None:
        self._release_tick += 1
        if self._release_tick % 8 != 0:
            return

        released = [p for p in self.store.game_projects.values() if p.status == "Released"]
        if not released:
            return

        released.sort(key=lambda g: g.updated_at, reverse=True)
        for gp in released[:3]:
            kpi_gate = self.store.release_kpi_gate(since_minutes=180, project_id=gp.id)
            tag = f"[{gp.id}]"
            open_tasks = [
                t for t in self.store.tasks.values()
                if tag in str(t.title or "") and t.status in {"Todo", "Doing", "Blocked"}
            ]
            if open_tasks:
                continue
            # Not enough live traffic yet for this project.
            if kpi_gate["installs"] == 0 and kpi_gate["sessions"] == 0 and random.random() < 0.8:
                continue
            # If KPI is healthy, iterate less frequently.
            if kpi_gate["passed"] and random.random() < 0.65:
                continue

            self.store.create_task(
                title=f"{tag} Post-launch KPI 개선 스프린트",
                description=(
                    f"Release {gp.release_version or '-'} 성과 점검 및 개선안 생성 "
                    f"(kpi_score={kpi_gate['score']:.1f})"
                ),
                type="OPS",
                priority="P1",
                assignee_id=None,
            )
            self.store.create_task(
                title=f"{tag} Patch: retention/난이도 조정",
                description="초반 이탈 구간 개선, 난이도 곡선 및 보상 밸런스 조정",
                type="DEV",
                priority="P1",
                assignee_id=None,
            )
            self.store.create_task(
                title=f"{tag} QA 회귀 테스트 (라이브)",
                description="핵심 루프, 크래시, 튜토리얼, 결제/보상 경로 재검증",
                type="QA",
                priority="P1",
                assignee_id=None,
            )
            self.store.add_event(
                type="release.post_launch_iteration_started",
                actor_id="ceo",
                summary=f"{gp.id} post-launch improvement sprint opened",
                refs={},
                payload={"project_id": gp.id, "kpi_gate": kpi_gate},
                source="orchestrator",
            )
            await self._emit_latest_event()

    async def auto_refine_learning_policy(self) -> None:
        self._learning_tick += 1
        if self._learning_tick % 10 != 0:
            return
        released = [p for p in self.store.game_projects.values() if p.status == "Released"]
        if not released:
            return
        released.sort(key=lambda x: x.updated_at, reverse=True)
        # Learn from one recent released project per cycle.
        target = released[0]
        learned = self.store.learn_from_project_outcome(target.id, reason="periodic_runtime")
        if learned:
            await self._emit_latest_event()

    async def auto_scan_trends(self) -> None:
        self._trend_tick += 1
        if self._trend_tick % 5 != 0:
            return
        for _ in range(2):
            topic, genre, score, source, summary = random.choice(TREND_SIGNALS)
            jitter = random.uniform(-0.06, 0.06)
            self.store.add_trend_signal(
                topic=topic,
                genre=genre,
                score=max(0.0, min(1.0, score + jitter)),
                source=source,
                summary=summary,
            )
        added = self.store.refresh_mode_extensions_from_trends(max_new=1)
        if added > 0:
            self.store.add_event(
                type="mode_extension.refresh_completed",
                actor_id="mkt",
                summary=f"Trend scan added {added} new mode extension(s)",
                refs={},
                payload={"added": added, "total": len(self.store.mode_extensions)},
                source="orchestrator",
            )
        await self._emit_latest_event()

    async def auto_drive_game_factory(self) -> None:
        self._project_tick += 1
        if self._project_tick % 4 != 0:
            return

        # 1) Ensure at least one ideation meeting exists for trend discussion.
        active_meetings = [m for m in self.store.meetings.values() if m.status in {"Scheduled", "Ongoing"}]
        if not active_meetings and len(self.store.trend_signals) >= 2:
            top = sorted(list(self.store.trend_signals)[:8], key=lambda t: t.score, reverse=True)[:2]
            topic = f"Trend-to-Game Meeting: {top[0].genre} + {top[1].genre}"
            agenda = f"Pick next game concept from trends: {top[0].topic} / {top[1].topic}"
            self.store.create_meeting(
                title=topic,
                agenda=agenda,
                participant_ids=["ceo", "mkt", "dev_a", "qa"],
                created_by="ceo",
                source="orchestrator",
            )
            await self._emit_latest_event()

        # 2) For each project, drive lifecycle automatically.
        projects = sorted(self.store.game_projects.values(), key=lambda g: g.created_at)
        for gp in projects:
            # Meeting-first governance: no execution without closed meeting decisions/action items.
            if not self.store.project_has_meeting_alignment(gp.id):
                self.store.ensure_alignment_meeting_for_project(gp.id, created_by="ceo")
                await self._emit_latest_event()
                continue

            if gp.status == "Ideation":
                self.store.ensure_project_tasks(gp.id)
                if not gp.demo_url:
                    self.store.generate_project_demo(gp.id, actor_id="dev_a")
                self.store.update_game_project_status(gp.id, "Prototype", actor_id="ceo")
                await self._emit_latest_event()
                continue

            if gp.status in {"Prototype", "QA"}:
                self.store.ensure_project_tasks(gp.id)
                if not gp.demo_url:
                    self.store.generate_project_demo(gp.id, actor_id="dev_a")
                    await self._emit_latest_event()
                target_builds = 4
                # Agent-driven upgrade executor: developers improve game quality through structured passes.
                if gp.task_ids and (gp.demo_build_count < target_builds or random.random() < 0.28):
                    dev_tasks = [
                        tid for tid in gp.task_ids
                        if tid in self.store.tasks and self.store.tasks[tid].type == "DEV"
                    ]
                    if dev_tasks:
                        await self.execute(
                            {
                                "tool": "run_task_executor",
                                    "args": {
                                        "task_id": dev_tasks[0],
                                        "executor": "project_autoupgrade",
                                        "actor_id": random.choice(["dev_a", "dev_b"]),
                                        "config": {"project_id": gp.id},
                                    },
                                }
                            )
                        await self._emit_latest_event()
                done = [tid for tid in gp.task_ids if tid in self.store.tasks and self.store.tasks[tid].status == "Done"]
                dev_done = [
                    tid for tid in gp.task_ids
                    if tid in self.store.tasks and self.store.tasks[tid].type == "DEV" and self.store.tasks[tid].status == "Done"
                ]
                qa_all = [
                    tid for tid in gp.task_ids
                    if tid in self.store.tasks and self.store.tasks[tid].type == "QA"
                ]
                qa_done = [
                    tid for tid in qa_all
                    if tid in self.store.tasks and self.store.tasks[tid].status == "Done"
                ]
                if dev_done and len(qa_done) < len(qa_all):
                    if gp.status != "QA":
                        self.store.update_game_project_status(gp.id, "QA", actor_id="qa")
                        await self._emit_latest_event()
                # execution hardening: generate build + git evidence before release gate.
                health = self.store.project_artifact_health(gp.id)
                if dev_done and health["test_build_reports"] < 1:
                    await self.execute(
                        {
                            "tool": "run_task_executor",
                            "args": {
                                "task_id": dev_done[0],
                                "executor": "dev_test_build",
                                "actor_id": "dev_b",
                                "config": {
                                    "commands": [os.getenv("STUDIO_BUILD_CMD", "python -m compileall -q app")],
                                    "timeout_sec": 240,
                                },
                            },
                        }
                    )
                    await self._emit_latest_event()
                if dev_done and health["git_reports"] < 1 and random.random() < 0.7:
                    await self.execute(
                        {
                            "tool": "run_task_executor",
                            "args": {
                                "task_id": dev_done[0],
                                "executor": "dev_git_ops",
                                "actor_id": "dev_b",
                                "config": {"operation": "status", "timeout_sec": 120},
                            },
                        }
                    )
                    await self._emit_latest_event()
                # only request release after a minimum quality pass
                quality = self.store.evaluate_project_quality(gp.id)
                if gp.demo_build_count >= target_builds and len(done) >= max(2, len(gp.task_ids) - 1) and quality >= 70.0:
                    self.store.try_prepare_project_release(gp.id, requested_by="qa")
                    await self._emit_latest_event()
                continue


async def run_orchestrator(store: Store, emit: Callable[[Dict[str, Any]], Awaitable[None]]) -> None:
    """
    Rule-based orchestrator skeleton.
    Later, replace action planning with real LLM responses that emit only tool actions.
    """
    tick = 1.0
    tools = ToolExecutor(store, emit)
    planner = PlannerRouter()
    while True:
        await asyncio.sleep(tick)
        if not store.auto_run:
            continue

        try:
            speed = max(0.2, min(store.speed, 5.0))
            dt = tick * speed

            # 0) Gate maintenance + execute approved high-risk actions
            await tools.process_action_gate_timeouts()
            await tools.execute_approved_action_gates()
            await tools.auto_scan_trends()
            await tools.auto_manage_meetings()
            await tools.auto_drive_game_factory()
            await tools.auto_optimize_experiments()
            await tools.auto_generate_kpi_events()
            await tools.auto_manage_releases()
            await tools.auto_post_release_improvements()
            await tools.auto_refine_learning_policy()

            # 1) Progress current work
            for agent in store.agents.values():
                if agent.status in ("Working", "Testing", "Drafting") and agent.work_remaining > 0:
                    agent.work_remaining -= dt
                    if agent.work_remaining > 0:
                        continue

                    task_id = agent.current_task_id
                    if task_id and task_id in store.tasks:
                        task = store.tasks[task_id]
                        skill_before = store.agent_skill_score_for_task(agent.id, task.type)
                        if task.type in ("DEV", "QA"):
                            executor_name = "dev_test_build"
                            executor_config: Dict[str, Any] = {}
                            if task.type == "QA":
                                executor_config = {
                                    "commands": [
                                        os.getenv("STUDIO_QA_TEST_CMD", "python -m compileall -q app"),
                                    ],
                                    "timeout_sec": 240,
                                }
                            await tools.execute(
                                {
                                    "tool": "run_task_executor",
                                    "args": {
                                        "task_id": task_id,
                                        "executor": executor_name,
                                        "actor_id": agent.id,
                                        "config": executor_config,
                                    },
                                }
                            )
                        else:
                            await tools.execute(
                                {"tool": "update_task", "args": {"task_id": task_id, "changes": {"status": "Done"}}}
                            )
                        grown = store.improve_agent_skills(agent.id, task.type, delta=random.uniform(0.2, 0.7))
                        if grown:
                            skill_after = store.agent_skill_score_for_task(agent.id, task.type)
                            store.add_event(
                                type="agent.skill_improved",
                                actor_id=agent.id,
                                summary=f"{agent.name} skill improved on {task.type} ({skill_before:.1f}->{skill_after:.1f})",
                                refs={"task_id": task_id},
                                payload={"task_type": task.type, "updated_skills": grown},
                                source="orchestrator",
                            )
                            await emit({"type": "event", "data": store.event_to_dict(store.events[0])})
                        if task.type == "MKT" and random.random() < 0.35:
                            await tools.execute(
                                {
                                    "tool": "request_approval",
                                    "args": {
                                        "kind": "post",
                                        "title": "Approve community post draft",
                                        "requested_by": agent.id,
                                        "payload": {"task_id": task_id},
                                    },
                                }
                            )
                        if task.type in ("DEV", "QA") and random.random() < 0.2:
                            await tools.execute(
                                {
                                    "tool": "request_approval",
                                    "args": {
                                        "kind": "release",
                                        "title": "Approve release candidate build",
                                        "requested_by": "qa",
                                        "payload": {"related_task": task_id},
                                    },
                                }
                            )

                    await tools.execute(
                        {
                            "tool": "set_agent_state",
                            "args": {
                                "agent_id": agent.id,
                                "status": "Idle",
                                "current_task_id": None,
                                "work_remaining": 0,
                                "summary": f"{agent.name} finished work and is now idle",
                            },
                        }
                    )

            # 2) Plan actions for idle agents (LLM-first, rule fallback)
            for agent in store.agents.values():
                plan = await planner.plan_for_agent(store, agent)
                if plan.error:
                    store.add_event(
                        type="orchestrator.plan_fallback",
                        actor_id="ops",
                        summary=f"{agent.name} plan fallback: {plan.error[:120]}",
                        refs={},
                        payload={"agent_id": agent.id, "source": plan.source},
                        source="orchestrator",
                    )
                    await emit({"type": "event", "data": store.event_to_dict(store.events[0])})
                await tools.execute_many(plan.actions, default_agent_id=agent.id, source=plan.source)
        except Exception as exc:
            store.add_event(
                type="orchestrator.loop_error",
                actor_id="ops",
                summary=f"orchestrator loop error: {str(exc)[:160]}",
                refs={},
                payload={"error": str(exc)},
                source="orchestrator",
            )
            await emit({"type": "event", "data": store.event_to_dict(store.events[0])})
