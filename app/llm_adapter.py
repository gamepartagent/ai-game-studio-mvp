from __future__ import annotations

import json
import os
import random
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from .action_schema import sanitize_actions
from .role_policy import profile_for_agent
from .store import Agent, Store, Task


ROLE_TYPES = {
    "CEO": ["CEO"],
    "MKT": ["MKT"],
    "DEV": ["DEV"],
    "QA": ["QA"],
    "OPS": ["OPS"],
}

TASK_TEMPLATES = [
    ("DEV", "P0", "런타임 크래시 수정", "재현 경로를 찾고 원인 분석 후 패치합니다."),
    ("DEV", "P1", "튜토리얼 UX 개선", "이탈이 많은 구간의 안내/피드백을 개선합니다."),
    ("QA", "P1", "회귀 테스트 실행", "핵심 루프와 시작 흐름을 집중 검증합니다."),
    ("MKT", "P2", "패치노트 초안 작성", "변경점과 유저 영향 중심으로 요약합니다."),
    ("OPS", "P2", "라이브 운영 리포트", "어제 KPI/이슈/대응을 1장 요약으로 정리합니다."),
    ("CEO", "P1", "스프린트 우선순위 재정렬", "P0/P1 항목과 릴리즈 리스크를 재점검합니다."),
]

ALLOWED_TOOLS = [
    "create_task",
    "update_task",
    "request_approval",
    "create_artifact",
    "set_agent_state",
    "run_task_executor",
]

PRIORITY_BONUS = {"P0": 12.0, "P1": 7.0, "P2": 2.0}


@dataclass
class PlanResult:
    actions: List[Dict[str, Any]]
    source: str  # "rule" | "llm" | "llm_fallback"
    error: Optional[str] = None


def _pick_template(types: List[str]) -> Optional[Tuple[str, str, str, str]]:
    candidates = [t for t in TASK_TEMPLATES if t[0] in types]
    return random.choice(candidates) if candidates else None


def _work_status_for_role(role: str) -> str:
    if role == "QA":
        return "Testing"
    if role == "MKT":
        return "Drafting"
    return "Working"


def _estimate_work_seconds(skill_score: float, priority: str) -> float:
    base = 13.0 - (skill_score / 22.0)  # high skill -> faster
    if priority == "P0":
        base += 1.2  # high pressure tasks take longer
    jitter = random.uniform(-1.0, 1.4)
    return max(3.5, min(18.0, base + jitter))


def _task_fit_score(store: Store, agent: Agent, task: Task) -> float:
    skill = store.agent_skill_score_for_task(agent.id, task.type)
    bonus = PRIORITY_BONUS.get(task.priority, 0.0)
    backlog = max(0.0, min(4.0, (random.random() - 0.5) * 2.0))
    return skill + bonus + backlog


class RulePlanner:
    """Deterministic fallback planner that emits the same action JSON contract as LLM."""

    async def plan_for_agent(self, store: Store, agent: Agent) -> PlanResult:
        if agent.status != "Idle":
            return PlanResult(actions=[], source="rule")

        allowed_types = ROLE_TYPES.get(agent.role, [])
        todo = store.find_todo_tasks(allowed_types=allowed_types)
        actions: List[Dict[str, Any]] = []

        if not todo:
            if random.random() < 0.2:
                tpl = _pick_template(allowed_types or ["OPS"])
                if tpl:
                    ttype, pr, title, desc = tpl
                    actions.append(
                        {
                            "tool": "create_task",
                            "args": {
                                "title": title,
                                "description": desc,
                                "type": ttype,
                                "priority": pr,
                                "assignee_id": None,
                            },
                        }
                    )
            return PlanResult(actions=actions, source="rule")

        ranked = sorted(todo, key=lambda t: _task_fit_score(store, agent, t), reverse=True)
        task = ranked[0]
        skill_score = store.agent_skill_score_for_task(agent.id, task.type)
        next_status = _work_status_for_role(agent.role)
        work_remaining = _estimate_work_seconds(skill_score, task.priority)

        actions.append(
            {
                "tool": "update_task",
                "args": {"task_id": task.id, "changes": {"status": "Doing", "assignee_id": agent.id}},
            }
        )
        actions.append(
            {
                "tool": "set_agent_state",
                "args": {
                    "agent_id": agent.id,
                    "status": next_status,
                    "current_task_id": task.id,
                    "work_remaining": work_remaining,
                    "summary": (
                        f"{agent.name} started {task.id} "
                        f"(skill={skill_score:.1f}, est={work_remaining:.1f}s)"
                    ),
                },
            }
        )
        return PlanResult(actions=actions, source="rule")


class OpenAIPlanner:
    """
    Minimal OpenAI Responses API adapter.
    Set env vars to enable:
    - OPENAI_API_KEY
    - OPENAI_MODEL (optional, default: gpt-4.1-mini)
    """

    def __init__(self, api_key: str, model: str = "gpt-4.1-mini") -> None:
        self.api_key = api_key
        self.model = model

    async def plan_for_agent(self, store: Store, agent: Agent) -> PlanResult:
        if agent.status != "Idle":
            return PlanResult(actions=[], source="llm")

        payload = self._build_payload(store, agent)
        try:
            raw = self._call_responses_api(payload)
            parsed = self._parse_action_json(raw)
            actions, errors = sanitize_actions(parsed.get("actions", []), default_agent_id=agent.id, max_actions=6)
            if errors and not actions:
                raise RuntimeError("; ".join(errors[:2]))
            return PlanResult(actions=actions, source="llm")
        except Exception as exc:
            return PlanResult(actions=[], source="llm_fallback", error=str(exc))

    def _build_payload(self, store: Store, agent: Agent) -> Dict[str, Any]:
        allowed_types = ROLE_TYPES.get(agent.role, [])
        todo = store.find_todo_tasks(allowed_types=allowed_types)[:8]
        pending_approvals = [a for a in store.approvals.values() if a.status == "Pending"][:6]
        profile = profile_for_agent(agent.id, agent.role)
        ranked = sorted(todo, key=lambda t: _task_fit_score(store, agent, t), reverse=True)
        recommended_id = ranked[0].id if ranked else None

        system_prompt = (
            "You are an autonomous game studio agent. "
            "You must output JSON only with shape: "
            '{"actions":[{"tool":"...","args":{...}}]}. '
            "No markdown, no explanation. "
            f"Global allowed tools: {ALLOWED_TOOLS}. "
            f"Your role={agent.role}, title={profile.title}, department={profile.department}. "
            f"Role-allowed tools={sorted(profile.allowed_tools)}. "
            f"Can create task types={sorted(profile.can_create_task_types)}. "
            f"Can request approval kinds={sorted(profile.can_request_approval_kinds)}. "
            f"Can run executors={sorted(profile.can_execute_executors)}. "
            "Prefer the task with highest skill fit and priority unless blocked."
        )

        user_prompt = {
            "agent": {"id": agent.id, "name": agent.name, "role": agent.role},
            "job_profile": {
                "title": profile.title,
                "level": profile.level,
                "department": profile.department,
                "responsibilities": profile.responsibilities,
            },
            "capabilities": {
                "skills": dict(agent.skills or {}),
                "recommended_task_id": recommended_id,
            },
            "control": {"auto_run": store.auto_run, "speed": store.speed},
            "todo_candidates": [
                {
                    "id": t.id,
                    "title": t.title,
                    "type": t.type,
                    "priority": t.priority,
                    "status": t.status,
                    "skill_fit": round(_task_fit_score(store, agent, t), 2),
                }
                for t in todo
            ],
            "pending_approvals": [
                {"id": a.id, "kind": a.kind, "title": a.title, "requested_by": a.requested_by}
                for a in pending_approvals
            ],
            "instructions": [
                "If there is a matching todo task, assign yourself and start working.",
                "Otherwise you may create one new relevant task.",
                "Use set_agent_state when starting work.",
                "Never violate your role permissions.",
                "Never use tools outside the allowed list.",
            ],
        }
        return {
            "model": self.model,
            "input": [
                {"role": "system", "content": [{"type": "input_text", "text": system_prompt}]},
                {"role": "user", "content": [{"type": "input_text", "text": json.dumps(user_prompt)}]},
            ],
            "max_output_tokens": 500,
            "text": {"format": {"type": "json_object"}},
        }

    def _call_responses_api(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        req = urllib.request.Request(
            "https://api.openai.com/v1/responses",
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=20) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", errors="ignore")
            raise RuntimeError(f"OpenAI HTTP {e.code}: {body[:300]}") from e

    def _parse_action_json(self, response_json: Dict[str, Any]) -> Dict[str, Any]:
        if isinstance(response_json.get("output_text"), str) and response_json["output_text"].strip():
            return json.loads(response_json["output_text"])

        for item in response_json.get("output", []):
            for content in item.get("content", []):
                txt = content.get("text")
                if isinstance(txt, str) and txt.strip():
                    return json.loads(txt)
        raise RuntimeError("No parseable JSON text in OpenAI response")


class PlannerRouter:
    """Selects LLM planner when configured; falls back to rule planner."""

    def __init__(self) -> None:
        self.rule = RulePlanner()
        self.llm: Optional[OpenAIPlanner] = None
        api_key = os.getenv("OPENAI_API_KEY", "").strip()
        if api_key:
            model = os.getenv("OPENAI_MODEL", "gpt-4.1-mini").strip() or "gpt-4.1-mini"
            self.llm = OpenAIPlanner(api_key=api_key, model=model)

    async def plan_for_agent(self, store: Store, agent: Agent) -> PlanResult:
        if self.llm is None:
            return await self.rule.plan_for_agent(store, agent)

        llm_result = await self.llm.plan_for_agent(store, agent)
        if llm_result.source == "llm" and llm_result.actions:
            return llm_result

        fallback = await self.rule.plan_for_agent(store, agent)
        if llm_result.error:
            return PlanResult(actions=fallback.actions, source="llm_fallback", error=llm_result.error)
        return fallback
