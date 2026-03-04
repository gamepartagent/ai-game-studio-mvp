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
from .store import Agent, Store


ROLE_TYPES = {
    "CEO": ["CEO"],
    "MKT": ["MKT"],
    "DEV": ["DEV"],
    "QA": ["QA"],
    "OPS": ["OPS"],
}


TASK_TEMPLATES = [
    ("DEV", "P0", "전투 장면 크래시 수정(random null ref)", "로그 조사, 재현, 패치, 테스트 추가."),
    ("DEV", "P1", "설정 메뉴 구현(오디오 슬라이더)", "UI 추가 및 설정 저장 처리."),
    ("DEV", "P1", "로딩 시간 최적화(에셋 프리패치)", "프로파일링 후 콜드스타트 지연 감소."),
    ("DEV", "P2", "UX 소규모 개선(버튼 피드백)", "햅틱/오디오/애니메이션 타이밍 조정."),
    ("QA", "P1", "스모크 테스트 실행(신규 빌드)", "온보딩/코어루프/상점/종료-재실행 점검."),
    ("QA", "P2", "리포트 버그 재현 절차 작성", "유저 리포트를 재현 가능한 절차로 정리."),
    ("MKT", "P2", "프로모션 게시물 초안 작성", "짧은 훅 + 장점 + CTA 구성."),
    ("MKT", "P2", "패치노트 요약 작성", "유저 대상 요약을 간결하게 작성."),
    ("OPS", "P2", "일일 리포트: 진행/블로커", "이벤트와 현재 스프린트 상태 요약."),
    ("OPS", "P2", "백로그 태그 정리", "업무 유형/우선순위 정규화 및 메모 보강."),
    ("CEO", "P1", "스프린트 계획 및 릴리즈 준비 검토", "백로그/릴리즈 블로커 검토."),
]

ALLOWED_TOOLS = [
    "create_task",
    "update_task",
    "request_approval",
    "create_artifact",
    "set_agent_state",
    "run_task_executor",
]


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

        task = todo[0]
        next_status = _work_status_for_role(agent.role)
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
                    "work_remaining": random.uniform(5.0, 14.0),
                    "summary": f"{agent.name} started {next_status.lower()} on {task.id}",
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
        todo = store.find_todo_tasks(allowed_types=allowed_types)[:5]
        pending_approvals = [a for a in store.approvals.values() if a.status == "Pending"][:5]
        profile = profile_for_agent(agent.id, agent.role)
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
        )
        user_prompt = {
            "agent": {"id": agent.id, "name": agent.name, "role": agent.role},
            "job_profile": {
                "title": profile.title,
                "level": profile.level,
                "department": profile.department,
                "responsibilities": profile.responsibilities,
            },
            "control": {"auto_run": store.auto_run, "speed": store.speed},
            "todo_candidates": [
                {"id": t.id, "title": t.title, "type": t.type, "priority": t.priority, "status": t.status}
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
