from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Set


@dataclass(frozen=True)
class RoleProfile:
    role: str
    title: str
    level: str
    department: str
    responsibilities: List[str]
    allowed_tools: Set[str]
    can_create_task_types: Set[str]
    can_request_approval_kinds: Set[str]
    can_execute_executors: Set[str]


ROLE_PROFILES: Dict[str, RoleProfile] = {
    # A) CEO / Producer
    "CEO": RoleProfile(
        role="CEO",
        title="Executive Producer / Studio Head",
        level="Executive",
        department="Product Leadership",
        responsibilities=[
            "Direction, prioritization, final approval decisions",
            "Roadmap and release risk management",
        ],
        allowed_tools={"update_task", "request_approval", "set_agent_state"},
        can_create_task_types=set(),
        can_request_approval_kinds={"release_gate", "release", "action_gate", "policy", "budget", "post"},
        can_execute_executors=set(),
    ),
    # B) Product Designer
    "MKT": RoleProfile(
        role="MKT",
        title="Lead Game Designer / Product Designer",
        level="Lead",
        department="Design",
        responsibilities=[
            "Define specs, economy, and UX flow",
            "Create DEV/QA-ready requirements",
        ],
        allowed_tools={"create_task", "update_task", "set_agent_state"},
        can_create_task_types={"DEV", "QA", "MKT"},
        can_request_approval_kinds=set(),
        can_execute_executors=set(),
    ),
    # C/D) Client/Backend Engineers (shared role base, agent-specific overrides below)
    "DEV": RoleProfile(
        role="DEV",
        title="Game Engineer",
        level="Senior IC",
        department="Engineering",
        responsibilities=[
            "Implement gameplay/client/server features",
            "Open PR/build/test tasks",
        ],
        allowed_tools={"create_task", "update_task", "set_agent_state", "run_task_executor"},
        can_create_task_types={"DEV"},
        can_request_approval_kinds=set(),
        can_execute_executors={"dev_dryrun", "dev_test_build", "dev_git_ops", "project_autoupgrade"},
    ),
    # E) QA / Release Manager
    "QA": RoleProfile(
        role="QA",
        title="QA Lead / Release Manager",
        level="Lead",
        department="Quality",
        responsibilities=[
            "Testing, bug triage, release quality gate",
            "Mark Done and create release approvals",
        ],
        allowed_tools={"create_task", "update_task", "request_approval", "set_agent_state", "run_task_executor"},
        can_create_task_types={"QA"},
        can_request_approval_kinds={"release", "release_gate"},
        can_execute_executors={"dev_dryrun", "dev_test_build"},
    ),
    # F) Growth / LiveOps
    "OPS": RoleProfile(
        role="OPS",
        title="Growth & LiveOps Manager",
        level="Manager",
        department="Growth/Operations",
        responsibilities=[
            "Community/notice/event draft and live operations",
            "Request approvals for external publishing",
        ],
        allowed_tools={"create_task", "update_task", "request_approval", "create_artifact", "set_agent_state"},
        can_create_task_types={"OPS", "MKT"},
        can_request_approval_kinds={"post", "campaign", "deploy", "policy"},
        can_execute_executors=set(),
    ),
}


# Agent-level specialization to mimic real company split with 6 agents.
AGENT_PROFILE_OVERRIDES: Dict[str, Dict[str, object]] = {
    "ceo": {
        "title": "Executive Producer / Studio Head",
        "level": "Executive",
        "department": "Product Leadership",
    },
    "mkt": {
        "title": "Lead Game Designer / Product Designer",
        "level": "Lead",
        "department": "Design",
    },
    "dev_a": {
        "title": "Senior Client Engineer",
        "level": "Senior IC",
        "department": "Engineering(Client)",
        "can_execute_executors": {"dev_dryrun", "dev_test_build", "project_autoupgrade"},
    },
    "dev_b": {
        "title": "Backend/Tools Engineer",
        "level": "Senior IC",
        "department": "Engineering(Backend/Tools)",
        "can_execute_executors": {"dev_dryrun", "dev_test_build", "dev_git_ops"},
    },
    "qa": {
        "title": "QA Lead / Release Manager",
        "level": "Lead",
        "department": "Quality",
    },
    "ops": {
        "title": "Growth & LiveOps Manager",
        "level": "Manager",
        "department": "Growth/Operations",
    },
}


def profile_for_role(role: str) -> RoleProfile:
    return ROLE_PROFILES.get(role, ROLE_PROFILES["OPS"])


def profile_for_agent(agent_id: str, role: str) -> RoleProfile:
    base = profile_for_role(role)
    ov = AGENT_PROFILE_OVERRIDES.get(agent_id, {})
    return RoleProfile(
        role=base.role,
        title=str(ov.get("title", base.title)),
        level=str(ov.get("level", base.level)),
        department=str(ov.get("department", base.department)),
        responsibilities=list(base.responsibilities),
        allowed_tools=set(base.allowed_tools),
        can_create_task_types=set(base.can_create_task_types),
        can_request_approval_kinds=set(base.can_request_approval_kinds),
        can_execute_executors=set(ov.get("can_execute_executors", base.can_execute_executors)),
    )
