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


ROLE_BASE_SKILLS: Dict[str, Dict[str, float]] = {
    "CEO": {
        "planning": 89.0,
        "risk_management": 91.0,
        "release_judgment": 88.0,
        "communication": 84.0,
    },
    "MKT": {
        "game_design": 88.0,
        "economy_balance": 82.0,
        "ux_flow": 84.0,
        "trend_research": 86.0,
        "copywriting": 75.0,
    },
    "DEV": {
        "client_gameplay": 80.0,
        "backend_api": 78.0,
        "build_tooling": 77.0,
        "debugging": 82.0,
        "performance": 76.0,
        "test_automation": 74.0,
    },
    "QA": {
        "test_design": 90.0,
        "bug_repro": 88.0,
        "release_gate": 86.0,
        "regression": 89.0,
    },
    "OPS": {
        "liveops": 85.0,
        "community": 82.0,
        "marketing_ops": 78.0,
        "kpi_analysis": 79.0,
        "release_ops": 81.0,
    },
}

TASK_SKILL_FOCUS: Dict[str, List[str]] = {
    "CEO": ["planning", "risk_management", "release_judgment"],
    "MKT": ["game_design", "ux_flow", "trend_research", "copywriting"],
    "DEV": ["client_gameplay", "backend_api", "build_tooling", "debugging", "performance"],
    "QA": ["test_design", "bug_repro", "regression", "release_gate"],
    "OPS": ["liveops", "community", "marketing_ops", "kpi_analysis", "release_ops"],
}


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
        can_execute_executors={
            "dev_dryrun",
            "dev_test_build",
            "dev_git_ops",
            "dev_github_pr",
            "dev_github_merge",
            "dev_gameplay_smoke",
            "project_autoupgrade",
        },
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
        "skills": {"planning": 92.0, "risk_management": 93.0, "release_judgment": 90.0},
    },
    "mkt": {
        "title": "Lead Game Designer / Product Designer",
        "level": "Lead",
        "department": "Design",
        "skills": {"game_design": 91.0, "trend_research": 89.0, "economy_balance": 84.0},
    },
    "dev_a": {
        "title": "Senior Client Engineer",
        "level": "Senior IC",
        "department": "Engineering(Client)",
        "can_execute_executors": {
            "dev_dryrun",
            "dev_test_build",
            "dev_github_pr",
            "dev_github_merge",
            "dev_gameplay_smoke",
            "project_autoupgrade",
        },
        "skills": {"client_gameplay": 90.0, "performance": 84.0, "backend_api": 66.0},
    },
    "dev_b": {
        "title": "Backend/Tools Engineer",
        "level": "Senior IC",
        "department": "Engineering(Backend/Tools)",
        "can_execute_executors": {
            "dev_dryrun",
            "dev_test_build",
            "dev_git_ops",
            "dev_github_pr",
            "dev_github_merge",
            "dev_gameplay_smoke",
        },
        "skills": {"backend_api": 89.0, "build_tooling": 90.0, "client_gameplay": 65.0},
    },
    "qa": {
        "title": "QA Lead / Release Manager",
        "level": "Lead",
        "department": "Quality",
        "skills": {"test_design": 92.0, "bug_repro": 91.0, "release_gate": 89.0},
    },
    "ops": {
        "title": "Growth & LiveOps Manager",
        "level": "Manager",
        "department": "Growth/Operations",
        "skills": {"liveops": 88.0, "community": 85.0, "kpi_analysis": 83.0},
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


def default_skills_for_agent(agent_id: str, role: str) -> Dict[str, float]:
    base = dict(ROLE_BASE_SKILLS.get(role, ROLE_BASE_SKILLS["OPS"]))
    ov = AGENT_PROFILE_OVERRIDES.get(agent_id, {})
    custom = dict(ov.get("skills", {}) or {})
    for key, value in custom.items():
        try:
            base[str(key)] = max(1.0, min(100.0, float(value)))
        except Exception:
            continue
    return base


def skill_focus_for_task(task_type: str) -> List[str]:
    return list(TASK_SKILL_FOCUS.get(task_type, TASK_SKILL_FOCUS["OPS"]))
