from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional, Tuple

from pydantic import BaseModel, ConfigDict, Field, ValidationError


ToolName = Literal["create_task", "update_task", "request_approval", "create_artifact", "set_agent_state", "run_task_executor"]
TaskType = Literal["DEV", "QA", "MKT", "OPS", "CEO"]
Priority = Literal["P0", "P1", "P2"]
TaskStatus = Literal["Todo", "Doing", "Done", "Blocked"]
AgentStatus = Literal["Idle", "Working", "Testing", "Drafting", "Reviewing"]


class CreateTaskArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")
    title: str = Field(min_length=1, max_length=200)
    description: str = ""
    type: TaskType = "DEV"
    priority: Priority = "P2"
    assignee_id: Optional[str] = None
    set_doing: bool = False


class UpdateTaskChanges(BaseModel):
    model_config = ConfigDict(extra="forbid")
    status: Optional[TaskStatus] = None
    assignee_id: Optional[str] = None
    priority: Optional[Priority] = None
    title: Optional[str] = None
    description: Optional[str] = None


class UpdateTaskArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")
    task_id: str = Field(min_length=1)
    changes: UpdateTaskChanges


class RequestApprovalArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")
    kind: str = Field(min_length=1, max_length=50)
    title: str = Field(min_length=1, max_length=200)
    requested_by: str = Field(min_length=1, max_length=50)
    payload: Dict[str, Any] = Field(default_factory=dict)


class CreateArtifactArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")
    artifact_id: Optional[str] = None
    title: str = Field(min_length=1, max_length=200)
    actor_id: str = "ops"
    task_id: Optional[str] = None
    content: Dict[str, Any] = Field(default_factory=dict)


class SetAgentStateArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")
    agent_id: str = Field(min_length=1, max_length=50)
    status: AgentStatus
    current_task_id: Optional[str] = None
    work_remaining: Optional[float] = Field(default=None, ge=0)
    summary: Optional[str] = None


class RunTaskExecutorArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")
    task_id: str = Field(min_length=1)
    executor: str = Field(min_length=1, max_length=80)
    actor_id: Optional[str] = None
    config: Dict[str, Any] = Field(default_factory=dict)


def sanitize_actions(
    actions: Any,
    *,
    default_agent_id: Optional[str] = None,
    max_actions: int = 6,
) -> Tuple[List[Dict[str, Any]], List[str]]:
    if not isinstance(actions, list):
        return [], ["actions must be a list"]

    valid: List[Dict[str, Any]] = []
    errors: List[str] = []

    for idx, raw in enumerate(actions[:max_actions]):
        if not isinstance(raw, dict):
            errors.append(f"action[{idx}] is not an object")
            continue

        tool = raw.get("tool")
        args = raw.get("args", {})
        if tool not in {"create_task", "update_task", "request_approval", "create_artifact", "set_agent_state", "run_task_executor"}:
            errors.append(f"action[{idx}] invalid tool: {tool}")
            continue
        if not isinstance(args, dict):
            errors.append(f"action[{idx}] args must be an object")
            continue

        try:
            if tool == "create_task":
                parsed = CreateTaskArgs.model_validate(args).model_dump()
            elif tool == "update_task":
                parsed = UpdateTaskArgs.model_validate(args).model_dump()
                parsed["changes"] = UpdateTaskChanges.model_validate(parsed["changes"]).model_dump(exclude_none=True)
            elif tool == "request_approval":
                parsed = RequestApprovalArgs.model_validate(args).model_dump()
            elif tool == "create_artifact":
                parsed = CreateArtifactArgs.model_validate(args).model_dump()
            elif tool == "run_task_executor":
                if default_agent_id and "actor_id" not in args:
                    args["actor_id"] = default_agent_id
                parsed = RunTaskExecutorArgs.model_validate(args).model_dump(exclude_none=True)
            else:
                if default_agent_id and "agent_id" not in args:
                    args["agent_id"] = default_agent_id
                parsed = SetAgentStateArgs.model_validate(args).model_dump(exclude_none=True)

            valid.append({"tool": tool, "args": parsed})
        except ValidationError as exc:
            msg = exc.errors()[0].get("msg", "validation error")
            errors.append(f"action[{idx}] {tool}: {msg}")

    return valid, errors
