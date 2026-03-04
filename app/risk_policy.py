from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict


@dataclass
class RiskPolicy:
    gate_create_artifact: bool = True
    gate_release_request: bool = True

    @classmethod
    def from_env(cls) -> "RiskPolicy":
        return cls(
            gate_create_artifact=_as_bool(os.getenv("GATE_CREATE_ARTIFACT", "1")),
            gate_release_request=_as_bool(os.getenv("GATE_RELEASE_REQUEST", "1")),
        )

    def is_high_risk(self, action: Dict[str, Any]) -> bool:
        tool = action.get("tool")
        args = action.get("args", {})
        if tool == "create_artifact" and self.gate_create_artifact:
            return True
        if tool == "request_approval" and self.gate_release_request:
            return str(args.get("kind", "")).lower() == "release"
        return False


def _as_bool(v: str) -> bool:
    return str(v).strip().lower() in {"1", "true", "yes", "on"}
