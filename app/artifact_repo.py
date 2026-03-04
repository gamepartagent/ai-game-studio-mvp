from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict


class ArtifactRepo:
    """Stores artifact versions as JSON files under a local directory."""

    def __init__(self, root_dir: str = "data/artifacts") -> None:
        self.root = Path(root_dir)
        self.root.mkdir(parents=True, exist_ok=True)

    def save_version(self, artifact_id: str, version: int, payload: Dict[str, Any]) -> str:
        artifact_dir = self.root / artifact_id
        artifact_dir.mkdir(parents=True, exist_ok=True)
        file_path = artifact_dir / f"v{version:03d}.json"
        file_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return str(file_path).replace("\\", "/")
