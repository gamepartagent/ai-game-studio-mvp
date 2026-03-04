from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, Optional


class SnapshotSQLite:
    """
    Persistence backend for MVP.
    - Primary: normalized tables (control/agents/tasks/approvals/events)
    - Compatibility: legacy state_snapshot row
    """

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        path = Path(db_path)
        if path.parent and not path.parent.exists():
            path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            # Normalized schema
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS control_state (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    auto_run INTEGER NOT NULL,
                    speed REAL NOT NULL,
                    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS agents (
                    id TEXT PRIMARY KEY,
                    data_json TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS tasks (
                    id TEXT PRIMARY KEY,
                    data_json TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS approvals (
                    id TEXT PRIMARY KEY,
                    data_json TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS artifacts (
                    id TEXT PRIMARY KEY,
                    data_json TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS meetings (
                    id TEXT PRIMARY KEY,
                    data_json TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS kpi_events (
                    id TEXT PRIMARY KEY,
                    seq INTEGER NOT NULL,
                    data_json TEXT NOT NULL
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_kpi_seq ON kpi_events(seq DESC)")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS experiments (
                    id TEXT PRIMARY KEY,
                    data_json TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS releases (
                    id TEXT PRIMARY KEY,
                    data_json TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS trend_signals (
                    id TEXT PRIMARY KEY,
                    seq INTEGER NOT NULL,
                    data_json TEXT NOT NULL
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_trend_seq ON trend_signals(seq DESC)")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS game_projects (
                    id TEXT PRIMARY KEY,
                    data_json TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS events (
                    id TEXT PRIMARY KEY,
                    seq INTEGER NOT NULL,
                    data_json TEXT NOT NULL
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_events_seq ON events(seq DESC)")

            # Legacy snapshot table (kept for backward compatibility)
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS state_snapshot (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    snapshot_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
                )
                """
            )
            conn.commit()

    def load(self) -> Optional[Dict[str, Any]]:
        normalized = self._load_normalized()
        if normalized is not None:
            return normalized
        return self._load_legacy_snapshot()

    def _load_normalized(self) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            ctrl = conn.execute("SELECT auto_run, speed FROM control_state WHERE id = 1").fetchone()
            if not ctrl:
                return None
            agents = [
                json.loads(r["data_json"])
                for r in conn.execute("SELECT data_json FROM agents ORDER BY id").fetchall()
            ]
            tasks = [
                json.loads(r["data_json"])
                for r in conn.execute("SELECT data_json FROM tasks ORDER BY id").fetchall()
            ]
            approvals = [
                json.loads(r["data_json"])
                for r in conn.execute("SELECT data_json FROM approvals ORDER BY id").fetchall()
            ]
            artifacts = [
                json.loads(r["data_json"])
                for r in conn.execute("SELECT data_json FROM artifacts ORDER BY id").fetchall()
            ]
            meetings = [
                json.loads(r["data_json"])
                for r in conn.execute("SELECT data_json FROM meetings ORDER BY id").fetchall()
            ]
            kpi_events = [
                json.loads(r["data_json"])
                for r in conn.execute("SELECT data_json FROM kpi_events ORDER BY seq ASC").fetchall()
            ]
            experiments = [
                json.loads(r["data_json"])
                for r in conn.execute("SELECT data_json FROM experiments ORDER BY id").fetchall()
            ]
            releases = [
                json.loads(r["data_json"])
                for r in conn.execute("SELECT data_json FROM releases ORDER BY id").fetchall()
            ]
            trend_signals = [
                json.loads(r["data_json"])
                for r in conn.execute("SELECT data_json FROM trend_signals ORDER BY seq ASC").fetchall()
            ]
            game_projects = [
                json.loads(r["data_json"])
                for r in conn.execute("SELECT data_json FROM game_projects ORDER BY id").fetchall()
            ]
            events = [
                json.loads(r["data_json"])
                for r in conn.execute("SELECT data_json FROM events ORDER BY seq ASC").fetchall()
            ]
            return {
                "control": {"auto_run": bool(ctrl["auto_run"]), "speed": float(ctrl["speed"])},
                "agents": agents,
                "tasks": tasks,
                "approvals": approvals,
                "artifacts": artifacts,
                "meetings": meetings,
                "kpi_events": kpi_events,
                "experiments": experiments,
                "releases": releases,
                "trend_signals": trend_signals,
                "game_projects": game_projects,
                "events": events,
            }

    def _load_legacy_snapshot(self) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            row = conn.execute("SELECT snapshot_json FROM state_snapshot WHERE id = 1").fetchone()
            return json.loads(row["snapshot_json"]) if row else None

    def save(self, snapshot: Dict[str, Any]) -> None:
        self._save_normalized(snapshot)
        self._save_legacy_snapshot(snapshot)

    def _save_normalized(self, snapshot: Dict[str, Any]) -> None:
        with self._connect() as conn:
            control = snapshot.get("control", {})
            conn.execute(
                """
                INSERT INTO control_state(id, auto_run, speed, updated_at)
                VALUES(1, ?, ?, datetime('now'))
                ON CONFLICT(id) DO UPDATE SET
                  auto_run=excluded.auto_run,
                  speed=excluded.speed,
                  updated_at=datetime('now')
                """,
                (1 if bool(control.get("auto_run", True)) else 0, float(control.get("speed", 1.0))),
            )

            conn.execute("DELETE FROM agents")
            conn.executemany(
                "INSERT INTO agents(id, data_json) VALUES(?, ?)",
                [(a["id"], json.dumps(a, ensure_ascii=False)) for a in snapshot.get("agents", [])],
            )

            conn.execute("DELETE FROM tasks")
            conn.executemany(
                "INSERT INTO tasks(id, data_json) VALUES(?, ?)",
                [(t["id"], json.dumps(t, ensure_ascii=False)) for t in snapshot.get("tasks", [])],
            )

            conn.execute("DELETE FROM approvals")
            conn.executemany(
                "INSERT INTO approvals(id, data_json) VALUES(?, ?)",
                [(a["id"], json.dumps(a, ensure_ascii=False)) for a in snapshot.get("approvals", [])],
            )

            conn.execute("DELETE FROM artifacts")
            conn.executemany(
                "INSERT INTO artifacts(id, data_json) VALUES(?, ?)",
                [(a["id"], json.dumps(a, ensure_ascii=False)) for a in snapshot.get("artifacts", [])],
            )

            conn.execute("DELETE FROM meetings")
            conn.executemany(
                "INSERT INTO meetings(id, data_json) VALUES(?, ?)",
                [(m["id"], json.dumps(m, ensure_ascii=False)) for m in snapshot.get("meetings", [])],
            )

            conn.execute("DELETE FROM kpi_events")
            kpi_events = snapshot.get("kpi_events", [])
            conn.executemany(
                "INSERT INTO kpi_events(id, seq, data_json) VALUES(?, ?, ?)",
                [(e["id"], idx, json.dumps(e, ensure_ascii=False)) for idx, e in enumerate(kpi_events)],
            )

            conn.execute("DELETE FROM experiments")
            conn.executemany(
                "INSERT INTO experiments(id, data_json) VALUES(?, ?)",
                [(e["id"], json.dumps(e, ensure_ascii=False)) for e in snapshot.get("experiments", [])],
            )

            conn.execute("DELETE FROM releases")
            conn.executemany(
                "INSERT INTO releases(id, data_json) VALUES(?, ?)",
                [(r["id"], json.dumps(r, ensure_ascii=False)) for r in snapshot.get("releases", [])],
            )

            conn.execute("DELETE FROM trend_signals")
            trend_signals = snapshot.get("trend_signals", [])
            conn.executemany(
                "INSERT INTO trend_signals(id, seq, data_json) VALUES(?, ?, ?)",
                [(t["id"], idx, json.dumps(t, ensure_ascii=False)) for idx, t in enumerate(trend_signals)],
            )

            conn.execute("DELETE FROM game_projects")
            conn.executemany(
                "INSERT INTO game_projects(id, data_json) VALUES(?, ?)",
                [(g["id"], json.dumps(g, ensure_ascii=False)) for g in snapshot.get("game_projects", [])],
            )

            conn.execute("DELETE FROM events")
            events = snapshot.get("events", [])
            conn.executemany(
                "INSERT INTO events(id, seq, data_json) VALUES(?, ?, ?)",
                [(e["id"], idx, json.dumps(e, ensure_ascii=False)) for idx, e in enumerate(events)],
            )
            conn.commit()

    def _save_legacy_snapshot(self, snapshot: Dict[str, Any]) -> None:
        payload = json.dumps(snapshot, ensure_ascii=False)
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO state_snapshot(id, snapshot_json, updated_at)
                VALUES(1, ?, datetime('now'))
                ON CONFLICT(id) DO UPDATE SET
                  snapshot_json=excluded.snapshot_json,
                  updated_at=datetime('now')
                """,
                (payload,),
            )
            conn.commit()
