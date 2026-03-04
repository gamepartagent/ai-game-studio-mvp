# AI Game Studio MVP (Minimal Dashboard + Office Map)

This is a runnable MVP that simulates an "AI-only" game studio:
- 6 agents (CEO, Marketing, Dev A, Dev B, QA/Release, Ops)
- Tasks move Todo → Doing → Done
- A real-time Activity Feed updates through WebSocket events
- UI: Minimal dashboard with central office map, task list, details panel, and activity feed

> This MVP uses a **simulator** (no real LLM calls yet). Next step is to replace the simulator decisions with LLM + tool-calls.

---

## 1) Run locally

### A) Create venv + install deps
```bash
python -m venv .venv
# macOS/Linux
source .venv/bin/activate
# Windows (PowerShell)
# .venv\Scripts\Activate.ps1

pip install -r requirements.txt
```

Optional persistence path (SQLite snapshot):
```bash
# macOS/Linux
export STUDIO_DB_PATH=data/studio.db
# Windows PowerShell
$env:STUDIO_DB_PATH="data/studio.db"
# Artifact files root
export ARTIFACT_ROOT_DIR=data/artifacts
```

Risk gating policy (optional):
```bash
# defaults are enabled (1)
export GATE_CREATE_ARTIFACT=1
export GATE_RELEASE_REQUEST=1
# action gate timeout (seconds, default 900)
export ACTION_GATE_TTL_SECONDS=900
```

### B) Start server
```bash
uvicorn app.main:app --reload --port 8000
```

### C) Open the UI
Open:
- http://localhost:8000

---

## 2) What’s inside

- `app/main.py` : FastAPI app + REST + WebSocket broadcast
- `app/store.py` : in-memory store (agents/tasks/events/approvals)
- `app/simulator.py` : background loop that creates work and moves tasks
- `static/` : UI (index.html + app.js + styles.css)

---

## 3) Key endpoints

- `GET /api/state` → current agents/tasks/events/approvals
- `POST /api/tasks` → create a new task (optional)
- `POST /api/control` → set `auto_run` and `speed`
- `WS /ws` → pushes events in realtime

---

## 4) Next step (real "AI agents")
Replace `app/simulator.py` with:
- Orchestrator (who acts next)
- Tool APIs (create_task/update_task/create_artifact/request_approval)
- LLM agents (each role outputs an action list JSON)

---

## 5) LLM planner (optional)

The orchestrator now supports an LLM planner adapter with rule fallback.

Set environment variables before running:

```bash
# macOS/Linux
export OPENAI_API_KEY=your_key_here
export OPENAI_MODEL=gpt-4.1-mini

# Windows PowerShell
$env:OPENAI_API_KEY="your_key_here"
$env:OPENAI_MODEL="gpt-4.1-mini"
```

If `OPENAI_API_KEY` is not set (or if API planning fails), it automatically falls back to the built-in rule planner.

All planned actions are validated against a strict schema before execution (`app/action_schema.py`), and invalid actions are rejected with an orchestrator event log.

High-risk actions are gated:
- `create_artifact`
- `request_approval` with `kind="release"`

These are not executed immediately. They are queued as `action_gate` approvals and run only after manual approval in the Approvals tab.

Operational observability:
- `orchestrator.actions_processed` event summarizes executed vs queued actions by source (`rule`/`llm`).
- `action_gate.executed` and `action_gate.execution_failed` track post-approval execution results.
- `orchestrator.action_retry` and `orchestrator.action_failed` provide retry/failure traces.

Audit API:
- `GET /api/audit/summary` returns task/approval/event aggregates and recent runtime events.
- Query params: `source`, `event_type`, `since_minutes`, `recent_limit`.
- Includes `action_gate_by_status` aggregation.

Artifacts API:
- `GET /api/artifacts`
- `GET /api/artifacts/{artifact_id}`

Executors API:
- `GET /api/executors` (currently includes `dev_dryrun`)

Meetings API:
- `GET /api/meetings`
- `POST /api/meetings`
- `POST /api/meetings/{meeting_id}/start`
- `POST /api/meetings/{meeting_id}/note`
- `POST /api/meetings/{meeting_id}/close`

Runtime config API:
- `GET /api/runtime/config` returns DB path, LLM on/off, model, and risk-gate policy settings.

Health endpoints:
- `GET /healthz`
- `GET /readyz`

State durability:
- Runtime state is persisted to SQLite snapshot (`app/persistence.py`) so restart resumes tasks/approvals/events.

Quick smoke check:
```bash
python scripts/smoke_check.py
```
Expected output includes `SMOKE_OK`.

---

## 6) Deploy to Render (Free)

This repo now includes:
- `Dockerfile`
- `.dockerignore`
- `render.yaml`

### A) Push this project to GitHub
Render deploys from GitHub/GitLab.

### B) Create Render service
1. In Render dashboard: **New +** -> **Blueprint**
2. Select this repo
3. Render detects `render.yaml` automatically
4. Click **Apply**

### C) Environment variables
Already defined in `render.yaml`:
- `STUDIO_DB_PATH=data/studio.db`
- `ARTIFACT_ROOT_DIR=data/artifacts`
- `GATE_CREATE_ARTIFACT=1`
- `GATE_RELEASE_REQUEST=1`
- `ACTION_GATE_TTL_SECONDS=900`

Optional (if using LLM planner):
- `OPENAI_API_KEY`
- `OPENAI_MODEL` (e.g. `gpt-4.1-mini`)

### D) Notes for free tier
- Free web services can sleep when idle.
- On wake-up, first request can be slow.
- Use a paid plan later for always-on runtime.
