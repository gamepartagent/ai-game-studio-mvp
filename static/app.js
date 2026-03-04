const state = {
  control: { auto_run: true, speed: 1.0 },
  agents: [],
  tasks: [],
  approvals: [],
  artifacts: [],
  meetings: [],
  releases: [],
  game_projects: [],
  events: [],
  kpi_events: [],
  trend_signals: [],
  experiments: [],
  mode_extensions: [],
  completion: null,
  project_kpi: null,
  learning: null,
};

let currentTab = "details";
const kanbanView = {
  todoLimit: 18,
  doingLimit: 10,
  doneLimit: 12,
  showDone: false,
  showAllTodo: false,
  showAllDoing: false,
  showAllDone: false,
};

function $(id){ return document.getElementById(id); }
function esc(v){ return String(v ?? "").replace(/[&<>"']/g, (m) => ({"&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;","'":"&#39;"}[m])); }
function short(v, n=80){ const s = String(v ?? ""); return s.length > n ? s.slice(0,n-1)+"…" : s; }

const statusKo = { Todo:"할 일", Doing:"진행중", Done:"완료" };
const typeKo = { DEV:"개발", QA:"QA", MKT:"마케팅", OPS:"운영", CEO:"전략" };

function taskByStatus(st){ return (state.tasks || []).filter(t => t.status === st); }

function renderKanban(){
  const pr = { P0: 0, P1: 1, P2: 2 };
  const todoAll = taskByStatus("Todo").sort((a, b) => (pr[a.priority] ?? 9) - (pr[b.priority] ?? 9));
  const doingAll = taskByStatus("Doing").sort((a, b) => (pr[a.priority] ?? 9) - (pr[b.priority] ?? 9));
  const doneAll = taskByStatus("Done").sort((a, b) => String(b.updated_at || "").localeCompare(String(a.updated_at || "")));

  const todo = kanbanView.showAllTodo ? todoAll : todoAll.slice(0, kanbanView.todoLimit);
  const doing = kanbanView.showAllDoing ? doingAll : doingAll.slice(0, kanbanView.doingLimit);
  const done = kanbanView.showAllDone ? doneAll : doneAll.slice(0, kanbanView.doneLimit);
  $("taskCount").textContent = String((state.tasks || []).length);

  const priColor = (p) => {
    if(p === "P0") return "#ff6b6b";
    if(p === "P1") return "#36c9a2";
    return "#b0bccb";
  };
  const card = (t) => `
    <div class="task-node ${String(t.status || "").toLowerCase()}" style="--pri:${priColor(t.priority)}">
      <div class="task-node-top">
        <div class="task-node-id">${esc(t.id)}</div>
        <div class="task-node-pri">${esc(t.priority || "P2")}</div>
      </div>
      <div class="task-node-title">${esc(short(t.title, 64))}</div>
      <div class="task-node-foot">
        <div class="task-node-type">${esc(typeKo[t.type] || t.type || "-")}</div>
        <div class="task-node-assignee"><i style="background:${priColor(t.priority)}"></i>${esc(t.assignee_id || "미할당")}</div>
      </div>
    </div>
  `;

  $("colTodo").innerHTML = `
    <div style="display:flex;gap:6px;align-items:center;justify-content:space-between;">
      <div class="hint">우선순위 높은 순</div>
      ${todoAll.length > kanbanView.todoLimit ? `<button class="btn js-kanban-toggle" data-k="todo">${kanbanView.showAllTodo ? "접기" : "더보기"} (${todoAll.length})</button>` : ""}
    </div>
    ${todo.map(card).join("") || `<div class="hint">할 일이 없습니다.</div>`}
  `;

  $("colDoing").innerHTML = `
    <div style="display:flex;gap:6px;align-items:center;justify-content:space-between;">
      <div class="hint">핵심 진행 업무</div>
      ${doingAll.length > kanbanView.doingLimit ? `<button class="btn js-kanban-toggle" data-k="doing">${kanbanView.showAllDoing ? "접기" : "더보기"} (${doingAll.length})</button>` : ""}
    </div>
    ${doing.map(card).join("") || `<div class="hint">진행중 업무가 없습니다.</div>`}
  `;

  $("colDone").innerHTML = `
    <div style="display:flex;gap:6px;align-items:center;justify-content:space-between;">
      <div class="hint">최근 완료 중심</div>
      <button class="btn js-kanban-toggle" data-k="done-open">${kanbanView.showDone ? "완료 숨기기" : "완료 보기"} (${doneAll.length})</button>
    </div>
    ${kanbanView.showDone
      ? `${done.map(card).join("") || `<div class="hint">완료 업무가 없습니다.</div>`}
         ${doneAll.length > kanbanView.doneLimit ? `<div style="margin-top:6px;"><button class="btn js-kanban-toggle" data-k="done">${kanbanView.showAllDone ? "접기" : "더보기"} (${doneAll.length})</button></div>` : ""}`
      : `<div class="hint" style="padding:8px 0;">완료 업무는 기본 숨김입니다.</div>`}
  `;

  document.querySelectorAll(".js-kanban-toggle").forEach((b) => {
    b.onclick = () => {
      const k = b.dataset.k;
      if(k === "todo") kanbanView.showAllTodo = !kanbanView.showAllTodo;
      if(k === "doing") kanbanView.showAllDoing = !kanbanView.showAllDoing;
      if(k === "done") kanbanView.showAllDone = !kanbanView.showAllDone;
      if(k === "done-open") kanbanView.showDone = !kanbanView.showDone;
      renderKanban();
    };
  });
}

function renderOffice(){
  $("agentCount").textContent = String((state.agents || []).length);
  const tasksById = Object.fromEntries((state.tasks || []).map((t) => [t.id, t]));
  const ongoingMeetings = (state.meetings || []).filter((m) => m.status === "Ongoing");
  const meetingSet = new Set(ongoingMeetings.flatMap((m) => m.participant_ids || []));
  const agents = state.agents || [];

  const stations = agents.map((a, idx) => {
    const inMeeting = meetingSet.has(a.id);
    const busy = a.status !== "Idle" && !inMeeting;
    const sx = idx % 3;
    const sy = Math.floor(idx / 3);
    const task = a.current_task_id ? tasksById[a.current_task_id] : null;
    const taskText = inMeeting
      ? "회의실 이동"
      : (task ? short(task.title, 42) : "다음 업무 대기 중");
    const workRem = Number(a.work_remaining || 0);

    return `
      <div class="office-station" style="--sx:${sx}; --sy:${sy};">
        <div class="agent-seat-card ${busy ? "busy" : "idle"} ${inMeeting ? "in-meeting" : ""}">
          <div class="seat-header">
            <span class="seat-number">#${idx + 1}</span>
            <span class="seat-live ${inMeeting ? "meeting" : (busy ? "live" : "")}">
              ${inMeeting ? "회의중" : (busy ? "업무중" : "대기")}
            </span>
          </div>
          <div class="seat-scene ${busy ? "active" : ""}">
            <div class="seat-monitor"><span>${esc(typeKo[(task && task.type) || "OPS"] || "업무")}</span></div>
            <div class="seat-desk"></div>
            <div class="seat-chair"></div>
            <div class="seat-worker ${inMeeting ? "away" : ""}" style="--agent-color:${a.color || "#7aa2ff"}">
              <div class="seat-head"></div>
              <div class="seat-body"></div>
            </div>
            ${inMeeting ? `<div class="seat-meeting-flag">회의 진행 중</div>` : ""}
          </div>
          <div class="seat-name">${esc(a.name)}</div>
          <div class="seat-task">${esc(taskText)}</div>
          <div class="seat-time">남은 시간 ${inMeeting ? "-" : workRem.toFixed(1)}초</div>
        </div>
      </div>
    `;
  }).join("");

  $("office").innerHTML = `
    <div class="office-skyline"></div>
    <div class="office-floor"></div>
    <svg class="office-links" viewBox="0 0 100 100" preserveAspectRatio="none" aria-hidden="true">
      <path class="office-link task_flow" d="M17,52 C28,44 39,60 50,52 C61,44 72,60 83,52"></path>
      <path class="office-link approval_flow" d="M17,79 C28,71 39,87 50,79 C61,71 72,87 83,79"></path>
    </svg>
    ${stations || `<div class="hint" style="padding:12px;">에이전트 정보가 없습니다.</div>`}
  `;
}

function renderMeetingRoom(){
  const ongoing = (state.meetings || []).filter(m => m.status === "Ongoing");
  const scheduled = (state.meetings || []).filter(m => m.status === "Scheduled");
  const done = (state.meetings || []).filter(m => m.status === "Done");
  const active = ongoing[0] || null;
  const participants = [...new Set((active?.participant_ids || []))]
    .map((id) => (state.agents || []).find((a) => a.id === id))
    .filter(Boolean);
  const latestNote = active?.notes?.length ? active.notes[active.notes.length - 1].text : "";
  const speakingId = active?.notes?.length ? active.notes[active.notes.length - 1].author_id : "";

  $("meetingRoom").innerHTML = `
    <div class="meeting-head">
      <h3>회의실</h3>
      <div class="hint">예정 ${scheduled.length} · 진행중 ${ongoing.length} · 종료 ${done.length}</div>
    </div>
    <div class="meeting-stage cinematic">
      <div class="meeting-stage-title">컨퍼런스 룸</div>
      <div class="meeting-room-scene">
        <div class="meeting-screen">
          <div class="meeting-screen-title">${active ? esc(active.title) : "진행 중인 회의 없음"}</div>
          <div class="meeting-screen-sub">${active ? esc(active.agenda || "아젠다 없음") : "참여자 대기 중..."}</div>
        </div>
        <div class="meeting-table"></div>
        ${participants.length
          ? participants.map((a, idx) => `
              <div class="meeting-seat seat-${idx % 6} entering ${speakingId === a.id ? "speaking" : ""}" style="--agent-color:${a.color || "#7aa2ff"}">
                <div class="meeting-actor">
                  <div class="meeting-actor-head"></div>
                  <div class="meeting-actor-body"></div>
                </div>
                <div class="meeting-actor-name">${esc(a.name)}</div>
              </div>
            `).join("")
          : `<div class="meeting-empty">현재 회의실에 참여자가 없습니다</div>`
        }
        ${latestNote ? `<div class="meeting-bubble">${esc(short(latestNote, 88))}</div>` : ""}
      </div>
    </div>
  `;
}

function renderFeed(){
  const rows = (state.events || []).slice(0,120).map((e) => `
    <div class="feed-item">
      <div class="feed-top">
        <div class="feed-sum">${esc(short(e.summary || e.type, 96))}</div>
        <div class="feed-ts">${esc((e.ts || "").replace("T"," ").slice(0,19))}</div>
      </div>
      <div class="feed-meta">
        <span class="badge">${esc(e.id || "-")}</span>
        <span>${esc(e.actor_id || "-")}</span>
        <span>${esc(e.type || "-")}</span>
      </div>
    </div>
  `).join("");
  $("feed").innerHTML = rows || `<div class="hint">이벤트가 없습니다.</div>`;
}

function renderDetails(){
  const el = $("tab-details");
  if(!el) return;
  const doing = taskByStatus("Doing").slice(0,8);
  el.innerHTML = `
    <div class="detail">
      <h3>현재 진행중</h3>
      ${doing.length ? doing.map(t => `<div class="kv"><div>${esc(t.id)}</div><b>${esc(short(t.title, 44))}</b></div>`).join("") : `<div class="hint">진행중 업무가 없습니다.</div>`}
    </div>
  `;
}

function renderPreview(){
  const el = $("tab-preview");
  if(!el) return;
  const p = [...(state.game_projects||[])].sort((a,b)=>String(b.id).localeCompare(String(a.id)))[0];
  if(!p){ el.innerHTML = `<div class="hint">프로젝트가 없습니다.</div>`; return; }
  const mode = (p.game_blueprint && (p.game_blueprint.mode_base || p.game_blueprint.mode)) || "-";
  const modeLabel = (p.game_blueprint && (p.game_blueprint.mode_label || p.game_blueprint.mode)) || "-";
  const k = state.project_kpi && state.project_kpi.project_id === p.id ? state.project_kpi : null;
  const meetingsById = Object.fromEntries((state.meetings || []).map((m) => [m.id, m]));
  const linked = (p.meeting_ids || []).map((id) => meetingsById[id]).filter(Boolean);
  const latestMeeting = linked.sort((a,b)=>String(b.created_at||"").localeCompare(String(a.created_at||"")))[0];
  const aligned = !!(latestMeeting && latestMeeting.status === "Done" && (latestMeeting.decisions||[]).length > 0 && (latestMeeting.action_items||[]).length > 0);
  const controlHintMap = {
    aim: "마우스로 표적을 클릭해 점수를 올립니다.",
    runner: "스페이스바로 점프하며 장애물을 피합니다.",
    dodge: "화살표 키로 이동해 충돌을 피합니다.",
    clicker: "화면 클릭으로 코인을 수집합니다.",
    memory: "카드를 뒤집어 같은 짝을 맞춥니다.",
    rhythm: "화면 클릭 후 A/S/D/F 키로 노트를 맞춥니다.",
  };
  const controlHint = controlHintMap[mode] || "화면 클릭 후 키보드/마우스로 조작해보세요.";
  el.innerHTML = `
    <div class="detail">
      <h3>${esc(p.id)} · ${esc(p.title || "프로젝트")}</h3>
      <div class="hint">${esc(p.genre || "-")} · ${esc(p.status || "-")} · 모드 ${esc(modeLabel)} · 빌드 ${esc(String(p.demo_build_count || 0))}</div>
      <div class="hint">회의 합의 상태: ${aligned ? "정렬됨(실행 가능)" : "대기(회의 결정/액션 필요)"}</div>
      ${k ? `<div class="hint">프로젝트 KPI(최근 ${esc(String(k.since_minutes || 180))}분): 설치 ${esc(String(k.installs || 0))} · 세션 ${esc(String(k.sessions || 0))} · 매출 ${esc(Number(k.revenue_total || 0).toFixed(2))}</div>` : ""}
      ${p.demo_url ? `
        <div style="display:flex;gap:8px;align-items:center;margin:8px 0 10px;">
          <a class="btn" href="${esc(p.demo_url)}" target="_blank" rel="noopener">데모 새 창 열기</a>
          <button class="btn js-preview-reload">프리뷰 새로고침</button>
          <span class="hint">조작: ${esc(controlHint)}</span>
        </div>
        <iframe id="previewFrame" src="${esc(p.demo_url)}" style="width:100%;height:560px;border:1px solid var(--line);border-radius:12px;background:#101726;"></iframe>
      ` : `<div class="hint">데모가 아직 생성되지 않았습니다.</div>`}
    </div>
  `;

  const btn = el.querySelector(".js-preview-reload");
  if(btn){
    btn.onclick = () => {
      const frame = $("previewFrame");
      if(!frame) return;
      frame.src = p.demo_url + (p.demo_url.includes("?") ? "&" : "?") + "t=" + Date.now();
      setTimeout(() => frame.focus(), 80);
    };
  }
}

function renderAgentsTab(){
  const el = $("tab-agents");
  if(!el) return;
  el.innerHTML = `
    <div class="detail">
      <h3>에이전트</h3>
      ${(state.agents||[]).map(a => `<div class="kv"><div>${esc(a.name)}</div><b>${esc(a.status || "Idle")}</b></div>`).join("") || `<div class="hint">없음</div>`}
    </div>
  `;
}

function renderApprovals(){
  const el = $("tab-approvals");
  if(!el) return;
  const pending = (state.approvals||[]).filter(a => a.status === "Pending");
  const decided = (state.approvals||[]).filter(a => a.status !== "Pending").slice(0,30);
  el.innerHTML = `
    <div class="detail">
      <h3>승인 대기</h3>
      ${pending.length ? pending.map(a => `
        <div class="detail" style="margin-bottom:8px;">
          <div class="kv"><div>${esc(a.id)} · ${esc(a.kind)}</div><b>${esc(short(a.title,40))}</b></div>
          <div style="display:flex;gap:8px;margin-top:8px;">
            <button class="btn js-apr" data-id="${esc(a.id)}" data-d="approve">승인</button>
            <button class="btn js-apr" data-id="${esc(a.id)}" data-d="reject">거절</button>
          </div>
        </div>
      `).join("") : `<div class="hint">대기 승인 없음</div>`}
      <h3 style="margin-top:12px;">최근 결정</h3>
      ${decided.map(a => `<div class="kv"><div>${esc(a.id)}</div><b>${esc(a.status)}</b></div>`).join("") || `<div class="hint">기록 없음</div>`}
    </div>
  `;

  for(const b of el.querySelectorAll('.js-apr')){
    b.onclick = async () => {
      await decideApproval(b.dataset.id, b.dataset.d);
      await fetchState();
    };
  }
}

function renderKpi(){
  const el = $("tab-kpi");
  if(!el) return;
  const arr = state.kpi_events || [];
  const revenue = arr.filter(x=>x.event_type==="revenue").reduce((s,x)=>s+Number(x.value||0),0);
  el.innerHTML = `
    <div class="detail">
      <h3>KPI 요약</h3>
      <div class="kv"><div>이벤트 수</div><b>${arr.length}</b></div>
      <div class="kv"><div>매출 합계</div><b>${revenue.toFixed(2)}</b></div>
    </div>
  `;
}

function renderReleases(){
  const el = $("tab-releases");
  if(!el) return;
  const rows = [...(state.releases||[])].sort((a,b)=>String(b.id).localeCompare(String(a.id)));
  el.innerHTML = `
    <div class="detail">
      <h3>릴리즈</h3>
      ${rows.length ? rows.map(r => `
        <div class="detail" style="margin-bottom:8px;">
          <div class="kv">
            <div>${esc(r.id)} · ${esc(r.version)}</div><b>${esc(r.status)}</b>
            <div>롤아웃</div><b>${esc(`${r.rollout_stage || "PreDeploy"} · ${Number(r.rollout_percent || 0)}%`)}</b>
            <div>차단 여부</div><b>${r.rollout_blocked ? "차단됨" : "정상"}</b>
          </div>
          ${r.rollback_reason ? `<div class="hint">롤백 사유: ${esc(short(r.rollback_reason, 90))}</div>` : ""}
        </div>
      `).join("") : `<div class="hint">릴리즈 없음</div>`}
    </div>
  `;
}

function projectChecklistDone(p){
  const c = p?.review_checklist || {};
  return !!(c.no_personal_data && c.no_third_party_ip && c.license_checked && c.policy_checked);
}

function kpiGateClient(){
  const arr = state.kpi_events || [];
  const recent = arr.slice(0, 240);
  let installs = 0, sessions = 0, revenue = 0;
  for(const e of recent){
    if(e.event_type === "acquisition.install") installs += 1;
    if(e.event_type === "engagement.session_start") sessions += 1;
    if(e.event_type === "revenue") revenue += Number(e.value || 0);
  }
  const spi = installs > 0 ? (sessions / installs) : 0;
  const passed = installs >= 8 && sessions >= 14 && revenue >= 2.0 && spi >= 1.1;
  return { passed, installs, sessions, revenue, spi };
}

function renderProjectsTab(){
  const el = $("tab-projects");
  if(!el) return;
  const events = state.events || [];
  const artifacts = state.artifacts || [];
  const taskById = Object.fromEntries((state.tasks || []).map((t) => [t.id, t]));

  function upgradeHistoryFor(pid){
    const rows = [];
    for(const e of events){
      const ep = e.payload || {};
      const hit = ep.project_id === pid || String(e.summary || "").includes(pid);
      if(!hit) continue;
      if(!["game_project.auto_upgraded","game_project.demo_upgraded","task.executor_succeeded"].includes(e.type)) continue;
      if(e.type === "task.executor_succeeded"){
        const ex = (ep.result && ep.result.executor) || ep.executor;
        if(ex !== "project_autoupgrade") continue;
      }
      const ts = String(e.ts || "").replace("T"," ").slice(0,19);
      const pick = ep.upgrade_pick ? ` · ${ep.upgrade_pick}` : "";
      const build = ep.build_count ? ` · build ${ep.build_count}` : "";
      rows.push(`${ts} · ${e.type}${pick}${build}`);
      if(rows.length >= 3) break;
    }

    const reports = artifacts
      .filter((a) => String(a.title || "").includes(`Auto-upgrade report for ${pid}`))
      .sort((a,b)=>String(b.updated_at||"").localeCompare(String(a.updated_at||"")))
      .slice(0,2)
      .map((a) => {
        const ts = String(a.updated_at || a.created_at || "").replace("T"," ").slice(0,19);
        const task = a.task_id && taskById[a.task_id] ? short(taskById[a.task_id].title, 36) : "-";
        return `${ts} · 리포트 ${a.id} · ${task}`;
      });
    return { rows, reports, reportCount: artifacts.filter((a) => String(a.title || "").includes(`Auto-upgrade report for ${pid}`)).length };
  }

  const rows = [...(state.game_projects||[])]
    .filter((p) => p.submitted_for_human || p.status === "Released")
    .sort((a,b)=>String(b.id).localeCompare(String(a.id)));
  el.innerHTML = `
    <div class="detail">
      <h3>프로젝트 (최종본)</h3>
      <div class="hint" style="margin-bottom:8px;">에이전트가 품질 기준을 통과해 제출한 최종 후보만 표시됩니다.</div>
      ${rows.length ? rows.map(p => `
        <div class="detail" style="margin-bottom:8px;">
          ${(() => { const h = upgradeHistoryFor(p.id); return `
          <div class="kv">
            <div>${esc(p.id)} · ${esc(short(p.title,28))}</div><b>${esc(p.status || "-")}</b>
            <div>장르/모드</div><b>${esc(p.genre || "-")} / ${esc((p.game_blueprint && p.game_blueprint.mode) || "-")}</b>
            <div>품질 점수</div><b>${esc(Number(p.quality_score || 0).toFixed(1))}</b>
            <div>업그레이드 리포트</div><b>${esc(String(h.reportCount))}건</b>
          </div>
          ${p.submission_reason ? `<div class="hint" style="margin-top:6px;">제출 사유: ${esc(short(p.submission_reason, 120))}</div>` : ""}
          <div class="hint" style="margin-top:6px;">최근 업그레이드 히스토리</div>
          ${h.rows.length ? h.rows.map((x)=>`<div class="hint">${esc(x)}</div>`).join("") : `<div class="hint">최근 업그레이드 이벤트 없음</div>`}
          ${h.reports.length ? `<div class="hint" style="margin-top:4px;">최근 업그레이드 리포트</div>${h.reports.map((x)=>`<div class="hint">${esc(x)}</div>`).join("")}` : ""}
          <div style="display:flex;gap:8px;margin-top:8px;">
            ${p.demo_url ? `<a class="btn" href="${esc(p.demo_url)}" target="_blank" rel="noopener">데모 시연</a>` : ""}
          </div>
        </div>` })()}
      `).join("") : `<div class="hint">아직 제출된 최종본이 없습니다. 에이전트가 업그레이드 후 제출하면 여기에 올라옵니다.</div>`}
    </div>
  `;
}

function renderFinalApprovalTab(){
  const el = $("tab-final_approval");
  if(!el) return;

  const rows = (state.game_projects || []).map((p) => {
    const rel = (state.releases || []).find((r) => r.id === p.release_id);
    const kpi = kpiGateClient();
    const quality = Number(p.quality_score || 0);
    const reasons = [];
    if(!p.submitted_for_human) reasons.push("에이전트 최종 제출 대기");
    if(!rel) reasons.push("릴리즈 후보 없음");
    if(rel && rel.status !== "Approved") reasons.push(`내부 승인 대기(${rel.status})`);
    if(rel && rel.final_confirmed) reasons.push("이미 최종 승인 완료");
    if(!projectChecklistDone(p)) reasons.push("법무/정책 체크리스트 미완료");
    if(Number(p.demo_build_count || 0) < 2) reasons.push("데모 빌드 수 부족");
    if(quality < 70) reasons.push("품질 점수 70 미만");
    if(!kpi.passed) reasons.push("KPI 게이트 미통과");
    const ready = reasons.length === 0;
    return { p, rel, ready, reasons, kpi };
  });

  const readyRows = rows.filter(x => x.ready);
  const waitingRows = rows.filter(x => !x.ready && (!x.rel || !x.rel.final_confirmed));

  el.innerHTML = `
    <div class="detail">
      <h3>최종 승인</h3>
      <div class="hint" style="margin-bottom:8px;">내부 승인 완료 후 여기서만 출시합니다.</div>
      <h3>출시 가능</h3>
      ${readyRows.length ? readyRows.map(({p,rel,kpi}) => `
        <div class="detail" style="margin-bottom:8px;">
          <div class="kv">
            <div>${esc(p.id)} · ${esc(short(p.title,30))}</div><b>${esc(rel.id)} / ${esc(rel.version)}</b>
            <div>장르/모드</div><b>${esc(p.genre || "-")} / ${esc((p.game_blueprint && p.game_blueprint.mode) || "-")}</b>
            <div>품질 점수</div><b>${esc(Number(p.quality_score || 0).toFixed(1))}</b>
            <div>KPI 게이트</div><b>${esc(`통과 (설치 ${kpi.installs}, 세션 ${kpi.sessions}, 매출 ${kpi.revenue.toFixed(2)})`)}</b>
          </div>
          ${p.submission_reason ? `<div class="hint" style="margin-top:6px;">제출 사유: ${esc(short(p.submission_reason, 120))}</div>` : ""}
          <div style="display:flex;gap:8px;margin-top:8px;">
            ${p.demo_url ? `<a class="btn" href="${esc(p.demo_url)}" target="_blank" rel="noopener">데모 시연</a>` : ""}
            <button class="btn btn-primary js-final" data-id="${esc(p.id)}">최종 승인 후 출시</button>
          </div>
        </div>
      `).join("") : `<div class="hint">지금은 출시 가능한 프로젝트가 없습니다.</div>`}

      <h3 style="margin-top:12px;">대기 중</h3>
      ${waitingRows.length ? waitingRows.map(({p,rel,reasons,kpi}) => `
        <div class="detail" style="margin-bottom:8px;">
          <div class="kv">
            <div>${esc(p.id)} ${esc(short(p.title,24))}</div><b>${esc(rel ? rel.status : "-")}</b>
            <div>KPI 상태</div><b>${esc(`설치 ${kpi.installs} / 세션 ${kpi.sessions} / 매출 ${kpi.revenue.toFixed(2)}`)}</b>
          </div>
          <div class="hint">대기 사유: ${esc(reasons.slice(0,3).join(", ") || "-")}</div>
        </div>
      `).join("") : `<div class="hint">대기 항목 없음</div>`}
    </div>
  `;

  for(const b of el.querySelectorAll('.js-final')){
    b.onclick = async () => {
      const msg = prompt("최종 승인 코멘트를 입력하세요.", "출시 승인: 기본 QA/정책/KPI 게이트 확인");
      if(msg === null) return;
      await confirmDeployProject(b.dataset.id, msg);
      await fetchState();
    };
  }
}

function renderMinutesTab(){
  const el = $("tab-minutes");
  if(!el) return;
  const statusMap = { Scheduled: "예정", Ongoing: "진행중", Done: "종료" };
  const agentById = Object.fromEntries((state.agents || []).map((a) => [a.id, a.name]));
  const toDate = (v) => {
    if(!v) return null;
    const d = new Date(v);
    return Number.isNaN(d.getTime()) ? null : d;
  };
  const durationText = (m) => {
    const s = toDate(m.started_at || m.created_at);
    const e = toDate(m.ended_at || (m.status === "Done" ? m.updated_at : null));
    if(!s) return "-";
    const end = e || new Date();
    const mins = Math.max(0, Math.round((end - s) / 60000));
    return `${mins}분`;
  };
  const oneLine = (m) => {
    const decision = (m.decisions || [])[0];
    const note = (m.notes || [])[0]?.text;
    return short(decision || note || m.agenda || m.title || "-", 78);
  };
  const impactMeta = (m) => {
    const text = `${m.title || ""} ${m.agenda || ""} ${(m.decisions || []).join(" ")} ${(m.action_items || []).map((x) => x.text || "").join(" ")}`.toLowerCase();
    const highKeys = ["release", "ship", "go/no-go", "출시", "릴리즈", "승인", "중단", "blocker", "crash", "매출", "revenue"];
    const midKeys = ["qa", "테스트", "밸런스", "balance", "kpi", "지표", "실험", "ab", "a/b", "튜토리얼"];
    let score = 0;
    score += (m.decisions?.length || 0) * 2;
    score += (m.action_items?.length || 0);
    if(highKeys.some((k) => text.includes(k))) score += 5;
    if(midKeys.some((k) => text.includes(k))) score += 2;
    if(score >= 8) return { label: "중요도 높음", color: "#ff6b6b", bg: "#ffe7e7" };
    if(score >= 4) return { label: "중요도 보통", color: "#b06a00", bg: "#fff1dc" };
    return { label: "중요도 낮음", color: "#2f6bb0", bg: "#eaf4ff" };
  };
  const meetings = [...(state.meetings||[])].sort((a,b)=>String(b.id).localeCompare(String(a.id)));
  el.innerHTML = `
    <div class="detail">
      <h3>회의록</h3>
      <div class="hint" style="margin-bottom:8px;">한 줄 요약 중심으로 최근 회의를 빠르게 확인합니다.</div>
      ${meetings.length ? meetings.map(m => `
        <div class="detail" style="margin-bottom:8px;">
          <div style="display:flex;justify-content:flex-end;">
            <span class="badge" style="background:${impactMeta(m).bg}; color:${impactMeta(m).color}; border-color:${impactMeta(m).color};">${impactMeta(m).label}</span>
          </div>
          <div class="kv">
            <div>${esc(m.id)} · ${esc(short(m.title,28))}</div><b>${esc(statusMap[m.status] || m.status || "-")}</b>
            <div>참여자</div><b>${esc(((m.participant_ids || []).map((id) => agentById[id] || id)).join(", ") || "-")}</b>
            <div>주제</div><b>${esc(short(m.agenda || "-", 60))}</b>
            <div>회의 시간</div><b>${esc(durationText(m))}</b>
          </div>
          <div class="hint">요약: ${esc(oneLine(m))}</div>
          <div class="hint">진행도: 노트 ${m.notes?.length || 0} · 결정 ${m.decisions?.length || 0} · 액션 ${m.action_items?.length || 0}</div>
        </div>
      `).join("") : `<div class="hint">회의 기록 없음</div>`}
    </div>
  `;
}

function renderAbout(){
  const el = $("tab-about");
  if(!el) return;
  const c = state.completion || {};
  const d = c.details || {};
  const l = state.learning || {};
  const mb = l.mode_bias || {};
  const topMode = Object.entries(mb).sort((a,b)=>Number(b[1]||0)-Number(a[1]||0))[0];
  el.innerHTML = `
    <div class="about">
      <h3>소개</h3>
      <p>AI 에이전트 회사 운영 대시보드입니다.</p>
      <div class="detail" style="margin-top:10px;">
        <div class="kv">
          <div>운영 인프라 완성도</div><b>${esc(Number(c.infra_percent || 0).toFixed(1))}%</b>
          <div>자율 수익화 완성도</div><b>${esc(Number(c.business_percent || 0).toFixed(1))}%</b>
          <div>프로젝트(제출/출시)</div><b>${esc(`${d.projects_submitted_for_human || 0} / ${d.projects_released || 0}`)}</b>
          <div>평균 품질 점수</div><b>${esc(Number(d.average_quality || 0).toFixed(1))}</b>
          <div>학습 누적 건수</div><b>${esc(String(l.outcomes_count || 0))}</b>
          <div>현재 선호 모드</div><b>${esc(topMode ? `${topMode[0]} (${Number(topMode[1]).toFixed(2)})` : "-")}</b>
        </div>
      </div>
    </div>
  `;
}

function setTab(tab){
  currentTab = tab;
  for(const b of document.querySelectorAll('.tab')) b.classList.toggle('active', b.dataset.tab === tab);
  const tabs = ["details","preview","agents","approvals","kpi","releases","projects","final_approval","minutes","about"];
  for(const t of tabs){
    const el = $(`tab-${t}`);
    if(el) el.classList.toggle('hidden', t !== tab);
  }

  if(tab === "details") renderDetails();
  if(tab === "preview"){
    renderPreview();
    const p = [...(state.game_projects||[])].sort((a,b)=>String(b.id).localeCompare(String(a.id)))[0];
    if(p && p.id) fetchProjectKpi(p.id);
  }
  if(tab === "agents") renderAgentsTab();
  if(tab === "approvals") renderApprovals();
  if(tab === "kpi") renderKpi();
  if(tab === "releases") renderReleases();
  if(tab === "projects") renderProjectsTab();
  if(tab === "final_approval") renderFinalApprovalTab();
  if(tab === "minutes") renderMinutesTab();
  if(tab === "about"){
    renderAbout();
    fetchCompletion();
    fetchLearning();
  }
}

function renderAll(){
  renderKanban();
  renderOffice();
  renderMeetingRoom();
  renderFeed();
  renderDetails();
  renderPreview();
  renderAgentsTab();
  renderApprovals();
  renderKpi();
  renderReleases();
  renderProjectsTab();
  renderFinalApprovalTab();
  renderMinutesTab();
  renderAbout();
  if($("btnAutoplay")) $("btnAutoplay").textContent = `자동 실행: ${state.control?.auto_run ? "ON" : "OFF"}`;
}

async function fetchState(){
  const res = await fetch('/api/state');
  const snap = await res.json();
  Object.assign(state, snap);
  renderAll();
}

async function fetchCompletion(){
  try{
    const res = await fetch('/api/completion');
    const data = await res.json();
    if(data && data.completion) state.completion = data.completion;
    if(currentTab === "about") renderAbout();
  }catch(_){ }
}

async function fetchLearning(){
  try{
    const res = await fetch('/api/learning/status');
    const data = await res.json();
    if(data && data.learning) state.learning = data.learning;
    if(currentTab === "about") renderAbout();
  }catch(_){ }
}

async function fetchProjectKpi(projectId){
  try{
    const res = await fetch(`/api/projects/${projectId}/kpi_summary?since_minutes=180`);
    const data = await res.json();
    if(data && data.summary) state.project_kpi = data.summary;
    if(currentTab === "preview") renderPreview();
  }catch(_){ }
}

function connectWS(){
  const proto = location.protocol === 'https:' ? 'wss' : 'ws';
  const ws = new WebSocket(`${proto}://${location.host}/ws`);
  ws.onmessage = (ev) => {
    try{
      const msg = JSON.parse(ev.data);
      if(msg.type === 'snapshot' && msg.data){ Object.assign(state, msg.data); renderAll(); }
      if(msg.type === 'event' && msg.data){
        state.events = [msg.data, ...(state.events||[])].slice(0,120);
        renderFeed();
      }
    }catch(_){ }
  };
}

async function setControl(patch){
  await fetch('/api/control', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(patch) });
}

async function decideApproval(id, decision){
  await fetch(`/api/approvals/${id}/decision`, { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({decision}) });
}

async function generateProjectDemo(projectId){
  await fetch(`/api/projects/${projectId}/generate_demo`, { method:'POST' });
}

async function confirmDeployProject(projectId, comment){
  await fetch(`/api/projects/${projectId}/confirm_deploy`, {
    method:'POST',
    headers:{'Content-Type':'application/json'},
    body: JSON.stringify({ confirmer_id: "human_ceo", comment: String(comment || "") }),
  });
}

function setupUI(){
  for(const b of document.querySelectorAll('.tab')) b.onclick = () => setTab(b.dataset.tab);

  if($("btnAutoplay")){
    $("btnAutoplay").onclick = async () => {
      await setControl({ auto_run: !(state.control?.auto_run) });
      await fetchState();
    };
  }

  const modal = $("modal");
  if($("btnNewTask")) $("btnNewTask").onclick = () => modal.classList.remove('hidden');
  if($("btnCloseModal")) $("btnCloseModal").onclick = () => modal.classList.add('hidden');
  if(modal) modal.onclick = (e) => { if(e.target === modal) modal.classList.add('hidden'); };

  if($("btnCreate")){
    $("btnCreate").onclick = async () => {
      const body = {
        title: $("newTitle").value || "새 업무",
        description: $("newDesc").value || "",
        type: $("newType").value,
        priority: $("newPri").value,
      };
      await fetch('/api/tasks', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(body) });
      $("newTitle").value = "";
      $("newDesc").value = "";
      modal.classList.add('hidden');
      await fetchState();
    };
  }

  setTab('details');
}

(async function init(){
  setupUI();
  await fetchState();
  await fetchCompletion();
  await fetchLearning();
  const p0 = [...(state.game_projects||[])].sort((a,b)=>String(b.id).localeCompare(String(a.id)))[0];
  if(p0 && p0.id) await fetchProjectKpi(p0.id);
  connectWS();
  setInterval(() => {
    fetchState();
    fetchCompletion();
    fetchLearning();
    if(currentTab === "preview"){
      const p = [...(state.game_projects||[])].sort((a,b)=>String(b.id).localeCompare(String(a.id)))[0];
      if(p && p.id) fetchProjectKpi(p.id);
    }
  }, 10000);
})();
