const MAX_BLOCK_WEIGHT = 4_000_000;
const API_POLL_INTERVAL = 4_000;
// The event stream does not announce tip changes; this poll retires stale candidates.
const TEMPLATE_REFRESH_INTERVAL = 5_000;
// The node's prioritised list only moves when a background poll changes it, so
// refreshing faster than this just re-reads the same answer.
const ACCELERATIONS_REFRESH_INTERVAL = 30_000;

const ROUTES = {
  "/dashboard/overview": "Overview",
  "/dashboard/accelerations": "mempool.space accelerations",
  "/dashboard/rsk": "RSK merge mining",
};

// Ranking criteria; keys are the backend policy names. `value` is null when a
// template cannot be scored.
const POLICIES = [
  {
    key: "highest_fees",
    label: "Highest fees",
    copy: "The candidate that pays the most in fees.",
    value: (template) => template.total_fees_sat,
  },
  {
    key: "block_weight",
    label: "Block weight",
    copy: "The candidate that fills the most of a block.",
    value: (template) => template.total_weight,
  },
];

function policyCopy(key) {
  return POLICIES.find((policy) => policy.key === key)?.copy || "";
}

// Ribbon sort options: arrival plus every policy criterion.
const TEMPLATE_SORTS = [
  {
    key: "received",
    label: "Arrival",
    value: (template) => template.received_at,
  },
  ...POLICIES,
];

// In-memory only, since the page opened.
const LOG_LIMIT = 100;

const LOG_FILTERS = [
  ["all", "All"],
  ["info", "Info"],
  ["warning", "Warnings"],
  ["error", "Errors"],
];

const state = {
  route: normalizeRoute(window.location.pathname),
  mode: localStorage.getItem("demand-mode") || "light",
  stats: {
    loading: true,
    health: null,
    pool: null,
    aggregate: null,
    system: null,
    error: null,
    errors: {},
  },
  // Copy-all text for the open modal; too large for a data attribute.
  modalCopyText: "",
  logs: [],
  logFilter: "all",
  miners: null,
  // The newest template the poll has shown, so the next one is noticed.
  newestTemplateId: null,
  // Candidate summaries from /api/templates/recent, newest first.
  templates: [],
  templatesLoaded: false,
  templatesError: null,
  // The status the templates request failed with, so the section can say
  // whether the browser needs a token or the proxy was started without one.
  templatesErrorStatus: null,
  // How many candidates the backend keeps for one tip.
  templatesCandidateLimit: null,
  // The chosen criterion, and which candidate it currently resolves to.
  policy: "highest_fees",
  policyPick: null,
  // The declaration the pool has accepted.
  activeDeclaration: null,
  // The template whose modal is open.
  openTemplateId: null,
  // How the candidate ribbon is ordered, and which kinds of candidate it shows.
  templateSort: { key: "received", direction: "desc" },
  templateFilter: "all",
  // Prioritisation state.
  // What the proxy was started with. Resolved once, before the dashboard renders.
  capabilities: null,
  polling: false,
  prioritizing: false,
  miningActivity: {
    accelerations: null,
    rsk: null,
    accelerationError: null,
    rskError: null,
    accelerationLoading: false,
    rskLoading: false,
  },
  // The proxy's API_TX_TOKEN. Guards the job declaration and prioritisation
  // endpoints alike, so both features read it from here.
  apiToken: localStorage.getItem("demand-tx-token") || "",
  tokenNotice: "",
  // Current block height, used to notice tip changes.
  blockHeight: null,
  blockSeenAt: null,
};

// null/undefined is unknown; 0 is a real value.
function known(value) {
  return value !== null && value !== undefined;
}

const ICON_PATHS = {
  dashboard:
    '<rect width="6" height="6" x="3" y="3" rx="1"/><rect width="6" height="6" x="15" y="3" rx="1"/><rect width="6" height="6" x="3" y="15" rx="1"/><rect width="6" height="6" x="15" y="15" rx="1"/>',
  history:
    '<path d="M3 12a9 9 0 1 0 3-6.7L3 8"/><path d="M3 3v5h5"/><path d="M12 7v5l3 2"/>',
  panel: '<rect width="18" height="18" x="3" y="3" rx="2"/><path d="M9 3v18"/>',
  theme:
    '<circle cx="12" cy="12" r="9"/><path d="M12 3v18M12 12l6.4-6.4M12 12l6.4 6.4"/>',
  chart: '<path d="M3 3v18h18"/><path d="M7 16v-5M12 16V7M17 16v-9"/>',
  globe:
    '<circle cx="12" cy="12" r="10"/><path d="M2 12h20M12 2a15.3 15.3 0 0 1 0 20M12 2a15.3 15.3 0 0 0 0 20"/><path d="M8 22h8"/>',
  monitor:
    '<rect width="20" height="14" x="2" y="3" rx="2"/><path d="M8 21h8M12 17v4"/>',
  server:
    '<rect width="20" height="8" x="2" y="2" rx="2"/><rect width="20" height="8" x="2" y="14" rx="2"/><path d="M6 6h.01M6 18h.01"/>',
  cpu: '<rect width="16" height="16" x="4" y="4" rx="2"/><rect width="6" height="6" x="9" y="9" rx="1"/><path d="M9 1v3M15 1v3M9 20v3M15 20v3M20 9h3M20 14h3M1 9h3M1 14h3"/>',
  activity: '<path d="M3 12h4l2-7 4 14 2-7h6"/>',
  trend: '<path d="m3 17 6-6 4 4 8-8"/><path d="M15 7h6v6"/>',
  check: '<path d="M20 6 9 17l-5-5"/>',
  checkCircle: '<circle cx="12" cy="12" r="10"/><path d="m8 12 3 3 5-6"/>',
  play: '<path d="m6 3 14 9-14 9z"/>',
  sliders:
    '<path d="M4 21v-7M4 10V3M12 21v-9M12 8V3M20 21v-5M20 12V3"/><path d="M1 14h6M9 8h6M17 16h6"/>',
  pause:
    '<rect width="4" height="16" x="6" y="4" rx="1"/><rect width="4" height="16" x="14" y="4" rx="1"/>',
  sort: '<path d="m3 8 4-4 4 4M7 4v16M21 16l-4 4-4-4M17 20V4"/>',
  plus: '<circle cx="12" cy="12" r="9"/><path d="M12 8v8M8 12h8"/>',
  x: '<path d="M18 6 6 18M6 6l12 12"/>',
  refresh: '<path d="M20 11a8 8 0 1 0 2 5.3"/><path d="M20 4v7h-7"/>',
  user: '<path d="M19 21v-2a7 7 0 0 0-14 0v2"/><circle cx="12" cy="7" r="4"/>',
  bell: '<path d="M10.3 21a2 2 0 0 0 3.4 0M18 8a6 6 0 0 0-12 0c0 7-3 7-3 9h18c0-2-3-2-3-9"/>',
  download:
    '<path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4M7 10l5 5 5-5M12 15V3"/>',
  upload:
    '<path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4M17 8l-5-5-5 5M12 3v12"/>',
  copy: '<rect width="14" height="14" x="8" y="8" rx="2"/><path d="M16 8V6a2 2 0 0 0-2-2H6a2 2 0 0 0-2 2v8a2 2 0 0 0 2 2h2"/>',
  hash: '<path d="M4 9h16M4 15h16M10 3 8 21M16 3l-2 18"/>',
  undo: '<path d="M3 7v6h6"/><path d="M3 13a9 9 0 1 0 3-7.7L3 8"/>',
  info: '<circle cx="12" cy="12" r="10"/><path d="M12 16v-4M12 8h.01"/>',
  layers:
    '<path d="m12 2 9 5-9 5-9-5 9-5Z"/><path d="m3 12 9 5 9-5"/><path d="m3 17 9 5 9-5"/>',
  pin: '<path d="M12 17v5"/><path d="M9 10.8V4h6v6.8a2 2 0 0 0 .4 1.2l1.6 2.1a1 1 0 0 1-.8 1.6H7.8a1 1 0 0 1-.8-1.6l1.6-2.1a2 2 0 0 0 .4-1.2Z"/>',
};

function icon(name, size = 18, className = "") {
  const paths = ICON_PATHS[name] || ICON_PATHS.info;
  return `<svg class="${className}" width="${size}" height="${size}" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">${paths}</svg>`;
}

function normalizeRoute(path) {
  const clean = path.replace(/\.html$/, "").replace(/\/$/, "") || "/";
  if (clean === "/" || clean === "/dashboard") return "/dashboard/overview";
  if (clean === "/overview") return "/dashboard/overview";
  return ROUTES[clean] ? clean : "/dashboard/overview";
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function formatNumber(value, maximumFractionDigits = 0) {
  const number = Number(value);
  if (!Number.isFinite(number)) return "N/A";
  return number.toLocaleString(undefined, { maximumFractionDigits });
}

function formatBytes(value) {
  const bytes = Number(value);
  if (!Number.isFinite(bytes)) return "N/A";
  const units = ["Bytes", "KB", "MB", "GB", "TB"];
  if (bytes === 0) return "0 Bytes";
  const index = Math.min(
    Math.floor(Math.log(bytes) / Math.log(1024)),
    units.length - 1,
  );
  return `${(bytes / 1024 ** index).toFixed(index === 0 ? 0 : 1)} ${units[index]}`;
}

function shortHash(value, front = 8, back = 8) {
  const text = String(value || "");
  return text.length > front + back + 3
    ? `${text.slice(0, front)}...${text.slice(-back)}`
    : text;
}

function currentPageElement() {
  return document.querySelector("#page-content");
}

function applyAppearance() {
  document.documentElement.dataset.mode = state.mode;
  document
    .querySelector('meta[name="theme-color"]')
    ?.setAttribute("content", state.mode === "dark" ? "#0a0a0a" : "#e5e5e5");
}

function renderShell() {
  const app = document.querySelector("#app");
  app.innerHTML = `<div class="app-shell">
    <aside id="sidebar" class="sidebar">
      <nav class="sidebar-nav" aria-label="Dashboard navigation">
        ${sidebarLink("/dashboard/overview", "dashboard", "Dashboard")}
        ${sidebarLink("/dashboard/accelerations", "trend", "mempool.space accelerations")}
        ${sidebarLink("/dashboard/rsk", "layers", "RSK merge mining")}
      </nav>
    </aside>
    <div class="main-shell">
      <header class="topbar">
        <div class="topbar-left">
          <img class="topbar-logo" src="/dmnd-logo.svg" alt="DMND" width="64" height="27" draggable="false" />
          <button class="icon-btn btn ghost" type="button" data-action="toggle-sidebar" aria-label="Toggle sidebar">${icon("panel", 19)}</button>
          <nav class="breadcrumbs" aria-label="Breadcrumb">
            <a class="breadcrumb-parent" href="/dashboard/overview" data-nav="/dashboard/overview">Dashboard</a>
            <span class="breadcrumb-separator">/</span>
            <span id="breadcrumb-current" class="breadcrumb-current">${escapeHtml(ROUTES[state.route])}</span>
          </nav>
        </div>
        <div class="topbar-actions">
          <span id="health-badge" class="status-badge"><span class="status-dot"></span>Connecting...</span>
          <button class="icon-btn secondary" type="button" data-action="toggle-mode" aria-label="Toggle dark mode">${icon("theme", 18)}</button>
        </div>
      </header>
      <div class="page-scroll"><main id="page-content"></main></div>
    </div>
  </div>`;
  updateNavigation();
  renderRoute();
}

function sidebarLink(route, iconName, title) {
  return `<a class="sidebar-link" href="${route}" data-nav="${route}">
    ${icon(iconName, 18)}<span class="sidebar-label">${title}</span><span class="sidebar-tooltip">${title}</span>
  </a>`;
}

function updateNavigation() {
  document.querySelectorAll("[data-nav]").forEach((link) => {
    link.classList.toggle("active", link.dataset.nav === state.route);
  });
  const breadcrumb = document.querySelector("#breadcrumb-current");
  if (breadcrumb) breadcrumb.textContent = ROUTES[state.route] || "Overview";
  document.title = `${ROUTES[state.route] || "Dashboard"} · Demand Dashboard`;
}

function navigate(route, replace = false) {
  const normalized = normalizeRoute(route);
  if (replace) window.history.replaceState({}, "", normalized);
  else if (normalized !== state.route)
    window.history.pushState({}, "", normalized);
  state.route = normalized;
  updateNavigation();
  renderRoute();
  document.querySelector("#sidebar")?.classList.remove("mobile-open");
}

function renderRoute() {
  closeModal();
  closePanel();
  if (state.route === "/dashboard/overview") renderOverview();
  else renderActivityPage();
}

async function apiRequest(path, options = {}) {
  const headers = new Headers(options.headers || {});
  if (options.body && !headers.has("Content-Type"))
    headers.set("Content-Type", "application/json");
  const response = await fetch(path, { ...options, headers });
  let payload = null;
  const contentType = response.headers.get("content-type") || "";
  if (contentType.includes("application/json")) payload = await response.json();
  if (!response.ok) {
    // Callers branch on status, not message text.
    const error = new Error(
      payload?.message || `Request failed (${response.status})`,
    );
    error.status = response.status;
    throw error;
  }
  return payload;
}

async function envelopeRequest(path, options = {}) {
  const payload = await apiRequest(path, options);
  if (!payload?.success) throw new Error(payload?.message || "Request failed");
  return payload.data;
}

function authHeaders() {
  return state.apiToken ? { Authorization: `Bearer ${state.apiToken}` } : {};
}

// The proxy returns 401 for a missing or invalid token. The dashboard clears the
function handleRejectedToken(error) {
  if (error.status !== 401) return false;
  state.apiToken = "";
  localStorage.removeItem("demand-tx-token");
  state.tokenNotice = "The API token is invalid.";
  closeModal();
  closePanel();
  state.templates = [];
  state.miningActivity.accelerations = null;
  state.miningActivity.rsk = null;
  renderMiningActivity();
  renderTemplatesSection();
  return true;
}

function saveApiToken(token) {
  state.apiToken = token;
  localStorage.setItem("demand-tx-token", token);
  state.templatesErrorStatus = null;
  state.tokenNotice = "";
}

function addLog(event, level, message, atSeconds) {
  state.logs.unshift({
    event,
    level,
    message,
    timestamp: (atSeconds
      ? new Date(atSeconds * 1000)
      : new Date()
    ).toLocaleString(),
  });
  state.logs = state.logs.slice(0, LOG_LIMIT);
  if (state.route === "/dashboard/overview") renderLogs();
}

function toast(title, description = "", type = "info", duration = 5_000) {
  const root = document.querySelector("#toast-root");
  const item = document.createElement("div");
  item.className = `toast ${type}`;
  item.innerHTML = `<div class="toast-text">
      <div class="toast-title">${escapeHtml(title)}</div>${description ? `<div class="toast-description">${escapeHtml(description)}</div>` : ""}
    </div>
    <button class="toast-close" type="button" data-action="dismiss-toast" aria-label="Dismiss">${icon("x", 14)}</button>`;
  root.append(item);
  window.setTimeout(() => item.remove(), duration);
}

function openModal({ title, description = "", body = "", size = "large" }) {
  const root = document.querySelector("#modal-root");
  root.innerHTML = `<div class="modal-backdrop" data-action="close-modal">
    <section class="modal ${size}" role="dialog" aria-modal="true" aria-labelledby="modal-title" data-modal-panel>
      <header class="modal-header">
        <div><h3 id="modal-title" class="modal-heading">${escapeHtml(title)}</h3>${description ? `<p class="modal-description">${escapeHtml(description)}</p>` : ""}</div>
        <button class="icon-btn btn ghost" type="button" data-action="close-modal" aria-label="Close">${icon("x", 18)}</button>
      </header>
      <div class="modal-body">${body}</div>
    </section>
  </div>`;
  requestAnimationFrame(() =>
    root.querySelector("button, input, select")?.focus(),
  );
}

function closeModal() {
  const root = document.querySelector("#modal-root");
  if (root) root.innerHTML = "";
  state.openTemplateId = null;
}

async function copyText(text) {
  if (navigator.clipboard?.writeText) {
    await navigator.clipboard.writeText(text);
    return;
  }
  const textarea = document.createElement("textarea");
  textarea.value = text;
  textarea.style.position = "fixed";
  textarea.style.opacity = "0";
  document.body.append(textarea);
  textarea.select();
  document.execCommand("copy");
  textarea.remove();
}

function downloadText(text, filename, type = "text/plain;charset=utf-8") {
  const url = URL.createObjectURL(new Blob([text], { type }));
  const link = document.createElement("a");
  link.href = url;
  link.download = filename;
  document.body.append(link);
  link.click();
  link.remove();
  URL.revokeObjectURL(url);
}

async function pollStats() {
  const requests = await Promise.allSettled([
    envelopeRequest("/api/health"),
    envelopeRequest("/api/pool/info"),
    envelopeRequest("/api/stats/aggregate"),
    envelopeRequest("/api/stats/system"),
  ]);
  state.stats.loading = false;
  state.stats.health =
    requests[0].status === "fulfilled" ? requests[0].value : null;
  state.stats.pool =
    requests[1].status === "fulfilled" ? requests[1].value : null;
  state.stats.aggregate =
    requests[2].status === "fulfilled" ? requests[2].value : null;
  state.stats.system =
    requests[3].status === "fulfilled" ? requests[3].value : null;
  // Only failures the page shows: the health badge and the pool card.
  state.stats.errors = {
    pool:
      requests[1].status === "rejected" ? requests[1].reason?.message : null,
  };
  state.stats.error =
    requests[0].status === "rejected" ? requests[0].reason?.message : null;
  updateStatsUI();
  if (state.route === "/dashboard/overview") loadMiners();
}

function renderOverview() {
  const page = currentPageElement();
  page.className = "page compact-top";
  page.innerHTML = `<section class="page-header">
    <div><h1 class="page-title">Welcome back, DMND'er</h1></div>
    <div class="page-actions">
      <button class="btn primary" id="prio-open" type="button" data-action="open-prioritize">${icon("pin", 16)} Prioritise transaction</button>
    </div>
  </section>
  <section class="stats-grid" aria-label="Mining statistics">
    ${statCard("pool-card", "globe", "Pool Address", "Loading…", "Latency:", "Loading…", "Round trip of the last job declaration")}
    ${statCard("devices-card", "monitor", "Connected Devices", "—", "", "s")}
    ${statCard("bandwidth-card", "server", "Bandwidth", "—", "Pool connection", "", "Average pool traffic, both directions, since start.")}
    ${statCard("cpu-card", "cpu", "CPU Usage", "—", "System performance", "Memory: —")}
  </section>
  <div class="block-row">
  <section class="card block-card">
    <div id="block-facts"></div>
  </section>
  <section class="card templates-card">
    <div class="validation-head">
      <div class="validation-title">${icon("layers", 17)} Block templates</div>
      <div class="button-row" style="gap:.4rem">
        <button class="btn small" type="button" id="auto-declare-open" data-action="open-auto-declare"></button>
        <button class="btn small" type="button" data-action="refresh-templates">${icon("refresh", 15)} Refresh</button>
      </div>
    </div>
    <!-- Updated in place so the open select survives polling. -->
    <div class="tplx-controls">
      <label class="policy-label nowrap">Sort by
        <select id="template-sort" class="select-control small" aria-label="Sort the candidates">
          ${TEMPLATE_SORTS.map((option) => `<option value="${option.key}">${escapeHtml(option.label)}</option>`).join("")}
        </select>
      </label>
      <button class="btn small" type="button" id="sort-direction" data-action="toggle-sort-direction"></button>
      <span class="tplx-controls-gap"></span>
      <span id="template-filters" class="button-row" style="gap:.35rem"></span>
    </div>
    <div id="tplx-root" class="tplx"></div>
  </section>
  </div>
  <div class="bottom-row">
    <section class="card miners-card">
      <div class="validation-head">
        <div class="validation-title">${icon("monitor", 17)} Connected miners <span id="miners-count" class="badge"></span></div>
      </div>
      <div id="miners-list" class="miners-scroll"></div>
    </section>
    <section class="card logs-card">
      <div class="validation-head">
        <div class="validation-title">${icon("history", 17)} Logs</div>
        <div class="button-row" style="gap:.35rem">
          ${LOG_FILTERS.map(([key, label]) => `<button class="filter-chip small ${state.logFilter === key ? "active" : ""}" type="button" data-action="filter-logs" data-log-filter="${key}">${label} <span class="filter-count" data-log-count="${key}"></span></button>`).join("")}
          <span class="logs-cap">last ${formatNumber(LOG_LIMIT)}</span>
        </div>
      </div>
      <div id="logs-list" class="logs-list"></div>
    </section>
  </div>`;
  updateStatsUI();
  renderTemplatesSection();
  renderMiners();
  renderLogs();
  renderMiningActivity();
  if (!state.templatesLoaded && state.apiToken) loadTemplates();
  loadMiners();
}

// One stat card; `hint` becomes a hover question mark.
function statCard(id, iconName, label, value, footTitle, footMuted, hint = "") {
  return `<article id="${id}" class="card stat-card">
    <div><div class="stat-card-top"><div><div class="stat-label">${icon(iconName, 17)} ${label}${
      hint
        ? `<span class="stat-hint" tabindex="0" role="note" aria-label="${escapeHtml(hint)}">?<span class="stat-tip">${escapeHtml(hint)}</span></span>`
        : ""
    }</div><div class="stat-value" data-stat-value>${value}</div></div></div></div>
    <div><div class="stat-foot-title"><span data-stat-foot-title>${footTitle}</span></div><div class="stat-foot-muted" data-stat-foot-muted>${footMuted}</div></div>
  </article>`;
}

function activityEmpty(message) {
  return `<div class="tplx-empty">${escapeHtml(message)}</div>`;
}

function activityTxid(txid) {
  return `<span class="activity-hash"><a href="https://mempool.space/tx/${encodeURIComponent(txid)}" target="_blank" rel="noopener noreferrer" title="${escapeHtml(txid)}"><code>${escapeHtml(shortHash(txid, 10, 8))}</code> ↗</a><button class="icon-btn btn ghost" type="button" data-copy="${escapeHtml(txid)}" aria-label="Copy transaction ID">${icon("copy", 13)}</button></span>`;
}

function renderActivityPage() {
  const isRsk = state.route === "/dashboard/rsk";
  const page = currentPageElement();
  page.className = "page compact-top activity-page";
  page.innerHTML = `<section class="page-header">
    <div><h1 class="page-title">${escapeHtml(ROUTES[state.route])}</h1>
      <p class="activity-description">${isRsk ? "Mine RSK alongside Bitcoin with the same mining power." : "Transactions paid for on mempool.space, boosted on your node so they are included in your blocks."}</p>
    </div>
    <div class="page-header-actions">
      ${isRsk ? "" : `<a class="btn" href="https://mempool.space/accelerator" target="_blank" rel="noopener noreferrer">${icon("globe", 15)} Accelerator</a>
      <button class="btn primary" type="button" data-action="open-prioritize">${icon("pin", 15)} Prioritise transaction</button>`}
      <button class="btn" type="button" data-action="refresh-mining-activity">${icon("refresh", 15)} Refresh</button>
    </div>
  </section>
  <section id="activity-stats" class="stats-grid" aria-label="${isRsk ? "RSK statistics" : "Acceleration statistics"}"></section>
  <section class="card activity-card" aria-label="${isRsk ? "RSK activity" : "Accelerated transactions"}">
    <div id="${isRsk ? "rsk" : "accelerations"}-content"></div>
  </section>`;
  updateStatsUI();
  renderMiningActivity();
  loadMiningActivity();
  if (!isRsk && !state.templatesLoaded && state.apiToken) loadTemplates();
}

function activityNeedsToken(root) {
  if (state.apiToken) return false;
  root.innerHTML =
    state.capabilities?.templates === false
      ? activityEmpty("Set API_TX_TOKEN on the client to view mining activity.")
      : `<div class="tplx-gate"><p class="tplx-gate-lead">Enter your API token to view this page.</p>${tokenFormHtml()}</div>`;
  return true;
}

function renderActivityStats(values = []) {
  const root = document.querySelector("#activity-stats");
  if (!root) return;
  const definitions =
    state.route === "/dashboard/rsk"
      ? [
          ["checkCircle", "Block candidates", "Found since the client started"],
          ["upload", "Awaiting collection", "Candidates queued for the bridge to collect"],
          ["layers", "Last activated template", "Last RSK job activated by the client"],
        ]
      : [
          ["trend", "Tracked transactions", "mempool.space accelerations tracked on your node"],
          ["checkCircle", "In your mempool", "Available for block selection"],
          [
            "layers",
            "In active template",
            "Accelerated transactions in the template accepted by the pool",
          ],
          ["plus", "Total fee adjustment", "Virtual fee adjustment, in satoshis"],
        ];
  if (root.dataset.statsRoute !== state.route) {
    root.innerHTML = definitions
      .map(([symbol, label, description], index) =>
        statCard(`activity-stat-${index}`, symbol, label, "—", description, ""),
      )
      .join("");
    root.dataset.statsRoute = state.route;
  }
  definitions.forEach((_, index) => {
    const value = root.querySelector(
      `#activity-stat-${index} [data-stat-value]`,
    );
    if (value) value.textContent = values[index] ?? "—";
  });
}

function renderMiningActivity() {
  renderActivityStats();
  const accelerationRoot = document.querySelector("#accelerations-content");
  const rskRoot = document.querySelector("#rsk-content");
  if (accelerationRoot) renderAccelerations(accelerationRoot);
  if (rskRoot) renderRskActivity(rskRoot);
}

const accelerationMarkup = new WeakMap();

function updateAccelerationContent(root, html) {
  // Keep the table and keyboard focus intact when polling returns unchanged data.
  if (accelerationMarkup.get(root) === html) return;
  const scroll = root.querySelector(".activity-table-scroll");
  const position = scroll ? [scroll.scrollLeft, scroll.scrollTop] : null;
  const focused = root.contains(document.activeElement)
    ? document.activeElement
    : null;
  const copy = focused?.dataset.copy;
  const href = focused?.getAttribute("href");
  root.innerHTML = html;
  accelerationMarkup.set(root, html);
  const replacement = [...root.querySelectorAll("[data-copy], a[href]")].find(
    (element) =>
      known(copy)
        ? element.dataset.copy === copy
        : href && element.getAttribute("href") === href,
  );
  replacement?.focus({ preventScroll: true });
  const nextScroll = root.querySelector(".activity-table-scroll");
  if (position && nextScroll) nextScroll.scrollTo(...position);
}

function renderAccelerations(accelerationRoot) {
  // The token prompt writes its own markup, so invalidate the table cache.
  if (!state.apiToken) accelerationMarkup.delete(accelerationRoot);
  if (activityNeedsToken(accelerationRoot)) return;
  const activity = state.miningActivity;
  if (state.capabilities?.transaction_prioritization === false) {
    updateAccelerationContent(
      accelerationRoot,
      activityEmpty("Transaction prioritisation is not configured on this client."),
    );
  } else if (activity.accelerationError) {
    updateAccelerationContent(
      accelerationRoot,
      activityEmpty(`Could not load accelerations: ${activity.accelerationError}`),
    );
  } else if (activity.accelerations === null) {
    updateAccelerationContent(
      accelerationRoot,
      activityEmpty("Loading accelerations…"),
    );
  } else {
    const entries = Object.entries(activity.accelerations).sort(
      ([left], [right]) => left.localeCompare(right),
    );
    const inMempool = entries.filter(([, tx]) => tx.in_mempool).length;
    const totalAdjustment = entries.reduce(
      (total, [, tx]) => total + (Number(tx.fee_delta) || 0),
      0,
    );
    const template = state.templates.find(
      (candidate) => candidate.template_id === state.activeDeclaration,
    );
    const included = template?.mempool_space_included?.length;
    renderActivityStats([
      formatNumber(entries.length),
      `${formatNumber(inMempool)} / ${formatNumber(entries.length)}`,
      included === undefined ? "—" : formatNumber(included),
      `${totalAdjustment > 0 ? "+" : ""}${formatNumber(totalAdjustment)} sats`,
    ]);
    updateAccelerationContent(
      accelerationRoot,
      `<h2 class="validation-title activity-section-title">Accelerated transactions</h2>` +
      (entries.length
        ? `<div class="activity-table-scroll"><table class="pz-table"><thead><tr><th>Transaction</th><th class="right">Fee adjustment</th><th>Local status</th></tr></thead><tbody>${entries
            .map(
              ([txid, tx]) => `<tr>
        <td>${activityTxid(txid)}</td>
        <td class="right nowrap">${tx.fee_delta > 0 ? "+" : ""}${formatNumber(tx.fee_delta)} sats</td>
        <td><span class="badge ${tx.in_mempool ? "success" : ""}">${tx.in_mempool ? "In mempool" : "Not in mempool"}</span></td>
      </tr>`,
            )
            .join("")}</tbody></table></div>`
        : activityEmpty("No accelerated transactions right now.")),
    );
  }
}

const rskSeen = { template: null, proofs: null };

function rskShellHtml() {
  const node = (key, glyph, label) =>
    `<li class="rsk-node" data-node="${key}">
      <span class="rsk-node-glyph" aria-hidden="true">${glyph}</span>
      <span class="rsk-node-body"><span class="rsk-node-label">${label}</span>
      <span class="rsk-node-value" data-value="${key}">—</span>
      <span class="rsk-node-note" data-note="${key}"></span></span>
    </li>`;
  return `<ol class="rsk-strip" aria-label="Merge mining status">
      ${node("work", "R", "Cached RSK work")}
      <li class="rsk-link" data-link="work" aria-hidden="true"></li>
      ${node("job", "₿", "Last activated job")}
      <li class="rsk-link" data-link="job" aria-hidden="true"></li>
      ${node("candidates", "✓", "Candidates")}
    </ol>
    <p class="rsk-strip-hint" data-rsk-hint></p>`;
}

function rskSetNode(root, key, live, value, note = "") {
  const element = root.querySelector(`[data-node="${key}"]`);
  if (!element) return;
  element.classList.toggle("is-live", live);
  element.querySelector(`[data-value="${key}"]`).textContent = value;
  element.querySelector(`[data-note="${key}"]`).textContent = note;
}

function rskFlash(root, key) {
  const element = root.querySelector(`[data-value="${key}"]`);
  if (!element) return;
  element.classList.remove("is-bumped");
  void element.offsetWidth; // restart the animation on a repeat change
  element.classList.add("is-bumped");
}

function renderRskActivity(rskRoot) {
  if (activityNeedsToken(rskRoot)) return;
  const activity = state.miningActivity;
  if (activity.rskError) {
    rskRoot.innerHTML = activityEmpty(
      `Could not load RSK activity: ${activity.rskError}`,
    );
    return;
  }
  if (!activity.rsk) {
    rskRoot.innerHTML = activityEmpty("Loading RSK activity…");
    return;
  }
  const { configured, job_declaration, activity: rsk } = activity.rsk;
  const hasJob = known(rsk.active_template_id);
  renderActivityStats([
    formatNumber(rsk.proofs_found),
    formatNumber(rsk.pending_proofs),
    hasJob ? `#${formatNumber(rsk.active_template_id)}` : "—",
  ]);

  if (!rskRoot.querySelector(".rsk-strip")) {
    rskRoot.innerHTML = rskShellHtml();
    rskSeen.template = rskSeen.proofs = null;
  }

  rskSetNode(rskRoot, "work", rsk.work_available,
    rsk.work_available ? "Ready" : "None",
    rsk.work_available ? "from the bridge" : "waiting");
  rskSetNode(rskRoot, "job", hasJob,
    hasJob ? `#${formatNumber(rsk.active_template_id)}` : "—",
    hasJob ? "template" : "none");
  rskSetNode(rskRoot, "candidates", rsk.observer_ready,
    formatNumber(rsk.proofs_found),
    rsk.pending_proofs
      ? `${formatNumber(rsk.pending_proofs)} awaiting bridge`
      : "found");
  rskRoot
    .querySelector('[data-node="candidates"]')
    ?.classList.toggle("has-results", rsk.proofs_found > 0);

  const live = {
    work: rsk.work_available,
    job: hasJob,
    candidates: rsk.observer_ready,
  };
  for (const [from, to] of [["work", "job"], ["job", "candidates"]])
    rskRoot
      .querySelector(`[data-link="${from}"]`)
      ?.classList.toggle("is-flowing", live[from] && live[to]);

  if (rskSeen.template !== null && rsk.active_template_id !== rskSeen.template)
    rskFlash(rskRoot, "job");
  if (rskSeen.proofs !== null && rsk.proofs_found > rskSeen.proofs)
    rskFlash(rskRoot, "candidates");
  rskSeen.template = rsk.active_template_id;
  rskSeen.proofs = rsk.proofs_found;

  // Only ever says what to do next, and only when something needs doing.
  const hint = rskRoot.querySelector("[data-rsk-hint]");
  if (hint)
    hint.textContent = !configured
      ? "Connect an RSK bridge to get started."
      : !job_declaration
        ? "Connect a Template Provider to get started."
        : !rsk.work_available
          ? "Waiting for RSK work from the bridge."
          : !hasJob
            ? "Waiting for your next mining job."
            : "";
}

const ACTIVITY_SOURCES = {
  "/dashboard/accelerations": ["acceleration", "/api/tx/prioritized"],
  "/dashboard/rsk": ["rsk", "/api/merge-mining/status"],
};

async function loadMiningActivity(only) {
  const source = ACTIVITY_SOURCES[state.route];
  // Only the page being looked at is worth fetching, and only its own half.
  if (!state.apiToken || !source) return;
  const [kind, path] = source;
  if (only && only !== kind) return;
  const activity = state.miningActivity;
  if (activity[`${kind}Loading`]) return;
  if (
    kind === "acceleration" &&
    state.capabilities?.transaction_prioritization === false
  )
    return;
  const token = state.apiToken;
  const dataKey = kind === "acceleration" ? "accelerations" : "rsk";
  activity[`${kind}Loading`] = true;
  try {
    const data = await envelopeRequest(path, {
      headers: { Authorization: `Bearer ${token}` },
    });
    if (token !== state.apiToken) return;
    activity[dataKey] = kind === "acceleration" ? (data.mempool_space ?? {}) : data;
    activity[`${kind}Error`] = null;
  } catch (error) {
    if (token !== state.apiToken) return;
    if (handleRejectedToken(error)) return;
    activity[dataKey] = null;
    activity[`${kind}Error`] = error.message;
  } finally {
    activity[`${kind}Loading`] = false;
    renderMiningActivity();
  }
}

function updateStatsUI() {
  const healthBadge = document.querySelector("#health-badge");
  if (!healthBadge) return;
  if (state.stats.loading) {
    healthBadge.className = "status-badge";
    healthBadge.innerHTML = '<span class="status-dot"></span>Connecting...';
  } else if (state.stats.error) {
    healthBadge.className = "status-badge error";
    healthBadge.innerHTML = `<span class="status-dot"></span>${escapeHtml(state.stats.error)}`;
  } else {
    healthBadge.className = "status-badge";
    healthBadge.innerHTML = `<span class="status-dot"></span>${escapeHtml(typeof state.stats.health === "string" ? state.stats.health : state.stats.health?.status || "Proxy OK")}`;
  }

  const pool = state.stats.pool;
  updateStatCard(
    "pool-card",
    pool?.address ||
      (state.stats.errors.pool ? `Error: ${state.stats.errors.pool}` : "N/A"),
    "Latency:",
    known(pool?.declaration_latency_ms)
      ? `${formatNumber(pool.declaration_latency_ms / 1000, 3)} secs`
      : "N/A",
  );
  updateStatCard(
    "bandwidth-card",
    known(pool?.bandwidth_bytes_per_sec)
      ? `${formatBytes(pool.bandwidth_bytes_per_sec)}/s`
      : "N/A",
    "",
    "",
  );
  const aggregate = state.stats.aggregate;
  updateStatCard(
    "devices-card",
    aggregate?.total_connected_device ?? "N/A",
    "",
    "",
  );
  const system = state.stats.system;
  const cpu = Number(system?.["cpu_usage_%"] ?? system?.cpu_usage);
  updateStatCard(
    "cpu-card",
    Number.isFinite(cpu) ? `${cpu.toFixed(1)}%` : "N/A",
    "",
    `Memory: ${system ? formatBytes(system.memory_usage_bytes ?? system.memory_usage) : "N/A"}`,
  );
}

function updateStatCard(id, value, footTitle, footMuted) {
  const card = document.querySelector(`#${id}`);
  if (!card) return;
  card.querySelector("[data-stat-value]").textContent = value;
  card.querySelector("[data-stat-foot-title]").textContent = footTitle;
  card.querySelector("[data-stat-foot-muted]").textContent = footMuted;
}

function renderLogs() {
  const root = document.querySelector("#logs-list");
  if (!root) return;

  const counts = { all: state.logs.length, info: 0, warning: 0, error: 0 };
  for (const log of state.logs) {
    const level = log.level.toLowerCase();
    if (level in counts) counts[level] += 1;
  }
  document.querySelectorAll("[data-log-count]").forEach((element) => {
    element.textContent = formatNumber(counts[element.dataset.logCount] ?? 0);
  });
  document.querySelectorAll("[data-log-filter]").forEach((element) => {
    element.classList.toggle(
      "active",
      element.dataset.logFilter === state.logFilter,
    );
  });

  const shown = state.logs.filter(
    (log) =>
      state.logFilter === "all" || log.level.toLowerCase() === state.logFilter,
  );

  root.innerHTML = shown.length
    ? shown
        .map(
          (log) =>
            `<div class="log-row"><span>${escapeHtml(log.timestamp)}</span><strong class="log-level-${log.level.toLowerCase()}">${escapeHtml(log.level)}</strong><span><strong>${escapeHtml(log.event)}</strong> · ${escapeHtml(log.message)}</span></div>`,
        )
        .join("")
    : `<div class="empty-cell" style="display:grid;place-items:center">${
        state.logs.length ? "Nothing at this level." : "No logs yet."
      }</div>`;
}

function renderMiners() {
  const root = document.querySelector("#miners-list");
  const count = document.querySelector("#miners-count");
  if (!root) return;

  const miners = state.miners ? Object.entries(state.miners) : [];
  if (count)
    count.textContent = miners.length ? formatNumber(miners.length) : "—";

  if (!miners.length) {
    root.innerHTML = `<div class="empty-cell" style="display:grid;place-items:center">${
      state.miners ? "No miner is connected." : "Reading the connected miners…"
    }</div>`;
    return;
  }

  root.innerHTML = `<div class="table-shell miners-scroll"><table class="pz-table"><thead><tr>
      <th>Miner</th><th class="right">Difficulty</th>
    </tr></thead><tbody>${miners
      .map(
        ([id, miner]) => `<tr>
        <td>${escapeHtml(miner.device_name || `Miner ${id}`)}</td>
        <td class="right">${formatNumber(miner.current_difficulty, 2)}</td>
      </tr>`,
      )
      .join("")}</tbody></table></div>`;
}

async function loadMiners() {
  try {
    state.miners = await envelopeRequest("/api/stats/miners");
  } catch (error) {
    state.miners = null;
  }
  if (state.route === "/dashboard/overview") renderMiners();
}

function summaryMetric(label, value) {
  return `<div class="summary-metric"><div class="summary-metric-label">${label}</div><div class="summary-metric-value">${value}</div></div>`;
}

async function copyWithToast(text) {
  try {
    await copyText(text);
    toast("Copied to clipboard", "", "success", 2_000);
  } catch (error) {
    toast("Copy failed", error.message, "error");
  }
}

// All click actions; `target` is the element carrying data-action.
const CLICK_ACTIONS = {
  "toggle-sidebar": () => {
    const sidebar = document.querySelector("#sidebar");
    if (window.innerWidth < 768) sidebar.classList.toggle("mobile-open");
    else sidebar.classList.toggle("expanded");
  },
  "toggle-mode": () => {
    state.mode = state.mode === "dark" ? "light" : "dark";
    localStorage.setItem("demand-mode", state.mode);
    applyAppearance();
  },
  // Backdrop clicks close; clicks inside the dialog do not, unless on a close button.
  "close-modal": (event, target) => {
    if (!event.target.closest("[data-modal-panel]") || target.closest("button"))
      closeModal();
  },
  "close-panel": (event, target) => {
    if (!event.target.closest("[data-panel]") || target.closest("button"))
      closePanel();
  },
  "dismiss-toast": (event, target) => target.closest(".toast")?.remove(),
  "refresh-templates": () => loadTemplates(),
  "refresh-mining-activity": () => loadMiningActivity(),
  "open-prioritize": () => openPrioritizePanel(),
  "open-auto-declare": () => openAutoDeclareModal(),
  "copy-modal-text": () => copyWithToast(state.modalCopyText),
  "toggle-sort-direction": () => {
    state.templateSort = {
      ...state.templateSort,
      direction: state.templateSort.direction === "desc" ? "asc" : "desc",
    };
    renderTemplatesSection();
  },
  "filter-logs": (event, target) => {
    state.logFilter = target.dataset.logFilter;
    renderLogs();
  },
  "filter-templates": (event, target) => {
    state.templateFilter = target.dataset.filter;
    renderTemplatesSection();
  },
};

document.addEventListener("click", async (event) => {
  const nav = event.target.closest("[data-nav]");
  if (nav) {
    event.preventDefault();
    navigate(nav.dataset.nav);
    return;
  }

  // Actions win over opening: the declare button sits *inside* a template pad that
  // is itself clickable.
  const actionTarget = event.target.closest("[data-action]");
  const copyTarget = event.target.closest("[data-copy]");
  if (copyTarget && (!actionTarget || actionTarget.contains(copyTarget)))
    return copyWithToast(copyTarget.dataset.copy);
  if (actionTarget) {
    await CLICK_ACTIONS[actionTarget.dataset.action]?.(event, actionTarget);
    return;
  }

  const opener = event.target.closest("[data-template-open]");
  if (opener) openTemplateTransactions(Number(opener.dataset.templateOpen));
});

document.addEventListener("change", async (event) => {
  const target = event.target;
  if (target.name === "policy" && target.closest("#auto-declare-form")) {
    await saveDeclarationPolicy(target.value);
  } else if (target.id === "template-sort") {
    // Switching sort resets the direction to descending.
    state.templateSort = { key: target.value, direction: "desc" };
    renderTemplatesSection();
  }
});

document.addEventListener("submit", async (event) => {
  if (event.target.id === "token-panel-form") {
    event.preventDefault();
    const token = String(new FormData(event.target).get("token") || "").trim();
    if (token) await submitApiToken(event.target, token);
    return;
  }
  if (event.target.id === "prio-panel-form") {
    event.preventDefault();
    const data = new FormData(event.target);
    const txid = String(data.get("txid") || "").trim();
    const feeDelta = Number(data.get("feedelta"));
    if (txid && Number.isFinite(feeDelta) && feeDelta !== 0)
      await prioritizeTransaction(txid, feeDelta);
    return;
  }
});

window.addEventListener("popstate", () => {
  state.route = normalizeRoute(window.location.pathname);
  updateNavigation();
  renderRoute();
});

window.addEventListener("keydown", (event) => {
  if (event.key === "Escape") {
    closeModal();
    closePanel();
    return;
  }
  if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === "b") {
    event.preventDefault();
    document.querySelector('[data-action="toggle-sidebar"]')?.click();
    return;
  }
  // Enter/Space opens a focused template pad; buttons already handle these keys.
  if (event.key !== "Enter" && event.key !== " ") return;
  if (event.target.tagName === "BUTTON") return;
  const card = event.target.closest?.("[data-template-open]");
  if (!card) return;
  event.preventDefault();
  openTemplateTransactions(Number(card.dataset.templateOpen));
});

function currentTemplate() {
  return state.templates[0] || null;
}

// Single-flight: concurrent callers share the running request; one more runs after
// if anything asked meanwhile. Also prevents stale overwrites.
let templatesRequest = null;
let templatesRequestedAgain = false;

function loadTemplates() {
  if (!state.apiToken) {
    state.templatesLoaded = true;
    renderTemplatesSection();
    return;
  }
  if (templatesRequest) {
    templatesRequestedAgain = true;
    return templatesRequest;
  }
  templatesRequest = fetchTemplates().finally(() => {
    templatesRequest = null;
    if (templatesRequestedAgain) {
      templatesRequestedAgain = false;
      loadTemplates();
    }
  });
  return templatesRequest;
}

async function fetchTemplates() {
  if (!state.apiToken) return;
  try {
    const payload = await envelopeRequest("/api/templates/recent", {
      headers: authHeaders(),
    });
    state.templates = payload?.templates || [];
    state.templatesCandidateLimit = payload?.candidate_limit ?? null;
    state.policy = payload?.policy ?? state.policy;
    state.policyPick = payload?.policy_pick ?? null;
    state.activeDeclaration = payload?.active_declaration ?? null;
    state.templatesError = null;
    state.templatesErrorStatus = null;
    noticeNewBlock();
    noticeNewTemplate();
  } catch (error) {
    state.templatesLoaded = true;
    if (handleRejectedToken(error)) return;
    state.templates = [];
    state.templatesError = error.message;
    state.templatesErrorStatus = error.status ?? null;
  }
  state.templatesLoaded = true;
  renderTemplatesSection();
  if (document.querySelector("#accelerations-content")) renderMiningActivity();
}

function noticeNewTemplate() {
  const newest = currentTemplate()?.template_id ?? null;
  if (!known(newest) || newest === state.newestTemplateId) return;
  const first = state.newestTemplateId === null;
  state.newestTemplateId = newest;
  if (first) return;
  const message = `Template ${formatNumber(newest)} is ready to be declared`;
  toast("New template", message);
  addLog("NewTemplate", "INFO", message);
}

function noticeNewBlock() {
  const height = currentTemplate()?.height ?? null;
  if (!known(height) || height === state.blockHeight) return;

  const previous = state.blockHeight;
  state.blockHeight = height;
  state.blockSeenAt = Math.floor(Date.now() / 1000);

  // The first block seen after a page load is not news.
  if (previous === null) return;

  const gained = height - previous;
  const message =
    gained === 1
      ? `Block ${formatNumber(previous)} was mined. Templates for ${formatNumber(height)} start now.`
      : `The tip moved from block ${formatNumber(previous)} to ${formatNumber(height)}.`;
  toast("New block", message, "success", 8_000);
  addLog("NewBlock", "INFO", message);
}

function templateKind(template) {
  return template.future_template
    ? {
        label: "New block",
        hint: "New block: Template provider built for the next block because the previous one was mined.",
      }
    : {
        label: "Higher fees",
        hint: "Same tip: Template provider rebuilt because mempool fees rose past the set -sv2feedelta threshold.",
      };
}

function formatBtc(sats) {
  return (sats / 1e8).toFixed(8).replace(/0+$/, "").replace(/\.$/, "");
}

function relativeAge(seconds) {
  if (seconds < 60) return `${seconds}s ago`;
  if (seconds < 3_600) return `${Math.floor(seconds / 60)}m ago`;
  return `${Math.floor(seconds / 3_600)}h ago`;
}

// Update the controls in place, then redraw the ribbon.
function renderTemplatesSection() {
  if (state.route !== "/dashboard/overview") return;

  const gated = !state.apiToken;
  const controls = document.querySelector(".tplx-controls");
  if (controls) controls.hidden = gated;
  const declareButton = document.querySelector("#auto-declare-open");
  if (declareButton) declareButton.hidden = gated;
  if (gated) {
    renderTemplateCandidates();
    return;
  }

  const policy = document.querySelector("#auto-declare-open");
  if (policy) {
    policy.innerHTML = `${icon("sliders", 15)} Auto declare: ${escapeHtml(criterionLabel(state.policy))}`;
    policy.title = policyCopy(state.policy);
  }

  // Patch the open dialog rather than redrawing it.
  const pick = document.querySelector("#auto-declare-pick");
  if (pick) pick.innerHTML = autoDeclarePickHtml();

  const sort = document.querySelector("#template-sort");
  if (sort && sort.value !== state.templateSort.key)
    sort.value = state.templateSort.key;

  const direction = document.querySelector("#sort-direction");
  if (direction) {
    const label = sortDirectionLabel(
      activeSort(),
      state.templateSort.direction,
    );
    direction.innerHTML = `${icon("sort", 13)} ${escapeHtml(label)}`;
    direction.title = "Click for the other direction";
  }

  const filters = document.querySelector("#template-filters");
  if (filters) filters.innerHTML = renderTemplateFilters();

  renderTemplateCandidates();
}

function renderTemplateFilters() {
  const kinds = [
    [
      "all",
      "All",
      state.templates.length,
      "Every candidate held for this block.",
    ],
    [
      "future",
      "New block",
      state.templates.filter((template) => template.future_template).length,
      "The first template for this block",
    ],
    [
      "rebuild",
      "Higher fees",
      state.templates.filter((template) => !template.future_template).length,
      "Same tip: Template provider rebuilt because mempool fees rose past the set -sv2feedelta threshold.",
    ],
  ];

  const available = kinds.filter(([, , count]) => count > 0);
  const hideChips = available.length < 3;
  if (hideChips || !available.some(([key]) => key === state.templateFilter))
    state.templateFilter = "all";

  // Hide the chips when only one kind is present.
  if (hideChips) return "";

  return (
    `<span class="policy-label">Show</span>` +
    kinds
      .map(
        ([
          key,
          label,
          count,
          hint,
        ]) => `<button class="filter-chip small ${state.templateFilter === key ? "active" : ""}"
        type="button" data-action="filter-templates" data-filter="${key}" ${count ? "" : "disabled"}
        title="${escapeHtml(hint)}">${escapeHtml(label)} <span class="filter-count">${formatNumber(count)}</span></button>`,
      )
      .join("")
  );
}

async function saveDeclarationPolicy(policy) {
  const previous = state.policy;
  state.policy = policy;
  renderTemplatesSection();
  refreshAutoDeclareModal();
  try {
    const result = await envelopeRequest("/api/declaration-policy", {
      method: "POST",
      headers: authHeaders(),
      body: JSON.stringify({ policy }),
    });
    toast(
      "Declaration criterion set",
      `${criterionLabel(policy)}.`,
      "success",
      3_500,
    );
    addLog(
      "DeclarationPolicy",
      "INFO",
      `Declaration criterion set to "${policy}"`,
    );
    await loadTemplates();
  } catch (error) {
    state.policy = previous;
    if (handleRejectedToken(error)) return;
    toast("Could not change the criterion", error.message, "error");
    renderTemplatesSection();
  }
  refreshAutoDeclareModal();
}

function autoDeclarePickHtml() {
  if (!known(state.policyPick)) {
    return '<span class="muted">no candidate to pick from yet</span>';
  }
  return `${escapeHtml(criterionLabel(state.policy))} is <strong>#${formatNumber(state.policyPick)}</strong> right now`;
}

function openAutoDeclareModal() {
  openModal({
    title: "Auto declare",
    description:
      "Which criterion to use when declaring automatically on each new template.",
    size: "",
    body: `<form id="auto-declare-form" class="auto-declare">
      ${POLICIES.map(
        (
          option,
        ) => `<label class="auto-declare-option ${state.policy === option.key ? "is-chosen" : ""}">
          <input type="radio" name="policy" value="${escapeHtml(option.key)}" ${state.policy === option.key ? "checked" : ""} />
          <span class="auto-declare-copy">
            <strong>${escapeHtml(option.label)}</strong>
            <span>${escapeHtml(option.copy)}</span>
          </span>
        </label>`,
      ).join("")}
      <p class="auto-declare-pick" id="auto-declare-pick">${autoDeclarePickHtml()}</p>
    </form>`,
  });
}

// Patch the open dialog; rebuilding would steal focus mid-choice.
function refreshAutoDeclareModal() {
  const form = document.querySelector("#auto-declare-form");
  if (!form) return;
  form.querySelectorAll(".auto-declare-option").forEach((option) => {
    const input = option.querySelector("input");
    const chosen = input.value === state.policy;
    option.classList.toggle("is-chosen", chosen);
    input.checked = chosen;
  });
  const pick = form.querySelector("#auto-declare-pick");
  if (pick) pick.innerHTML = autoDeclarePickHtml();
}

// The candidate ribbon.
function renderTemplateCandidates() {
  const root = document.querySelector("#tplx-root");
  if (!root) return;

  const now = Math.floor(Date.now() / 1000);
  const current = currentTemplate();
  const declared =
    state.templates.find(
      (candidate) => candidate.template_id === state.activeDeclaration,
    ) ||
    (known(state.activeDeclaration)
      ? { template_id: state.activeDeclaration }
      : null);

  const facts = document.querySelector("#block-facts");
  if (facts) facts.innerHTML = renderBlockContext(current, declared, now);

  if (!state.apiToken) {
    root.innerHTML = tokenPromptHtml();
    return;
  }
  if (!state.templatesLoaded) {
    root.innerHTML = `<div class="tplx-empty">Loading templates…</div>`;
    return;
  }
  if (state.templatesError) {
    root.innerHTML = templatesErrorHtml();
    return;
  }
  if (!state.templates.length) {
    // Usually means a block was just found.
    root.innerHTML = `<div class="tplx-empty">
      <strong>No candidate for the current block yet.</strong><br />
    </div>`;
    return;
  }

  const sort = activeSort();
  const shown = sortedTemplates(sort);

  root.innerHTML = shown.length
    ? `<div class="tplx-ribbon">${shown
        .map((template) => renderPaper(template, { declared, sort, now }))
        .join("")}</div>`
    : `<div class="tplx-empty">No candidate matches this filter.</div>`;
}

// Block facts. Workers hash the newest template while its declaration settles, so
// "declared" and "hashing" can differ by design.
function renderBlockContext(current, declared, now) {
  const inSync = !declared || declared.template_id === current?.template_id;
  const oldest = state.templates[state.templates.length - 1];
  const feeGain =
    state.templates.length > 1 &&
    known(current?.total_fees_sat) &&
    known(oldest?.total_fees_sat)
      ? current.total_fees_sat - oldest.total_fees_sat
      : null;

  // Mark a block found in the last half minute.
  const fresh =
    known(state.blockSeenAt) &&
    now - state.blockSeenAt < 30 &&
    known(state.blockHeight);

  const rows = [
    [
      "Block",
      known(current?.height ?? state.blockHeight)
        ? `${formatNumber(current?.height ?? state.blockHeight)}${fresh ? ' <span class="tplx-chip tplx-chip-new">new</span>' : ""}`
        : "unknown",
      fresh ? "is-up" : "",
      fresh
        ? "The tip moved within the last half minute; these candidates are all for the new block."
        : "Decoded from the coinbase prefix, as BIP34 requires.",
    ],
    [
      "Declared",
      declared ? `#${formatNumber(declared.template_id)}` : "nothing yet",
      declared ? "is-up" : "",
      declared
        ? "The declaration the pool has accepted; shares are accounted against this set."
        : "No transaction set of ours is being accounted against for this block yet.",
    ],
    [
      "Workers hashing",
      current ? `#${formatNumber(current.template_id)}` : "—",
      inSync ? "" : "is-warn",
      inSync
        ? "The accepted declaration and the newest template agree."
        : "The newest template is already with the miners; its declaration is still settling.",
    ],
    [
      "Fees gained",
      feeGain === null
        ? "—"
        : `${feeGain > 0 ? "+" : ""}${formatBtc(feeGain)} BTC`,
      feeGain !== null && feeGain > 0 ? "is-up" : "",
      "Across every rebuild template has sent for this block.",
    ],
    [
      "Newest arrived",
      current ? relativeAge(Math.max(0, now - current.received_at)) : "—",
      "",
      "Template provider send a new one if the fees rose past -sv2feedelta or the previous one was mined.",
    ],
    [
      "Auto declare Criterion",
      escapeHtml(criterionLabel(state.policy)),
      "",
      `${policyCopy(state.policy)}${
        known(state.policyPick)
          ? ` As it stands that is template ${state.policyPick}, which changes with every rebuild sv2-tp sends.`
          : ""
      }`,
    ],
  ];

  return `<div class="tplx-facts">
    ${rows
      .map(
        ([
          label,
          value,
          valueClass,
          hint,
        ]) => `<span class="tplx-fact" title="${escapeHtml(hint)}">
          <span class="tplx-fact-label">${escapeHtml(label)}</span>
          <span class="tplx-fact-value ${valueClass}">${value}</span>
        </span>`,
      )
      .join("")}
  </div>`;
}

// Scroll artwork: a 512-square icon silhouette; the page and dowel are drawn again
// underneath in their own colours.
const SCROLL_PAGE_PATH =
  "M86 11H444C442 16 441 21 441 26V278L420 307L441 336V423H71V366L91 337L71 308V162L91 133L71 104V26C71 18 78 11 86 11Z";
const SCROLL_DOWEL_PATH =
  "M45 437H371A30 30 0 0 1 401 467A30 30 0 0 1 371 497H45A30 30 0 0 1 15 467A30 30 0 0 1 45 437Z";
const SCROLL_TOP_ROLL_HOLE = "M497 45A30 30 0 1 1 437 45A30 30 0 1 1 497 45Z";
const SCROLL_BOTTOM_ROLL_HOLE =
  "M437 467A30 30 0 1 1 377 467A30 30 0 1 1 437 467Z";
const SCROLL_BODY_PATH = `M105 0H467C492 0 512 20 512 45C512 70 492 90 467 90C462 90 457 89 452 87V292L436 307L452 322V467C452 492 432 512 407 512H45C20 512 0 492 0 467C0 442 20 422 45 422H60V353L76 337L60 321V149L76 133L60 117V45C60 20 80 0 105 0Z ${SCROLL_PAGE_PATH} ${SCROLL_DOWEL_PATH} ${SCROLL_TOP_ROLL_HOLE} ${SCROLL_BOTTOM_ROLL_HOLE}`;
const SCROLL_ROLL_PATH = `M452 467A45 45 0 1 1 362 467A45 45 0 1 1 452 467Z ${SCROLL_BOTTOM_ROLL_HOLE}`;

function scrollFrame() {
  return `<svg class="scroll-svg" viewBox="0 0 512 512" preserveAspectRatio="none" aria-hidden="true">
    <path class="scroll-page" d="${SCROLL_PAGE_PATH}" />
    <path class="scroll-dowel" d="${SCROLL_DOWEL_PATH}" />
    <path class="scroll-ink" fill-rule="evenodd" d="${SCROLL_BODY_PATH}" />
    <path class="scroll-ink" fill-rule="evenodd" d="${SCROLL_ROLL_PATH}" />
  </svg>`;
}

// One template, drawn as a scroll.
function renderPaper(template, { declared, sort, now }) {
  const isDeclared = declared?.template_id === template.template_id;
  const kind = templateKind(template);
  const fill = Math.min(100, (template.total_weight / MAX_BLOCK_WEIGHT) * 100);
  const rate = templateFeeRate(template);
  const age = Math.max(0, now - template.received_at);
  const delta =
    sort.key !== "received" && declared && !isDeclared
      ? deltaPercent(sort.value(template), sort.value(declared))
      : null;

  // Sortable metrics first; the active sort is marked.
  const metrics = [
    [
      "highest_fees",
      known(template.total_fees_sat) ? formatBtc(template.total_fees_sat) : "—",
      "BTC in fees",
    ],
    ["block_weight", `${fill.toFixed(2)}%`, "of block weight"],
    ["fee_rate", known(rate) ? rate.toFixed(2) : "—", "sat/vB"],
    ["tx_count", formatNumber(template.tx_count), "transactions"],
  ];

  return `<article class="scroll ${isDeclared ? "is-declared" : ""}"
      data-template-open="${escapeHtml(template.template_id)}"
      tabindex="0" role="button" aria-label="Show the transactions in template ${escapeHtml(template.template_id)}"
      title="${escapeHtml(kind.hint)} ${formatNumber(template.tx_count)} transactions, ${formatNumber(template.total_weight)} WU.">
    ${scrollFrame()}
    <div class="scroll-content">
      <div class="p3-head">
        <span class="p3-id">#${formatNumber(template.template_id)}${templateMark(template, declared)}</span>
        <span class="p3-kind ${template.future_template ? "is-block" : ""}">${escapeHtml(kind.label)}</span>
      </div>
      <div class="p3-metrics">
        ${metrics
          .map(
            ([
              key,
              value,
              unit,
            ]) => `<span class="p3-metric ${key === sort.key ? "is-sort" : ""}">
              <span>${escapeHtml(value)}</span><span class="p3-metric-unit">${escapeHtml(unit)}</span>
            </span>`,
          )
          .join("")}
      </div>
      ${
        (template.prioritized_included || []).length
          ? `<span class="p3-prio" title="${formatNumber(template.prioritized_included.length)} locally prioritised transaction${template.prioritized_included.length === 1 ? "" : "s"} ${template.prioritized_included.length === 1 ? "is" : "are"} in this template">${icon("pin", 10)} ${formatNumber(template.prioritized_included.length)} prioritised</span>`
          : ""
      }
      ${
        known(delta)
          ? `<span class="p3-delta ${delta > 0.005 ? "is-up" : ""}" title="${delta > 0 ? "beats" : delta < 0 ? "falls short of" : "matches"} the declared template on ${escapeHtml(sort.label.toLowerCase())} by ${Math.abs(delta).toFixed(2)}%">${delta > 0 ? "+" : delta < 0 ? "−" : ""}${Math.abs(delta).toFixed(2)}% more than declared</span>`
          : `<span class="p3-delta"></span>`
      }
      <div class="p3-foot">
        <span>${escapeHtml(relativeAge(age))}</span>
        ${isDeclared ? `<span class="tplx-in-force">${icon("check", 11)} declared</span>` : ""}
      </div>
      ${isDeclared ? '<span class="scroll-stamp">declared</span>' : ""}
    </div>
  </article>`;
}

function templateMark(template, declared) {
  if (declared?.template_id === template.template_id) {
    return `<span class="tplx-row-mark" title="The declaration the pool has accepted">${icon("check", 11)}</span>`;
  }
  if (template.template_id === currentTemplate()?.template_id) {
    return `<span class="tplx-row-mark is-hashing" title="The newest template — the workers are already hashing this one">${icon("activity", 11)}</span>`;
  }
  return "";
}

function activeSort() {
  return (
    TEMPLATE_SORTS.find((option) => option.key === state.templateSort.key) ||
    TEMPLATE_SORTS[0]
  );
}

function sortDirectionLabel(sort, direction) {
  if (sort.key === "received")
    return direction === "desc" ? "newest first" : "oldest first";
  return direction === "desc" ? "highest first" : "lowest first";
}

// Unscorable candidates go last; ties keep arrival order.
function sortedTemplates(sort) {
  const direction = state.templateSort.direction === "asc" ? 1 : -1;
  return state.templates
    .filter(matchesTemplateFilter)
    .map((template, index) => ({ template, index }))
    .sort((left, right) => {
      const leftValue = sort.value(left.template);
      const rightValue = sort.value(right.template);
      if (!known(leftValue) || !known(rightValue)) {
        if (known(leftValue)) return -1;
        if (known(rightValue)) return 1;
        return left.index - right.index;
      }
      if (leftValue !== rightValue) return (leftValue - rightValue) * direction;
      return left.index - right.index;
    })
    .map((entry) => entry.template);
}

function matchesTemplateFilter(template) {
  if (state.templateFilter === "future") return template.future_template;
  if (state.templateFilter === "rebuild") return !template.future_template;
  return true;
}

// A candidate's metric against the declared template's, in percent.
function deltaPercent(value, baseline) {
  if (!known(value) || !known(baseline) || baseline === 0) return null;
  return ((value - baseline) / baseline) * 100;
}

// Average fee rate in sat/vB (vsize = weight / 4).
function templateFeeRate(template) {
  if (!known(template.total_fees_sat) || !template.total_weight) return null;
  return template.total_fees_sat / (template.total_weight / 4);
}

function criterionLabel(key) {
  return POLICIES.find((policy) => policy.key === key)?.label || key;
}

// Right-side panel, for tasks rather than reading.
function openPanel({ title, description = "", body = "", footer = "" }) {
  const root = document.querySelector("#panel-root");
  const wasOpen = !!root.querySelector("[data-panel]");
  root.innerHTML = `<div class="panel-overlay" data-action="close-panel">
    <section class="panel" role="dialog" aria-modal="true" aria-label="${escapeHtml(title)}" data-panel>
      <header class="panel-head">
        <div>
          <h2 class="panel-title">${escapeHtml(title)}</h2>
          ${description ? `<p class="panel-description">${escapeHtml(description)}</p>` : ""}
        </div>
        <button class="panel-close" type="button" data-action="close-panel" aria-label="Close">${icon("x", 20)}</button>
      </header>
      <div class="panel-body">${body}</div>
      ${footer ? `<footer class="panel-foot">${footer}</footer>` : ""}
    </section>
  </div>`;
  if (!wasOpen)
    requestAnimationFrame(() =>
      root.querySelector("input, textarea, button")?.focus(),
    );
}

function closePanel() {
  const root = document.querySelector("#panel-root");
  if (root) root.innerHTML = "";
}

// A token this browser does not have is worth asking for; one the proxy was
// never given is not, so the two refusals read differently.
function tokenMissingOnProxyHtml(
  title = "Job declaration is not enabled.",
) {
  return `<div class="tplx-empty">
    <strong>${escapeHtml(title)}</strong><br />
    This proxy was started without an <code>API_TX_TOKEN</code>. Add it to
    <code>config.toml</code> and restart the client.
  </div>`;
}

function tokenPromptHtml() {
  if (state.capabilities?.templates === false) return tokenMissingOnProxyHtml();
  return `<div class="tplx-gate">
    <p class="tplx-gate-lead">Please provide the dmnd client's <code>API_TX_TOKEN</code> to see the block templates.</p>
    ${tokenFormHtml()}
  </div>`;
}

function tokenFormHtml() {
  return `<form id="token-panel-form" class="panel-form">
      ${state.tokenNotice ? `<p class="panel-error">${escapeHtml(state.tokenNotice)}</p>` : ""}
      <label class="panel-field">
        <input class="panel-input" type="password" name="token" placeholder="API_TX_TOKEN" autocomplete="off" required />
      </label>
      <p class="panel-error" id="token-panel-error" hidden></p>
      <div class="panel-form-actions">
        <button class="btn pill primary" id="token-submit" type="submit">Continue</button>
      </div>
    </form>`;
}

function templatesErrorHtml() {
  if (state.templatesErrorStatus === 503) return tokenMissingOnProxyHtml();
  return `<div class="tplx-empty">Could not load templates: ${escapeHtml(state.templatesError)}</div>`;
}

// Try the token before keeping it, so a wrong one is rejected without saving it.
async function submitApiToken(form, token) {
  const button = form.querySelector("#token-submit");
  const error = form.querySelector("#token-panel-error");
  const label = button.textContent;
  error.hidden = true;
  button.disabled = true;
  button.textContent = "Checking…";
  try {
    await envelopeRequest("/api/templates/recent", {
      headers: { Authorization: `Bearer ${token}` },
    });
  } catch (failure) {
    button.disabled = false;
    button.textContent = label;
    error.textContent =
      failure.status === 401
        ? "The proxy rejected that token. Check API_TX_TOKEN and try again."
        : failure.message;
    error.hidden = false;
    return;
  }
  saveApiToken(token);
  closePanel();
  renderMiningActivity();
  loadTemplates();
  loadMiningActivity();
}

// Prioritise a transaction, or say what is stopping it: what the proxy was
// started without, then the token this browser has yet to be given.
function openPrioritizePanel() {
  if (!state.capabilities) {
    openPanel({
      title: "Prioritise a transaction",
      body: '<p class="panel-lead">Could not read the proxy configuration. Reload the dashboard and try again.</p>',
      footer: '<button class="btn pill" type="button" data-action="close-panel">Close</button>',
    });
    return;
  }
  if (state.capabilities.templates === false) {
    openPanel({
      title: "Prioritise a transaction",
      body: tokenMissingOnProxyHtml(
        "Transaction prioritisation is not enabled.",
      ),
      footer: '<button class="btn pill" type="button" data-action="close-panel">Close</button>',
    });
    return;
  }
  if (state.capabilities?.transaction_prioritization !== true) {
    openPanel({
      title: "Prioritise a transaction",
      body: `<p class="panel-lead">To prioritise a transaction, the dmnd client needs the bitcoind RPC settings
        (<code>RPC_URL</code>, <code>RPC_USER</code>, <code>RPC_PWD</code>)  One or more of these are unset. Set them and restart the client to use this feature.</p>`,
      footer: `<button class="btn pill" type="button" data-action="close-panel">Close</button>`,
    });
    return;
  }
  if (!state.apiToken) {
    openPanel({
      title: "Prioritise a transaction",
      description: "This browser has not been given the proxy's API token yet.",
      body: tokenFormHtml(),
    });
    return;
  }
  openPanel({
    title: "Prioritise a transaction",
    description: "Free. This changes the transaction priority ONLY on your node and does not require you to pay an extra fee.",
    body: `<form id="prio-panel-form" class="panel-form">
        <label class="panel-field">
          <span class="panel-label">Transaction <span class="muted">txid</span></span>
          <input class="panel-input" type="text" name="txid" spellcheck="false" autocomplete="off" pattern="[0-9a-fA-F]{64}" placeholder="e3b0c44298fc1c14..." required />
          <span class="panel-hint">The transaction must already be in your node's mempool.</span>
        </label>
        <label class="panel-field">
          <span class="panel-label">Fee delta <span class="muted">sats</span></span>
          <input class="panel-input" type="number" name="feedelta" step="1" value="100000" required />
        </label>
        <div class="panel-form-actions">
          <button class="btn pill" type="button" data-action="close-panel">Cancel</button>
          <button class="btn pill primary" id="prio-submit" type="submit">Submit</button>
        </div>
      </form>`,
  });
}

function setPrioritizingUI(busy) {
  const button = document.querySelector("#prio-submit");
  if (!button) return;
  button.disabled = busy;
  button.textContent = busy ? "Sending…" : "Prioritise transaction";
}


// Submit a raw transaction and have bitcoind prioritise it.
// One transaction should not be prioritised by both bitcoind and mempool.space, so check the former first.
async function passesMempoolSpaceCheck(txid) {
  let accelerated;
  try {
    const data = await envelopeRequest("/api/tx/prioritized", {
      headers: authHeaders(),
    });
    accelerated = Object.keys(data?.mempool_space || {});
  } catch (error) {
    toast(
      "Check failed",
      "Could not verify this transaction. Try again.",
      "error",
    );
    addLog(
      "PrioritizeTransaction",
      "WARNING",
      `Check failed for ${txid}: ${error.message}`,
    );
    return false;
  }

  // bitcoind reports txids in lower case; the field accepts either.
  if (accelerated.some((id) => id.toLowerCase() === txid.toLowerCase())) {
    toast(
      "Already prioritised",
      "mempool.space is already prioritising this transaction.",
      "error",
    );
    addLog(
      "PrioritizeTransaction",
      "WARNING",
      `${txid} is already prioritised by mempool.space`,
    );
    return false;
  }

  return true;
}

async function prioritizeTransaction(txid, feeDelta) {
  state.prioritizing = true;
  setPrioritizingUI(true);
  try {
    if (!(await passesMempoolSpaceCheck(txid))) return;
    // Both the txid and the delta go in the path.
    await envelopeRequest(
      `/api/tx/prioritize/${encodeURIComponent(txid)}/${encodeURIComponent(feeDelta)}`,
      {
        method: "POST",
        headers: authHeaders(),
      },
    );
    toast(
      "Transaction prioritised",
      `Transaction ${txid} is successfully prioritised by ${feeDelta} sats`,
      "success",
    );
    addLog(
      "PrioritizeTransaction",
      "INFO",
      `Prioritised ${txid} by ${feeDelta} sats`,
    );
    closePanel();
  } catch (error) {
    toast("Prioritisation failed", error.message, "error");
    addLog(
      "PrioritizeTransactionError",
      "ERROR",
      `Prioritisation failed: ${error.message}`,
    );
  } finally {
    state.prioritizing = false;
    setPrioritizingUI(false);
  }
}

// ---------------------------------------------------------------------------
// One template's transactions
// ---------------------------------------------------------------------------

async function openTemplateTransactions(templateId) {
  state.openTemplateId = templateId;
  openModal({
    title: `Template ${templateId}`,
    description: "Transactions in this template, as sv2-tp sent them",
    size: "xlarge",
    body: '<div class="empty-cell" style="display:grid;place-items:center">Loading transactions…</div>',
  });
  await refreshTemplateModal();
}

async function refreshTemplateModal() {
  const templateId = state.openTemplateId;
  if (templateId === null) return;
  let template;
  try {
    template = await envelopeRequest(
      `/api/templates/${encodeURIComponent(templateId)}`,
      { headers: authHeaders() },
    );
  } catch (error) {
    // Closed or switched while in flight.
    if (state.openTemplateId !== templateId) return;
    if (handleRejectedToken(error)) return;
    openModal({
      title: `Template ${templateId}`,
      description: "Transactions in this template",
      size: "xlarge",
      body: `<div class="modal-error">Error: ${escapeHtml(error.message)}</div>`,
    });
    return;
  }
  // Closed or switched while in flight.
  if (state.openTemplateId !== templateId) return;

  const kind = templateKind(template);
  const age = Math.max(0, Math.floor(Date.now() / 1000) - template.received_at);
  // Still a candidate for the current tip, and so still declarable.
  const isLive = state.templates.some(
    (candidate) => candidate.template_id === template.template_id,
  );
  // Accepted by the pool, not merely declared earlier.
  const isInForce = state.activeDeclaration === template.template_id;

  state.modalCopyText = (template.transactions || [])
    .map((transaction) => transaction.txid)
    .join("\n");
  openModal({
    title: `Template ${template.template_id}`,
    description: `${kind.label} · ${template.height ? `block ${formatNumber(template.height)} · ` : ""}received ${relativeAge(age)} · ${kind.hint}`,
    size: "xlarge",
    body: `<div class="selection-summary-grid">
        ${summaryMetric("Transactions", formatNumber(template.tx_count))}
        ${summaryMetric("Total fees", template.total_fees_sat === null || template.total_fees_sat === undefined ? "unknown" : `${formatBtc(template.total_fees_sat)} BTC`)}
        ${summaryMetric("Block fill", `${((template.total_weight / MAX_BLOCK_WEIGHT) * 100).toFixed(1)}%`)}
        ${summaryMetric("Weight", `${formatNumber(template.total_weight)} WU`)}
        ${summaryMetric("Coinbase value", `${formatBtc(template.coinbase_value_sat)} BTC`)}
        ${summaryMetric("Subsidy", template.subsidy_sat === null || template.subsidy_sat === undefined ? "unknown" : `${formatBtc(template.subsidy_sat)} BTC`)}
      </div>
      
      
    
      <div class="validation-head" style="margin:1.25rem 0 .5rem">
        <div class="validation-title">${icon("hash", 16)} Transactions</div>
        <div class="button-row" style="gap:.5rem">
          <span class="badge">${formatNumber(template.tx_count)}</span>
          ${(template.prioritized_included || []).length ? `<span class="badge success">${icon("pin", 12)} ${formatNumber(template.prioritized_included.length)} prioritised</span>` : ""}
          <button class="btn small" type="button" data-action="copy-modal-text">${icon("copy", 14)} Copy txids</button>
          <button class="btn small" type="button" data-export-template="${escapeHtml(String(template.template_id))}">${icon("download", 14)} Export CSV</button>
        </div>
      </div>
      ${templateTransactionsTable(template)}
      ${
        isInForce || isLive
          ? ``
          : `<p class="validation-copy">${icon("info", 14)} The tip has moved since this template arrived, so it can no longer be mined and is no longer a candidate.</p>`
      }`,
  });

  const exportButton = document.querySelector("[data-export-template]");
  if (exportButton) {
    exportButton.addEventListener(
      "click",
      () => {
        const header = "txid,vsize,weight,fee_sat,fee_rate_sat_per_vb";
        const lines = (template.transactions || []).map((t) =>
          [
            t.txid,
            t.vsize,
            t.weight,
            t.fee_sat ?? "",
            t.fee_rate_sat_per_vb ?? "",
          ].join(","),
        );
        downloadText(
          [header, ...lines].join("\n"),
          `template-${template.template_id}.csv`,
          "text/csv;charset=utf-8",
        );
      },
      { once: true },
    );
  }
}

function templateTransactionsTable(template) {
  const transactions = template.transactions || [];
  if (!transactions.length) {
    return `<div class="pz-empty">This template has no transactions beyond the coinbase.</div>`;
  }

  const prioritized = new Set(template.prioritized_included || []);
  const accelerated = new Set(template.mempool_space_included || []);

  // Prioritised first, then by fee; the rest keep the node's order.
  const rows = [...transactions]
    .sort((left, right) => {
      const leftPrio = prioritized.has(left.txid);
      const rightPrio = prioritized.has(right.txid);
      if (leftPrio !== rightPrio) return leftPrio ? -1 : 1;
      return (right.fee_sat ?? -1) - (left.fee_sat ?? -1);
    })
    .map((transaction) => {
      const isPrioritized = prioritized.has(transaction.txid);
      return `<tr class="${isPrioritized ? "pz-row-prio" : ""}">
      <td><span class="inline" style="gap:.35rem">${isPrioritized ? `<span class="tplx-row-mark" title="${accelerated.has(transaction.txid) ? "mempool.space acceleration" : "Locally prioritised"}">${icon(accelerated.has(transaction.txid) ? "trend" : "pin", 11)}</span>` : ""}<span class="txid-value" title="${escapeHtml(transaction.txid)}">${escapeHtml(shortHash(transaction.txid, 12, 6))}</span><button class="icon-btn btn ghost" type="button" data-copy="${escapeHtml(transaction.txid)}" aria-label="Copy transaction ID">${icon("copy", 13)}</button>${accelerated.has(transaction.txid) ? '<span class="badge success">mempool.space</span>' : ""}</span></td>
      <td class="right">${transaction.fee_sat === null || transaction.fee_sat === undefined ? '<span class="muted">—</span>' : formatBtc(transaction.fee_sat)}</td>
      <td class="right">${transaction.fee_rate_sat_per_vb === null || transaction.fee_rate_sat_per_vb === undefined ? '<span class="muted">—</span>' : formatNumber(transaction.fee_rate_sat_per_vb, 2)}</td>
      <td class="right">${formatNumber(transaction.vsize)}</td>
      <td class="right">${formatNumber(transaction.weight)}</td>
    </tr>`;
    })
    .join("");

  return `<div class="table-shell template-tx-scroll"><table class="pz-table"><thead><tr>
      <th>TXID</th><th class="right">Fee (sats)</th><th class="right">sat/vB</th>
      <th class="right">vSize</th><th class="right">Weight</th>
    </tr></thead><tbody>${rows}</tbody></table></div>`;
}

// ---------------------------------------------------------------------------
// Declaring
// ---------------------------------------------------------------------------

// Declare one candidate; any candidate on the current tip is valid.

async function resolveCapabilities() {
  try {
    state.capabilities = await envelopeRequest("/api/capabilities");
    return true;
  } catch (error) {
    state.capabilities = null;
    return false;
  }
}

// The stats need no token, so the dashboard renders without one; only the
// templates wait for it.
function startDashboard() {
  renderShell();
  pollStats();
  loadMiningActivity();
  if (state.apiToken) loadTemplates();
  if (state.polling) return;
  state.polling = true;
  const visible = () => !document.hidden;
  setInterval(() => {
    if (visible()) pollStats();
  }, API_POLL_INTERVAL);
  setInterval(() => {
    if (!visible() || !state.apiToken) return;
    loadTemplates();
    // Cheap and genuinely fast-moving: a candidate can turn up any second.
    loadMiningActivity("rsk");
  }, TEMPLATE_REFRESH_INTERVAL);
  setInterval(() => {
    if (visible() && state.apiToken) loadMiningActivity("acceleration");
  }, ACCELERATIONS_REFRESH_INTERVAL);
  document.addEventListener("visibilitychange", () => {
    if (!visible()) return;
    pollStats();
    loadMiningActivity();
    if (state.apiToken) loadTemplates();
  });
}

async function initialize() {
  applyAppearance();
  if (window.location.pathname !== state.route)
    window.history.replaceState({}, "", state.route);
  await resolveCapabilities();
  startDashboard();
}

initialize();
