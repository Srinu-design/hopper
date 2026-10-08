// Hopper web page: tabs, the live status dot, copy buttons, and the console on "Try it".
//
// The console calls the public API with the key the person types in, exactly as any client
// does. Everything from the API is put on the page as text (textContent), never as HTML.
"use strict";

const POLL_MS = 3000;
const STATUS_MS = 15000;
const KEY_STORAGE = "hopper.apiKey";
const LIST_LIMIT = 20;

const TASKS = {
  sleep: {
    payload: { ms: 1000 },
    help: "Waits ms milliseconds, then succeeds.",
  },
  flaky: {
    payload: { p: 0.5, ms: 200 },
    help: "Fails with probability p, so you can watch it retry with backoff.",
  },
  fail_always: {
    payload: { permanent: false },
    help: "Always fails. It retries with backoff (5 attempts, within about half a minute), then lands in Dead letters. Set permanent to true to skip the retries.",
  },
  cpu: {
    payload: { n: 2000000 },
    help: "Hashes n times: real CPU work, about 2.4 million rounds a second.",
  },
  http: {
    payload: { url: "https://example.com/", method: "GET" },
    help: "Calls the URL with a signed request, retrying on 408, 429 and 5xx. Private and internal addresses are refused.",
  },
};

const $ = (selector) => document.querySelector(selector);

function el(tag, attrs = {}, ...children) {
  const node = document.createElement(tag);
  for (const [name, value] of Object.entries(attrs)) {
    if (value !== null && value !== undefined && value !== false) {
      node.setAttribute(name, value === true ? "" : String(value));
    }
  }
  node.append(...children.filter((c) => c !== null && c !== undefined && c !== false));
  return node;
}

// ------------------------------------------------------------------ tabs

let activeTab = "about";

function showFromHash() {
  const id = decodeURIComponent(location.hash.slice(1));
  const target = id ? document.getElementById(id) : null;
  const panel = target ? target.closest("[data-tab]") : null;
  const tab = panel ? panel.dataset.tab : "about";
  for (const p of document.querySelectorAll("[data-tab]")) p.hidden = p.dataset.tab !== tab;
  for (const a of document.querySelectorAll("[data-tab-link]")) {
    if (a.dataset.tabLink === tab) a.setAttribute("aria-current", "page");
    else a.removeAttribute("aria-current");
  }
  if (target && target !== panel) target.scrollIntoView();
  else window.scrollTo(0, 0);
  activeTab = tab;
  if (tab === "try") resumePolling();
}

// ------------------------------------------------------------------ light and dark

const THEME_STORAGE = "hopper.theme"; // theme.js reads it before the page is drawn

function currentTheme() {
  const chosen = document.documentElement.dataset.theme;
  if (chosen) return chosen;
  return window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
}

function labelThemeButton() {
  const label = `Switch to ${currentTheme() === "dark" ? "light" : "dark"} mode`;
  const button = $("#theme-toggle");
  button.setAttribute("aria-label", label);
  button.title = label;
}

function toggleTheme() {
  const next = currentTheme() === "dark" ? "light" : "dark";
  document.documentElement.dataset.theme = next;
  try {
    localStorage.setItem(THEME_STORAGE, next);
  } catch {
    // storage blocked: the choice lasts until the page is reloaded
  }
  labelThemeButton();
}

// ------------------------------------------------------------------ live status

async function checkStatus() {
  const box = $("#status");
  let state = "down";
  let text = "Unreachable";
  let title = "The server did not answer.";
  try {
    const response = await fetch("/readyz", { cache: "no-store" });
    const body = await response.json();
    const checks = body.checks || {};
    title = Object.entries(checks).map(([k, v]) => `${k}: ${v}`).join(", ") || body.status;
    if (body.status === "ok") [state, text] = ["ok", "Operational"];
    else if (body.status === "degraded") [state, text] = ["warn", "Degraded"];
    else if (body.status === "draining") [state, text] = ["warn", "Deploying"];
    else [state, text] = ["down", "Unavailable"];
  } catch {
    // keep "Unreachable"
  }
  box.dataset.state = state;
  box.title = title;
  $("#status-text").textContent = text;
}

// ------------------------------------------------------------------ code blocks

async function copyText(text) {
  try {
    await navigator.clipboard.writeText(text);
    return true;
  } catch {
    // navigator.clipboard needs HTTPS; plain HTTP falls back to a hidden textarea.
    const area = el("textarea", { readonly: true, class: "offscreen" });
    area.value = text;
    document.body.append(area);
    area.select();
    let ok = false;
    try {
      ok = document.execCommand("copy");
    } catch {
      ok = false;
    }
    area.remove();
    return ok;
  }
}

function setUpCodeBlocks() {
  for (const node of document.querySelectorAll("[data-origin]")) node.textContent = location.origin;
  for (const block of document.querySelectorAll(".code:not(.response)")) {
    const button = el("button", { type: "button", class: "copy" }, "Copy");
    button.addEventListener("click", async () => {
      const ok = await copyText(block.querySelector("code").textContent);
      button.textContent = ok ? "Copied" : "Select and copy";
      setTimeout(() => (button.textContent = "Copy"), 1600);
    });
    block.append(button);
  }
}

// ------------------------------------------------------------------ API calls

class ApiError extends Error {
  constructor(status, code, message, retryAfter) {
    super(message);
    this.status = status;
    this.code = code;
    this.retryAfter = retryAfter;
  }
}

let apiKey = null;

async function api(method, path, body, headers = {}) {
  const init = {
    method,
    cache: "no-store",
    headers: { Authorization: `Bearer ${apiKey}`, ...headers },
  };
  if (body !== undefined) {
    init.headers["Content-Type"] = "application/json";
    init.body = JSON.stringify(body);
  }
  let response;
  try {
    response = await fetch(path, init);
  } catch {
    throw new ApiError(0, "network", "The server can't be reached.", 5);
  }
  const data = response.status === 204 ? null : await response.json().catch(() => null);
  if (!response.ok) {
    const error = (data && data.error) || {};
    throw new ApiError(
      response.status,
      error.code || "http_error",
      error.message || `HTTP ${response.status}`,
      Number(response.headers.get("Retry-After")) || 0,
    );
  }
  return { status: response.status, data };
}

// ------------------------------------------------------------------ formatting

function badge(status) {
  return el("span", { class: `badge ${status}` }, status.replaceAll("_", " "));
}

function seconds(from, to) {
  return (new Date(to) - new Date(from)) / 1000;
}

// A duration: "350 ms", "1.2 s", "4 min". Whole seconds only when it says how long ago.
function span(s, whole = false) {
  const abs = Math.abs(s);
  if (abs < 1 && !whole) return `${Math.round(abs * 1000)} ms`;
  if (abs < 60) return `${abs < 10 && !whole ? abs.toFixed(1) : Math.round(abs)} s`;
  if (abs < 3600) return `${Math.round(abs / 60)} min`;
  if (abs < 86400) return `${Math.round(abs / 3600)} h`;
  return `${Math.round(abs / 86400)} d`;
}

function relative(iso) {
  if (!iso) return "—";
  const s = seconds(iso, Date.now());
  if (Math.abs(s) < 2) return "just now";
  return s > 0 ? `${span(s, true)} ago` : `in ${span(s, true)}`;
}

function when(iso) {
  return iso ? el("time", { datetime: iso, title: new Date(iso).toLocaleString() }, relative(iso)) : "—";
}

function took(job) {
  if (job.status === "queued") {
    const wait = seconds(job.run_at, Date.now());
    return wait < 0 ? `starts in ${span(wait, true)}` : "waiting";
  }
  if (job.status === "running" && job.started_at) return `running ${span(seconds(job.started_at, Date.now()), true)}`;
  if (job.started_at && job.finished_at) return span(seconds(job.started_at, job.finished_at));
  return "—";
}

function shortId(id) {
  return id.slice(0, 8);
}

function json(value) {
  return JSON.stringify(value, null, 2);
}

// ------------------------------------------------------------------ console: connect

function readStoredKey() {
  try {
    return sessionStorage.getItem(KEY_STORAGE);
  } catch {
    return null;
  }
}

function storeKey(value) {
  try {
    if (value) sessionStorage.setItem(KEY_STORAGE, value);
    else sessionStorage.removeItem(KEY_STORAGE);
  } catch {
    // storage blocked: the key lives in memory only, which is fine
  }
}

function keyLabel(key) {
  // hop_live_<prefix>_<secret>: show the prefix, which identifies the key, never the secret.
  const parts = key.split("_");
  return parts.length >= 4 ? `${parts.slice(0, 3).join("_")}_…` : `${key.slice(0, 12)}…`;
}

async function connect(event) {
  event.preventDefault();
  const input = $("#key-input");
  const errorBox = $("#key-error");
  const candidate = input.value.trim();
  errorBox.hidden = true;
  if (!candidate) {
    errorBox.textContent = "Paste an API key first.";
    errorBox.hidden = false;
    return;
  }
  apiKey = candidate;
  try {
    await api("GET", "/v1/jobs?limit=1");
  } catch (error) {
    apiKey = null;
    errorBox.textContent =
      error.status === 401 ? "This key was not accepted. Check it, or ask for a new one." : error.message;
    errorBox.hidden = false;
    return;
  }
  input.value = "";
  storeKey(candidate);
  showConsole();
}

function showConsole() {
  $("#signed-out").hidden = true;
  $("#signed-in").hidden = false;
  $("#key-label").textContent = keyLabel(apiKey);
  resumePolling();
}

function signOut(message) {
  apiKey = null;
  storeKey(null);
  clearTimeout(pollTimer);
  $("#jobs-body").replaceChildren();
  $("#dlq-body").replaceChildren();
  $("#job-dialog").close();
  $("#signed-in").hidden = true;
  $("#signed-out").hidden = false;
  const errorBox = $("#key-error");
  errorBox.textContent = message || "";
  errorBox.hidden = !message;
}

// ------------------------------------------------------------------ console: polling

let pollTimer = null;
let statusFilter = "";
let openJobId = null;
let shownJob = ""; // the open job as last drawn, so a poll that changes nothing redraws nothing

function schedulePoll(delay) {
  clearTimeout(pollTimer);
  pollTimer = setTimeout(poll, delay);
}

function resumePolling() {
  if (apiKey && activeTab === "try" && !document.hidden) schedulePoll(0);
}

function setLive(state, text) {
  $("#live").dataset.state = state;
  $("#live-text").textContent = text;
}

function showNotice(text) {
  const box = $("#notice");
  box.textContent = text || "";
  box.hidden = !text;
}

async function poll() {
  // Paused while the tab is hidden or another section is open; resumePolling() restarts it.
  if (!apiKey || activeTab !== "try" || document.hidden) return;
  try {
    await Promise.all([refreshJobs(), refreshDlq(), openJobId ? refreshDialog() : null]);
    setLive("ok", "Live");
    showNotice("");
    schedulePoll(POLL_MS);
  } catch (error) {
    if (error.status === 401) {
      signOut("The key is no longer accepted. It may have been revoked.");
      return;
    }
    const wait = Math.max(error.retryAfter || 0, 5);
    const why = {
      429: "This key's rate limit is used up",
      503: "The service is unavailable",
      0: "The server can't be reached",
    }[error.status] || error.message;
    setLive("warn", "Paused");
    showNotice(`${why}. Trying again in ${wait} s.`);
    schedulePoll(wait * 1000);
  }
}

async function refreshJobs() {
  const query = new URLSearchParams({ limit: LIST_LIMIT });
  if (statusFilter) query.set("status", statusFilter);
  const { data } = await api("GET", `/v1/jobs?${query}`);
  const focused = document.activeElement && document.activeElement.dataset.jobId;
  const rows = data.jobs.map((job) =>
    clickableRow(
      job.id,
      el("td", {}, badge(job.status)),
      el("td", {}, job.task),
      el("td", { class: "mono" }, `${job.attempts}/${job.max_attempts}`),
      el("td", {}, when(job.created_at)),
      el("td", { class: "muted" }, took(job)),
      el("td", { class: "mono muted" }, shortId(job.id)),
    ),
  );
  $("#jobs-body").replaceChildren(...rows);
  $("#jobs-empty").hidden = rows.length > 0;
  if (focused) {
    const again = document.querySelector(`#jobs-body [data-job-id="${CSS.escape(focused)}"]`);
    if (again) again.focus();
  }
}

async function refreshDlq() {
  const { data } = await api("GET", `/v1/dlq?limit=${LIST_LIMIT}`);
  const rows = data.jobs.map((job) => {
    const replay = el("button", { type: "button", class: "btn small" }, "Replay");
    replay.addEventListener("click", (event) => {
      event.stopPropagation();
      replayJob(job.id, replay);
    });
    return clickableRow(
      job.id,
      el("td", {}, job.task),
      el("td", { class: "error-cell" }, job.last_error || "—"),
      el("td", { class: "mono" }, `${job.attempts}/${job.max_attempts}`),
      el("td", {}, when(job.dead_at)),
      el("td", {}, replay),
    );
  });
  $("#dlq-body").replaceChildren(...rows);
  $("#dlq-empty").hidden = rows.length > 0;
  $("#replay-all").disabled = rows.length === 0;
}

function clickableRow(jobId, ...cells) {
  const row = el("tr", { class: "clickable", tabindex: 0, "data-job-id": jobId, title: "Open this job" }, ...cells);
  row.addEventListener("click", () => openJob(jobId));
  row.addEventListener("keydown", (event) => {
    if (event.target === row && (event.key === "Enter" || event.key === " ")) {
      event.preventDefault();
      openJob(jobId);
    }
  });
  return row;
}

function setFilter(button) {
  statusFilter = button.dataset.status;
  for (const b of document.querySelectorAll("#status-filter button")) {
    b.setAttribute("aria-pressed", String(b === button));
  }
  schedulePoll(0);
}

// ------------------------------------------------------------------ console: actions

async function replayJob(jobId, button) {
  button.disabled = true;
  try {
    await api("POST", `/v1/jobs/${jobId}/replay`);
    showNotice("");
  } catch (error) {
    showNotice(`Replay failed: ${error.message}`);
  }
  schedulePoll(0);
}

async function replayAll() {
  if (!window.confirm("Replay every dead job of this tenant now?")) return;
  const button = $("#replay-all");
  button.disabled = true;
  try {
    const { data } = await api("POST", "/v1/dlq/replay", { filter: {}, spread_seconds: 0 });
    showNotice(
      `Replayed ${data.replayed} job${data.replayed === 1 ? "" : "s"}.` +
        (data.has_more ? " More are left: press Replay all again." : ""),
    );
  } catch (error) {
    showNotice(`Replay failed: ${error.message}`);
  }
  schedulePoll(0);
}

function fillPayload() {
  const spec = TASKS[$("#task").value];
  $("#payload").value = json(spec.payload);
  $("#task-help").textContent = spec.help;
}

function formResult(kind, ...parts) {
  const box = $("#job-form-result");
  box.className = `form-result ${kind}`;
  box.replaceChildren(...parts);
}

function openLink(jobId, text) {
  const link = el("button", { type: "button", class: "linkish" }, text);
  link.addEventListener("click", () => openJob(jobId));
  return link;
}

async function enqueue(event) {
  event.preventDefault();
  let payload;
  try {
    payload = JSON.parse($("#payload").value || "{}");
  } catch {
    formResult("bad", "The payload is not valid JSON.");
    return;
  }
  if (payload === null || typeof payload !== "object" || Array.isArray(payload)) {
    formResult("bad", "The payload must be a JSON object, like {\"ms\": 100}.");
    return;
  }
  const count = Math.floor(Number($("#count").value) || 1);
  if (count < 1 || count > 50) {
    formResult("bad", "How many: between 1 and 50.");
    return;
  }
  const idempotencyKey = $("#idem").value.trim();
  if (idempotencyKey && count > 1) {
    formResult("bad", "An idempotency key names one job: set How many to 1.");
    return;
  }
  const body = { task: $("#task").value, payload };
  const delay = Number($("#delay").value);
  if ($("#delay").value !== "" && delay > 0) body.delay_seconds = delay;
  const headers = idempotencyKey ? { "Idempotency-Key": idempotencyKey } : {};

  const button = $("#enqueue-btn");
  button.disabled = true;
  formResult("", count > 1 ? `Enqueuing ${count} jobs…` : "Enqueuing…");
  let created = 0;
  let last = null;
  try {
    for (let i = 0; i < count; i++) {
      const response = await api("POST", "/v1/jobs", body, headers);
      last = response;
      if (response.status === 201) created++;
    }
    if (last.status === 200) {
      formResult(
        "ok",
        "This idempotency key was used before, so Hopper returned the existing job instead of creating a new one: ",
        openLink(last.data.id, shortId(last.data.id)),
      );
    } else if (count === 1) {
      formResult("ok", "Created job ", openLink(last.data.id, shortId(last.data.id)), ".");
    } else {
      formResult("ok", `Created ${created} jobs.`);
    }
  } catch (error) {
    const done = created ? `Created ${created}, then stopped: ` : "";
    formResult("bad", `${done}${error.message}`);
    if (error.status === 401) signOut("The key is no longer accepted.");
  } finally {
    button.disabled = false;
  }
  schedulePoll(0);
}

// ------------------------------------------------------------------ console: one job

async function openJob(jobId) {
  openJobId = jobId;
  shownJob = "";
  $("#dialog-title").replaceChildren("Job");
  $("#dialog-body").replaceChildren(el("p", { class: "muted" }, "Loading…"));
  const dialog = $("#job-dialog");
  if (!dialog.open) dialog.showModal();
  try {
    await refreshDialog();
  } catch (error) {
    $("#dialog-body").replaceChildren(el("p", { class: "form-error" }, error.message));
  }
}

async function refreshDialog() {
  const jobId = openJobId;
  const { data } = await api("GET", `/v1/jobs/${jobId}`);
  const drawn = JSON.stringify(data);
  if (openJobId === jobId && drawn !== shownJob) {
    shownJob = drawn;
    renderDialog(data);
  }
}

function fact(name, value) {
  return el("div", {}, el("dt", {}, name), el("dd", {}, value));
}

function renderDialog(job) {
  $("#dialog-title").replaceChildren(job.task, " ", badge(job.status));
  const message = el("span", { class: "dialog-message", role: "status" });
  const actions = el("div", { class: "dialog-actions" });
  if (job.status === "queued") actions.append(actionButton("Cancel job", "danger", `/v1/jobs/${job.id}/cancel`, message));
  if (job.status === "dead") actions.append(actionButton("Replay", "primary", `/v1/jobs/${job.id}/replay`, message));
  actions.append(message);

  const attempts = job.attempt_history.length
    ? el(
        "div",
        { class: "table-wrap" },
        el(
          "table",
          { class: "jobs" },
          el(
            "thead",
            {},
            el("tr", {}, ...["#", "Worker", "Started", "Took", "Outcome", "Error"].map((h) => el("th", { scope: "col" }, h))),
          ),
          el(
            "tbody",
            {},
            ...job.attempt_history.map((a) =>
              el(
                "tr",
                {},
                el("td", { class: "mono" }, String(a.attempt)),
                el("td", { class: "mono muted" }, a.worker_id),
                el("td", {}, when(a.started_at)),
                el("td", { class: "muted" }, a.finished_at ? span(seconds(a.started_at, a.finished_at)) : "—"),
                el("td", {}, badge(a.outcome)),
                el("td", { class: "error-cell" }, a.error || "—"),
              ),
            ),
          ),
        ),
      )
    : el("p", { class: "muted" }, "Not run yet.");

  const outcome = job.result
    ? [el("h3", {}, "Result"), el("pre", {}, json(job.result))]
    : job.last_error
      ? [el("h3", {}, "Last error"), el("pre", { class: "error" }, job.last_error)]
      : [];

  $("#dialog-body").replaceChildren(
    el(
      "dl",
      { class: "facts" },
      fact("ID", el("span", { class: "mono" }, job.id)),
      fact("Queue", job.queue),
      fact("Attempts", `${job.attempts} of ${job.max_attempts}`),
      fact("Created", when(job.created_at)),
      fact("Runs at", when(job.run_at)),
      fact("Finished", when(job.finished_at || job.dead_at)),
      fact("Priority", String(job.priority)),
      fact("Timeout", `${job.timeout_seconds} s`),
      fact("Replayed", `${job.replay_count} time${job.replay_count === 1 ? "" : "s"}`),
    ),
    el("h3", {}, "Payload"),
    el("pre", {}, json(job.payload)),
    ...outcome,
    el("h3", {}, "Attempts"),
    attempts,
    actions,
  );
}

function actionButton(label, kind, path, message) {
  const button = el("button", { type: "button", class: `btn small ${kind}` }, label);
  button.addEventListener("click", async () => {
    button.disabled = true;
    try {
      const { data } = await api("POST", path);
      shownJob = JSON.stringify(data);
      renderDialog(data);
      schedulePoll(0);
    } catch (error) {
      button.disabled = false;
      message.textContent = error.message;
    }
  });
  return button;
}

// ------------------------------------------------------------------ start

document.addEventListener("DOMContentLoaded", () => {
  setUpCodeBlocks();
  labelThemeButton();
  $("#theme-toggle").addEventListener("click", toggleTheme);
  window.matchMedia("(prefers-color-scheme: dark)").addEventListener("change", labelThemeButton);

  $("#key-form").addEventListener("submit", connect);
  $("#sign-out").addEventListener("click", () => signOut());
  $("#job-form").addEventListener("submit", enqueue);
  $("#task").addEventListener("change", fillPayload);
  $("#replay-all").addEventListener("click", replayAll);
  for (const b of document.querySelectorAll("#status-filter button")) {
    b.addEventListener("click", () => setFilter(b));
  }
  const dialog = $("#job-dialog");
  $("#dialog-close").addEventListener("click", () => dialog.close());
  dialog.addEventListener("close", () => (openJobId = null));
  dialog.addEventListener("click", (event) => {
    if (event.target === dialog) dialog.close(); // a click on the backdrop
  });
  document.addEventListener("visibilitychange", resumePolling);
  window.addEventListener("hashchange", showFromHash);

  fillPayload();
  apiKey = readStoredKey();
  if (apiKey) showConsole();
  showFromHash();
  checkStatus();
  setInterval(checkStatus, STATUS_MS);
});
