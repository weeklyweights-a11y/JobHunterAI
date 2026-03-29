/**
 * JobHunter AI dashboard — Phase 2: config UI, save/load, jobs shell.
 */

const ALLOWED_SCHEDULE = [2, 4, 6, 8, 12, 24];

const LLM_KEY_LABELS = {
    gemini: "Google API Key",
    openai: "OpenAI API Key",
    anthropic: "Anthropic API Key",
    ollama: "",
};

const state = {
    roles: [],
    locations: [],
    careerPages: [],
    customSites: [],
    jobsFilter: "all",
    jobFoundCount: 0,
};

let huntEventSource = null;

function showToast(message, type) {
    const root = document.getElementById("toast-root");
    if (!root) {
        return;
    }
    const t = document.createElement("div");
    t.className = "toast " + (type || "");
    t.textContent = message;
    root.appendChild(t);
    setTimeout(function () {
        t.remove();
    }, 4000);
}

function updateLlmUi() {
    const provider = document.getElementById("llm-provider").value;
    const keyGroup = document.getElementById("llm-key-group");
    const keyLabel = document.getElementById("llm-key-label");
    const ollamaHint = document.getElementById("ollama-hint");
    const keyHint = document.getElementById("llm-key-hint");
    const validateBtn = document.getElementById("btn-validate-llm-key");
    if (provider === "ollama") {
        keyGroup.classList.add("hidden");
        ollamaHint.classList.remove("hidden");
        if (keyHint) {
            keyHint.classList.add("hidden");
        }
        if (validateBtn) {
            validateBtn.classList.add("hidden");
        }
    } else {
        keyGroup.classList.remove("hidden");
        ollamaHint.classList.add("hidden");
        if (keyHint) {
            keyHint.classList.remove("hidden");
        }
        keyLabel.textContent = LLM_KEY_LABELS[provider] || "API Key";
        if (validateBtn) {
            validateBtn.classList.toggle("hidden", provider !== "gemini");
        }
    }
}

function renderChips(containerId, items, onRemoveAt) {
    const el = document.getElementById(containerId);
    el.innerHTML = "";
    items.forEach(function (text, index) {
        const chip = document.createElement("span");
        chip.className = "chip";
        const label = document.createElement("span");
        label.textContent = text;
        chip.appendChild(label);
        const btn = document.createElement("button");
        btn.type = "button";
        btn.className = "chip-remove";
        btn.setAttribute("aria-label", "Remove " + text);
        btn.textContent = "×";
        btn.addEventListener("click", function () {
            onRemoveAt(index);
        });
        chip.appendChild(btn);
        el.appendChild(chip);
    });
}

function renderRoleChips() {
    renderChips("role-chips", state.roles, function (idx) {
        state.roles.splice(idx, 1);
        renderRoleChips();
    });
}

function renderLocationChips() {
    renderChips("location-chips", state.locations, function (idx) {
        state.locations.splice(idx, 1);
        renderLocationChips();
    });
}

function renderUrlList(listId, urls, onRemove) {
    const ul = document.getElementById(listId);
    ul.innerHTML = "";
    urls.forEach(function (url) {
        const li = document.createElement("li");
        const span = document.createElement("span");
        span.textContent = url;
        li.appendChild(span);
        const rm = document.createElement("button");
        rm.type = "button";
        rm.className = "btn btn-secondary";
        rm.style.fontSize = "0.75rem";
        rm.style.padding = "0.2rem 0.45rem";
        rm.textContent = "Remove";
        rm.addEventListener("click", function () {
            onRemove(url);
        });
        li.appendChild(rm);
        ul.appendChild(li);
    });
}

function setSourcesFromData(sources) {
    const keys = ["linkedin", "indeed", "yc", "career_page"];
    keys.forEach(function (key) {
        const cb = document.querySelector('input[data-source-key="' + key + '"]');
        if (cb) {
            cb.checked = sources[key] !== false;
        }
    });
}

function getSourcesPayload() {
    return {
        linkedin: document.getElementById("src-linkedin").checked,
        indeed: document.getElementById("src-indeed").checked,
        yc: document.getElementById("src-yc").checked,
        career_page: document.getElementById("src-career").checked,
    };
}

/** Same fields as Save settings; used so Start Hunting runs against the UI, not stale DB rows. */
function buildSettingsPayload(includeSecretsIfPresent) {
    const provider = document.getElementById("llm-provider").value;
    const payload = {
        roles: state.roles,
        locations: state.locations,
        experience: document.getElementById("experience-select").value,
        email_address: document.getElementById("email-address").value.trim(),
        sources: getSourcesPayload(),
        career_pages: state.careerPages,
        custom_sites: state.customSites,
        schedule_hours: Number(document.getElementById("schedule-hours").value),
        llm_provider: provider,
        browser_cdp_url: (function () {
            const el = document.getElementById("browser-cdp-url");
            return el && el.value ? el.value.trim() : "";
        })(),
    };
    if (includeSecretsIfPresent) {
        const ep = document.getElementById("email-password").value;
        if (ep) {
            payload.email_app_password = ep;
        }
        const lk = document.getElementById("llm-api-key").value;
        if (lk && provider !== "ollama") {
            payload.llm_api_key = lk;
        }
    }
    return payload;
}

async function apiJson(url, options) {
    const res = await fetch(url, options);
    const data = await res.json().catch(function () {
        return {};
    });
    if (!res.ok) {
        let detail = data.detail || res.statusText || "Request failed";
        if (Array.isArray(detail)) {
            detail = detail
                .map(function (x) {
                    return x.msg || JSON.stringify(x);
                })
                .join("; ");
        } else if (typeof detail !== "string") {
            detail = JSON.stringify(detail);
        }
        throw new Error(detail);
    }
    return data;
}

async function loadConfig() {
    const data = (await apiJson("/api/config", { method: "GET" })).data;
    state.roles = data.roles.slice();
    state.locations = data.locations.slice();
    state.careerPages = data.career_pages.slice();
    state.customSites = data.custom_sites.slice();

    renderRoleChips();
    renderLocationChips();

    document.getElementById("experience-select").value = data.experience || "any";
    setSourcesFromData(data.sources || {});
    document.getElementById("email-address").value = data.email_address || "";
    document.getElementById("email-password").value = "";
    document.getElementById("llm-provider").value = data.llm_provider || "gemini";
    document.getElementById("llm-api-key").value = "";
    const cdpEl = document.getElementById("browser-cdp-url");
    if (cdpEl) {
        cdpEl.value = data.browser_cdp_url || "";
    }
    updateLlmUi();

    let sh = Number(data.schedule_hours);
    if (!ALLOWED_SCHEDULE.includes(sh)) {
        sh = 4;
    }
    document.getElementById("schedule-hours").value = String(sh);

    renderUrlList("career-url-list", state.careerPages, removeCareerUrl);
    renderUrlList("custom-url-list", state.customSites, removeCustomUrl);
}

function wireTagInput(inputId, addBtnId, listKey, renderFn) {
    const input = document.getElementById(inputId);
    const addBtn = document.getElementById(addBtnId);

    function add() {
        const v = input.value.trim();
        if (!v) {
            return;
        }
        state[listKey].push(v);
        input.value = "";
        renderFn();
    }

    addBtn.addEventListener("click", add);
    input.addEventListener("keydown", function (e) {
        if (e.key === "Enter") {
            e.preventDefault();
            add();
        }
    });
}

async function addCareerUrl() {
    const input = document.getElementById("career-url-input");
    const raw = input.value.trim();
    if (!raw) {
        return;
    }
    try {
        const res = await apiJson("/api/career-pages", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ url: raw }),
        });
        state.careerPages = res.data.career_pages.slice();
        input.value = "";
        renderUrlList("career-url-list", state.careerPages, removeCareerUrl);
    } catch (e) {
        showToast(e.message, "error");
    }
}

async function removeCareerUrl(url) {
    try {
        const res = await apiJson("/api/career-pages", {
            method: "DELETE",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ url: url }),
        });
        state.careerPages = res.data.career_pages.slice();
        renderUrlList("career-url-list", state.careerPages, removeCareerUrl);
    } catch (e) {
        showToast(e.message, "error");
    }
}

async function addCustomUrl() {
    const input = document.getElementById("custom-url-input");
    const raw = input.value.trim();
    if (!raw) {
        return;
    }
    try {
        const res = await apiJson("/api/custom-sites", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ url: raw }),
        });
        state.customSites = res.data.custom_sites.slice();
        input.value = "";
        renderUrlList("custom-url-list", state.customSites, removeCustomUrl);
    } catch (e) {
        showToast(e.message, "error");
    }
}

async function removeCustomUrl(url) {
    try {
        const res = await apiJson("/api/custom-sites", {
            method: "DELETE",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ url: url }),
        });
        state.customSites = res.data.custom_sites.slice();
        renderUrlList("custom-url-list", state.customSites, removeCustomUrl);
    } catch (e) {
        showToast(e.message, "error");
    }
}

async function validateLlmKey() {
    const provider = document.getElementById("llm-provider").value;
    const keyEl = document.getElementById("llm-api-key");
    const key = keyEl && keyEl.value ? keyEl.value.trim() : "";
    if (provider === "ollama") {
        showToast("Ollama has no API key to test here.", "");
        return;
    }
    if (provider !== "gemini") {
        showToast("Key test is only implemented for Gemini. Save and use your provider's console for others.", "error");
        return;
    }
    try {
        const res = await apiJson("/api/validate-llm-key", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                provider: provider,
                api_key: key || null,
            }),
        });
        const msg = (res.data && res.data.message) || "API key is valid.";
        showToast(msg, "success");
    } catch (e) {
        showToast(e.message || "Validation failed", "error");
    }
}

async function saveSettings(e) {
    e.preventDefault();
    const payload = buildSettingsPayload(true);
    try {
        const res = await apiJson("/api/config", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload),
        });
        document.getElementById("email-password").value = "";
        document.getElementById("llm-api-key").value = "";
        showToast("Settings saved.", "success");
        if (res.meta && res.meta.scheduler_activated) {
            showToast(
                "Auto-run enabled! First run starting in 30 seconds...",
                "success"
            );
        }
        await loadDownloadAvailability();
        await loadJobStats();
        await loadSchedulerStatus();
    } catch (err) {
        showToast(err.message, "error");
    }
}

function sourceDisplayName(source) {
    const m = {
        linkedin: "LinkedIn",
        indeed: "Indeed",
        yc: "YC",
        career_page: "Career Page",
    };
    return m[source] || source || "";
}

function formatRelativeTime(iso) {
    if (!iso) {
        return "";
    }
    const t = new Date(iso).getTime();
    if (Number.isNaN(t)) {
        return String(iso);
    }
    const sec = Math.round((t - Date.now()) / 1000);
    const rtf = new Intl.RelativeTimeFormat("en", { numeric: "auto" });
    const abs = Math.abs(sec);
    if (abs < 60) {
        return rtf.format(sec, "second");
    }
    const min = Math.round(sec / 60);
    if (Math.abs(min) < 60) {
        return rtf.format(min, "minute");
    }
    const hr = Math.round(min / 60);
    if (Math.abs(hr) < 24) {
        return rtf.format(hr, "hour");
    }
    const day = Math.round(hr / 24);
    return rtf.format(day, "day");
}

function formatDurationSeconds(sec) {
    if (sec == null || Number.isNaN(sec)) {
        return "—";
    }
    if (sec < 60) {
        /* Math.round(0.1) === 0 — show one decimal under 10s */
        if (sec > 0 && sec < 10) {
            return (Math.round(sec * 10) / 10).toFixed(1).replace(/\.0$/, "") + "s";
        }
        return Math.round(sec) + "s";
    }
    const m = Math.floor(sec / 60);
    const s = Math.round(sec % 60);
    return m + "m " + s + "s";
}

function renderJobs(jobs) {
    const tbody = document.getElementById("jobs-tbody");
    const card = document.querySelector(".jobs-card");
    const empty = document.getElementById("jobs-empty");
    tbody.innerHTML = "";
    const list = jobs || [];
    if (list.length === 0) {
        card.classList.remove("has-rows");
        empty.classList.remove("hidden");
        return;
    }
    card.classList.add("has-rows");
    empty.classList.add("hidden");
    list.forEach(function (job) {
        const tr = document.createElement("tr");
        tr.dataset.source = job.source || "";

        const tdTitle = document.createElement("td");
        tdTitle.textContent = job.title != null ? String(job.title) : "";
        tr.appendChild(tdTitle);

        const tdCo = document.createElement("td");
        tdCo.textContent = job.company != null ? String(job.company) : "";
        tr.appendChild(tdCo);

        const tdSrc = document.createElement("td");
        tdSrc.textContent = sourceDisplayName(job.source || "");
        tr.appendChild(tdSrc);

        const tdFound = document.createElement("td");
        tdFound.textContent = formatRelativeTime(job.found_at);
        tr.appendChild(tdFound);

        const tdLink = document.createElement("td");
        const a = document.createElement("a");
        a.href = job.url || "#";
        a.textContent = "Apply →";
        a.target = "_blank";
        a.rel = "noopener noreferrer";
        tdLink.appendChild(a);
        tr.appendChild(tdLink);

        tbody.appendChild(tr);
    });
}

function jobsTodayQueryUrl() {
    const params = new URLSearchParams();
    if (state.jobsFilter !== "all") {
        params.set("source", state.jobsFilter);
    }
    const q = params.toString();
    return q ? "/api/jobs/today?" + q : "/api/jobs/today";
}

async function loadJobsToday() {
    try {
        const res = await apiJson(jobsTodayQueryUrl(), { method: "GET" });
        renderJobs(res.data || []);
    } catch (e) {
        renderJobs([]);
    }
}

async function loadJobStats() {
    try {
        const res = await apiJson("/api/jobs/stats", { method: "GET" });
        const d = res.data || {};
        const n = typeof d.today === "number" ? d.today : 0;
        document.getElementById("jobs-today-badge").textContent =
            n + " new job" + (n === 1 ? "" : "s") + " today";
    } catch (e) {
        document.getElementById("jobs-today-badge").textContent = "0 new jobs today";
    }
}

async function loadRuns() {
    try {
        const res = await apiJson("/api/runs?limit=10", { method: "GET" });
        const tbody = document.getElementById("runs-tbody");
        tbody.innerHTML = "";
        (res.data || []).forEach(function (r) {
            const tr = document.createElement("tr");
            function cell(t) {
                const td = document.createElement("td");
                td.textContent = t != null ? String(t) : "";
                return td;
            }
            tr.appendChild(cell(r.started_at));
            tr.appendChild(cell(formatDurationSeconds(r.duration_seconds)));
            tr.appendChild(cell(r.jobs_found));
            tr.appendChild(cell(r.status));
            tbody.appendChild(tr);
        });
    } catch (e) {
        /* empty */
    }
}

function renderSchedulerStatus(data) {
    const badge = document.getElementById("scheduler-autorun-badge");
    const nextEl = document.getElementById("scheduler-next-run");
    if (!badge || !nextEl) {
        return;
    }
    const d = data || {};
    const active = !!d.active;
    badge.textContent = active ? "Auto-run: ON" : "Auto-run: OFF";
    badge.classList.toggle("scheduler-badge--on", active);
    badge.classList.toggle("scheduler-badge--off", !active);
    if (!active) {
        nextEl.textContent = "Auto-run disabled (incomplete config)";
        return;
    }
    if (d.next_run) {
        const dt = new Date(d.next_run);
        const local =
            dt && !Number.isNaN(dt.getTime())
                ? dt.toLocaleString(undefined, {
                      dateStyle: "medium",
                      timeStyle: "short",
                  })
                : String(d.next_run);
        nextEl.textContent = "Next run: " + local;
    } else {
        nextEl.textContent = "Next run: —";
    }
}

async function loadSchedulerStatus() {
    try {
        const res = await apiJson("/api/scheduler", { method: "GET" });
        renderSchedulerStatus(res.data || {});
    } catch (e) {
        renderSchedulerStatus({ active: false, next_run: null, interval_hours: 4 });
    }
}

async function loadDownloadAvailability() {
    const btn = document.getElementById("btn-download");
    if (!btn) {
        return;
    }
    try {
        const [statsRes, infoRes] = await Promise.all([
            apiJson("/api/jobs/stats", { method: "GET" }),
            apiJson("/api/download/info", { method: "GET" }),
        ]);
        const total = (statsRes.data && statsRes.data.total) || 0;
        const avail = !!(infoRes.data && infoRes.data.available);
        btn.disabled = !(total > 0 || avail);
    } catch (e) {
        btn.disabled = true;
    }
}

function flashDownloadButton() {
    const btn = document.getElementById("btn-download");
    if (!btn) {
        return;
    }
    btn.classList.add("download-flash");
    setTimeout(function () {
        btn.classList.remove("download-flash");
    }, 4000);
}

function clearStatusLog() {
    const log = document.getElementById("status-log");
    if (log) {
        log.innerHTML = "";
    }
}

function appendLogLine(text, kind) {
    const log = document.getElementById("status-log");
    if (!log) {
        return;
    }
    const line = document.createElement("div");
    line.className =
        "status-log-line status-log--" +
        (kind === "success" ? "success" : kind === "error" ? "error" : "default");
    const ts = new Date().toLocaleTimeString();
    line.textContent = "[" + ts + "] " + text;
    log.appendChild(line);
    log.scrollTop = log.scrollHeight;
}

function setJobFoundCounterVisible(on) {
    const el = document.getElementById("job-found-counter");
    if (!el) {
        return;
    }
    if (on && state.jobFoundCount > 0) {
        el.classList.remove("hidden");
        el.textContent =
            state.jobFoundCount +
            " job" +
            (state.jobFoundCount === 1 ? "" : "s") +
            " found this run";
    } else {
        el.classList.add("hidden");
        el.textContent = "";
    }
}

function closeHuntEventSource() {
    if (huntEventSource) {
        huntEventSource.close();
        huntEventSource = null;
    }
}

function setHuntButtonsRunning(running) {
    const startBtn = document.getElementById("btn-start-hunt");
    const stopBtn = document.getElementById("btn-stop-hunt");
    if (running) {
        startBtn.classList.add("hidden");
        stopBtn.classList.remove("hidden");
    } else {
        stopBtn.classList.add("hidden");
        startBtn.classList.remove("hidden");
    }
}

function wireSseEventListeners() {
    if (!huntEventSource) {
        return;
    }
    const types = ["status", "job_found", "source_complete", "error", "complete"];
    types.forEach(function (t) {
        huntEventSource.addEventListener(t, function (ev) {
            handleSsePayload(t, ev.data);
        });
    });
}

function handleSsePayload(type, rawData) {
    let payload;
    try {
        payload = JSON.parse(rawData);
    } catch (e) {
        appendLogLine(String(rawData), type === "error" ? "error" : "default");
        return;
    }
    const msg = payload.message != null ? String(payload.message) : "";
    if (type === "error") {
        appendLogLine(msg, "error");
    } else if (type === "source_complete") {
        appendLogLine("✓ " + msg, "success");
    } else if (type === "complete") {
        appendLogLine(msg, "success");
        closeHuntEventSource();
        setHuntButtonsRunning(false);
        setJobFoundCounterVisible(false);
        loadJobsToday();
        loadJobStats();
        const rh = document.getElementById("run-history-panel");
        if (rh && rh.open) {
            loadRuns();
        }
        const d = payload.data || {};
        const tn = d.total_new != null ? Number(d.total_new) : 0;
        if (tn > 0 && !d.stopped && !d.failed) {
            loadDownloadAvailability().then(function () {
                flashDownloadButton();
            });
        } else {
            loadDownloadAvailability();
        }
        loadSchedulerStatus();
        if (msg && !d.stopped && !d.failed) {
            showToast(msg, "success");
        }
    } else if (type === "job_found") {
        state.jobFoundCount += 1;
        setJobFoundCounterVisible(true);
        appendLogLine(msg, "default");
    } else {
        appendLogLine(msg, "default");
    }
}

async function startHunt() {
    try {
        await apiJson("/api/config", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(buildSettingsPayload(true)),
        });
        await loadSchedulerStatus();
        await apiJson("/api/start", { method: "POST" });
        state.jobFoundCount = 0;
        setJobFoundCounterVisible(false);
        clearStatusLog();
        setHuntButtonsRunning(true);
        closeHuntEventSource();
        huntEventSource = new EventSource("/api/events");
        wireSseEventListeners();
    } catch (e) {
        const m = e.message || "";
        if (m.indexOf("already running") >= 0) {
            showToast("Agent is already running", "error");
        } else {
            showToast(m, "error");
        }
    }
}

async function stopHunt() {
    closeHuntEventSource();
    setHuntButtonsRunning(false);
    setJobFoundCounterVisible(false);
    try {
        await apiJson("/api/stop", { method: "POST" });
    } catch (e) {
        showToast(e.message, "error");
    }
    appendLogLine("Agent stopped", "default");
    showToast("Agent stopped", "");
}

async function syncStatus() {
    try {
        const res = await apiJson("/api/status", { method: "GET" });
        const d = res.data || {};
        const log = document.getElementById("status-log");
        if (log && log.childElementCount === 0 && d.progress) {
            appendLogLine(d.progress, "default");
        }
    } catch (e) {
        /* keep default */
    }
}

function wireFilters() {
    document.querySelectorAll(".filter-btn").forEach(function (btn) {
        btn.addEventListener("click", function () {
            document.querySelectorAll(".filter-btn").forEach(function (b) {
                b.classList.remove("active");
            });
            btn.classList.add("active");
            state.jobsFilter = btn.getAttribute("data-filter") || "all";
            loadJobsToday();
        });
    });
}

document.addEventListener("DOMContentLoaded", function () {
    document.body.classList.add("config-loaded");

    wireTagInput("role-input", "role-add-btn", "roles", renderRoleChips);
    wireTagInput("location-input", "location-add-btn", "locations", renderLocationChips);

    document.getElementById("career-add-btn").addEventListener("click", addCareerUrl);
    document.getElementById("custom-add-btn").addEventListener("click", addCustomUrl);

    document.getElementById("llm-provider").addEventListener("change", updateLlmUi);

    const validateBtn = document.getElementById("btn-validate-llm-key");
    if (validateBtn) {
        validateBtn.addEventListener("click", validateLlmKey);
    }

    document.getElementById("config-form").addEventListener("submit", saveSettings);

    wireFilters();

    document.getElementById("btn-start-hunt").addEventListener("click", startHunt);
    document.getElementById("btn-stop-hunt").addEventListener("click", stopHunt);

    document.getElementById("btn-download").addEventListener("click", function () {
        window.open("/api/download/latest", "_blank", "noopener,noreferrer");
    });

    document.getElementById("run-history-panel").addEventListener("toggle", function (ev) {
        const det = ev.target;
        if (det.open && det.dataset.loaded !== "1") {
            det.dataset.loaded = "1";
            loadRuns();
        }
    });

    window.addEventListener("beforeunload", function () {
        closeHuntEventSource();
    });

    loadConfig()
        .then(function () {
            return Promise.all([
                loadJobsToday(),
                loadJobStats(),
                loadDownloadAvailability(),
                loadSchedulerStatus(),
                syncStatus(),
            ]);
        })
        .catch(function (e) {
            showToast(e.message || "Failed to load config", "error");
        });

    setInterval(function () {
        loadSchedulerStatus();
    }, 60000);
});
