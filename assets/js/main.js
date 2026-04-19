const DATA_URL     = "data/latest.json";
const HISTORY_URL  = "data/history.json";

// Backtest stats + display names per strategy ID (from research screenshots).
const STRATEGY_META = {
  tqqq_tfsa: { name: "Quad Trend",   cagr: "+61.09%", maxdd: "−49.64%" },
  tqqq_rrsp: { name: "Dip Hunter",   cagr: "+70.55%", maxdd: "−43.82%" },
  spxl_tfsa: { name: "Score Filter", cagr: "+40.10%", maxdd: "−40.48%" },
  spxl_rrsp: { name: "Score Filter", cagr: "+40.10%", maxdd: "−40.48%" },
};

// Trigger group display labels per strategy.
const GROUP_LABELS = {
  tqqq_tfsa: { trigger: "DIP DETECTOR",  filter: "TREND FILTERS" },
  tqqq_rrsp: { trigger: "DIP DETECTOR",  filter: "RISK GATES"    },
  spxl_tfsa: { trigger: "TREND SCORE",   filter: "VOL GATE"      },
  spxl_rrsp: { trigger: "TREND SCORE",   filter: "VOL GATE"      },
};

// How many triggers need to fire per strategy.
const TRIGGER_THRESHOLD = {
  tqqq_tfsa: 1,
  tqqq_rrsp: 1,
  spxl_tfsa: 3,
  spxl_rrsp: 3,
};

// ─── Value formatting ─────────────────────────────────────────────────────────

const IND_FMT = {
  SSLP7_3:   "signed", ESLP100_3: "signed", MOM100: "signed",
  MOM150:    "signed",  MOM180:   "signed",  MOM90:  "signed",
  ROC5:      "signed",  RV5:      "signed",  RV7:    "signed",
  SLP5_1:    "signed",  SLP20_1:  "signed",  SLP20_3:"signed",
  SR50_150:  "ratio",   SR63_126: "ratio",   SR150_200:"ratio",
  VR20_100:  "ratio",   ABVMA100: "raw",     last_open:"price",
};

function fmtValue(labelOrKey, value) {
  if (value === null || value === undefined || (typeof value === "number" && isNaN(value))) return "—";
  const key = labelOrKey.split(/[\s<>=!]+/)[0].replace("/", "_");
  const fmt = IND_FMT[key] ?? "raw";
  if (fmt === "signed") {
    const sign = value >= 0 ? "+" : "";
    return `${sign}${value.toFixed(4)}`;
  }
  if (fmt === "ratio") return value.toFixed(3);
  if (fmt === "price") return `$${value.toFixed(2)}`;
  return value.toFixed(4);
}

// ─── Compute streak from history ─────────────────────────────────────────────

function computeStreak(history, id, currentSignal) {
  if (!history || history.length === 0) return null;
  let count = 0;
  for (let i = history.length - 1; i >= 0; i--) {
    const sig = history[i]?.signals?.[id];
    if (sig === currentSignal) count++;
    else break;
  }
  return count;
}

function lastChangeEntry(history, id, currentSignal) {
  if (!history || history.length === 0) return null;
  for (let i = history.length - 1; i >= 0; i--) {
    const sig = history[i]?.signals?.[id];
    if (sig !== currentSignal) {
      const next = history[i + 1];
      return next?.generated_at_utc?.slice(0, 10) ?? null;
    }
  }
  // Signal has been the same for the entire history.
  return history[0]?.generated_at_utc?.slice(0, 10) ?? null;
}

// ─── Render helpers ───────────────────────────────────────────────────────────

function el(tag, cls, html) {
  const e = document.createElement(tag);
  if (cls) e.className = cls;
  if (html != null) e.innerHTML = html;
  return e;
}

function renderRule(cond) {
  const d = el("div", "rule");
  d.innerHTML = `
    <span class="rule-dot ${cond.passed ? "pass" : "fail"}"></span>
    <span class="rule-expr">${escHtml(cond.label)}</span>
    <span class="rule-val ${cond.passed ? "pass" : "fail"}">${fmtValue(cond.label, cond.value)}</span>
  `;
  return d;
}

function escHtml(s) {
  return s.replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;");
}

function renderGroup(conditions, badgeCls, badgeText, groupLabel, miniHtml) {
  const wrap = el("div", "group");
  const labelRow = el("div", "group-label");
  labelRow.innerHTML = `
    <span class="group-badge ${badgeCls}">${badgeText}</span>
    ${escHtml(groupLabel)}
    <span class="group-mini">${miniHtml}</span>
  `;
  wrap.appendChild(labelRow);
  const rows = el("div", "rule-rows");
  conditions.forEach((c) => rows.appendChild(renderRule(c)));
  wrap.appendChild(rows);
  return wrap;
}

function renderCard(s, history) {
  const meta   = STRATEGY_META[s.id] ?? { name: s.name, cagr: "—", maxdd: "—" };
  const labels = GROUP_LABELS[s.id]  ?? { trigger: "TRIGGERS", filter: "FILTERS" };
  const thresh = TRIGGER_THRESHOLD[s.id] ?? 1;

  const triggers = s.conditions.filter((c) => c.group === "trigger");
  const filters  = s.conditions.filter((c) => c.group === "filter");

  const triggerPass  = triggers.filter((c) => c.passed).length;
  const filterPass   = filters.filter((c) => c.passed).length;
  const totalPass    = triggerPass + filterPass;
  const totalConds   = triggers.length + filters.length;

  const streak       = computeStreak(history, s.id, s.buy_signal);
  const changeDate   = lastChangeEntry(history, s.id, s.buy_signal);

  const card = el("div", "card");

  // ── Header ──
  const head = el("div", "card-head");
  const headLeft = el("div");
  headLeft.innerHTML = `
    <div class="card-tag">${escHtml(s.etf)} · ${escHtml(s.account)}</div>
    <div class="card-name">${escHtml(meta.name)}</div>
  `;
  const pillCls  = s.buy_signal ? "long" : (s.conditions[0]?.value === null ? "pend" : "cash");
  const pillText = s.buy_signal ? "HOLD LONG" : (s.conditions[0]?.value === null ? "PENDING" : "IN CASH");
  const pill = el("span", `signal-pill ${pillCls}`, pillText);
  head.appendChild(headLeft);
  head.appendChild(pill);
  card.appendChild(head);

  // ── Streak ──
  const streakEl = el("div", "streak");
  if (streak === null || streak === 0) {
    streakEl.innerHTML = `<span style="color:var(--text-faint);font-size:1rem">—</span><small>${s.buy_signal ? "days in position" : "days in cash"}</small>`;
  } else {
    streakEl.innerHTML = `${streak}<small>${s.buy_signal ? "days in position" : "days in cash"}</small>`;
  }
  card.appendChild(streakEl);

  // ── Metrics ──
  const metrics = el("div", "metrics-row");
  metrics.innerHTML = `
    <div>
      <div class="metric-label">CAGR</div>
      <div class="metric-val good">${meta.cagr}</div>
    </div>
    <div>
      <div class="metric-label">MAX DD</div>
      <div class="metric-val bad">${meta.maxdd}</div>
    </div>
  `;
  card.appendChild(metrics);

  // ── Signals section ──
  const sigSection = el("div", "signals-section");

  const sigHeader = el("div", "signals-header");
  const sigTitle  = el("span", "signals-title", "BUY SIGNAL");
  const summaryOk = s.buy_signal;
  const sigSum    = el("span", `signals-summary ${summaryOk ? "pass" : "fail"}`);
  if (s.conditions[0]?.value === null) {
    sigSum.innerHTML = `<span class="num">PENDING</span>`;
  } else {
    const statusText = summaryOk ? "PASS" : "FAIL";
    sigSum.innerHTML = `<span class="num">${statusText}</span> · ${totalPass}/${totalConds} rules met`;
  }
  sigHeader.appendChild(sigTitle);
  sigHeader.appendChild(sigSum);
  sigSection.appendChild(sigHeader);

  // Trigger group
  const trigThreshBadge = thresh === 1 ? "ANY" : `≥${thresh} OF ${triggers.length}`;
  const trigBadgeCls    = thresh === 1 ? "any"  : "score";
  const trigMini = `<span class="${triggerPass >= thresh ? "p" : "f"}">${triggerPass}</span>/${triggers.length} firing`;
  sigSection.appendChild(renderGroup(triggers, trigBadgeCls, trigThreshBadge, labels.trigger, trigMini));

  // AND joiner
  const joiner = el("div", "joiner");
  joiner.innerHTML = `<div class="joiner-line"></div><div class="joiner-word">AND</div><div class="joiner-line"></div>`;
  sigSection.appendChild(joiner);

  // Filter group
  const filterMini = `<span class="${filterPass === filters.length ? "p" : "f"}">${filterPass}</span>/${filters.length} holding`;
  sigSection.appendChild(renderGroup(filters, "all", "ALL", labels.filter, filterMini));

  card.appendChild(sigSection);

  // ── Footer ──
  const footer = el("div", "card-footer");
  if (changeDate) {
    footer.textContent = `Last change: ${changeDate}`;
  } else {
    footer.textContent = "No history yet · run workflow to populate";
  }
  card.appendChild(footer);

  return card;
}

// ─── Status bar ───────────────────────────────────────────────────────────────

function renderStatusBar(strategies) {
  const bar  = document.getElementById("status-bar");
  const buys = strategies.filter((s) => s.buy_signal);
  const all  = strategies.length;
  const cls  = buys.length === all ? "all-buy" : buys.length > 0 ? "some-buy" : "no-buy";

  const pills = strategies.map((s) => {
    const cls = s.buy_signal ? "buy" : "flat";
    const txt = s.buy_signal ? "BUY" : "FLAT";
    return `<span class="status-pill ${cls}">${escHtml(s.etf)} ${escHtml(s.account)} · ${txt}</span>`;
  }).join(" ");

  bar.innerHTML = `
    <span>Signals today:</span>
    <span class="count ${cls}">${buys.length} / ${all} BUY</span>
    <span class="status-divider"></span>
    ${pills}
  `;
}

// ─── Sources table ────────────────────────────────────────────────────────────

const IND_ORDER = {
  QQQ: ["last_open","SSLP7_3","ESLP100_3","MOM100","MOM150","MOM180","ROC5","RV5","RV7","SR50_150","SR63_126","SR150_200"],
  SPY: ["last_open","MOM90","MOM100","SLP5_1","SLP20_1","SLP20_3","ABVMA100","VR20_100"],
};

function renderSources(sources) {
  const root = document.getElementById("sources-grid");
  root.innerHTML = Object.entries(sources).map(([ticker, ind]) => {
    const rows = (IND_ORDER[ticker] || Object.keys(ind))
      .filter((k) => k in ind && k !== "asof_open_date" && k !== "ABVMA100_true")
      .map((k) => `<tr><th>${k}</th><td>${fmtValue(k, ind[k])}</td></tr>`)
      .join("");
    return `
      <div class="source-card">
        <div class="source-card-head">
          <span class="source-card-name">${escHtml(ticker)}</span>
          <span class="source-card-asof">as of ${ind.asof_open_date ?? "—"}</span>
        </div>
        <table class="source-table"><tbody>${rows}</tbody></table>
      </div>`;
  }).join("");
}

// ─── Main render ──────────────────────────────────────────────────────────────

function renderMeta(data) {
  document.getElementById("meta-generated").textContent =
    `Updated ${data.generated_at_display || data.generated_at_utc || "—"}`;
  const qasof = data.sources?.QQQ?.asof_open_date;
  const sasof = data.sources?.SPY?.asof_open_date;
  document.getElementById("meta-source").textContent =
    `QQQ ${qasof ?? "—"} · SPY ${sasof ?? "—"}`;
}

function renderError(err) {
  document.getElementById("strategies").innerHTML = `
    <div class="card" style="border-color:var(--red-dim)">
      <div class="card-head">
        <div><div class="card-tag">ERROR</div><div class="card-name" style="color:var(--red)">No data yet</div></div>
        <span class="signal-pill pend">PENDING</span>
      </div>
      <div class="card-footer">${escHtml(err.message)} — run the GitHub Action workflow to generate data/latest.json</div>
    </div>`;
  document.getElementById("meta-generated").textContent = "No data file found";
  document.getElementById("meta-source").textContent = "";
}

async function refresh() {
  const btn = document.getElementById("refresh-btn");
  btn.classList.add("loading");
  btn.disabled = true;
  try {
    const [data, history] = await Promise.all([
      fetch(`${DATA_URL}?t=${Date.now()}`,    { cache: "no-store" }).then((r) => { if (!r.ok) throw new Error(`latest.json: ${r.status}`); return r.json(); }),
      fetch(`${HISTORY_URL}?t=${Date.now()}`, { cache: "no-store" }).then((r) => r.ok ? r.json() : []).catch(() => []),
    ]);

    renderMeta(data);
    renderStatusBar(data.strategies);

    const stratRoot = document.getElementById("strategies");
    stratRoot.innerHTML = "";
    data.strategies.forEach((s) => stratRoot.appendChild(renderCard(s, history)));

    renderSources(data.sources);
  } catch (err) {
    console.error(err);
    renderError(err);
  } finally {
    btn.classList.remove("loading");
    btn.disabled = false;
  }
}

document.getElementById("refresh-btn").addEventListener("click", refresh);
refresh();
