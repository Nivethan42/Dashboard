const DATA_URL = "data/latest.json";

const FORMATTERS = {
  percent: (v) => `${(v * 100).toFixed(3)}%`,
  ratio:   (v) => v.toFixed(4),
  raw:     (v) => v.toFixed(4),
  price:   (v) => `$${v.toFixed(2)}`,
};

// Which formatter to use per indicator key.
const IND_FMT = {
  SSLP7_3: "percent",
  ESLP100_3: "percent",
  MOM100: "percent",
  MOM150: "percent",
  MOM180: "percent",
  MOM90: "percent",
  ROC5: "percent",
  RV5: "percent",
  RV7: "percent",
  SR50_150: "ratio",
  SR63_126: "ratio",
  SR150_200: "ratio",
  SLP5_1: "percent",
  SLP20_1: "percent",
  SLP20_3: "percent",
  VR20_100: "ratio",
  ABVMA100: "raw",
  last_open: "price",
};

const INDICATOR_ORDER = {
  QQQ: [
    "last_open", "SSLP7_3", "ESLP100_3",
    "MOM100", "MOM150", "MOM180",
    "ROC5", "RV5", "RV7",
    "SR50_150", "SR63_126", "SR150_200",
  ],
  SPY: [
    "last_open", "MOM90", "MOM100",
    "ABVMA100", "SLP5_1", "SLP20_1", "SLP20_3",
    "VR20_100",
  ],
};

const fmtValue = (key, value) => {
  if (value === null || value === undefined || Number.isNaN(value)) return "—";
  const f = FORMATTERS[IND_FMT[key]] ?? FORMATTERS.raw;
  return f(value);
};

async function loadData() {
  // Bust any CDN cache when user manually refreshes.
  const res = await fetch(`${DATA_URL}?t=${Date.now()}`, { cache: "no-store" });
  if (!res.ok) throw new Error(`Fetch failed (${res.status})`);
  return res.json();
}

function renderOverview(data) {
  const root = document.getElementById("overview");
  const buyCount = data.strategies.filter((s) => s.buy_signal).length;
  const total = data.strategies.length;

  const cards = [
    { k: "Active BUY signals", v: `${buyCount} / ${total}`, cls: buyCount > 0 ? "buy" : "flat" },
    ...data.strategies.map((s) => ({
      k: `${s.etf} · ${s.account}`,
      v: s.buy_signal ? "BUY" : "FLAT",
      cls: s.buy_signal ? "buy" : "flat",
    })),
  ];
  root.innerHTML = cards
    .map((c) => `<div class="overview-card"><span class="k">${c.k}</span><span class="v ${c.cls}">${c.v}</span></div>`)
    .join("");
}

function conditionItem(cond) {
  const valueText = fmtValue(cond.label.split(" ")[0], cond.value);
  return `
    <li class="condition ${cond.passed ? "pass" : "fail"}">
      <span class="icon" aria-hidden="true"></span>
      <span>
        <span class="label">${cond.label}</span>
        <span class="formula">${cond.formula}</span>
      </span>
      <span class="value">${valueText}</span>
    </li>
  `;
}

function renderStrategies(data) {
  const root = document.getElementById("strategies");
  const tpl = document.getElementById("strategy-card-template");
  root.innerHTML = "";

  data.strategies.forEach((s) => {
    const node = tpl.content.firstElementChild.cloneNode(true);
    node.querySelector(".tag-etf").textContent = s.etf;
    node.querySelector(".tag-account").textContent = s.account;
    node.querySelector(".tag-source").textContent = `Signal: ${s.source}`;
    node.querySelector(".card-title").textContent = s.name;

    const triggerCount = s.conditions.filter((c) => c.group === "trigger").length;
    const filterCount  = s.conditions.filter((c) => c.group === "filter").length;
    const sub = [];
    if (triggerCount > 0) sub.push(`${triggerCount} entry trigger${triggerCount > 1 ? "s" : ""}`);
    if (filterCount > 0)  sub.push(`${filterCount} filter${filterCount > 1 ? "s" : ""}`);
    node.querySelector(".card-sub").textContent = sub.join(" · ");

    const badge = node.querySelector(".signal-badge");
    const label = node.querySelector(".signal-label");
    const hint  = node.querySelector(".signal-hint");
    if (s.buy_signal) {
      badge.classList.add("buy");
      label.textContent = "BUY AT OPEN";
      hint.textContent  = `Execute ${s.etf} in ${s.account}`;
    } else {
      badge.classList.add("flat");
      label.textContent = "STAY FLAT";
      const failed = s.conditions.filter((c) => !c.passed).map((c) => c.label);
      hint.textContent = failed.length
        ? `${failed.length} condition${failed.length > 1 ? "s" : ""} failing`
        : "Awaiting data";
    }

    const triggerList = node.querySelector('[data-group="trigger"] .condition-list');
    const filterList  = node.querySelector('[data-group="filter"] .condition-list');
    const triggerGroup = node.querySelector('[data-group="trigger"]');
    const filterGroup  = node.querySelector('[data-group="filter"]');

    const triggers = s.conditions.filter((c) => c.group === "trigger");
    const filters  = s.conditions.filter((c) => c.group === "filter");

    if (triggers.length === 0) triggerGroup.remove();
    else triggerList.innerHTML = triggers.map(conditionItem).join("");

    if (filters.length === 0) filterGroup.remove();
    else filterList.innerHTML = filters.map(conditionItem).join("");

    node.querySelector(".card-notes").textContent = s.notes || "";

    root.appendChild(node);
  });
}

function renderSources(data) {
  const root = document.getElementById("sources-grid");
  root.innerHTML = Object.entries(data.sources)
    .map(([ticker, ind]) => {
      const rows = (INDICATOR_ORDER[ticker] || Object.keys(ind))
        .filter((k) => k in ind && k !== "asof_open_date")
        .map((k) => `<tr><th>${k}</th><td>${fmtValue(k, ind[k])}</td></tr>`)
        .join("");
      return `
        <div class="source-card">
          <h3>${ticker} <span class="asof">as of ${ind.asof_open_date ?? "—"}</span></h3>
          <table class="source-table"><tbody>${rows}</tbody></table>
        </div>`;
    })
    .join("");
}

function renderMeta(data) {
  document.getElementById("meta-generated").textContent =
    `Data generated ${data.generated_at_display || data.generated_at_utc || "—"}`;

  const qqqAsof = data.sources?.QQQ?.asof_open_date;
  const spyAsof = data.sources?.SPY?.asof_open_date;
  document.getElementById("meta-source").textContent =
    `Last open · QQQ ${qqqAsof ?? "—"} · SPY ${spyAsof ?? "—"}`;
}

function renderError(err) {
  const root = document.getElementById("strategies");
  root.innerHTML = `
    <div class="card">
      <div class="signal-badge err">
        <span class="signal-label">NO DATA YET</span>
        <span class="signal-hint">${err.message}</span>
      </div>
      <p class="card-notes">Run the <code>Update dashboard data</code> GitHub Action once (Actions tab → Run workflow) to generate <code>data/latest.json</code>.</p>
    </div>`;
  document.getElementById("meta-generated").textContent = "No data file yet";
  document.getElementById("meta-source").textContent = "";
}

async function refresh() {
  const btn = document.getElementById("refresh-btn");
  btn.classList.add("loading");
  btn.disabled = true;
  try {
    const data = await loadData();
    renderMeta(data);
    renderOverview(data);
    renderStrategies(data);
    renderSources(data);
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
