// basis 대시보드 화면. 서버(main.py)에서 받는 것:
//  - /api/history : 심볼별 30분 시계열 (ClickHouse) → 처음 한 번 그래프를 채운다
//  - /ws          : 접속 직후 snapshot(Redis 최신값) → 이후 1초마다 update(바뀐 심볼만)
//  - /api/latency : 지연 p50 (10초마다)
// 받은 값은 latest(표 · KPI)와 series(그래프)에 넣고, 화면은 1초에 한 번만 다시 그린다.

const WINDOW_MS = 30 * 60 * 1000;   // 그래프 범위 30분
const Z_THRESHOLD = 2;              // BasisStatsRule.zThreshold와 같은 값 (표의 Z 강조용)
const GAP_MS = 5000;                // markPrice는 1초마다 온다. 이보다 오래 비면 그래프 선을 끊는다

const STATUS = {
  warmup:     { text: "◌ 준비 중",    cls: "muted" },
  check_data: { text: "? 데이터 확인", cls: "amber" },
  normal:     { text: "✓ 정상",       cls: "green" },
  warning:    { text: "● 주의",       cls: "amber" },
  alert:      { text: "▲ 과열",       cls: "red" },
};

const latest = {};   // 심볼 → 최신 BasisSignal (Redis)
const series = {};   // 심볼 → { t: [], bps: [], mean: [], std: [], status: [] }
const charts = {};   // 심볼 → Chart.js 객체
let dirty = true;    // 다시 그릴 것이 있나

// ── 데이터 받기 ──────────────────────────────────────────

async function loadHistory() {
  const response = await fetch("/api/history?minutes=30");
  const body = await response.json();
  for (const [symbol, points] of Object.entries(body.series)) {
    series[symbol] = points;
  }
  dirty = true;
}

async function loadLatency() {
  try {
    const body = await (await fetch("/api/latency")).json();
    const value = document.getElementById("kpi-latency");
    value.textContent = body.p50_ms === null ? "-" : `${Math.round(body.p50_ms)} ms`;
    document.getElementById("kpi-latency-sub").textContent = `collector → basis (${body.symbol.replace("USDT", "")} 5분)`;
  } catch (error) {
    console.warn("latency", error);
  }
}

function applySignal(signal) {
  latest[signal.symbol] = signal;
  // 그래프에도 점 하나를 붙인다 (이미 있는 시각보다 새것만)
  const points = series[signal.symbol] ??= { t: [], bps: [], mean: [], std: [], status: [] };
  const lastT = points.t.length ? points.t[points.t.length - 1] : 0;
  if (signal.event_time > lastT) {
    points.t.push(signal.event_time);
    points.bps.push(signal.bps);
    points.mean.push(signal.mean_bps);
    points.std.push(signal.std_bps);
    points.status.push(signal.status);
  }
  dirty = true;
}

function connect() {
  const scheme = location.protocol === "https:" ? "wss" : "ws";
  const socket = new WebSocket(`${scheme}://${location.host}/ws`);
  const badge = document.getElementById("conn");
  socket.onopen = () => { badge.textContent = "실시간 · 연결됨"; badge.className = "badge badge-on"; };
  socket.onmessage = (event) => {
    const message = JSON.parse(event.data);
    for (const signal of message.signals) applySignal(signal);
  };
  socket.onclose = () => {
    badge.textContent = "연결 끊김 · 다시 연결 중";
    badge.className = "badge badge-off";
    setTimeout(connect, 2000);   // 서버 재시작 등으로 끊기면 2초 뒤 다시 붙는다
  };
}

// ── 화면 그리기 ──────────────────────────────────────────

function fmtSigned(value, digits) {
  const text = Math.abs(value).toFixed(digits);
  return (value > 0 ? "+" : value < 0 ? "-" : "") + text;
}

function fmtBasisDollar(basisText) {
  const value = Number(basisText);
  const digits = Math.abs(value) < 0.01 ? 5 : 2;   // DOGE · XRP처럼 가격이 작은 코인은 자릿수를 늘린다
  return `${value.toFixed(digits)} $`;
}

function renderKpis(signals) {
  const alerts = signals.filter((s) => s.status === "alert").length;
  const warnings = signals.filter((s) => s.status === "warning").length;
  document.getElementById("kpi-symbols").textContent = signals.length;
  document.getElementById("kpi-alert").textContent = `▲ ${alerts}`;
  document.getElementById("kpi-warning").textContent = `● ${warnings}`;

  const judged = signals.filter((s) => s.status !== "warmup" && s.status !== "check_data");
  if (judged.length) {
    const top = judged.reduce((a, b) => (Math.abs(b.z) > Math.abs(a.z) ? b : a));
    document.getElementById("kpi-maxz").textContent = Math.abs(top.z).toFixed(1);
    document.getElementById("kpi-maxz-symbol").textContent = top.symbol;
  } else {
    document.getElementById("kpi-maxz").textContent = "-";
    document.getElementById("kpi-maxz-symbol").textContent = "판정 전 (warmup)";
  }
  if (signals.length) {
    const avg = signals.reduce((sum, s) => sum + s.bps, 0) / signals.length;
    document.getElementById("kpi-avgbps").textContent = `${fmtSigned(avg, 1)} bps`;
  }
}

function renderTable(signals) {
  const rows = [...signals].sort((a, b) => Math.abs(b.z) - Math.abs(a.z));
  document.getElementById("status-rows").innerHTML = rows.map((s) => {
    const status = STATUS[s.status] ?? { text: s.status, cls: "muted" };
    const zHot = Math.abs(s.z) > Z_THRESHOLD ? "z-hot" : "";
    return `<tr>
      <td>${s.symbol}</td>
      <td class="num">${fmtBasisDollar(s.basis)}</td>
      <td class="num">${s.bps.toFixed(1)}</td>
      <td class="num ${zHot}">${fmtSigned(s.z, 1)}</td>
      <td class="num">${fmtSigned(Number(s.funding_rate) * 100, 4)}%</td>
      <td class="${status.cls}">${status.text}</td>
    </tr>`;
  }).join("") || `<tr><td colspan="6" class="muted">데이터를 기다리는 중…</td></tr>`;
}

function makeChart(symbol) {
  const box = document.createElement("div");
  box.className = "chart-box";
  box.innerHTML = `
    <div class="chart-title"><b>${symbol}</b><span id="sum-${symbol}"></span></div>
    <div class="chart-canvas"><canvas id="cv-${symbol}"></canvas></div>
    <div class="chart-status" id="st-${symbol}"></div>`;
  document.getElementById("charts").appendChild(box);

  const line = (label, color, extra = {}) => ({
    label, data: [], borderColor: color, borderWidth: 1, pointRadius: 0, tension: 0, spanGaps: false, ...extra,
  });
  charts[symbol] = new Chart(document.getElementById(`cv-${symbol}`), {
    type: "line",
    data: {
      datasets: [
        line("upper", "transparent"),                                                          // 0 평균 + 2σ
        line("lower", "transparent", { fill: "-1", backgroundColor: "rgba(59,130,246,0.12)" }), // 1 평균 − 2σ (0과 사이를 칠한다)
        line("mean", "#9a9ba1", { borderDash: [6, 4] }),                                        // 2 5분 평균
        line("bps", "#3b82f6", { borderWidth: 1.2 }),                                           // 3 basis bps
        { label: "alert", type: "scatter", data: [], pointRadius: 2.5, backgroundColor: "#ef4444" }, // 4 과열 점
      ],
    },
    options: {
      animation: false,
      parsing: false,
      normalized: true,
      maintainAspectRatio: false,
      plugins: { legend: { display: false }, tooltip: { enabled: false } },
      scales: {
        x: {
          type: "linear",
          grid: { color: "#26272b" },
          ticks: {
            color: "#9a9ba1",
            stepSize: 5 * 60 * 1000,
            callback: (value) => Math.round((value - Date.now()) / 60000),   // 몇 분 전 (-30 … 0)
          },
        },
        y: { grid: { color: "#26272b" }, ticks: { color: "#9a9ba1" } },
      },
    },
  });
}

function renderCharts() {
  const now = Date.now();
  for (const symbol of Object.keys(series).sort()) {
    if (!charts[symbol]) makeChart(symbol);
    const points = series[symbol];

    // 30분이 지난 점은 버린다 (배열 앞쪽)
    let cut = 0;
    while (cut < points.t.length && points.t[cut] < now - WINDOW_MS) cut++;
    if (cut) for (const key of Object.keys(points)) points[key].splice(0, cut);

    const upper = [], lower = [], mean = [], bps = [], alerts = [];
    points.t.forEach((t, index) => {
      // 직전 점과 5초 넘게 떨어져 있으면(파이프라인이 멈췄던 구간) 선을 끊는다. y가 null인 점에서 선이 끊긴다
      if (index > 0 && t - points.t[index - 1] > GAP_MS) {
        for (const data of [upper, lower, mean, bps]) data.push({ x: t - 1, y: null });
      }
      const judged = points.status[index] !== "warmup" && points.status[index] !== "check_data";
      if (judged) {   // warmup 동안은 평균 · 표준편차가 불안정해 밴드를 그리지 않는다
        upper.push({ x: t, y: points.mean[index] + 2 * points.std[index] });
        lower.push({ x: t, y: points.mean[index] - 2 * points.std[index] });
        mean.push({ x: t, y: points.mean[index] });
      }
      bps.push({ x: t, y: points.bps[index] });
      if (points.status[index] === "alert") alerts.push({ x: t, y: points.bps[index] });
    });

    const chart = charts[symbol];
    [upper, lower, mean, bps, alerts].forEach((data, index) => { chart.data.datasets[index].data = data; });
    chart.options.scales.x.min = now - WINDOW_MS;
    chart.options.scales.x.max = now;
    chart.update("none");

    const signal = latest[symbol];
    if (signal) {
      const status = STATUS[signal.status] ?? { text: signal.status, cls: "muted" };
      document.getElementById(`sum-${symbol}`).textContent = `${signal.bps.toFixed(1)} bps · Z ${fmtSigned(signal.z, 1)}`;
      const badge = document.getElementById(`st-${symbol}`);
      badge.textContent = status.text;
      badge.className = `chart-status ${status.cls}`;
    }
  }
}

function render() {
  if (!dirty) return;
  dirty = false;
  const signals = Object.values(latest);
  renderKpis(signals);
  renderTable(signals);
  renderCharts();
}

// ── 시작 ────────────────────────────────────────────────

(async () => {
  try {
    await loadHistory();
  } catch (error) {
    console.warn("history", error);   // ClickHouse가 안 돼도 표 · KPI(Redis)는 보여 준다
  }
  connect();
  loadLatency();
  setInterval(loadLatency, 10_000);
  setInterval(render, 1000);
  setInterval(() => { dirty = true; }, 5000);   // 값이 안 와도 x축(지금 시각)은 움직이게
})();
