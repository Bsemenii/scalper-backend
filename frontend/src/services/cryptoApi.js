// src/services/cryptoApi.js
// Єдиний API-сервіс без axios, тільки fetch

const API_BASE = import.meta.env.VITE_API_URL || "http://localhost:8000";

// Базовий helper
async function request(path, options = {}) {
  const url =
    path.startsWith("http") ? path : `${API_BASE}${path.startsWith("/") ? path : `/${path}`}`;

  try {
    const res = await fetch(url, {
      ...options,
      headers: {
        "Content-Type": "application/json",
        ...(options.headers || {}),
      },
    });

    if (!res.ok) {
      const text = await res.text();
      throw new Error(text || `Request failed: ${res.status}`);
    }

    const ct = res.headers.get("content-type") || "";
    if (ct.includes("application/json")) {
      return res.json();
    }
    return res.text();
  } catch (err) {
    console.error("API network error:", err);
    throw err;
  }
}

// ---------- LIVE PNL + TRADES (для LiveDashboard / CryptoDashboard) ----------

export async function getPnlNow() {
  // очікується ендпоінт /pnl/now
  return request("/pnl/now");
}

export async function getTrades({ limit = 50, symbol, since } = {}) {
  const params = new URLSearchParams();
  if (limit) params.set("limit", String(limit));
  if (symbol) params.set("symbol", String(symbol).toLowerCase());
  if (since) params.set("since", since);

  const qs = params.toString();
  return request(`/trades${qs ? `?${qs}` : ""}`);
}

// Get trades with stats from /trades/recent endpoint (single source of truth)
export async function getTradesRecent({ limit = 100, offset = 0 } = {}) {
  const params = new URLSearchParams();
  if (limit) params.set("limit", String(limit));
  if (offset) params.set("offset", String(offset));

  const qs = params.toString();
  return request(`/trades/recent${qs ? `?${qs}` : ""}`);
}

// ---------- Simple API (як було в твоєму попередньому варіанті) ----------

const SIMPLE_API_BASE = `${API_BASE}/api`;

export async function fetchTrades(limit = 10) {
  const res = await fetch(`${SIMPLE_API_BASE}/trades?limit=${limit}`);
  return res.json();
}

export async function fetchOrderBook() {
  const res = await fetch(`${SIMPLE_API_BASE}/orderbook`);
  return res.json();
}

export async function fetchTicker() {
  const res = await fetch(`${SIMPLE_API_BASE}/ticker`);
  return res.json();
}

// Trading control endpoints
export async function startTrading(symbol) {
  const params = new URLSearchParams({ symbol });
  const res = await fetch(
    `${SIMPLE_API_BASE}/trading/start?${params.toString()}`,
    { method: "POST" }
  );
  return res.json();
}

export async function stopTrading(symbol) {
  const params = new URLSearchParams({ symbol });
  const res = await fetch(
    `${SIMPLE_API_BASE}/trading/stop?${params.toString()}`,
    { method: "POST" }
  );
  return res.json();
}

export async function getTradingStatus(symbol) {
  const params = new URLSearchParams({ symbol });
  const res = await fetch(
    `${SIMPLE_API_BASE}/trading/status?${params.toString()}`
  );
  return res.json();
}

// ---------- PAPER-ТРЕЙДИНГ / МЕТРИКИ ----------

export async function getPaperSummary() {
  return request("/paper/summary");
}

export async function getMetricsState() {
  return request("/paper/metrics_state");
}

export async function resetMetrics(baseline = "now") {
  // Use new clear stats endpoint
  return request("/stats/clear", {
    method: "POST",
  });
}

export async function getPaperTrades(symbol, limit = 20, since) {
  const params = new URLSearchParams({
    symbol: String(symbol || "").toLowerCase(),
    limit: String(limit),
  });
  if (since) params.set("since", since);

  return request(`/paper/trades?${params.toString()}`);
}

export async function paperStart(symbol) {
  return request("/paper/start", {
    method: "POST",
    body: JSON.stringify({ symbol }),
  });
}

export async function paperStop(symbol) {
  return request("/paper/stop", {
    method: "POST",
    body: JSON.stringify({ symbol }),
  });
}

export async function getPaperStatus() {
  return request("/paper/status");
}

// Open positions (fast path)
export async function getOpenPositions(symbol) {
  const params = symbol
    ? new URLSearchParams({ symbol: String(symbol).toLowerCase() })
    : null;
  const path = params
    ? `/paper/open_positions?${params.toString()}`
    : "/paper/open_positions";
  return request(path);
}

// Runtime heartbeat
export async function getRuntimeStatus() {
  return request("/runtime/status");
}

// ---------- BACKTEST ----------

export async function runBacktest(params) {
  const body = {
    symbol: params.symbol || "BTCUSDT",
    start: params.start,
    end: params.end,
    interval: params.interval || "1m",
    risk_usd: Number(params.risk_usd ?? 25),
    atr_mult_stop: Number(params.atr_mult_stop ?? 1.2),
    take_r_mult: Number(params.take_r_mult ?? 1.6),
    max_hold_min: Number(params.max_hold_min ?? 45),
    min_signal_score: Number(params.min_signal_score ?? 0.1),
    min_bars_warmup: Number(params.min_bars_warmup ?? 250),
    allow_flip_close_open: Boolean(params.allow_flip_close_open ?? false),
    debug: Boolean(params.debug ?? false),
  };

  return request("/backtest/run", {
    method: "POST",
    body: JSON.stringify(body),
  });
}

// ---------- PAPER CONFIG ----------

export async function getPaperConfig(symbol = "BTCUSDT") {
  const params = new URLSearchParams({
    symbol: String(symbol).toLowerCase(),
  });
  return request(`/paper/config?${params.toString()}`);
}

export async function setPaperConfig(symbol, patch) {
  const params = new URLSearchParams({
    symbol: String(symbol).toLowerCase(),
  });
  return request(`/paper/config?${params.toString()}`, {
    method: "POST",
    body: JSON.stringify(patch || {}),
  });
}
// -------- AUTO TRADING CONTROL --------

export async function startAuto(symbol) {
  const params = symbol ? `?symbol=${encodeURIComponent(symbol)}` : '';
  return request(`/auto/start${params}`, {
    method: 'POST',
  });
}

export async function stopAuto(symbol) {
  const params = symbol ? `?symbol=${encodeURIComponent(symbol)}` : '';
  return request(`/auto/stop${params}`, {
    method: 'POST',
  });
}

export async function closeAllPositions() {
  // ендпоінт можеш підправити під свій бекенд (/paper/close_all /paper/flatten і т.д.)
  return request('/paper/close_all', {
    method: 'POST',
  });
}

