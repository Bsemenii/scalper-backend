const BASE = "/api"; // works in dev with Vite proxy and in Docker via nginx

async function toJson(res) {
  if (!res.ok) throw new Error(await res.text());
  return res.json();
}

export const api = {
  start: (symbol) =>
    fetch(`${BASE}/paper/start${symbol ? `?symbol=${symbol}` : ""}`, { method: "POST" }).then(toJson),
  stop: (symbol) =>
    fetch(`${BASE}/paper/stop${symbol ? `?symbol=${symbol}` : ""}`, { method: "POST" }).then(toJson),
  status: (symbol) =>
    fetch(`${BASE}/paper/status${symbol ? `?symbol=${symbol}` : ""}`).then(toJson),
  trades: (symbol, limit = 100) =>
    fetch(`${BASE}/paper/trades?limit=${limit}${symbol ? `&symbol=${symbol}` : ""}`).then(toJson),
  open: (body) =>
    fetch(`${BASE}/paper/open`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    }).then(toJson),
  close: (body = {}) =>
    fetch(`${BASE}/paper/close`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    }).then(toJson),
};
