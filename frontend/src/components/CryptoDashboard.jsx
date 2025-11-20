// src/components/CryptoDashboard.jsx
import React, { useEffect, useMemo, useState } from "react";
import CandleChart from "./CandleChart";
import TradesTable from "./TradesTable";
import OpenPositionsPanel from "./OpenPositionsPanel";
import { getPnlNow, getTrades, resetMetrics } from "../services/cryptoApi";

// Ті самі 3 монети
const SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"];

// Базовий URL бекенду
const API_BASE = import.meta.env.VITE_API_URL || "http://localhost:8000";

function formatNumber(v, digits = 2) {
  if (v === null || v === undefined) return "0.00";
  const n = Number(v);
  if (!Number.isFinite(n)) return "0.00";
  return n.toFixed(digits);
}

const cardBase = {
  flex: "1 1 0",
  minWidth: 120,
  borderRadius: 12,
  padding: "10px 12px",
  background: "#020617",
  border: "1px solid #111827",
  display: "flex",
  flexDirection: "column",
  gap: 4,
};

// ---------- raw helpers ------------------------------------------------

// Получить все позиции
async function fetchPositionsRaw() {
  const res = await fetch(`${API_BASE}/positions`);
  if (!res.ok) {
    const text = await res.text();
    throw new Error(text || "Failed to load positions");
  }
  return res.json();
}

// Включить/выключить автоторговлю (общий флаг, без symbol)
async function setAutoTrading(enabled) {
  const res = await fetch(`${API_BASE}/control/auto`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ enabled }),
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(text || "Failed to toggle auto trading");
  }
  return res.json();
}

// Получить текущий статус автоторговли
async function fetchAutoStatus() {
  const res = await fetch(`${API_BASE}/control/auto`);
  if (!res.ok) {
    const text = await res.text();
    throw new Error(text || "Failed to load auto status");
  }
  return res.json();
}

// Закрыть все позиции (flatten-all)
async function postCloseAll() {
  const res = await fetch(`${API_BASE}/control/flatten-all`, {
    method: "POST",
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(text || "Failed to close all positions");
  }
  return res.json();
}

// ----------------------------------------------------------------------

const CryptoDashboard = () => {
  const [pnlNow, setPnlNow] = useState(null);
  const [trades, setTrades] = useState([]);
  const [positions, setPositions] = useState({});
  const [loading, setLoading] = useState(true);
  const [errorMsg, setErrorMsg] = useState("");

  const [actionsLoading, setActionsLoading] = useState(false);
  const [actionsError, setActionsError] = useState("");
  const [autoEnabled, setAutoEnabled] = useState(null); // null = не знаем ещё

  // --- periodic load ---------------------------------------------------
  useEffect(() => {
    let cancelled = false;
    let timer;

    async function load() {
      try {
        setErrorMsg("");
        const [pnlData, tradesData, positionsData, autoData] = await Promise.all([
          getPnlNow(),
          getTrades({ limit: 50 }),
          fetchPositionsRaw(),
          fetchAutoStatus().catch(() => null), // не падаем, если endpoint вернёт ошибку
        ]);
        if (cancelled) return;

        setPnlNow(pnlData);
        setTrades(tradesData?.items || []);
        setPositions(positionsData?.positions ?? positionsData ?? {});

        if (autoData && typeof autoData.auto_signal_enabled === "boolean") {
          setAutoEnabled(autoData.auto_signal_enabled);
        }

        setLoading(false);
      } catch (err) {
        console.error("LiveDashboard load error", err);
        if (!cancelled) {
          setErrorMsg(err?.message || "Failed to load data");
          setLoading(false);
        }
      }
    }

    load();
    timer = setInterval(load, 5000);

    return () => {
      cancelled = true;
      if (timer) clearInterval(timer);
    };
  }, []);

  // --- группировка трейдов по символу -----------------------------------
  const tradesBySymbol = useMemo(() => {
    const map = {};
    SYMBOLS.forEach((s) => {
      map[s] = [];
    });
    (trades || []).forEach((t) => {
      const sym = String(t.symbol || "").toUpperCase();
      if (!map[sym]) map[sym] = [];
      map[sym].push(t);
    });
    return map;
  }, [trades]);

  // --- массив открытых позиций + группировка по символу -----------------
  const allOpenPositions = useMemo(() => {
    if (Array.isArray(positions)) return positions;

    if (positions && typeof positions === "object") {
      const arr = [];
      Object.values(positions).forEach((val) => {
        if (Array.isArray(val)) arr.push(...val);
        else if (val) arr.push(val);
      });
      return arr;
    }

    return [];
  }, [positions]);

  const openPositionsBySymbol = useMemo(() => {
    const map = {};
    SYMBOLS.forEach((s) => {
      map[s] = [];
    });
    (allOpenPositions || []).forEach((p) => {
      const sym = String(p.symbol || "").toUpperCase();
      if (!map[sym]) map[sym] = [];
      map[sym].push(p);
    });
    return map;
  }, [allOpenPositions]);

  // --- агрегаты по PnL -------------------------------------------------
  const pnlDay = pnlNow?.pnl_day || {};
  const unrealized = pnlNow?.unrealized || {};
  const openPositionsCount = pnlNow?.open_positions ?? 0;

  const pnlUsd = formatNumber(pnlDay.pnl_usd, 2);
  const pnlR = formatNumber(pnlDay.pnl_r, 2);
  const winrate =
    pnlDay.winrate != null ? `${formatNumber(pnlDay.winrate * 100, 1)}%` : "—";
  const uPnlUsd = formatNumber(unrealized.total_usd, 2);

  const pnlUsdColor =
    Number(pnlDay.pnl_usd || 0) > 0
      ? "#22c55e"
      : Number(pnlDay.pnl_usd || 0) < 0
      ? "#f97316"
      : "#e5e7eb";

  const uPnlColor =
    Number(unrealized.total_usd || 0) > 0
      ? "#22c55e"
      : Number(unrealized.total_usd || 0) < 0
      ? "#f97316"
      : "#e5e7eb";

  // --- handlers for buttons --------------------------------------------

  const handleStartAuto = async () => {
    try {
      setActionsLoading(true);
      setActionsError("");
      const resp = await setAutoTrading(true);
      if (typeof resp.auto_signal_enabled === "boolean") {
        setAutoEnabled(resp.auto_signal_enabled);
      } else {
        setAutoEnabled(true);
      }
    } catch (err) {
      console.error("startAuto failed", err);
      setActionsError(err?.message || "Failed to start auto trading");
    } finally {
      setActionsLoading(false);
    }
  };

  const handleStopAuto = async () => {
    try {
      setActionsLoading(true);
      setActionsError("");
      const resp = await setAutoTrading(false);
      if (typeof resp.auto_signal_enabled === "boolean") {
        setAutoEnabled(resp.auto_signal_enabled);
      } else {
        setAutoEnabled(false);
      }
    } catch (err) {
      console.error("stopAuto failed", err);
      setActionsError(err?.message || "Failed to stop auto trading");
    } finally {
      setActionsLoading(false);
    }
  };

  const handleCloseAll = async () => {
    try {
      setActionsLoading(true);
      setActionsError("");
      await postCloseAll();
      // Можно мягко обновить состояния: просто заново стянуть данные
      // (пусть это сделает следующий 5-секундный тик, чтобы не дёргать лишний раз)
    } catch (err) {
      console.error("closeAllPositions failed", err);
      setActionsError(err?.message || "Failed to close all positions");
    } finally {
      setActionsLoading(false);
    }
  };

  const handleResetStats = async () => {
    try {
      setActionsLoading(true);
      setActionsError("");
      // 1) чистим метрики на бэкенде (через твой сервис)
      await resetMetrics("today"); // или "now" – как реализовано в cryptoApi.js

      // 2) локально обнуляем UI
      setTrades([]);
      setPnlNow((prev) => ({
        ...(prev || {}),
        pnl_day: {
          pnl_usd: 0,
          pnl_r: 0,
          trades: 0,
          winrate: 0,
        },
        unrealized: {
          total_usd: 0,
        },
        open_positions: prev?.open_positions ?? 0,
      }));
    } catch (err) {
      console.error("resetStats failed", err);
      setActionsError(err?.message || "Failed to reset stats");
    } finally {
      setActionsLoading(false);
    }
  };

  // --- render ----------------------------------------------------------
  const autoLabel =
    autoEnabled === null ? "Auto ?" : autoEnabled ? "Auto ON" : "Auto OFF";

  const autoLabelColor =
    autoEnabled === null
      ? "#9ca3af"
      : autoEnabled
      ? "#4ade80"
      : "#f97316";

  return (
    <div style={{ padding: 16, paddingTop: 12 }}>
      {/* Title + buttons */}
      <div
        style={{
          marginBottom: 14,
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
          gap: 12,
        }}
      >
        <h1
          style={{
            margin: 0,
            fontSize: 20,
            fontWeight: 600,
            color: "#f9fafb",
          }}
        >
          AI Scalping — Live Dashboard
        </h1>

        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
          {/* статус авто */}
          <span
            style={{
              fontSize: 11,
              padding: "4px 8px",
              borderRadius: 999,
              border: "1px solid #1f2937",
              color: autoLabelColor,
              background: "#020617",
            }}
          >
            {autoLabel}
          </span>

          {loading && (
            <span style={{ fontSize: 11, color: "#6b7280" }}>Updating…</span>
          )}
          {actionsError && (
            <span style={{ fontSize: 11, color: "#fca5a5" }}>
              {actionsError}
            </span>
          )}

          <button
            onClick={handleStartAuto}
            disabled={actionsLoading}
            style={{
              background: "#16a34a",
              color: "#f9fafb",
              border: "1px solid #15803d",
              padding: "6px 10px",
              fontSize: 11,
              borderRadius: 6,
              cursor: actionsLoading ? "default" : "pointer",
            }}
          >
            Start Auto
          </button>

          <button
            onClick={handleStopAuto}
            disabled={actionsLoading}
            style={{
              background: "#0f172a",
              color: "#e5e7eb",
              border: "1px solid #334155",
              padding: "6px 10px",
              fontSize: 11,
              borderRadius: 6,
              cursor: actionsLoading ? "default" : "pointer",
            }}
          >
            Stop Auto
          </button>

          <button
            onClick={handleCloseAll}
            disabled={actionsLoading}
            style={{
              background: "#b91c1c",
              color: "#f9fafb",
              border: "1px solid #7f1d1d",
              padding: "6px 10px",
              fontSize: 11,
              borderRadius: 6,
              cursor: actionsLoading ? "default" : "pointer",
            }}
          >
            Close All
          </button>

          {/* Кнопка, которая чистит сделки и статистику на дашборде */}
          <button
            onClick={handleResetStats}
            disabled={actionsLoading}
            title="Clear today's trades & stats"
            style={{
              background: "transparent",
              color: "#9ca3af",
              border: "1px dashed #4b5563",
              padding: "6px 10px",
              fontSize: 11,
              borderRadius: 6,
              cursor: actionsLoading ? "default" : "pointer",
            }}
          >
            Reset stats
          </button>
        </div>
      </div>

      {/* PnL cards */}
      <div
        style={{
          display: "flex",
          gap: 10,
          marginBottom: 16,
          flexWrap: "wrap",
        }}
      >
        <div style={cardBase}>
          <span
            style={{
              fontSize: 11,
              color: "#9ca3af",
              textTransform: "uppercase",
              letterSpacing: "0.04em",
            }}
          >
            PnL (USD)
          </span>
          <span
            style={{ fontSize: 18, fontWeight: 600, color: pnlUsdColor }}
          >
            {pnlUsd}
          </span>
        </div>

        <div style={cardBase}>
          <span
            style={{
              fontSize: 11,
              color: "#9ca3af",
              textTransform: "uppercase",
              letterSpacing: "0.04em",
            }}
          >
            PnL (R)
          </span>
          <span style={{ fontSize: 18, fontWeight: 600, color: "#e5e7eb" }}>
            {pnlR}
          </span>
        </div>

        <div style={cardBase}>
          <span
            style={{
              fontSize: 11,
              color: "#9ca3af",
              textTransform: "uppercase",
              letterSpacing: "0.04em",
            }}
          >
            Trades
          </span>
          <span style={{ fontSize: 18, fontWeight: 600, color: "#e5e7eb" }}>
            {pnlDay.trades ?? 0}
          </span>
        </div>

        <div style={cardBase}>
          <span
            style={{
              fontSize: 11,
              color: "#9ca3af",
              textTransform: "uppercase",
              letterSpacing: "0.04em",
            }}
          >
            Winrate
          </span>
          <span style={{ fontSize: 18, fontWeight: 600, color: "#e5e7eb" }}>
            {winrate}
          </span>
        </div>

        <div style={cardBase}>
          <span
            style={{
              fontSize: 11,
              color: "#9ca3af",
              textTransform: "uppercase",
              letterSpacing: "0.04em",
            }}
          >
            uPnL (USD)
          </span>
          <span style={{ fontSize: 18, fontWeight: 600, color: uPnlColor }}>
            {uPnlUsd}
          </span>
        </div>

        <div style={cardBase}>
          <span
            style={{
              fontSize: 11,
              color: "#9ca3af",
              textTransform: "uppercase",
              letterSpacing: "0.04em",
            }}
          >
            Open Positions
          </span>
          <span style={{ fontSize: 18, fontWeight: 600, color: "#e5e7eb" }}>
            {openPositionsCount}
          </span>
        </div>
      </div>

      {errorMsg && (
        <div
          style={{
            marginBottom: 12,
            padding: "8px 10px",
            borderRadius: 8,
            background: "#111827",
            border: "1px solid #b91c1c",
            color: "#fecaca",
            fontSize: 12,
          }}
        >
          {errorMsg}
        </div>
      )}

      {/* Charts row */}
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(3, minmax(0, 1fr))",
          gap: 12,
          marginBottom: 16,
        }}
      >
        {SYMBOLS.map((sym) => (
          <div
            key={sym}
            style={{
              borderRadius: 14,
              border: "1px solid #111827",
              background: "#020617",
              padding: 8,
              display: "flex",
              flexDirection: "column",
              minHeight: 380,
            }}
          >
            <CandleChart
              symbol={sym}
              timeframe="1m"
              title={sym}
              trades={tradesBySymbol[sym] || []}
              openPositions={openPositionsBySymbol[sym] || []}
            />
          </div>
        ))}
      </div>

      {/* Open positions + trades */}
      <OpenPositionsPanel positions={positions} />
      <TradesTable trades={trades} />
    </div>
  );
};

export default CryptoDashboard;
