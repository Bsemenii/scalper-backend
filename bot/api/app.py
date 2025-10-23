# bot/api/app.py
from __future__ import annotations

import os
import time
from typing import Any, Dict, List, Optional, Tuple, Iterable, Literal

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from bot.core.config import get_settings
from bot.worker import Worker
from features.microstructure import MicroFeatureEngine
from features.indicators import IndiEngine

APP_VERSION = "0.6.1"

# ------------------------------------------------------------------------------
# App init & CORS
# ------------------------------------------------------------------------------

app = FastAPI(title="AI Scalping Backend", version=APP_VERSION)
app.state.worker: Optional[Worker] = None

if os.getenv("ENABLE_CORS", "1") == "1":
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[o for o in os.getenv("CORS_ORIGINS", "*").split(",") if o],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------

def _symbols_from_settings() -> List[str]:
    try:
        s = get_settings()
        return [sym.upper() for sym in s.symbols]
    except Exception:
        return ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

def _coalesce_from_settings() -> int:
    try:
        s = get_settings()
        return int(s.streams.coalesce_ms)
    except Exception:
        return int(os.getenv("COALESCE_MS", "75"))

def _worker_required() -> Worker:
    w: Optional[Worker] = app.state.worker
    if not w:
        raise HTTPException(status_code=503, detail="Worker is not started yet")
    return w

def _normalize_symbol(symbol: str, allowed: Iterable[str]) -> str:
    sym = symbol.upper()
    if sym not in allowed:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown symbol '{symbol}'. Allowed: {', '.join(list(allowed))}",
        )
    return sym

def _now_ms() -> int:
    return int(time.time() * 1000)

# ------------------------------------------------------------------------------
# Lifecycle
# ------------------------------------------------------------------------------

@app.on_event("startup")
async def _startup() -> None:
    symbols = _symbols_from_settings()
    coalesce_ms = _coalesce_from_settings()
    w = Worker(symbols=symbols, futures=True, coalesce_ms=coalesce_ms, history_maxlen=4000)
    await w.start()
    app.state.worker = w

@app.on_event("shutdown")
async def _shutdown() -> None:
    w: Optional[Worker] = app.state.worker
    if w:
        try:
            await w.stop()
        finally:
            app.state.worker = None

# ------------------------------------------------------------------------------
# Basic endpoints
# ------------------------------------------------------------------------------

@app.get("/")
async def root() -> Dict[str, Any]:
    return {"ok": True, "app": "ai-scalping-backend", "version": APP_VERSION}

@app.get("/healthz")
async def healthz() -> Dict[str, Any]:
    try:
        w = _worker_required()
        d = w.diag()
        ws_msgs = (d.get("ws_detail", {}) or {}).get("messages", 0)
        emits = (d.get("coal", {}) or {}).get("emit_count", 0)
        return {"ok": (ws_msgs > 0 and emits > 0), "ws_messages": ws_msgs, "emits": emits}
    except Exception:
        return {"ok": False}

# ------------------------------------------------------------------------------
# Status (config + coalescer telemetry)
# ------------------------------------------------------------------------------

@app.get("/status")
def status() -> Dict[str, Any]:
    """
    Конфиг + краткая телеметрия. Безопасно вызывать часто.
    """
    s = get_settings()
    w: Optional[Worker] = app.state.worker

    coalescer_stats: Optional[Dict[str, Any]] = None
    if w:
        # 1) взять аггрегированную диагностику
        try:
            d = w.diag()
            coal = d.get("coal")
            if isinstance(coal, dict) and coal:
                coalescer_stats = coal
        except Exception:
            coalescer_stats = None
        # 2) фолбэк — прямой вызов stats()
        if coalescer_stats is None:
            try:
                st = w.coalescer.stats()
                if isinstance(st, dict) and st:
                    coalescer_stats = st
            except Exception:
                coalescer_stats = None

    return {
        "mode": getattr(s, "mode", "paper"),
        "symbols": getattr(s, "symbols", []),
        "streams": {
            "coalesce_ms": getattr(getattr(s, "streams", object()), "coalesce_ms", 75),
            "coalescer": coalescer_stats if coalescer_stats is not None else None,
        },
        "risk": {
            "risk_per_trade_pct": getattr(getattr(s, "risk", object()), "risk_per_trade_pct", 0.25),
            "daily_stop_r": getattr(getattr(s, "risk", object()), "daily_stop_r", -10.0),
            "daily_target_r": getattr(getattr(s, "risk", object()), "daily_target_r", 15.0),
            "max_consec_losses": getattr(getattr(s, "risk", object()), "max_consec_losses", 3),
            "cooldown_after_sl_s": getattr(getattr(s, "risk", object()), "cooldown_after_sl_s", 120),
            "min_risk_usd_floor": getattr(getattr(s, "risk", object()), "min_risk_usd_floor", 0.25),
        },
        "safety": {
            "max_spread_ticks": getattr(getattr(s, "safety", object()), "max_spread_ticks", 3),
            "min_top5_liquidity_usd": getattr(getattr(s, "safety", object()), "min_top5_liquidity_usd", 300000),
            "skip_funding_minute": getattr(getattr(s, "safety", object()), "skip_funding_minute", True),
            "skip_minute_zero": getattr(getattr(s, "safety", object()), "skip_minute_zero", True),
            "min_liq_buffer_sl_mult": getattr(getattr(s, "safety", object()), "min_liq_buffer_sl_mult", 3.0),
        },
        "strategy": {
            "regime": {
                "vol_window_s": getattr(getattr(getattr(s, "strategy", object()), "regime", object()), "vol_window_s", 60),
            },
            "momentum": {
                "obi_t": getattr(getattr(getattr(s, "strategy", object()), "momentum", object()), "obi_t", 0.12),
                "tp_r": getattr(getattr(getattr(s, "strategy", object()), "momentum", object()), "tp_r", 1.6),
                "stop_k_sigma": getattr(getattr(getattr(s, "strategy", object()), "momentum", object()), "stop_k_sigma", 1.1),
            },
            "reversion": {
                "bb_z": getattr(getattr(getattr(s, "strategy", object()), "reversion", object()), "bb_z", 2.0),
                "rsi": getattr(getattr(getattr(s, "strategy", object()), "reversion", object()), "rsi", 70),
                "tp_to_vwap": getattr(getattr(getattr(s, "strategy", object()), "reversion", object()), "tp_to_vwap", True),
                "tp_r": getattr(getattr(getattr(s, "strategy", object()), "reversion", object()), "tp_r", 1.3),
            },
        },
        "execution": {
            "limit_offset_ticks": getattr(getattr(s, "execution", object()), "limit_offset_ticks", 1),
            "limit_timeout_ms": getattr(getattr(s, "execution", object()), "limit_timeout_ms", 500),
            "max_slippage_bp": getattr(getattr(s, "execution", object()), "max_slippage_bp", 6),
            "time_in_force": getattr(getattr(s, "execution", object()), "time_in_force", "GTC"),
            "fee_bps_maker": getattr(getattr(s, "execution", object()), "fee_bps_maker", 1.0),
            "fee_bps_taker": getattr(getattr(s, "execution", object()), "fee_bps_taker", 3.5),
            "min_stop_ticks": getattr(getattr(s, "execution", object()), "min_stop_ticks", 2),
        },
        "ml": {
            "enabled": getattr(getattr(s, "ml", object()), "enabled", False),
            "p_threshold_mom": getattr(getattr(s, "ml", object()), "p_threshold_mom", 0.55),
            "p_threshold_rev": getattr(getattr(s, "ml", object()), "p_threshold_rev", 0.60),
            "model_path": getattr(getattr(s, "ml", object()), "model_path", "./ml_artifacts/model.bin"),
        },
        "log_dir": getattr(s, "log_dir", "./logs"),
        "log_level": getattr(s, "log_level", "INFO"),
    }

# ------------------------------------------------------------------------------
# Diagnostics
# ------------------------------------------------------------------------------

@app.get("/debug/diag")
async def debug_diag() -> Dict[str, Any]:
    """
    Расширенная диагностика пайплайна (стабильная форма для фронта) + явные risk/safety cfg.
    """
    w = _worker_required()
    d = w.diag()
    s = get_settings()

    return {
        "ws": {
            "count": (d.get("ws_detail", {}) or {}).get("messages", 0),
            "last_rx_ts_ms": (d.get("ws_detail", {}) or {}).get("last_rx_ms", 0),
        },
        "ws_detail": d.get("ws_detail", {}),
        "coal": d.get("coal", {}),
        "symbols": d.get("symbols", []),
        "coalesce_ms": (d.get("coal", {}) or {}).get("window_ms"),
        "history_seconds": 600,
        "best": d.get("best", {}),
        "exec_cfg": d.get("exec_cfg", {}),
        # чтобы фронт не видел null
        "risk_cfg": {
            "risk_per_trade_pct": getattr(getattr(s, "risk", object()), "risk_per_trade_pct", 0.25),
            "daily_stop_r": getattr(getattr(s, "risk", object()), "daily_stop_r", -10.0),
            "daily_target_r": getattr(getattr(s, "risk", object()), "daily_target_r", 15.0),
            "max_consec_losses": getattr(getattr(s, "risk", object()), "max_consec_losses", 3),
            "cooldown_after_sl_s": getattr(getattr(s, "risk", object()), "cooldown_after_sl_s", 120),
            "min_risk_usd_floor": getattr(getattr(s, "risk", object()), "min_risk_usd_floor", 0.25),
        },
        "safety_cfg": {
            "max_spread_ticks": getattr(getattr(s, "safety", object()), "max_spread_ticks", 3),
            "min_top5_liquidity_usd": getattr(getattr(s, "safety", object()), "min_top5_liquidity_usd", 300000),
            "skip_funding_minute": getattr(getattr(s, "safety", object()), "skip_funding_minute", True),
            "skip_minute_zero": getattr(getattr(s, "safety", object()), "skip_minute_zero", True),
            "min_liq_buffer_sl_mult": getattr(getattr(s, "safety", object()), "min_liq_buffer_sl_mult", 3.0),
        },
        "consec_losses": d.get("consec_losses", None),
    }

@app.get("/symbols")
async def symbols_summary() -> Dict[str, Any]:
    w = _worker_required()
    out: Dict[str, Tuple[float, float]] = {s: w.best_bid_ask(s) for s in w.symbols}
    return {"symbols": list(w.symbols), "best": out}

@app.get("/best")
async def best(symbol: str = Query("BTCUSDT", description="Символ, например BTCUSDT")) -> Dict[str, Any]:
    w = _worker_required()
    sym = _normalize_symbol(symbol, w.symbols)
    bid, ask = w.best_bid_ask(sym)
    return {"symbol": sym, "bid": bid, "ask": ask}

# ------------------------------------------------------------------------------
# Ticks (latest / peek)
# ------------------------------------------------------------------------------

def _worker_latest_tick(w: Worker, symbol: str) -> Optional[Dict[str, Any]]:
    # последний тик; если истории нет — слепок из best bid/ask
    if hasattr(w, "latest_tick"):
        try:
            t = w.latest_tick(symbol)  # type: ignore[attr-defined]
            if t:
                return dict(t)
        except Exception:
            pass
    bid, ask = w.best_bid_ask(symbol)
    if bid > 0 and ask > 0:
        mid = (bid + ask) / 2.0
        return {
            "symbol": symbol,
            "ts_ms": _now_ms(),
            "price": mid,
            "bid": bid,
            "ask": ask,
            "bid_size": 0.0,
            "ask_size": 0.0,
            "mark_price": None,
        }
    return None

def _worker_history_ticks(w: Worker, symbol: str, since_ms: int, limit: int) -> List[Dict[str, Any]]:
    if hasattr(w, "history_ticks"):
        try:
            items = w.history_ticks(symbol, since_ms, limit)  # type: ignore[attr-defined]
            return [dict(t) for t in items]
        except Exception:
            pass
    last = _worker_latest_tick(w, symbol)
    return [last] if last else []

@app.get("/ticks/latest")
async def ticks_latest(symbol: str = Query("BTCUSDT", description="Напр. BTCUSDT")) -> Dict[str, Any]:
    w = _worker_required()
    sym = _normalize_symbol(symbol, w.symbols)
    item = _worker_latest_tick(w, sym)
    return {"symbol": sym, "item": item}

@app.get("/ticks/peek")
async def ticks_peek(
    symbol: str = Query("BTCUSDT", description="Напр. BTCUSDT"),
    ms: int = Query(1500, ge=1, le=60_000, description="Окно, миллисекунды"),
    limit: int = Query(512, ge=1, le=4096, description="Максимум элементов в ответе"),
) -> Dict[str, Any]:
    w = _worker_required()
    sym = _normalize_symbol(symbol, w.symbols)
    since = _now_ms() - ms
    items = _worker_history_ticks(w, sym, since_ms=since, limit=limit)
    if len(items) > limit:
        items = items[-limit:]
    return {"symbol": sym, "count": len(items), "items": items}

# ------------------------------------------------------------------------------
# Features debug (micro + indicators)
# ------------------------------------------------------------------------------

@app.get("/debug/features")
async def debug_features(
    symbol: str = Query("BTCUSDT", description="Символ, напр. BTCUSDT"),
    lookback_ms: int = Query(10_000, ge=100, le=120_000, description="Окно истории для индикаторов"),
) -> Dict[str, Any]:
    w = _worker_required()
    sym = _normalize_symbol(symbol, w.symbols)

    last = _worker_latest_tick(w, sym)
    if not last:
        return {"symbol": sym, "micro": None, "indi": None, "reason": "no_tick"}

    micro = MicroFeatureEngine(tick_size=0.1, lot_usd=10_000.0).update(
        price=last["price"],
        bid=last["bid"],
        ask=last["ask"],
        bid_sz=last.get("bid_size", 0.0),
        ask_sz=last.get("ask_size", 0.0),
    )

    since = _now_ms() - lookback_ms
    hist = _worker_history_ticks(w, sym, since_ms=since, limit=2048)
    prices: List[float] = [float(t.get("price", last["price"])) for t in hist] or [last["price"]] * 10

    indi_eng = IndiEngine(price_step=0.1)
    indi = None
    for p in prices:
        indi = indi_eng.update(price=p)

    indi_dict = None if indi is None else {
        "ema9": indi.ema9,
        "ema21": indi.ema21,
        "ema_slope_ticks": indi.ema_slope_ticks,
        "vwap_drift": indi.vwap_drift,
        "bb_z": indi.bb_z,
        "rsi": indi.rsi,
        "realized_vol_bp": indi.realized_vol_bp,
    }

    sl_usd_est = (
        round((indi.realized_vol_bp / 10_000.0) * last["price"], 4)
        if (indi and indi.realized_vol_bp is not None) else None
    )
    risk = {
        "allow": sl_usd_est is not None,
        "reasons": [] if sl_usd_est is not None else ["no_volatility"],
        "sl_usd_est": sl_usd_est,
    }

    return {
        "symbol": sym,
        "micro": {
            "spread_ticks": micro.spread_ticks,
            "mid": micro.mid,
            "top_liq_usd": micro.top_liq_usd,
            "microprice_drift": micro.microprice_drift,
            "tick_velocity": micro.tick_velocity,
            "aggressor_ratio": micro.aggressor_ratio,
            "obi": micro.obi,
        },
        "indi": indi_dict,
        "risk": risk,
    }

# ------------------------------------------------------------------------------
# Control (paper executor)
# ------------------------------------------------------------------------------

class EntryReq(BaseModel):
    symbol: str
    side: Literal["BUY", "SELL"]
    qty: float
    def normalize(self, allowed: List[str]) -> None:
        s = self.symbol.upper()
        if s not in allowed:
            raise ValueError(f"Unknown symbol '{self.symbol}'. Allowed: {', '.join(allowed)}")
        self.symbol = s
        if not (self.qty > 0):
            raise ValueError("qty must be > 0")

@app.post("/control/test-entry")
async def control_test_entry(req: EntryReq) -> Dict[str, Any]:
    w = _worker_required()
    try:
        req.normalize(w.symbols)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    report = await w.place_entry(req.symbol, req.side, req.qty)
    return {"ok": True, **report}

@app.get("/control/test-entry")
async def control_test_entry_get(
    symbol: str = Query(..., description="Напр. BTCUSDT"),
    side: Literal["BUY", "SELL"] = Query(...),
    qty: float = Query(..., gt=0),
) -> Dict[str, Any]:
    w = _worker_required()
    sym = _normalize_symbol(symbol, w.symbols)
    report = await w.place_entry(sym, side, qty)
    return {"ok": True, **report}

@app.post("/control/flatten")
@app.get("/control/flatten")
async def control_flatten(symbol: str = Query("BTCUSDT")) -> Dict[str, Any]:
    w = _worker_required()
    sym = _normalize_symbol(symbol, w.symbols)
    return await w.flatten(sym)

@app.post("/control/flatten-all")
@app.get("/control/flatten-all")
async def control_flatten_all() -> Dict[str, Any]:
    w = _worker_required()
    results: Dict[str, Any] = {}
    for sym in w.symbols:
        try:
            results[sym] = await w.flatten(sym)
        except Exception as e:
            results[sym] = {"ok": False, "error": str(e)}
    return {
        "ok": all(v.get("ok", False) for v in results.values()),
        "results": results,
    }

@app.post("/control/set-timeout")
async def control_set_timeout(
    symbol: str = Query("BTCUSDT"),
    timeout_ms: int = Query(20_000, ge=5_000, le=600_000),
) -> Dict[str, Any]:
    w = _worker_required()
    sym = _normalize_symbol(symbol, w.symbols)
    return w.set_timeout_ms(sym, timeout_ms)

# ------------------------------------------------------------------------------
# Positions (snapshot)
# ------------------------------------------------------------------------------

@app.get("/positions")
async def positions() -> Dict[str, Any]:
    w = _worker_required()
    d = w.diag()
    positions = d.get("positions", {})
    return {"symbols": list(w.symbols), "positions": positions}

@app.get("/position")
async def position(symbol: str = Query("BTCUSDT", description="Символ, например BTCUSDT")) -> Dict[str, Any]:
    w = _worker_required()
    sym = _normalize_symbol(symbol, w.symbols)
    d = w.diag()
    pos = (d.get("positions", {}) or {}).get(sym)
    if pos is None:
        pos = {
            "state": "FLAT", "side": None, "qty": 0.0, "entry_px": 0.0,
            "sl_px": None, "tp_px": None, "opened_ts_ms": 0, "timeout_ms": 180_000
        }
    return {"symbol": sym, "position": pos}

# ------------------------------------------------------------------------------
# Metrics (compact JSON)
# ------------------------------------------------------------------------------

@app.get("/metrics")
async def metrics() -> Dict[str, Any]:
    """
    Компактные метрики для мониторинга/панели.
    Источники: worker.diag()['ws_detail'], ['coal'], ['exec_counters'], ['pnl_day'].
    """
    w = _worker_required()
    d = w.diag()

    ws = d.get("ws_detail", {}) or {}
    coal = d.get("coal", {}) or {}
    exec_counters = d.get("exec_counters", {}) or {}
    pnl_day = d.get("pnl_day", {}) or {}

    positions = d.get("positions", {}) or {}
    open_positions = sum(1 for p in positions.values() if (p or {}).get("state") == "OPEN")

    return {
        "ws_messages": ws.get("messages", 0),
        "ws_last_rx_ms": ws.get("last_rx_ms", 0),
        "ws_reconnects": ws.get("reconnects", 0),

        "coalesce_window_ms": coal.get("window_ms"),
        "coalesce_emit_count": coal.get("emit_count", 0),
        "coalesce_emits_per_sec": coal.get("emits_per_sec", 0.0),
        "coalesce_queue_max": coal.get("max_queue_depth", 0),

        "orders_limit_total": exec_counters.get("limit_total", 0),
        "orders_market_total": exec_counters.get("market_total", 0),
        "orders_cancel_total": exec_counters.get("cancel_total", 0),

        "open_positions": open_positions,
        "symbols": list(w.symbols),

        "pnl_r_day": pnl_day.get("pnl_r", 0.0),
        "pnl_usd_day": pnl_day.get("pnl_usd", 0.0),
        "winrate_day": pnl_day.get("winrate"),
        "trades_day": pnl_day.get("trades", 0),
        "max_dd_r_day": pnl_day.get("max_dd_r"),
    }

# ------------------------------------------------------------------------------
# Debug reasons (why entries were blocked)
# ------------------------------------------------------------------------------

@app.get("/debug/reasons")
async def debug_reasons() -> Dict[str, Any]:
    w = _worker_required()
    d = w.diag()
    reasons = d.get("block_reasons", {}) or d.get("reasons", {}) or {}
    return {"reasons": reasons}

# ------------------------------------------------------------------------------
# Trades history (best-effort)
# ------------------------------------------------------------------------------

@app.get("/trades")
async def trades(
    symbol: Optional[str] = Query(None, description="Фильтр по символу, напр. BTCUSDT"),
    limit: int = Query(50, ge=1, le=500),
) -> Dict[str, Any]:
    w = _worker_required()
    d = w.diag()
    store = d.get("trades")

    items: List[Dict[str, Any]] = []
    if isinstance(store, list):
        items = [t for t in store if (not symbol or t.get("symbol") == symbol)]
    elif isinstance(store, dict):
        if symbol:
            items = list(store.get(symbol.upper(), []))
        else:
            for arr in store.values():
                items.extend(list(arr))

    items.sort(key=lambda x: x.get("closed_ts", x.get("opened_ts", 0)), reverse=True)
    if len(items) > limit:
        items = items[:limit]
    return {"items": items, "count": len(items)}

# ------------------------------------------------------------------------------
# Daily PnL (best-effort)
# ------------------------------------------------------------------------------

@app.get("/pnl/daily")
async def pnl_daily() -> Dict[str, Any]:
    s = get_settings()
    w = _worker_required()
    d = w.diag()
    pnl_day = d.get("pnl_day", {}) or {}
    return {
        "day": pnl_day.get("day"),
        "trades": pnl_day.get("trades", 0),
        "winrate": pnl_day.get("winrate"),
        "avg_r": pnl_day.get("avg_r"),
        "pnl_r": pnl_day.get("pnl_r", 0.0),
        "pnl_usd": pnl_day.get("pnl_usd", 0.0),
        "max_dd_r": pnl_day.get("max_dd_r"),
        "risk_per_trade_pct": getattr(getattr(s, "risk", object()), "risk_per_trade_pct", 0.25),
    }
