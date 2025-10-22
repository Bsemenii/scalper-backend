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

APP_VERSION = "0.5.0"

# ------------------------------------------------------------------------------
# App init & CORS
# ------------------------------------------------------------------------------

app = FastAPI(title="AI Scalping Backend", version=APP_VERSION)
app.state.worker: Optional[Worker] = None

if os.getenv("ENABLE_CORS", "1") == "1":
    app.add_middleware(
        CORSMiddleware,
        allow_origins=os.getenv("CORS_ORIGINS", "*").split(","),
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
        await w.stop()
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
        ws_msgs = d.get("ws_detail", {}).get("messages", 0)
        emits = d.get("coal", {}).get("emit_count", 0)
        return {"ok": (ws_msgs > 0 and emits > 0), "ws_messages": ws_msgs, "emits": emits}
    except Exception:
        return {"ok": False}


@app.get("/status")
def status() -> Dict[str, Any]:
    s = get_settings()
    return {
        "mode": s.mode,
        "symbols": s.symbols,
        "streams": {"coalesce_ms": s.streams.coalesce_ms},
        "risk": {
            "risk_per_trade_pct": s.risk.risk_per_trade_pct,
            "daily_stop_r": s.risk.daily_stop_r,
            "daily_target_r": s.risk.daily_target_r,
            "max_consec_losses": s.risk.max_consec_losses,
            "cooldown_after_sl_s": s.risk.cooldown_after_sl_s,
        },
        "safety": {
            "max_spread_ticks": s.safety.max_spread_ticks,
            "min_top5_liquidity_usd": s.safety.min_top5_liquidity_usd,
            "skip_funding_minute": s.safety.skip_funding_minute,
            "skip_minute_zero": s.safety.skip_minute_zero,
            "min_liq_buffer_sl_mult": s.safety.min_liq_buffer_sl_mult,
        },
        "execution": {
            "limit_offset_ticks": s.execution.limit_offset_ticks,
            "limit_timeout_ms": s.execution.limit_timeout_ms,
            "max_slippage_bp": s.execution.max_slippage_bp,
            "time_in_force": getattr(s.execution, "time_in_force", "GTC"),
        },
        "ml": {
            "enabled": s.ml.enabled,
            "p_threshold_mom": s.ml.p_threshold_mom,
            "p_threshold_rev": s.ml.p_threshold_rev,
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
    Расширенная диагностика пайплайна (стабильная форма для фронта).
    """
    w = _worker_required()
    d = w.diag()
    return {
        "ws": {
            "count": d.get("ws_detail", {}).get("messages", 0),
            "last_rx_ts_ms": d.get("ws_detail", {}).get("last_rx_ms", 0),
        },
        "ws_detail": d.get("ws_detail", {}),
        "coal": d.get("coal", {}),
        "symbols": d.get("symbols", []),
        "coalesce_ms": d.get("coal", {}).get("window_ms"),
        "history_seconds": 600,  # совместимость с фронтом
        "best": d.get("best", {}),
        "exec_cfg": d.get("exec_cfg", {}),
    }


@app.get("/symbols")
async def symbols_summary() -> Dict[str, Any]:
    """
    Сводка по лучшим ценам всех символов.
    """
    w = _worker_required()
    out: Dict[str, Tuple[float, float]] = {s: w.best_bid_ask(s) for s in w.symbols}
    return {"symbols": list(w.symbols), "best": out}


@app.get("/best")
async def best(
    symbol: str = Query("BTCUSDT", description="Символ, например BTCUSDT"),
) -> Dict[str, Any]:
    """
    Последняя лучшая цена (bid/ask) по символу.
    """
    w = _worker_required()
    sym = _normalize_symbol(symbol, w.symbols)
    bid, ask = w.best_bid_ask(sym)
    return {"symbol": sym, "bid": bid, "ask": ask}


# ------------------------------------------------------------------------------
# Ticks (latest / peek)
# ------------------------------------------------------------------------------

def _worker_latest_tick(w: Worker, symbol: str) -> Optional[Dict[str, Any]]:
    """
    Последний тик по символу. Если истории нет — соберём снимок из best bid/ask.
    """
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
    """
    История тиков с ts_ms >= since_ms, обрезанная хвостом до limit.
    Если истории нет — вернём <=1 элемента (latest).
    """
    if hasattr(w, "history_ticks"):
        try:
            items = w.history_ticks(symbol, since_ms, limit)  # type: ignore[attr-defined]
            return [dict(t) for t in items]
        except Exception:
            pass

    last = _worker_latest_tick(w, symbol)
    return [last] if last else []


@app.get("/ticks/latest")
async def ticks_latest(
    symbol: str = Query("BTCUSDT", description="Напр. BTCUSDT"),
) -> Dict[str, Any]:
    """
    Последний тик по символу (если истории нет — снимок из best bid/ask).
    """
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
    """
    История тик-снапшотов за последние `ms` (если воркер не хранит историю — вернём последний тик).
    """
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
    """
    Быстрый расчёт микроструктуры и индикаторов.
    Если нет истории — считаем по последнему тіку и псевдо-ценам.
    """
    w = _worker_required()
    sym = _normalize_symbol(symbol, w.symbols)

    last = _worker_latest_tick(w, sym)
    if not last:
        return {"symbol": sym, "micro": None, "indi": None, "reason": "no_tick"}

    # Micro по последнему снапу
    micro = MicroFeatureEngine(tick_size=0.1, lot_usd=10_000.0).update(
        price=last["price"],
        bid=last["bid"],
        ask=last["ask"],
        bid_sz=last.get("bid_size", 0.0),
        ask_sz=last.get("ask_size", 0.0),
    )

    # Indicators по истории (если есть); иначе — псевдо-ряд из последней цены
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
async def control_flatten(
    symbol: str = Query("BTCUSDT"),
) -> Dict[str, Any]:
    """
    Форс-закрыть позицию (paper), сбросить FSM в FLAT. Удобно для повторных входов.
    Поддерживаются и POST, и GET для удобства из браузера.
    """
    w = _worker_required()
    sym = _normalize_symbol(symbol, w.symbols)
    res = await w.flatten(sym)
    return res

@app.post("/control/flatten-all")
@app.get("/control/flatten-all")
async def control_flatten_all() -> Dict[str, Any]:
    """
    Форс-закрыть позиции по всем символам и сбросить FSM в FLAT.
    Поддерживаются GET и POST (для удобства из браузера).
    """
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
    """
    Изменить таймаут позиции для отладки (например, 20 000 мс).
    """
    w = _worker_required()
    sym = _normalize_symbol(symbol, w.symbols)
    return w.set_timeout_ms(sym, timeout_ms)

# ------------------------------------------------------------------------------
# Positions (snapshot)
# ------------------------------------------------------------------------------

@app.get("/positions")
async def positions() -> Dict[str, Any]:
    """
    Снимок состояний позиций по всем символам.
    Формат берём из w.diag()['positions'], чтобы совпадал с тем, что уже видишь в /debug/diag.
    """
    w = _worker_required()
    d = w.diag()
    positions = d.get("positions", {})
    return {"symbols": list(w.symbols), "positions": positions}


@app.get("/position")
async def position(
    symbol: str = Query("BTCUSDT", description="Символ, например BTCUSDT"),
) -> Dict[str, Any]:
    """
    Снимок состояния позиции по одному символу.
    """
    w = _worker_required()
    sym = _normalize_symbol(symbol, w.symbols)
    d = w.diag()
    pos = d.get("positions", {}).get(sym, None)
    if pos is None:
        # если почему-то нет ключа — отдадим FLAT по умолчанию
        pos = {"state": "FLAT", "side": None, "qty": 0.0, "entry_px": 0.0, "sl_px": None, "tp_px": None,
               "opened_ts_ms": 0, "timeout_ms": 180_000}
    return {"symbol": sym, "position": pos}
