# bot/api/app.py
from __future__ import annotations

import asyncio
import inspect
import os
import time
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

# Внутренние модули
from stream.hub import TickerHub            # хаб сам управляет WS и коалесцером
from stream.coalescer import TickerTick      # стабилизированный тик
from features.microstructure import MicroFeatureEngine
from features.indicators import IndiEngine


app = FastAPI(title="AI Scalping Backend", version="0.2.3")

# -------------------------
# КОНФИГ ПО УМОЛЧАНИЮ/ENV
# -------------------------

DEFAULT_SYMBOLS: List[str] = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

# Параметры коалесера/истории можно переопределять через ENV без изменения кода
COALESCE_MS = int(os.getenv("COALESCE_MS", "75"))
HISTORY_SECONDS = int(os.getenv("HISTORY_SECONDS", "600"))  # 10 минут истории по умолчанию

# (Опционально) включить CORS, если фронт будет дёргать API из браузера
if os.getenv("ENABLE_CORS", "1") == "1":
    app.add_middleware(
        CORSMiddleware,
        allow_origins=os.getenv("CORS_ORIGINS", "*").split(","),
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )


# -------------------------
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# -------------------------

def _ensure_inited(app: FastAPI) -> None:
    """
    Ленивая инициализация app.state, чтобы ручки работали и в тестах, и без on_startup.
    """
    st = app.state
    if not hasattr(st, "symbols"):
        st.symbols = [s.upper() for s in DEFAULT_SYMBOLS]
    if not hasattr(st, "hub"):
        # Хаб сам крутит коалесцер внутри (идём по ENV-конфигу)
        st.hub = TickerHub(
            symbols=st.symbols,
            coalesce_ms=COALESCE_MS,
            history_seconds=HISTORY_SECONDS,
        )


def _normalize_symbol(app: FastAPI, symbol: str) -> str:
    sym = symbol.upper()
    if sym not in app.state.symbols:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown symbol '{symbol}'. Allowed: {', '.join(app.state.symbols)}",
        )
    return sym


def _now_ms() -> int:
    return int(time.time() * 1000)


def _latest_to_dict(hub: TickerHub, symbol: str) -> Optional[Dict[str, Any]]:
    t: Optional[TickerTick] = hub.latest(symbol)
    return t.__dict__ if t else None


def _history_last_ms(hub: TickerHub, symbol: str, ms: int) -> List[TickerTick]:
    """
    Достаём недавние тики за окно `ms`:
    1) если у хаба есть history_window(symbol, since_ms) — используем,
    2) иначе, если есть history(symbol, n) — берём разумный хвост и фильтруем,
    3) fallback: только последний тик (если есть).
    """
    since = _now_ms() - ms

    if hasattr(hub, "history_window"):
        try:
            return list(getattr(hub, "history_window")(symbol, since))  # type: ignore[attr-defined]
        except Exception:
            pass

    if hasattr(hub, "history"):
        try:
            raw: List[TickerTick] = list(getattr(hub, "history")(symbol, 512))  # type: ignore[attr-defined]
            return [t for t in raw if getattr(t, "ts_ms", 0) >= since]
        except Exception:
            pass

    last = hub.latest(symbol)
    return [last] if last else []


async def _maybe_call_async(fn, *args, **kwargs):
    """
    Безопасно вызываем fn, поддерживая и sync, и async.
    """
    if fn is None or not callable(fn):
        return
    if inspect.iscoroutinefunction(fn):
        return await fn(*args, **kwargs)
    # если вернули корутину из обычной функции — тоже подождём
    res = fn(*args, **kwargs)
    if inspect.isawaitable(res):
        return await res
    return res


# -------------------------
# LIFE-CYCLE (on_event)
# -------------------------

@app.on_event("startup")
async def _startup() -> None:
    _ensure_inited(app)
    hub: TickerHub = app.state.hub
    # если у хаба есть явный start — запустим (поддержка sync/async)
    await _maybe_call_async(getattr(hub, "start", None))


@app.on_event("shutdown")
async def _shutdown() -> None:
    hub: Optional[TickerHub] = getattr(app.state, "hub", None)
    if hub is not None:
        await _maybe_call_async(getattr(hub, "stop", None))


# -------------
# БАЗОВЫЕ РУЧКИ
# -------------

@app.get("/")
async def root() -> Dict[str, Any]:
    return {"ok": True, "app": "ai-scalping-backend"}


@app.get("/healthz")
async def healthz() -> Dict[str, Any]:
    return {"ok": True}


@app.get("/status")
async def status() -> JSONResponse:
    """
    Всегда отдаёт:
      {
        "ws":   {"count": int, "last_rx_ts_ms": int|null},
        "coal": {"push_count": int, "last_tick_ts_ms": int|null},
        "symbols": { "BTCUSDT": {...}|null, "ETHUSDT": {...}|null, ... }
      }
    """
    _ensure_inited(app)
    hub: TickerHub = app.state.hub
    symbols: List[str] = app.state.symbols

    ws_stats = hub.ws_stats()             # {count, last_rx_ts_ms}
    coal_stats = hub.coalescer_stats()    # {push_count, last_tick_ts_ms}

    symbols_map: Dict[str, Optional[Dict[str, Any]]] = {
        s: _latest_to_dict(hub, s) for s in symbols
    }
    return JSONResponse({"ws": ws_stats, "coal": coal_stats, "symbols": symbols_map})


@app.get("/symbols")
async def symbols_summary() -> Dict[str, Any]:
    """
    Короткая сводка по последним снапам символов.
    """
    _ensure_inited(app)
    hub: TickerHub = app.state.hub
    symbols: List[str] = app.state.symbols

    out: Dict[str, Optional[Dict[str, Any]]] = {s: _latest_to_dict(hub, s) for s in symbols}
    return {"symbols": out}


# ----------------
# DEBUG / DIАГНОСТИКА
# ----------------

@app.get("/debug/ws")
async def debug_ws() -> Dict[str, Any]:
    _ensure_inited(app)
    hub: TickerHub = app.state.hub
    return hub.ws_stats()


@app.get("/debug/diag")
async def debug_diag() -> Dict[str, Any]:
    """
    Расширенная диагностика:
      - ws: короткая статистика по приёму WS
      - ws_detail: опциональная диагностика адаптера (URL, ошибки, счётчики) — если реализована в hub
      - coal: статистика коалесцера
      - конфиг (символы, параметры окна/истории)
    """
    _ensure_inited(app)
    hub: TickerHub = app.state.hub
    ws_detail = {}
    try:
        # безопасно дернём опциональный метод
        ws_detail = getattr(hub, "ws_diag", lambda: {})()
    except Exception:
        ws_detail = {}
    return {
        "ws": hub.ws_stats(),
        "ws_detail": ws_detail,
        "coal": hub.coalescer_stats(),
        "symbols": app.state.symbols,
        "coalesce_ms": COALESCE_MS,
        "history_seconds": HISTORY_SECONDS,
    }


# -------------
# ТИКИ
# -------------

@app.get("/ticks/latest")
async def ticks_latest(
    symbol: str = Query("BTCUSDT", description="Напр. BTCUSDT"),
) -> Dict[str, Any]:
    """
    Возвращает последний тик по символу.
    """
    _ensure_inited(app)
    sym = _normalize_symbol(app, symbol)
    hub: TickerHub = app.state.hub
    t: Optional[TickerTick] = hub.latest(sym)
    return {"symbol": sym, "item": (t.__dict__ if t else None)}


@app.get("/ticks/peek")
async def ticks_peek(
    symbol: str = Query("BTCUSDT", description="Напр. BTCUSDT"),
    ms: int = Query(1500, ge=1, le=60_000, description="Окно, миллисекунды"),
    limit: int = Query(512, ge=1, le=4096, description="Максимум элементов в ответе (хвост)"),
) -> Dict[str, Any]:
    """
    Возвращает список тик-снапшотов за последние `ms` миллисекунд, обрезая хвостом до `limit`.
    """
    _ensure_inited(app)
    sym = _normalize_symbol(app, symbol)
    hub: TickerHub = app.state.hub
    items: List[TickerTick] = _history_last_ms(hub, sym, ms)
    if len(items) > limit:
        items = items[-limit:]
    return {
        "symbol": sym,
        "count": len(items),
        "items": [t.__dict__ for t in items],
    }


# ----------------
# FEATURES DEBUG
# ----------------

@app.get("/debug/features")
async def debug_features(
    symbol: str = Query("BTCUSDT", description="Торговый символ, например BTCUSDT"),
    lookback_ms: int = Query(10_000, ge=100, le=120_000, description="Окно истории для индикаторов"),
) -> Dict[str, Any]:
    """
    Быстрый расчёт микроструктуры и базовых индикаторов.
    - micro: spread_ticks, mid, top_liq_usd, microprice_drift, tick_velocity, aggressor_ratio (может быть None), obi
    - indi:  ema9/ema21/ema_slope_ticks, vwap_drift, bb_z, rsi, realized_vol_bp
    - risk:  allow|false + reasons и пример оценки stop-loss в USD
    """
    _ensure_inited(app)
    sym = _normalize_symbol(app, symbol)
    hub: TickerHub = app.state.hub

    last: Optional[TickerTick] = hub.latest(sym)
    if not last:
        return {"symbol": sym, "micro": None, "indi": None, "reason": "no_tick"}

    # --- Micro (по одному снапу)
    micro = MicroFeatureEngine(tick_size=0.1, lot_usd=10_000.0).update(
        price=last.price,
        bid=last.bid,
        ask=last.ask,
        bid_sz=last.bid_size,
        ask_sz=last.ask_size,
    )

    # --- Indicators (по истории)
    hist: List[TickerTick] = _history_last_ms(hub, sym, lookback_ms)
    prices: List[float] = [t.price for t in hist] or [last.price] * 10  # fallback

    indi_eng = IndiEngine(price_step=0.1)
    indi = None
    for p in prices:
        indi = indi_eng.update(price=p)

    if indi is None:
        return {"symbol": sym, "micro": micro.__dict__, "indi": None, "reason": "no_indicators"}

    # --- Условный риск (пример оценки SL из волатильности)
    sl_usd_est = (
        round((indi.realized_vol_bp / 10_000.0) * last.price, 4)
        if indi.realized_vol_bp is not None
        else None
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
        "indi": {
            "ema9": indi.ema9,
            "ema21": indi.ema21,
            "ema_slope_ticks": indi.ema_slope_ticks,
            "vwap_drift": indi.vwap_drift,
            "bb_z": indi.bb_z,
            "rsi": indi.rsi,
            "realized_vol_bp": indi.realized_vol_bp,
        },
        "risk": risk,
    }
