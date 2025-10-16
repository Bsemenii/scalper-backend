# bot/api/app.py
from __future__ import annotations

import time
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse

# Внутренние модули (ожидаются в твоём проекте)
from stream.hub import TickerHub, Tick
from stream.coalescer import Coalescer
from features.microstructure import MicroFeatureEngine
from features.indicators import IndiEngine

app = FastAPI(title="AI Scalping Backend", version="0.2.0")

# Базовый набор символов (можно переопределить в settings позже)
DEFAULT_SYMBOLS: List[str] = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]


# -------------------------
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# -------------------------

def _ensure_inited(app: FastAPI) -> None:
    """
    Ленивая инициализация app.state, чтобы ручки работали и в тестах, и без on_startup.
    """
    st = app.state
    if not hasattr(st, "hub"):
        st.hub = TickerHub(maxlen=4096)
    if not hasattr(st, "symbols"):
        st.symbols = list(DEFAULT_SYMBOLS)
    if not hasattr(st, "coalescer"):
        # В «бою» сюда обычно приходит реальный producer (WS → hub.push),
        # а Coalescer уже использует hub как буфер снапов.
        st.coalescer = Coalescer(hub=st.hub, symbols=st.symbols, interval_ms=75)


def _now_ms() -> int:
    return int(time.time() * 1000)


def _latest_to_dict(hub: TickerHub, symbol: str) -> Optional[Dict[str, Any]]:
    t: Optional[Tick] = hub.latest(symbol)
    return t.__dict__ if t else None


def _history_last_ms(hub: TickerHub, symbol: str, ms: int) -> List[Tick]:
    """
    Универсальный способ достать недавние тики:
    1) если есть метод history_window(symbol, since_ms) — используем его,
    2) иначе, если есть history(symbol, n) — достаём «разумное» количество и отфильтровываем,
    3) fallback: только последний тик (если есть).
    """
    since = _now_ms() - ms

    # Вариант 1: специализированный метод (предпочтительно)
    if hasattr(hub, "history_window"):
        try:
            return list(getattr(hub, "history_window")(symbol, since))  # type: ignore[attr-defined]
        except Exception:
            pass

    # Вариант 2: «хвост» из истории и фильтрация по ts_ms
    if hasattr(hub, "history"):
        try:
            raw: List[Tick] = list(getattr(hub, "history")(symbol, 512))  # type: ignore[attr-defined]
            return [t for t in raw if getattr(t, "ts_ms", 0) >= since]
        except Exception:
            pass

    # Fallback 3: только последний
    last = hub.latest(symbol)
    return [last] if last else []


# -------------------------
# LIFESPAN (дополнительно)
# -------------------------

@app.on_event("startup")
async def _startup() -> None:
    # В рантайме всё равно инициализируемся; в тестах _ensure_inited будет вызван из самих ручек.
    _ensure_inited(app)


@app.on_event("shutdown")
async def _shutdown() -> None:
    # Здесь можно корректно останавливать фоновые задачи, если они появятся.
    pass


# -------------
# БАЗОВЫЕ РУЧКИ
# -------------

@app.get("/")
async def root() -> Dict[str, Any]:
    return {"ok": True, "app": "ai-scalping-backend"}


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
    coal: Coalescer = app.state.coalescer
    symbols: List[str] = app.state.symbols

    ws_stats = hub.ws_stats()           # {count, last_rx_ts_ms}
    coal_stats = coal.stats()           # {push_count, last_tick_ts_ms}

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
# DEBUG / DIAGNOST
# ----------------

@app.get("/debug/ws")
async def debug_ws() -> Dict[str, Any]:
    _ensure_inited(app)
    hub: TickerHub = app.state.hub
    return hub.ws_stats()


@app.get("/debug/diag")
async def debug_diag() -> Dict[str, Any]:
    _ensure_inited(app)
    hub: TickerHub = app.state.hub
    coal: Coalescer = app.state.coalescer
    return {"ws": hub.ws_stats(), "coal": coal.stats()}


@app.get("/ticks/peek")
async def ticks_peek(
    symbol: str = Query(..., description="Напр. BTCUSDT"),
    ms: int = Query(1500, ge=1, le=60_000, description="Окно, миллисекунды"),
) -> Dict[str, Any]:
    """
    Возвращает список тик-снапшотов за последние `ms` миллисекунд.
    """
    _ensure_inited(app)
    hub: TickerHub = app.state.hub
    items: List[Tick] = _history_last_ms(hub, symbol, ms)
    return {
        "symbol": symbol,
        "count": len(items),
        "items": [t.__dict__ for t in items],
    }


@app.get("/debug/features")
async def debug_features(
    symbol: str = Query(..., description="Напр. BTCUSDT"),
    lookback_ms: int = Query(5_000, ge=200, le=120_000, description="Окно истории для индикаторов"),
) -> Dict[str, Any]:
    """
    Считает микроструктуру и базовые индикаторы:
    - micro: spread_ticks, mid, top_liq_usd, microprice_drift, tick_velocity, aggressor_ratio (может быть None), obi
    - indi:  ema9/ema21/ema_slope_ticks, vwap_drift, bb_z, rsi, realized_vol_bp
    - risk:  allow|false + reasons (на данном этапе reasons=[]) и пример оценки stop-loss в USD
    """
    _ensure_inited(app)
    hub: TickerHub = app.state.hub

    last = hub.latest(symbol)
    if not last:
        return {"symbol": symbol, "error": "no ticks yet"}

    # --- Micro (по одному снапу)
    micro = MicroFeatureEngine(tick_size=0.1, lot_usd=10_000.0).update(
        price=last.price,
        bid=last.bid,
        ask=last.ask,
        bid_sz=last.bid_size,
        ask_sz=last.ask_size,
    )

    # --- Indicators (по истории)
    prices: List[float] = [t.price for t in _history_last_ms(hub, symbol, lookback_ms)]
    if not prices:
        prices = [last.price] * 10  # fallback чтобы индикаторы не упали

    indi_eng = IndiEngine(price_step=0.1)
    for p in prices:
        indi = indi_eng.update(price=p)

    # --- Условный риск (заглушка — только пример SL)
    sl_usd_est = round((indi.realized_vol_bp / 10_000.0) * last.price, 4) if indi.realized_vol_bp is not None else None
    risk = {
        "allow": True if sl_usd_est is not None else False,
        "reasons": [] if sl_usd_est is not None else ["no_volatility"],
        "sl_usd_est": sl_usd_est,
    }

    return {
        "symbol": symbol,
        "micro": {
            "spread_ticks": micro.spread_ticks,
            "mid": micro.mid,
            "top_liq_usd": micro.top_liq_usd,
            "microprice_drift": micro.microprice_drift,
            "tick_velocity": micro.tick_velocity,
            "aggressor_ratio": micro.aggressor_ratio,  # может быть None
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
