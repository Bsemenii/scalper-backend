# bot/api/app.py
from __future__ import annotations

import time
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, Query, Request
from fastapi.responses import JSONResponse

# Внутренние модули
from stream.hub import TickerHub               # хаб сам управляет WS и коалесцером
from stream.coalescer import TickerTick        # тип стабилизированного тика
from features.microstructure import MicroFeatureEngine
from features.indicators import IndiEngine

app = FastAPI(title="AI Scalping Backend", version="0.2.1")

# Символы по умолчанию (переопределим позже из settings, если понадобится)
DEFAULT_SYMBOLS: List[str] = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]


# -------------------------
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# -------------------------

def _ensure_inited(app: FastAPI) -> None:
    """
    Ленивая инициализация app.state, чтобы ручки работали и в тестах, и без on_startup.
    """
    st = app.state
    if not hasattr(st, "symbols"):
        st.symbols = list(DEFAULT_SYMBOLS)
    if not hasattr(st, "hub"):
        # ✅ новый конструктор: хаб сам создаёт и крутит коалесцер внутри
        st.hub = TickerHub(symbols=st.symbols, coalesce_ms=75)


def _now_ms() -> int:
    return int(time.time() * 1000)


def _latest_to_dict(hub: TickerHub, symbol: str) -> Optional[Dict[str, Any]]:
    t: Optional[TickerTick] = hub.latest(symbol)
    return t.__dict__ if t else None


def _history_last_ms(hub: TickerHub, symbol: str, ms: int) -> List[TickerTick]:
    """
    Достаём недавние тики за окно `ms`:
    1) если есть history_window(symbol, since_ms) — используем,
    2) иначе, если есть history(symbol, n) — берём хвост и отфильтровываем,
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


# -------------------------
# LIFESPAN (запуск/останов)
# -------------------------

@app.on_event("startup")
async def _startup() -> None:
    _ensure_inited(app)
    # если у хаба есть явный старт — запустим
    if hasattr(app.state.hub, "start"):
        try:
            await app.state.hub.start()  # type: ignore[attr-defined]
        except TypeError:
            # если start синхронный или без await — проигнорируем
            pass


@app.on_event("shutdown")
async def _shutdown() -> None:
    if hasattr(app.state, "hub") and hasattr(app.state.hub, "stop"):
        try:
            await app.state.hub.stop()  # type: ignore[attr-defined]
        except TypeError:
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
    symbols: List[str] = app.state.symbols

    ws_stats = hub.ws_stats()            # {count, last_rx_ts_ms}
    coal_stats = hub.coalescer_stats()   # {push_count, last_tick_ts_ms}

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
# DEBUG / DIAGNОST
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
    return {"ws": hub.ws_stats(), "coal": hub.coalescer_stats()}


@app.get("/ticks/peek")
async def ticks_peek(
    symbol: str = Query("BTCUSDT", description="Напр. BTCUSDT"),
    ms: int = Query(1500, ge=1, le=60_000, description="Окно, миллисекунды"),
) -> Dict[str, Any]:
    """
    Возвращает список тик-снапшотов за последние `ms` миллисекунд.
    """
    _ensure_inited(app)
    hub: TickerHub = app.state.hub
    items: List[TickerTick] = _history_last_ms(hub, symbol, ms)
    return {
        "symbol": symbol,
        "count": len(items),
        "items": [t.__dict__ for t in items],
    }


@app.get("/debug/features")
async def debug_features(
    request: Request,
    symbol: str = Query(default="BTCUSDT", description="Торговый символ, например BTCUSDT"),
    lookback_ms: int = Query(default=10_000, ge=100, le=120_000),
):
    """
    Быстрый расчёт микроструктурных фич и индикаторов.
    Без внешних зависимостей: берём последний тик + короткую историю цен.
    """
    _ensure_inited(app)
    hub: TickerHub = request.app.state.hub

    last: Optional[TickerTick] = hub.latest(symbol)
    if not last:
        return {"symbol": symbol, "micro": None, "indi": None, "reason": "no_tick"}

    # --- Micro (по одному снапу)
    micro = MicroFeatureEngine(tick_size=0.1, lot_usd=10_000.0).update(
        price=last.price,
        bid=last.bid,
        ask=last.ask,
        bid_sz=last.bid_size,
        ask_sz=last.ask_size,
    )

    # --- Indicators (по истории)
    hist: List[TickerTick] = _history_last_ms(hub, symbol, lookback_ms)
    prices: List[float] = [t.price for t in hist] or [last.price] * 10  # fallback, чтобы не упало

    indi_eng = IndiEngine(price_step=0.1)
    indi = None
    for p in prices:
        indi = indi_eng.update(price=p)

    # На случай, если IndiEngine ничего не вернул (не должно случиться)
    if indi is None:
        return {"symbol": symbol, "micro": micro.__dict__, "indi": None, "reason": "no_indicators"}

    # --- Условный риск (пример оценки SL из волатильности)
    sl_usd_est = (
        round((indi.realized_vol_bp / 10_000.0) * last.price, 4)
        if indi.realized_vol_bp is not None
        else None
    )
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
