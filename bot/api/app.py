# bot/api/app.py
from __future__ import annotations

import os
import time
import json
import asyncio
import logging
from typing import Any, Dict, Iterable, List, Literal, Optional, Tuple

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field
from sqlalchemy import text

from bot.core.config import get_settings
from bot.worker import Worker
from storage.db import init_db, get_engine
from datetime import datetime, timezone

APP_VERSION = "0.9.0"


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

def _now_ms() -> int:
    return int(time.time() * 1000)


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

def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None:
            return default
        return float(v)
    except Exception:
        return default

def _safe_int(v: Any, default: int = 0) -> int:
    try:
        if v is None:
            return default
        return int(v)
    except Exception:
        return default


def _symbols_from_settings() -> List[str]:
    try:
        s = get_settings()
        return [sym.upper() for sym in getattr(s, "symbols", [])]
    except Exception:
        return ["BTCUSDT", "ETHUSDT", "SOLUSDT"]


def _coalesce_from_settings() -> int:
    try:
        s = get_settings()
        return int(getattr(getattr(s, "streams", object()), "coalesce_ms", 75))
    except Exception:
        return int(os.getenv("COALESCE_MS", "75"))



# --- risk & sizing helpers ----------------------------------------------------

def _account_cfg() -> Dict[str, float]:
    s = get_settings()
    acc = getattr(s, "account", object())
    exe = getattr(s, "execution", object())
    return {
        "equity": float(getattr(acc, "starting_equity_usd", 1000.0)),
        "lev": float(getattr(acc, "leverage", 15.0)),
        "min_notional": float(getattr(acc, "min_notional_usd", 5.0)),
        "fee_maker_bps": float(getattr(exe, "fee_bps_maker", 2.0)),
        "fee_taker_bps": float(getattr(exe, "fee_bps_taker", 4.0)),
    }


def _risk_cfg() -> Dict[str, float]:
    s = get_settings()
    r = getattr(s, "risk", object())
    return {
        "risk_pct": float(getattr(r, "risk_per_trade_pct", 0.15)),
        "min_risk_floor": float(getattr(r, "min_risk_usd_floor", 1.0)),
    }


def _exec_cfg() -> Dict[str, float | int | str]:
    s = get_settings()
    e = getattr(s, "execution", object())
    return {
        "limit_offset_ticks": int(getattr(e, "limit_offset_ticks", 1)),
        "limit_timeout_ms": int(getattr(e, "limit_timeout_ms", 900)),
        "min_stop_ticks": int(getattr(e, "min_stop_ticks", 6)),
        "time_in_force": str(getattr(e, "time_in_force", "GTX")),
    }


def _estimate_fees_usd(
    notional_entry: float, notional_exit: float, maker_bps: float, taker_bps: float, taker_like_entry: bool
) -> float:
    bps_entry = taker_bps if taker_like_entry else maker_bps
    bps_exit = taker_bps
    return (notional_entry * bps_entry + notional_exit * bps_exit) / 10_000.0


def _auto_size_qty(
    price: float,
    sl_distance_px: float,
    *,
    equity_usd: float,
    risk_pct: float,
    lev: float,
    min_notional_usd: float,
) -> float:
    """Простая sizing-формула под MVP. Возвращает qty без квантования шагами."""
    desired_risk_usd = max(0.0, equity_usd * (risk_pct / 100.0))
    if sl_distance_px <= 0 or price <= 0:
        return 0.0
    desired_qty = desired_risk_usd / sl_distance_px
    max_qty_by_lev = (equity_usd * lev) / price
    qty = min(desired_qty, max_qty_by_lev)
    if qty * price < min_notional_usd:
        return 0.0
    return max(0.0, qty)


def _get_symbol_steps_from_worker(w: Worker, symbol: str) -> Tuple[float, float]:
    """Шаги цены/кол-ва из воркера, безопасно (с дефолтом)."""
    try:
        specs = getattr(w, "specs", {}) or {}
        spec = specs.get(symbol)
        if spec:
            price_step = float(getattr(spec, "price_step", getattr(spec, "price_tick", 0.1)))
            qty_step = float(getattr(spec, "qty_step", 0.001))
            return price_step, qty_step
    except Exception:
        pass
    # дефолты ок для фьючей BTC/ETH
    return 0.1, 0.001


def _quantize(val: float, step: float) -> float:
    if step <= 0:
        return val
    return (int(val / step)) * step


def _unrealized_for_position(pos: dict, last_price: float) -> float:
    """Нереализованный PnL для открытой позиции pos при цене last_price."""
    try:
        if not pos or pos.get("state") != "OPEN":
            return 0.0
        side = (pos.get("side") or "").upper()
        qty = float(pos.get("qty") or 0.0)
        entry = float(pos.get("entry_px") or 0.0)
        if qty <= 0 or entry <= 0:
            return 0.0
        diff = (last_price - entry) if side == "BUY" else (entry - last_price)
        return diff * qty
    except Exception:
        return 0.0


# ------------------------------------------------------------------------------
# Lifecycle
# ------------------------------------------------------------------------------

@app.on_event("startup")
async def _startup() -> None:
    """
    Старт приложения:
      1) Инициализируем SQLite.
      2) Перечитываем конфиг.
      3) Поднимаем единственный Worker и сохраняем в app.state.
    """
    log = logging.getLogger("uvicorn.error")

    # 1) Инициализация БД (без падения, если модуль/файл отсутствует)
    try:
        init_db()
        log.info("[startup] SQLite initialized successfully.")
    except ModuleNotFoundError:
        log.warning("[startup] storage.db not found — skipping DB init (will add later).")
    except Exception as e:
        log.error(f"[startup] DB init failed: {e}")
        raise

    # 2) Перезагрузка настроек
    from bot.core.config import reload_settings  # локальный импорт
    reload_settings()
    log.info("[startup] settings reloaded.")

    # 3) Worker один раз
    symbols = [sym.upper() for sym in _symbols_from_settings()]
    coalesce_ms = _coalesce_from_settings()

    if getattr(app.state, "worker", None) is not None:
        log.warning("[startup] worker already present in app.state — skip creating a new one.")
        return

    w = Worker(symbols=symbols, futures=True, coalesce_ms=coalesce_ms, history_maxlen=4000)
    await w.start()
    app.state.worker = w
    log.info(f"[startup] worker started: symbols={symbols}, coalesce_ms={coalesce_ms}")


@app.on_event("shutdown")
async def _shutdown() -> None:
    """Корректная остановка: пытаемся остановить единственный Worker."""
    log = logging.getLogger("uvicorn.error")
    w = getattr(app.state, "worker", None)
    if w is None:
        return
    try:
        stop = getattr(w, "stop", None)
        if callable(stop):
            await stop()
            log.info("[shutdown] worker stopped.")
    except Exception as e:
        log.warning(f"[shutdown] failed to stop worker cleanly: {e}")
    finally:
        app.state.worker = None


# ------------------------------------------------------------------------------
# Config endpoints
# ------------------------------------------------------------------------------

@app.get("/config/active")
def config_active() -> dict:
    """
    Активный конфиг в стабильном виде для фронта.
    Поддерживает Pydantic v1/v2, dataclass и dict.
    Ничего не падает, если части конфига отсутствуют.
    """

    s = get_settings()

    def _dump(obj: Any) -> Optional[Dict[str, Any]]:
        if obj is None:
            return None

        # Pydantic v2
        if hasattr(obj, "model_dump"):
            try:
                return obj.model_dump()
            except Exception:
                pass

        # Pydantic v1
        if hasattr(obj, "dict"):
            try:
                return obj.dict()
            except Exception:
                pass

        # Уже dict
        if isinstance(obj, dict):
            return obj

        # Dataclass / generic object
        if hasattr(obj, "__dict__"):
            try:
                return dict(obj.__dict__)
            except Exception:
                pass

        return None

    return {
        "source_path": getattr(s, "source_path", None),
        "source_mtime": getattr(s, "source_mtime", None),
        "risk": _dump(getattr(s, "risk", None)),
        "execution": _dump(getattr(s, "execution", None)),
        "strategy": _dump(getattr(s, "strategy", None)),
    }




@app.post("/control/reload-settings")
async def control_reload_settings() -> dict:
    from bot.core.config import reload_settings
    s = reload_settings()
    w: Optional[Worker] = app.state.worker
    if w:
        await w.stop()
        symbols = [sym.upper() for sym in _symbols_from_settings()]
        coalesce_ms = _coalesce_from_settings()
        w2 = Worker(symbols=symbols, futures=True, coalesce_ms=coalesce_ms, history_maxlen=4000)
        await w2.start()
        app.state.worker = w2
    return {
        "ok": True,
        "source_path": getattr(s, "source_path", None),
        "risk.min_risk_usd_floor": getattr(getattr(s, "risk", object()), "min_risk_usd_floor", None),
        "execution.min_stop_ticks": getattr(getattr(s, "execution", object()), "min_stop_ticks", None),
    }


@app.post("/control/reload-strategy")
def control_reload_strategy() -> dict:
    w = _worker_required()
    applied = w.reload_strategy_cfg()
    return applied if isinstance(applied, dict) and applied.get("ok") is not None else {"ok": True, **(applied or {})}


# ------------------------------------------------------------------------------
# Basic and status
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


@app.get("/status")
def status() -> Dict[str, Any]:
    """Конфиг + краткая телеметрия. Безопасно вызывать часто."""
    s = get_settings()
    w: Optional[Worker] = app.state.worker

    coalescer_stats: Optional[Dict[str, Any]] = None
    if w:
        try:
            d = w.diag()
            coal = d.get("coal")
            if isinstance(coal, dict) and coal:
                coalescer_stats = coal
        except Exception:
            coalescer_stats = None
        if coalescer_stats is None:
            try:
                st = w.coalescer.stats()  # type: ignore[attr-defined]
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
            "min_top5_liquidity_usd": getattr(getattr(s, "safety", object()), "min_top5_liquidity_usd", 300000.0),
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
            "limit_timeout_ms": getattr(getattr(s, "execution", object()), "limit_timeout_ms", 900),
            "max_slippage_bp": getattr(getattr(s, "execution", object()), "max_slippage_bp", 6.0),
            "time_in_force": getattr(getattr(s, "execution", object()), "time_in_force", "GTX"),
            "fee_bps_maker": getattr(getattr(s, "execution", object()), "fee_bps_maker", 2.0),
            "fee_bps_taker": getattr(getattr(s, "execution", object()), "fee_bps_taker", 4.0),
            "min_stop_ticks": getattr(getattr(s, "execution", object()), "min_stop_ticks", 6),
        },
        "ml": {
            "enabled": getattr(getattr(s, "ml", object()), "enabled", False),
            "p_threshold_mom": getattr(getattr(s, "ml", object()), "p_threshold_mom", 0.55),
            "p_threshold_rev": getattr(getattr(s, "ml", object()), "p_threshold_rev", 0.60),
            "model_path": getattr(getattr(s, "ml", object()), "model_path", "./ml_artifacts/model.bin"),
        },
        "account": {
            "starting_equity_usd": getattr(getattr(s, "account", object()), "starting_equity_usd", 1000.0),
            "leverage": getattr(getattr(s, "account", object()), "leverage", 15.0),
            "min_notional_usd": getattr(getattr(s, "account", object()), "min_notional_usd", 5.0),
        },
        "log_dir": getattr(s, "log_dir", "./logs"),
        "log_level": getattr(s, "log_level", "INFO"),
    }


# ------------------------------------------------------------------------------
# Diagnostics
# ------------------------------------------------------------------------------

@app.get("/debug/diag")
async def debug_diag() -> Dict[str, Any]:
    """Расширенная диагностика пайплайна (стабильная форма для фронта)."""
    w = _worker_required()
    d = w.diag()
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
        "risk_cfg": d.get("risk_cfg"),
        "safety_cfg": d.get("safety_cfg"),
        "consec_losses": d.get("consec_losses"),
        "positions": d.get("positions", {}),
        "trades": d.get("trades", {}),
        "pnl_day": d.get("pnl_day", {}),
        "block_reasons": d.get("block_reasons", {}),
        "protection_cfg": d.get("protection_cfg", {}),
        "auto_signal_enabled": d.get("auto_signal_enabled", False),
        "strategy_cfg": d.get("strategy_cfg", None),
    }


@app.get("/symbols")
async def symbols_summary() -> Dict[str, Any]:
    w = _worker_required()
    out: Dict[str, Tuple[float, float]] = {s: w.best_bid_ask(s) for s in w.symbols}
    return {"symbols": list(w.symbols), "best": out}


@app.get("/best")
async def best(
    symbol: str = Query("BTCUSDT", description="Символ, например BTCUSDT"),
) -> Dict[str, Any]:
    w = _worker_required()
    sym = _normalize_symbol(symbol, w.symbols)
    bid, ask = w.best_bid_ask(sym)
    return {"symbol": sym, "bid": bid, "ask": ask}

@app.get("/demo/digest")
async def demo_digest() -> Dict[str, Any]:
    w = _worker_required()
    d = w.diag() or {}
    best = d.get("best", {}) or {}
    pnl = d.get("pnl_day", {}) or {}
    positions = d.get("positions", {}) or {}
    reasons = (d.get("block_reasons", {}) or {})
    # топ-5 причин блокировок
    top_reasons = sorted(reasons.items(), key=lambda kv: kv[1], reverse=True)[:5]
    # компакт по позициям
    pos_compact = {
        sym: {
            "state": (p or {}).get("state"),
            "side": (p or {}).get("side"),
            "qty": (p or {}).get("qty"),
            "entry_px": (p or {}).get("entry_px"),
            "sl_px": (p or {}).get("sl_px"),
            "tp_px": (p or {}).get("tp_px"),
            "opened_ts_ms": (p or {}).get("opened_ts_ms"),
        }
        for sym, p in (positions or {}).items()
    }
    return {
        "ts_ms": int(time.time() * 1000),
        "symbols": d.get("symbols", []),
        "best": best,
        "pnl_day": {
            "day": pnl.get("day"),
            "trades": pnl.get("trades"),
            "winrate": pnl.get("winrate"),
            "avg_r": pnl.get("avg_r"),
            "pnl_r": pnl.get("pnl_r"),
            "pnl_usd": pnl.get("pnl_usd"),
            "max_dd_r": pnl.get("max_dd_r"),
        },
        "positions": pos_compact,
        "exec_counters": d.get("exec_counters", {}),
        "auto": {
            "enabled": bool(d.get("auto_signal_enabled", False)),
            "cfg": d.get("strategy_cfg", {}),
        },
        "reasons_top": top_reasons,
    }


# ------------------------------------------------------------------------------
# Ticks (latest / peek)
# ------------------------------------------------------------------------------

def _worker_latest_tick(w: Worker, symbol: str) -> Optional[Dict[str, Any]]:
    """Последний тик по символу. Если истории нет — снимок из best bid/ask."""
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
async def ticks_latest(
    symbol: str = Query("BTCUSDT", description="Напр. BTCUSDT"),
) -> Dict[str, Any]:
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
# Control (paper executor)
# ------------------------------------------------------------------------------

class TestEntryReq(BaseModel):
    symbol: str = Field(..., examples=["BTCUSDT"])
    side: Literal["BUY", "SELL", "long", "short"]
    qty: Optional[float] = Field(None, description="Если не задано — авто-сайзинг по риску")
    limit_offset_ticks: Optional[int] = Field(None, description="Переопределить оффсет лимита (для превью)")
    dry: bool = Field(False, description="Если True — только расчёт плана без исполнения")

def _normalize_side(s: str) -> str:
    s = s.upper()
    if s == "LONG":
        return "BUY"
    if s == "SHORT":
        return "SELL"
    if s in ("BUY", "SELL"):
        return s
    raise HTTPException(status_code=400, detail="side must be BUY/SELL or long/short")

def _preview_limit_px(bid: float, ask: float, side: str, price_step: float, offset_ticks: int) -> float:
    """
    Консервативный превью-прайс под post-only/GTX:
      BUY  → ask - offset*tick;  SELL → bid + offset*tick; фолбэк — mid.
    """
    try:
        off = max(0, int(offset_ticks)) * float(max(price_step, 0.0))
    except Exception:
        off = 0.0
    px = 0.0
    if side == "BUY" and ask > 0:
        px = ask - off if off > 0 else ask
    elif side == "SELL" and bid > 0:
        px = bid + off if off > 0 else bid
    if px <= 0.0 and bid > 0 and ask > 0:
        px = (bid + ask) / 2.0
    return float(px or 0.0)

def _spread_ticks(bid: float, ask: float, tick: float) -> float:
    if bid > 0 and ask > 0 and tick > 0:
        return max(0.0, (ask - bid) / tick)
    return 0.0


@app.post("/control/test-entry")
async def control_test_entry(req: TestEntryReq) -> Dict[str, Any]:
    """
    Тестовый вход (улучшенный):
      - рассчитываем SL-дистанцию от min_stop_ticks и текущего спреда (как в Worker),
      - авто-сайзим через внутренний метод воркера (полная консистентность),
      - считаем комиссии: (A) conservative maker+taker, (B) maker-only (GTX+abort),
      - НЕ блокируем по комиссиям — только предупреждаем,
      - добавлен DRY-режим (dry=true): превью лимит-цены, SL/TP-план и safety-чек без сделки,
      - при исполнении используем тот же путь, что и раньше (Worker.place_entry / place_entry_auto).
    """
    # локальные импорты, чтобы не трогать верх файла
    from risk.filters import MicroCtx, TimeCtx, PositionalCtx, check_entry_safety
    from exec.sltp import compute_sltp

    w = _worker_required()
    sym = _normalize_symbol(req.symbol, w.symbols)
    side = _normalize_side(req.side)

    # 1) текущая цена/тик
    last = _worker_latest_tick(w, sym)
    if not last:
        return JSONResponse({"ok": False, "error": f"no ticks for {sym}"}, status_code=503)
    price = float(last["price"])
    bid = float(last.get("bid") or price)
    ask = float(last.get("ask") or price)

    # 2) шаги символа
    price_step, qty_step = _get_symbol_steps_from_worker(w, sym)

    # 3) конфиги (fees берём из execution)
    s = get_settings()
    exec_cfg = _exec_cfg()
    time_in_force = str(exec_cfg.get("time_in_force", "GTX")).upper()
    min_stop_ticks = int(exec_cfg.get("min_stop_ticks", 12))
    limit_offset_ticks = int(req.limit_offset_ticks) if req.limit_offset_ticks is not None else int(exec_cfg.get("limit_offset_ticks", 1))

    e = getattr(s, "execution", object())
    fee_maker_bps = float(getattr(e, "fee_bps_maker", 2.0))
    fee_taker_bps = float(getattr(e, "fee_bps_taker", 4.0))
    on_timeout = str(getattr(e, "on_timeout", "abort")).lower()  # "abort" | "market" | ...

    acc = _account_cfg()
    rsk = _risk_cfg()

    # 4) SL-дистанция: минимум из min_stop_ticks и текущего спреда в тиках (как в Worker)
    spr_ticks = _spread_ticks(bid, ask, price_step)
    sl_distance_px = max(min_stop_ticks * price_step, max(spr_ticks, 1.0) * price_step)

    # 5) qty: ручной или авто-сайзинг через воркер (консистентно с place_entry_auto)
    if req.qty is not None and req.qty > 0:
        qty = float(req.qty)
    else:
        qty = float(w._compute_auto_qty(sym, side, sl_distance_px))  # тот же алгоритм, что у Worker.place_entry_auto

    # 6) квантизация + exchange минимумы
    qty = float(_quantize(float(qty or 0.0), qty_step))
    if qty <= 0.0:
        return {
            "ok": False,
            "reason": "qty_rounded_to_zero",
            "hint": f"try qty ≥ {max(qty_step, 2*qty_step)} for {sym}",
            "symbol": sym,
            "side": side,
        }
    if qty * price < acc["min_notional"]:
        min_qty = _quantize((acc["min_notional"] / price) + qty_step, qty_step)
        return {
            "ok": False,
            "reason": "below_min_notional",
            "symbol": sym,
            "side": side,
            "qty": qty,
            "hint": f"min notional ${acc['min_notional']} → try qty ≥ {min_qty}",
        }

    # 7) превью лимит-цены (для пост-онли) + SL/TP-план и safety-чек (read-only)
    preview_px = _preview_limit_px(bid, ask, side, price_step, limit_offset_ticks)
    if preview_px <= 0.0:
        preview_px = (bid + ask) / 2.0 if (bid > 0 and ask > 0) else price

    # SLTP «на бумаге» (для превью)
    try:
        plan = compute_sltp(
            side=side,
            entry_px=float(preview_px),
            qty=qty,
            price_tick=price_step,
            sl_distance_px=sl_distance_px,
            rr=1.8,
        )
        sl_px = float(plan.sl_px) if getattr(plan, "sl_px", None) is not None else None
        tp_px = float(plan.tp_px) if getattr(plan, "tp_px", None) is not None else None
    except Exception:
        sl_px, tp_px = None, None

    # safety-чек до/после плана (не меняет состояние)
    micro = MicroCtx(spread_ticks=spr_ticks, top5_liq_usd=1e12)
    tctx = TimeCtx(ts_ms=w._now_ms(), server_time_offset_ms=getattr(w, "_server_time_offset_ms", 0))
    pre_ok = check_entry_safety(side, micro, tctx, pos_ctx=None, safety=w._safety_cfg)
    post_ok = pre_ok
    if sl_px is not None:
        post_ok = check_entry_safety(
            side,
            micro=micro,
            time_ctx=tctx,
            pos_ctx=PositionalCtx(entry_px=float(preview_px), sl_px=float(sl_px), leverage=float(w._leverage)),
            safety=w._safety_cfg,
        )

    # 8) оценка комиссий и риска
    notional = price * qty  # как раньше: считаем от текущей цены
    expected_risk_usd = sl_distance_px * qty  # линейные USDT-перпы: ΔPnL ≈ qty * Δprice

    taker_like_entry = (time_in_force != "GTX")
    est_fees_usd_conservative = _estimate_fees_usd(
        notional_entry=notional,
        notional_exit=notional,
        maker_bps=fee_maker_bps,
        taker_bps=fee_taker_bps,
        taker_like_entry=taker_like_entry,
    )
    est_fees_usd_maker_only = (notional * (fee_maker_bps / 10_000.0)) * 2.0

    warnings: List[str] = []
    if expected_risk_usd < rsk["min_risk_floor"]:
        warnings.append(
            f"expected_risk_usd({expected_risk_usd:.4f}) < min_risk_floor({rsk['min_risk_floor']:.4f})"
        )
    if est_fees_usd_maker_only > 0 and expected_risk_usd < (0.8 * est_fees_usd_maker_only):
        warnings.append(
            f"risk({expected_risk_usd:.4f}) < 0.8×maker_only_fees({est_fees_usd_maker_only:.4f})"
        )
    if est_fees_usd_conservative > 0 and expected_risk_usd < (0.5 * est_fees_usd_conservative):
        warnings.append(
            f"risk({expected_risk_usd:.4f}) < 0.5×conservative_fees({est_fees_usd_conservative:.4f})"
        )

    # 9) DRY-режим: только превью, без сделки
    if req.dry:
        return {
            "ok": True,
            "dry": True,
            "symbol": sym,
            "side": side,
            "qty": float(qty),
            "price": price,  # оставляем как раньше — текущая цена, а не превью
            "preview_px": float(preview_px),
            "sl_distance_px": round(float(sl_distance_px), 6),
            "expected_risk_usd": round(float(expected_risk_usd), 6),
            "fees_usd": {
                "maker_only": round(float(est_fees_usd_maker_only), 6),
                "conservative_maker_plus_taker": round(float(est_fees_usd_conservative), 6),
                "entry_taker_like": taker_like_entry,
            },
            "tif": time_in_force,
            "on_timeout": on_timeout,
            "warnings": warnings or None,
            "safety": {
                "pre": {"allow": bool(pre_ok.allow), "reasons": pre_ok.reasons},
                "post": {"allow": bool(post_ok.allow), "reasons": post_ok.reasons},
            },
            "plan": {"sl_px": sl_px, "tp_px": tp_px, "rr": 1.8},
        }

    # 10) Исполнение (совместимо с прежним поведением)
    try:
        # попытка передать offset, если у Executor поддержка появится — прозрачно заработает
        report = await w.place_entry(sym, side, float(qty), int(limit_offset_ticks))  # type: ignore[arg-type]
    except TypeError:
        # текущая версия — без offset
        if req.qty is not None and req.qty > 0:
            report = await w.place_entry(sym, side, float(qty))
        else:
            report = await w.place_entry_auto(sym, side)

    steps = (report or {}).get("steps") or []
    if "qty_rounded_to_zero" in steps:
        return {
            "ok": False,
            "reason": "qty_rounded_to_zero_by_executor",
            "symbol": sym,
            "side": side,
            "hint": f"try qty ≥ {max(qty_step, 2*qty_step)} for {sym}",
            "report": report,
        }

    # 11) финальный ответ (назад-совместимая форма)
    out = {
        "ok": True,
        "symbol": sym,
        "side": side,
        "qty": float(qty),
        "price": price,  # как раньше — текущая цена из тика
        "sl_distance_px": round(sl_distance_px, 6),
        "expected_risk_usd": round(expected_risk_usd, 6),
        "fees_usd": {
            "maker_only": round(est_fees_usd_maker_only, 6),
            "conservative_maker_plus_taker": round(est_fees_usd_conservative, 6),
            "entry_taker_like": taker_like_entry
        },
        "tif": time_in_force,
        "on_timeout": on_timeout,
        "warnings": warnings or None,
        "report": report,  # внутри структура воркера как была
    }
    if isinstance(report, dict) and report.get("ok") is False:
        out["ok"] = False
        if report.get("reason"):
            out["reason"] = report.get("reason")
    return out


class FlattenReq(BaseModel):
    symbol: Optional[str] = None


@app.post("/control/flatten")
async def control_flatten_post(payload: FlattenReq) -> Dict[str, Any]:
    w = _worker_required()
    sym = _normalize_symbol((payload.symbol or "BTCUSDT").upper(), w.symbols)
    return await w.flatten(sym)


@app.get("/control/flatten")
async def control_flatten_get(symbol: str = Query("BTCUSDT")) -> Dict[str, Any]:
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
    return {"ok": all(v.get("ok", False) for v in results.values()), "results": results}


@app.post("/control/set-timeout")
async def control_set_timeout_post(
    symbol: str = Query("BTCUSDT"),
    timeout_ms: int = Query(20_000, ge=5_000, le=600_000),
) -> Dict[str, Any]:
    w = _worker_required()
    sym = _normalize_symbol(symbol, w.symbols)
    return w.set_timeout_ms(sym, timeout_ms)


@app.get("/control/set-timeout")
async def control_set_timeout_get(
    symbol: str = Query("BTCUSDT"),
    timeout_ms: int = Query(20000, ge=5000, le=600000),
) -> Dict[str, Any]:
    w = _worker_required()
    sym = _normalize_symbol(symbol, w.symbols)
    return w.set_timeout_ms(sym, timeout_ms)



# ------------------------------------------------------------------------------
# Auto-trading toggle (MVP)
# ------------------------------------------------------------------------------

class AutoReq(BaseModel):
    enabled: bool


@app.get("/control/auto")
def control_auto_get() -> Dict[str, Any]:
    w = _worker_required()
    d = w.diag() or {}
    return {"ok": True, "auto_signal_enabled": bool(d.get("auto_signal_enabled", False))}


@app.post("/control/auto")
def control_auto_set(req: AutoReq) -> Dict[str, Any]:
    """
    Надёжно включает/выключает авто-сигналы, не ломая текущие cooldown/min_flat:
    используем Worker.set_strategy(enabled, cooldown_ms, min_flat_ms) если он есть.
    """
    w = _worker_required()
    d = w.diag() or {}
    cfg = (d.get("strategy_cfg") or {}) if isinstance(d, dict) else {}
    cooldown = int(cfg.get("cooldown_ms", 500))
    min_flat = int(cfg.get("min_flat_ms", 200))

    set_strategy = getattr(w, "set_strategy", None)
    if callable(set_strategy):
        res = set_strategy(enabled=bool(req.enabled), cooldown_ms=cooldown, min_flat_ms=min_flat)
        if isinstance(res, dict):
            return {"ok": True, **res}

    # фолбэк — выставим очевидное поле; diag всё равно прочитает актуальный флаг
    try:
        setattr(w, "_auto_enabled", bool(req.enabled))
    except Exception:
        pass
    nd = w.diag() or {}
    return {"ok": True, "auto_signal_enabled": bool(nd.get("auto_signal_enabled", req.enabled))}


# ------------------------------------------------------------------------------
# Positions / metrics / reasons
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
            "state": "FLAT",
            "side": None,
            "qty": 0.0,
            "entry_px": 0.0,
            "sl_px": None,
            "tp_px": None,
            "opened_ts_ms": 0,
            "timeout_ms": 180_000,
        }
    return {"symbol": sym, "position": pos}


@app.get("/metrics")
async def metrics() -> Dict[str, Any]:
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


@app.get("/debug/reasons")
async def debug_reasons() -> Dict[str, Any]:
    w = _worker_required()
    d = w.diag()
    reasons = d.get("block_reasons", {}) or d.get("reasons", {}) or {}
    return {"reasons": reasons}


@app.get("/debug/db-sanity")
def db_sanity():
    eng = get_engine()
    with eng.begin() as conn:
        rows = conn.execute(text("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")).fetchall()
    return {"tables": [r[0] for r in rows]}


# ------------------------------------------------------------------------------
# Trades / PnL
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


@app.get("/trades/last")
def trades_last(per_symbol: int = Query(1, ge=1, le=10)) -> Dict[str, Any]:
    """По одному (или N) последнему трейду на символ для быстрого дайджеста."""
    w = _worker_required()
    d = w.diag() or {}
    store = d.get("trades")
    out: Dict[str, List[Dict[str, Any]]] = {}

    def _sort_and_cut(arr: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        arr = sorted(arr, key=lambda x: x.get("closed_ts", x.get("opened_ts", 0)), reverse=True)
        return arr[:per_symbol]

    if isinstance(store, dict):
        for sym, arr in store.items():
            out[sym] = _sort_and_cut(list(arr))
    elif isinstance(store, list):
        by_sym: Dict[str, List[Dict[str, Any]]] = {}
        for t in store:
            by_sym.setdefault(str(t.get("symbol", "UNKNOWN")), []).append(t)
        for sym, arr in by_sym.items():
            out[sym] = _sort_and_cut(arr)

    return {"items": out}


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


@app.get("/pnl/now")
async def pnl_now():
    """
    Надёжный PnL-now:
    - не падает, если в diag() чего-то нет;
    - day заполняется текущей датой;
    - unrealized берётся из diag().unrealized, а если его нет — высчитывается из positions;
    - per_symbol = None, если пусто (чтобы фронт не рисовал пустой список);
    """
    w = _worker_required()
    try:
        d = w.diag() or {}
    except Exception:
        d = {}

    # ---- pnl_day (сейфовые дефолты) ----
    pnl_day = d.get("pnl_day") or {}
    try:
        day_str = pnl_day.get("day") or datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d")
    except Exception:
        day_str = datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d")

    def _f(v, dv=0.0):
        try:
            return float(v)
        except Exception:
            return float(dv)

    def _i(v, dv=0):
        try:
            return int(v)
        except Exception:
            return int(dv)

    out_day = {
        "day": day_str,
        "trades": _i(pnl_day.get("trades")),
        "winrate": pnl_day.get("winrate", None),
        "avg_r": pnl_day.get("avg_r", None),
        "pnl_r": _f(pnl_day.get("pnl_r")),
        "pnl_usd": _f(pnl_day.get("pnl_usd")),
        "max_dd_r": pnl_day.get("max_dd_r", None),
    }

    # ---- unrealized ----
    unrl = d.get("unrealized") or {}
    total_usd = _f(unrl.get("total_usd"))
    per_symbol = unrl.get("per_symbol")
    open_positions = _i(unrl.get("open_positions"))

    # если блочка unrealized нет — попробуем собрать из positions
    if not unrl:
        total_usd = 0.0
        per_symbol = {}
        open_positions = 0
        pos = d.get("positions") or {}
        if isinstance(pos, dict):
            for sym, p in pos.items():
                p = p or {}
                if p.get("state") == "OPEN":
                    open_positions += 1
                    # поддержка полей: unrealized_usd | upnl | pnl_unrealized
                    u = p.get("unrealized_usd", p.get("upnl", p.get("pnl_unrealized", 0.0)))
                    u = _f(u)
                    per_symbol[sym] = u
                    total_usd += u

        if not per_symbol:
            per_symbol = None  # чтобы фронт не рисовал пустой объект

    resp = {
        "pnl_day": out_day,
        "unrealized": {
            "total_usd": round(total_usd, 6),
            "per_symbol": per_symbol,
        },
        "open_positions": open_positions,
    }
    return JSONResponse(resp)

# ------------------------------------------------------------------------------
# SSE stream (compact snapshot each second)
# ------------------------------------------------------------------------------

@app.get("/stream/sse")
async def stream_sse():
    """
    Server-Sent Events: 1 раз/сек отдаём компактный снапшот:
    pnl_day, positions, best, по одному последнему трейду на символ.
    Добавлены: graceful shutdown (CancelledError), heartbeat, retry hint,
    заголовки против буферизации (nginx/proxies).
    """
    w = _worker_required()

    async def eventgen():
        # Подскажем клиенту интервал авто-реconnect
        yield "retry: 2000\n\n"
        try:
            while True:
                try:
                    d = w.diag() or {}
                    payload: Dict[str, Any] = {
                        "ts_ms": _now_ms(),
                        "pnl_day": d.get("pnl_day", {}) or {},
                        "positions": d.get("positions", {}) or {},
                        "best": d.get("best", {}) or {},
                        "trades_last": {},
                    }

                    store = d.get("trades")
                    if isinstance(store, dict):
                        for sym, arr in store.items():
                            if arr:
                                last = max(arr, key=lambda x: x.get("closed_ts", x.get("opened_ts", 0)))
                                payload["trades_last"][sym] = {
                                    k: last.get(k)
                                    for k in (
                                        "symbol", "side", "qty",
                                        "entry_px", "exit_px",
                                        "pnl_usd", "pnl_r", "reason",
                                        "opened_ts", "closed_ts",
                                    )
                                }

                    data = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
                    yield f"event: tick\ndata: {data}\n\n"

                except Exception as e:
                    # единичная ошибка в сборке снапшота — не рвём поток
                    err = json.dumps({"error": str(e)}, separators=(",", ":"), ensure_ascii=False)
                    yield f"event: error\ndata: {err}\n\n"

                # heartbeat против idle-таймаутов прокси
                yield ":keep-alive\n\n"
                await asyncio.sleep(1.0)

        except asyncio.CancelledError:
            # клиент отключился или сервер гасится — выходим тихо
            return

    return StreamingResponse(
        eventgen(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",  # для nginx
        },
    )
