# bot/api/app.py
from __future__ import annotations

import os
import time
from typing import Any, Dict, Iterable, List, Literal, Optional, Tuple

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from bot.core.config import get_settings
from bot.worker import Worker

APP_VERSION = "0.8.0"


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


def _price_qty_steps(w: Worker, symbol: str) -> Tuple[float, float]:
    """Достаём шаги цены/кол-ва из воркера, безопасно (с дефолтом)."""
    try:
        specs = getattr(w, "specs", {}) or {}
        spec = specs.get(symbol)
        if spec:
            price_step = float(getattr(spec, "price_step", getattr(spec, "price_tick", 0.1)))
            qty_step = float(getattr(spec, "qty_step", 0.0001))
            return price_step, qty_step
    except Exception:
        pass
    return 0.1, 0.0001  # дефолтные шаги (ОК для MVP)



# ------------------------------------------------------------------------------
# Lifecycle
# ------------------------------------------------------------------------------

@app.on_event("startup")
async def _startup() -> None:
    from bot.core.config import reload_settings
    reload_settings()
    symbols = [sym.upper() for sym in _symbols_from_settings()]
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
# Config endpoints
# ------------------------------------------------------------------------------

@app.get("/config/active")
def config_active() -> dict:
    from bot.core.config import get_settings
    s = get_settings()
    # аккуратно собираем strategy.* если модель его знает
    strat = getattr(s, "strategy", None)
    def _asdict(obj):
        try:
            return obj.dict() if hasattr(obj, "dict") else dict(obj)
        except Exception:
            return None
    return {
        "source_path": s.source_path,
        "source_mtime": s.source_mtime,
        "risk": s.risk.dict(),
        "execution": s.execution.dict(),
        "strategy": _asdict(strat),  # может быть None, ок
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
    # воркер сам перечитает settings.json (raw) и вернёт актуальные флаги
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
    """
    Конфиг + краткая телеметрия. Безопасно вызывать часто.
    """
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
    """
    Расширенная диагностика пайплайна (стабильная форма для фронта).
    """
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

        # 👉 добавили:
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
    qty: Optional[float] = Field(None, description="Если не задано — посчитаем из риска")
    limit_offset_ticks: Optional[int] = Field(None, description="Переопределить оффсет лимита")

def _normalize_side(s: str) -> str:
    s = s.upper()
    if s == "LONG":
        return "BUY"
    if s == "SHORT":
        return "SELL"
    if s in ("BUY", "SELL"):
        return s
    raise HTTPException(status_code=400, detail="side must be BUY/SELL or long/short")

def _get_symbol_steps(w: Worker, symbol: str) -> tuple[float, float]:
    # price_step, qty_step
    try:
        spec = (getattr(w, "specs", {}) or {}).get(symbol)
        if spec:
            return float(getattr(spec, "price_step", getattr(spec, "price_tick", 0.1))), float(getattr(spec, "qty_step", 0.001))
    except Exception:
        pass
    # дефолты: ок для BTCUSDT фьючей
    return 0.1, 0.001

def _quantize(val: float, step: float) -> float:
    if step <= 0:
        return val
    return (int(val / step)) * step

@app.post("/control/test-entry")
async def control_test_entry(req: TestEntryReq) -> Dict[str, Any]:
    w = _worker_required()
    sym = _normalize_symbol(req.symbol, w.symbols)
    side = _normalize_side(req.side)

    # 1) текущая цена
    last = _worker_latest_tick(w, sym)
    if not last:
        return JSONResponse({"ok": False, "error": f"no ticks for {sym}"}, status_code=503)
    price = float(last["price"])

    # 2) шаги символа
    price_step, qty_step = _get_symbol_steps(w, sym)

    # 3) SL-дистанция (минимум из конфигурации и не меньше текущего спреда)
    exec_cfg = _exec_cfg()
    sl_ticks = max(int(exec_cfg["min_stop_ticks"]), 4)
    sl_distance_px = max(price_step * sl_ticks, abs(float(last.get("ask", price)) - float(last.get("bid", price))))

    # 4) auto-qty при необходимости
    acc = _account_cfg()
    rsk = _risk_cfg()
    qty = req.qty
    if qty is None:
        qty = _auto_size_qty(
            price=price,
            sl_distance_px=sl_distance_px,
            equity_usd=acc["equity"],
            risk_pct=rsk["risk_pct"],
            lev=acc["lev"],
            min_notional_usd=acc["min_notional"],
        )

    # 5) квантизация по шагу количества + минимальный notional
    if not qty or qty <= 0:
        return {
            "ok": False,
            "reason": "size_below_exchange_min_notional_or_zero",
            "hint": f"use qty >= {qty_step} or increase risk",
            "symbol": sym,
            "side": side,
        }
    qty = _quantize(float(qty), qty_step)
    if qty < qty_step:
        return {
            "ok": False,
            "reason": "qty_below_qty_step",
            "hint": f"min qty step is {qty_step}",
            "symbol": sym,
            "side": side,
        }
    if qty * price < acc["min_notional"]:
        min_qty = (acc["min_notional"] / price)
        min_qty = _quantize(min_qty + qty_step, qty_step)  # чуть выше минимума
        return {
            "ok": False,
            "reason": "below_min_notional",
            "symbol": sym,
            "side": side,
            "qty": qty,
            "hint": f"min notional ${acc['min_notional']} → try qty ≥ {min_qty}",
        }

    # 6) комиссии vs риск — предупреждение, но НЕ блокируем
    taker_like_entry = False if str(exec_cfg["time_in_force"]).upper() == "GTX" else True
    expected_risk_usd = sl_distance_px * qty
    est_fees = _estimate_fees_usd(
        notional_entry=price * qty,
        notional_exit=price * qty,
        maker_bps=acc["fee_maker_bps"],
        taker_bps=acc["fee_taker_bps"],
        taker_like_entry=taker_like_entry,
    )
    warnings: list[str] = []
    if expected_risk_usd < rsk["min_risk_floor"]:
        warnings.append(f"expected_risk_usd({expected_risk_usd:.4f}) < min_risk_floor({rsk['min_risk_floor']:.4f})")
    if est_fees >= expected_risk_usd:
        warnings.append(f"fees({est_fees:.4f}) >= expected_risk({expected_risk_usd:.4f})")

    # 7) offset
    limit_offset_ticks = int(req.limit_offset_ticks) if req.limit_offset_ticks is not None else int(exec_cfg["limit_offset_ticks"])

    # 8) вход
    try:
        report = await w.place_entry(sym, side, float(qty), int(limit_offset_ticks))  # новый вариант
    except TypeError:
        report = await w.place_entry(sym, side, float(qty))  # совместимость

    # 9) если воркер всё равно округлил qty в 0
    steps = (report or {}).get("steps") or []
    if "qty_rounded_to_zero" in steps:
        return {
            "ok": False,
            "reason": "qty_rounded_to_zero",
            "hint": f"try qty ≥ {max(qty_step, 2*qty_step)} for {sym}",
            "report": report,
        }

    return {
        "ok": True,
        "symbol": sym,
        "side": side,
        "qty": float(qty),
        "price": price,
        "expected_risk_usd": round(expected_risk_usd, 6),
        "est_fees_usd": round(est_fees, 6),
        "warnings": warnings or None,
        "report": report,
    }


    # оффсет лимита
    limit_offset_ticks = req.limit_offset_ticks if req.limit_offset_ticks is not None else _exec_cfg()["limit_offset_ticks"]

    # Пытаемся выполнить вход
    try:
        report = await w.place_entry(sym, side, float(qty), int(limit_offset_ticks))  # type: ignore[arg-type]
    except TypeError:
        # совместимость: старый воркер мог быть place_entry(sym, side, qty)
        report = await w.place_entry(sym, side, float(qty))  # type: ignore[misc]

    # Проверка распространённой причины из твоего лога: "qty_rounded_to_zero"
    steps = (report or {}).get("steps") or []
    if "qty_rounded_to_zero" in steps:
        return {
            "ok": False,
            "reason": "qty_rounded_to_zero",
            "hint": "increase qty or reduce rounding: check qty_step/specs for symbol",
            "report": report,
        }

    return {"ok": True, "symbol": sym, "side": side, "qty": float(qty), "report": report}


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
async def control_set_timeout(
    symbol: str = Query("BTCUSDT"),
    timeout_ms: int = Query(20_000, ge=5_000, le=600_000),
) -> Dict[str, Any]:
    w = _worker_required()
    sym = _normalize_symbol(symbol, w.symbols)
    return w.set_timeout_ms(sym, timeout_ms)


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
