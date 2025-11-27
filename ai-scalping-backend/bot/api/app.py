# bot/api/app.py
from __future__ import annotations

import os
import time
import json
import asyncio
import logging
from typing import Any, Dict, Iterable, List, Literal, Optional, Tuple
from bot.core.config import reload_settings
from strategy.cross_guard import CrossCfg, decide_cross_guard, pick_best_symbol_by_speed


from fastapi import FastAPI, HTTPException, Query, Body, Depends, Header, Request
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

def _apply_execution_settings_to_worker(w, s):
    """
    Единая точка применения runtime-конфига:
    просто дергаем Worker.reload_runtime_cfg(), который сам:
      - перечитывает execution/safety/risk/account из settings,
      - обновляет _exec_cfg,
      - разносит политики по всем Executor'ам в w.execs.
    """
    if not w:
        return {}

    try:
        snap = w.reload_runtime_cfg()
    except Exception as e:
        logging.getLogger("uvicorn.error").warning(
            f"[apply-exec] reload_runtime_cfg failed: {e}"
        )
        return {}

    # для /status и отладки вернём только exec_cfg из diag()
    diag = {}
    if isinstance(snap, dict):
        diag = snap.get("diag") or snap  # reload_runtime_cfg уже может вернуть diag()
    exec_cfg = {}
    if isinstance(diag, dict):
        exec_cfg = diag.get("exec_cfg", {})

    return {"execution": {"exec_cfg": exec_cfg}}

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
        return ["GALAUSDT"]  # TEMP: single-symbol mode GALAUSDT, 10x leverage


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

# --- cross-guard helpers ------------------------------------------------------
def _cross_guard_enabled_from_settings() -> bool:
    try:
        s = get_settings()
        cg = getattr(getattr(s, "strategy", object()), "cross_guard", object())
        return bool(getattr(cg, "enabled", True))
    except Exception:
        return True

def _build_cross_cfg():
    """
    Безопасно собирает CrossCfg из settings, подстраиваясь под фактическую
    сигнатуру конструктора (разные имена полей в разных версиях модуля).
    Ничего не падает: при любой ошибке вернёт минимально валидную конфигурацию.
    """
    try:
        from strategy.cross_guard import CrossCfg  # твой класс
        import inspect

        s = get_settings()
        cg = getattr(getattr(s, "strategy", object()), "cross_guard", object())

        lookback = int(getattr(cg, "lookback_s", 60))

        # то, как поля могут называться в разных ревизиях:
        alt_names = {
            "align": ["align_tolerance_bp", "align_tol_bp", "align_bp_tolerance", "align_bp_tol"],
            "dom":   ["dominance_min_bp", "dom_min_bp", "dominance_bp_min"],
            "single": ["single_candidate", "one_candidate", "single_only"],
        }

        # значения по умолчанию (если их нет в конфиге)
        align_val = float(
            getattr(cg, "align_tolerance_bp",
                getattr(cg, "align_tol_bp", 2.5))
        )
        dom_val = float(
            getattr(cg, "dominance_min_bp",
                getattr(cg, "dom_min_bp", 4.0))
        )
        single_val = bool(
            getattr(cg, "single_candidate",
                getattr(cg, "one_candidate", True))
        )

        sig = inspect.signature(CrossCfg)
        params = {}

        # обязательное окно
        if "lookback_s" in sig.parameters:
            params["lookback_s"] = lookback

        # подсовываем значение в первое подходящее имя параметра
        def _maybe_apply(kind: str, value):
            for name in alt_names[kind]:
                if name in sig.parameters:
                    params[name] = value
                    return

        _maybe_apply("align", align_val)
        _maybe_apply("dom", dom_val)
        _maybe_apply("single", single_val)

        # финально
        return CrossCfg(**params)

    except Exception:
        # запасной минимальный вариант — только lookback, без тонких правил
        class _Mini:
            def __init__(self, lookback_s: int = 60, **_):
                self.lookback_s = lookback_s
        return _Mini(lookback_s=60)


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
    # STEP 1: Configure global logging to ensure all loggers are visible
    import sys
    root_logger = logging.getLogger()
    
    # Get log level from settings or environment
    try:
        s = get_settings()
        log_level_str = getattr(s, "log_level", os.getenv("LOG_LEVEL", "INFO")).upper()
    except Exception:
        log_level_str = os.getenv("LOG_LEVEL", "INFO").upper()
    log_level = getattr(logging, log_level_str, logging.INFO)
    
    # Configure root logger with console handler if not already configured
    if not root_logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(log_level)
        formatter = logging.Formatter(
            fmt='%(asctime)s [%(levelname)s] [%(name)s] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        handler.setFormatter(formatter)
        root_logger.addHandler(handler)
        root_logger.setLevel(log_level)
    
    # Ensure all relevant loggers propagate to root
    for logger_name in [
        "bot.worker",
        "bot.worker._strategy",
        "adapters.binance_ws",
        "adapters.binance_rest",
        "stream.coalescer",
        "strategy",
        "exec",
        "risk",
    ]:
        module_logger = logging.getLogger(logger_name)
        module_logger.setLevel(log_level)
        module_logger.propagate = True
    
    log = logging.getLogger("uvicorn.error")
    log.info(f"[startup] logging configured, level={log_level_str} (root level: {log_level})")

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
    try:
        s = get_settings()
        _apply_execution_settings_to_worker(w, s)
    except Exception as e:
        logging.getLogger("uvicorn.error").warning(f"[startup] apply exec settings failed: {e}")
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
        "account": _dump(getattr(s, "account", None)),
        "fees": _dump(getattr(s, "fees", None)),  
    }

@app.post("/control/reload-settings")
async def control_reload_settings() -> dict:
    s = reload_settings()
    w: Optional[Worker] = app.state.worker
    if w:
        await w.stop()
        symbols = [sym.upper() for sym in _symbols_from_settings()]
        coalesce_ms = _coalesce_from_settings()
        w2 = Worker(symbols=symbols, futures=True, coalesce_ms=coalesce_ms, history_maxlen=4000)
        await w2.start()
        try:
            _apply_execution_settings_to_worker(w2, s)
        except Exception as e:
            logging.getLogger("uvicorn.error").warning(f"[reload-settings] apply exec settings failed: {e}")
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

@app.post("/control/apply-exec")
def control_apply_exec() -> dict:
    """
    Применить execution-политику из текущих settings к действующему воркеру/исполнителю.
    Удобно, если правили settings.json и не хотим рестартовать процесс.
    """
    w = _worker_required()
    s = get_settings()
    applied = _apply_execution_settings_to_worker(w, s) or {}
    return {"ok": True, **applied}


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

    # --- coalescer stats ---
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

    # --- execution policy из воркера + из settings как дефолты ---
    exec_worker: Optional[Dict[str, Any]] = None
    if w:
        try:
            d_diag = w.diag() or {}
            # приоритет: то, что вернул diag(), потом — приватное поле
            exec_worker = dict(d_diag.get("exec_cfg", {}) or getattr(w, "_exec_cfg", {}) or {})
        except Exception:
            exec_worker = None

    exec_out = {
        "execution_worker": exec_worker,
        "limit_offset_ticks": getattr(getattr(s, "execution", object()), "limit_offset_ticks", 0),
        "limit_timeout_ms": getattr(getattr(s, "execution", object()), "limit_timeout_ms", 900),
        "max_slippage_bp": getattr(getattr(s, "execution", object()), "max_slippage_bp", 4.0),
        "time_in_force": getattr(getattr(s, "execution", object()), "time_in_force", "GTX"),
        "fee_bps_maker": getattr(getattr(s, "execution", object()), "fee_bps_maker", 2.0),
        "fee_bps_taker": getattr(getattr(s, "execution", object()), "fee_bps_taker", 4.0),
        "post_only": getattr(getattr(s, "execution", object()), "post_only", True),
        "on_timeout": getattr(getattr(s, "execution", object()), "on_timeout", "abort"),
        "min_stop_ticks": getattr(getattr(s, "execution", object()), "min_stop_ticks", 12),
    }
    if isinstance(exec_worker, dict) and exec_worker:
        for k in (
            "time_in_force", "post_only", "on_timeout",
            "limit_timeout_ms", "max_slippage_bp", "min_stop_ticks",
            "fee_bps_maker", "fee_bps_taker"
        ):
            if k in exec_worker:
                exec_out[k] = exec_worker[k]

    # --- стратегия: ключевые пороги наружу ---
    strat = getattr(s, "strategy", object())
    cross = getattr(strat, "cross_guard", object())
    mom = getattr(strat, "momentum", object())
    rev = getattr(strat, "reversion", object())
    regime = getattr(strat, "regime", object())

    return {
        "mode": getattr(s, "mode", "paper"),
        "symbols": getattr(s, "symbols", []),
        "streams": {
            "coalesce_ms": getattr(getattr(s, "streams", object()), "coalesce_ms", 75),
            "coalescer": coalescer_stats if coalescer_stats is not None else None,
        },
        "risk": {
            "risk_per_trade_pct": getattr(getattr(s, "risk", object()), "risk_per_trade_pct", 0.30),
            "daily_stop_r": getattr(getattr(s, "risk", object()), "daily_stop_r", -3.0),
            "daily_target_r": getattr(getattr(s, "risk", object()), "daily_target_r", 8.0),
            "max_consec_losses": getattr(getattr(s, "risk", object()), "max_consec_losses", 2),
            "cooldown_after_sl_s": getattr(getattr(s, "risk", object()), "cooldown_after_sl_s", 300),
            "min_risk_usd_floor": getattr(getattr(s, "risk", object()), "min_risk_usd_floor", 1.0),
        },
        "safety": {
            "max_spread_ticks": getattr(getattr(s, "safety", object()), "max_spread_ticks", 6),
            "min_top5_liquidity_usd": getattr(getattr(s, "safety", object()), "min_top5_liquidity_usd", 300000.0),
            "skip_funding_minute": getattr(getattr(s, "safety", object()), "skip_funding_minute", True),
            "skip_minute_zero": getattr(getattr(s, "safety", object()), "skip_minute_zero", True),
            "min_liq_buffer_sl_mult": getattr(getattr(s, "safety", object()), "min_liq_buffer_sl_mult", 3.0),
        },
        "strategy": {
            "auto_signal_enabled": getattr(strat, "auto_signal_enabled", True),
            "min_range_bps": getattr(strat, "min_range_bps", 2.0),
            "min_range_ticks": getattr(strat, "min_range_ticks", 3),
            "warmup_ms": getattr(strat, "warmup_ms", 45000),
            "stale_ms": getattr(strat, "stale_ms", 3000),
            "auto_min_flat_ms": getattr(strat, "auto_min_flat_ms", 900),
            "ev_guard_kR": getattr(strat, "ev_guard_kR", 0.30),
            "regime": {
                "vol_window_s": getattr(regime, "vol_window_s", 60),
            },
            "momentum": {
                "obi_t": getattr(mom, "obi_t", 0.003),
                "tp_r": getattr(mom, "tp_r", 1.9),
                "stop_k_sigma": getattr(mom, "stop_k_sigma", 1.2),
            },
            "reversion": {
                "bb_z": getattr(rev, "bb_z", 0.9),
                "rsi": getattr(rev, "rsi", 62),
                "tp_to_vwap": getattr(rev, "tp_to_vwap", True),
                "tp_r": getattr(rev, "tp_r", 1.6),
            },
            "cross_guard": {
                "enabled": getattr(cross, "enabled", True),
                "lookback_s": getattr(cross, "lookback_s", 60),
                "align_tolerance_bp": getattr(cross, "align_tolerance_bp", 2.5),
                "dominance_min_bp": getattr(cross, "dominance_min_bp", 4.0),
                "single_candidate": getattr(cross, "single_candidate", True),
            },
        },
        "execution": exec_out,
        "ml": {
            "enabled": getattr(getattr(s, "ml", object()), "enabled", False),
            "p_threshold_mom": getattr(getattr(s, "ml", object()), "p_threshold_mom", 0.55),
            "p_threshold_rev": getattr(getattr(s, "ml", object()), "p_threshold_rev", 0.60),
            "model_path": getattr(getattr(s, "ml", object()), "model_path", "./ml_artifacts/model.bin"),
        },
        "fees": {
            "maker_bps": getattr(getattr(s, "execution", object()), "fee_bps_maker", 2.0),
            "taker_bps": getattr(getattr(s, "execution", object()), "fee_bps_taker", 4.0),
            "binance_usdtm": getattr(getattr(s, "fees", object()), "binance_usdtm", None),
        },
        "account": {
            "starting_equity_usd": getattr(getattr(s, "account", object()), "starting_equity_usd", 1000.0),
            "leverage": getattr(getattr(s, "account", object()), "leverage", 15.0),
            "per_symbol_leverage": getattr(getattr(s, "account", object()), "per_symbol_leverage", None),
            "min_notional_usd": getattr(getattr(s, "account", object()), "min_notional_usd", 5.0),
        },
        "log_dir": getattr(s, "log_dir", "./logs"),
        "log_level": getattr(s, "log_level", "INFO"),
    }

@app.get("/account/state")
async def account_state() -> Dict[str, Any]:
    """
    Снапшот аккаунта из AccountState (Binance/бумага):
    - equity / available_margin
    - realized_pnl_today / num_trades_today
    - свежесть снапшота и ошибки обновления
    """
    w = _worker_required()

    # аккуратно достаём сервис стейта
    acc_state = getattr(w, "_account_state", None)
    if acc_state is None:
        return {
            "ok": False,
            "error": "account_state_not_configured",
        }

    # пробуем взять последний снапшот
    snap = acc_state.get_snapshot()
    if snap is None:
        # если ни разу не обновлялся — дергаем refresh
        try:
            snap = await acc_state.refresh()
        except Exception as e:
            return {
                "ok": False,
                "error": f"refresh_failed: {e}",
            }

    if snap is None:
        return {
            "ok": False,
            "error": "no_snapshot",
        }

    # возраст снапшота
    age_ms = acc_state.snapshot_age_ms()
    is_fresh = acc_state.is_fresh(max_age_s=10)

    return {
        "ok": True,
        "snapshot": {
            "ts": snap.ts.isoformat(),
            "equity": float(snap.equity),
            "available_margin": float(snap.available_margin),
            "realized_pnl_today": float(snap.realized_pnl_today),
            "num_trades_today": int(snap.num_trades_today),
            "open_positions": snap.open_positions,  # уже dict по символам
        },
        "meta": {
            "age_ms": age_ms,
            "is_fresh": bool(is_fresh),
            "consecutive_errors": acc_state.consecutive_errors,
        },
    }

# ------------------------------------------------------------------------------
# Diagnostics
# ------------------------------------------------------------------------------

@app.get("/debug/diag")
async def debug_diag() -> Dict[str, Any]:
    """
    Расширенная диагностика пайплайна в стабильном формате.

    Источник истины — Worker.diag():
      - никаких прямых обращений к приватным полям;
      - только чтение и лёгкая нормализация.
    """
    w = _worker_required()
    try:
        d = w.diag() or {}
    except Exception:
        d = {}

    ws_detail = d.get("ws_detail") or {}
    coal = d.get("coal") or {}
    
    # Get WS diagnostics from worker
    ws_diag = {}
    try:
        if hasattr(w, "ws") and hasattr(w.ws, "diag"):
            ws_diag = w.ws.diag() or {}
    except Exception:
        pass
    
    # Get coalescer stats
    coal_stats = {}
    try:
        if hasattr(w, "coal") and hasattr(w.coal, "stats"):
            coal_stats = w.coal.stats() or {}
    except Exception:
        pass
    
    # Get strategy loop status
    strategy_status = {
        "running": False,
        "heartbeat_ms": 0,
        "last_error": None,
    }
    try:
        if hasattr(w, "_strat_task"):
            strategy_status["running"] = w._strat_task is not None and not w._strat_task.done()
        if hasattr(w, "_strategy_heartbeat_ms"):
            strategy_status["heartbeat_ms"] = int(w._strategy_heartbeat_ms or 0)
        if hasattr(w, "_last_strategy_error"):
            strategy_status["last_error"] = w._last_strategy_error
    except Exception:
        pass

    return {
        "symbols": d.get("symbols", []),

        # WS / streams
        "ws": {
            "count": int((ws_detail or {}).get("messages", 0)),
            "last_rx_ts_ms": int((ws_detail or {}).get("last_rx_ms", 0)),
            "connects": int(ws_diag.get("connects", 0)),
            "errors": int(ws_diag.get("errors", 0)),
            "last_error": ws_diag.get("last_error", ""),
            "url": ws_diag.get("url", ""),
        },
        "ws_detail": ws_detail,
        "coal": coal,
        "coalesce_ms": coal.get("window_ms"),
        "coalescer_stats": coal_stats,
        "history": d.get("history", {}),
        "history_seconds": 600,  # для фронта как ориентир окна

        # Рынок / позиции / сделки
        "best": d.get("best", {}),
        "positions": d.get("positions", {}),
        "unrealized": d.get("unrealized", {}),
        "trades": d.get("trades", {}),

        # Риск / защита / безопасность
        "pnl_day": d.get("pnl_day", {}),
        "risk_cfg": d.get("risk_cfg", {}),
        "protection_cfg": d.get("protection_cfg", {}),
        "safety_cfg": d.get("safety_cfg", {}),
        "day_limits": d.get("day_limits", {}),
        "consec_losses": d.get("consec_losses", 0),

        # Аккаунт / исполнение
        "account_cfg": d.get("account_cfg", {}),
        "exec_cfg": d.get("exec_cfg", {}),
        "exec_counters": d.get("exec_counters", {}),
        "block_reasons": d.get("block_reasons", {}),

        # Стратегия / авто
        "auto_signal_enabled": bool(d.get("auto_signal_enabled", False)),
        "strategy_cfg": d.get("strategy_cfg", {}),
        "strategy_status": strategy_status,
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
    """
    Компактный дайджест для дашборда:
    - текущий день, pnl
    - uPnL
    - последние трейды по каждому символу
    - позиции (компактно)
    - статус авто и дневных лимитов
    - топ причин блокировок
    """
    w = _worker_required()
    try:
        d = w.diag() or {}
    except Exception:
        d = {}

    symbols = d.get("symbols", []) or []
    pnl_day = d.get("pnl_day", {}) or {}
    unrealized = d.get("unrealized", {}) or {}
    positions = d.get("positions", {}) or {}
    trades = d.get("trades", {}) or {}
    day_limits = d.get("day_limits", {}) or {}
    block_reasons = d.get("block_reasons", {}) or {}
    auto_enabled = bool(d.get("auto_signal_enabled", False))
    strat_cfg = d.get("strategy_cfg", {}) or {}

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

    # по одному последнему трейду на символ
    trades_last: Dict[str, Dict[str, Any]] = {}
    if isinstance(trades, dict):
        for sym, arr in trades.items():
            if not arr:
                continue
            last = max(arr, key=lambda x: x.get("closed_ts", x.get("opened_ts", 0)))
            trades_last[sym] = {
                "symbol": last.get("symbol"),
                "side": last.get("side"),
                "qty": last.get("qty"),
                "entry_px": last.get("entry_px"),
                "exit_px": last.get("exit_px"),
                "pnl_usd": last.get("pnl_usd"),
                "pnl_r": last.get("pnl_r"),
                "reason": last.get("reason"),
                "opened_ts": last.get("opened_ts"),
                "closed_ts": last.get("closed_ts"),
            }

    # топ-5 причин блокировок
    reasons_sorted = sorted(
        (block_reasons or {}).items(),
        key=lambda kv: kv[1],
        reverse=True,
    )[:5]

    return {
        "ts_ms": int(time.time() * 1000),
        "symbols": symbols,
        "pnl_day": pnl_day,
        "unrealized": unrealized,
        "positions": pos_compact,
        "trades_last": trades_last,
        "auto": {
            "enabled": auto_enabled,
            "cfg": strat_cfg,
        },
        "day_limits": day_limits,
        "reasons_top": reasons_sorted,
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
    Тестовый вход (превью + опциональное исполнение):
      • Свежесть тика (stale_ms), пауза между сделками (auto_min_flat_ms).
      • Квантование qty + проверка min_notional.
      • План SL/TP с целевым RR (fallback 1.8).
      • Safety-check (pre/post) через risk.filters.
      • Fee-aware EV-guard: чистая ожидаемая прибыль ≥ kR * риск.
      • Вход maker-friendly: GTX + post_only, on_timeout="abort".
      • dry=true — только превью, без исполнения.
    """
    from risk.filters import MicroCtx, TimeCtx, PositionalCtx, check_entry_safety
    from exec.sltp import compute_sltp

    w = _worker_required()
    sym = _normalize_symbol(req.symbol, w.symbols)
    side = _normalize_side(req.side)

    # ---- 1) последний тик + staleness guard ----
    last = _worker_latest_tick(w, sym)
    if not last:
        return JSONResponse({"ok": False, "error": f"no ticks for {sym}"}, status_code=503)

    price = float(last.get("price") or 0.0)
    bid = float(last.get("bid") or price)
    ask = float(last.get("ask") or price)
    last_ts = int(last.get("ts_ms", _now_ms()))
    now_ts = _now_ms()

    s = get_settings()
    try:
        stale_ms = int(getattr(getattr(s, "strategy", object()), "stale_ms", 3000))
    except Exception:
        stale_ms = 3000
    if now_ts - last_ts > stale_ms:
        return {
            "ok": False,
            "reason": "stale_tick",
            "stale_ms": now_ts - last_ts,
            "stale_threshold_ms": stale_ms,
            "symbol": sym
        }

    if price <= 0.0:
        if bid > 0 and ask > 0:
            price = (bid + ask) / 2.0
        else:
            return JSONResponse({"ok": False, "error": f"no valid price for {sym}"}, status_code=503)

    # ---- 2) шаги ----
    price_step, qty_step = _get_symbol_steps_from_worker(w, sym)

    # ---- 3) execution-параметры (maker-friendly превью) ----
    exec_cfg = _exec_cfg()
    time_in_force = "GTX"
    post_only = True
    on_timeout = "abort"
    limit_offset_ticks = int(req.limit_offset_ticks) if req.limit_offset_ticks is not None else int(exec_cfg.get("limit_offset_ticks", 0))

    e = getattr(s, "execution", object())
    fee_maker_bps = float(getattr(e, "fee_bps_maker", 2.0))
    fee_taker_bps = float(getattr(e, "fee_bps_taker", 4.0))

    try:
        if hasattr(w, "_fees_for_symbol"):
            eff_maker, eff_taker = w._fees_for_symbol(sym)
            fee_maker_bps = float(eff_maker)
            fee_taker_bps = float(eff_taker)
    except Exception:
        pass

    taker_like_entry = False  # превью считаем maker-ходом

    # ---- 4) таргеты RR и EV-guard ----
    rr_target = 1.8
    try:
        strat = getattr(s, "strategy", object())
        mom = getattr(strat, "momentum", object())
        rr_target = float(getattr(mom, "tp_r", rr_target))
    except Exception:
        pass
    rr_target = max(rr_target, 1.8)

    ev_guard_kR = 0.30  # мягче, чем 0.35, чтобы сделки не подавить
    try:
        ev_guard_kR = float(getattr(strat, "ev_guard_kR", ev_guard_kR))
        if not (0.05 <= ev_guard_kR <= 2.0):
            ev_guard_kR = 0.30
    except Exception:
        ev_guard_kR = 0.30

    # ---- 5) аккаунт/риск ----
    acc = _account_cfg()
    rsk = _risk_cfg()

    # ---- 6) flat-cooldown между сделками ----
    try:
        auto_min_flat_ms = int(getattr(getattr(s, "strategy", object()), "auto_min_flat_ms", 900))
    except Exception:
        auto_min_flat_ms = 900
    last_trade_ts = 0
    try:
        d_diag = w.diag() or {}
        store = d_diag.get("trades", {})
        if isinstance(store, dict):
            for arr in store.values():
                for t in arr or []:
                    last_trade_ts = max(last_trade_ts, int(t.get("closed_ts", 0) or 0), int(t.get("opened_ts", 0) or 0))
        elif isinstance(store, list):
            for t in store:
                last_trade_ts = max(last_trade_ts, int(t.get("closed_ts", 0) or 0), int(t.get("opened_ts", 0) or 0))
    except Exception:
        pass
    if last_trade_ts > 0 and (now_ts - last_trade_ts) < auto_min_flat_ms:
        return {
            "ok": False,
            "reason": "flat_cooldown",
            "since_last_ms": now_ts - last_trade_ts,
            "min_flat_ms": auto_min_flat_ms,
            "symbol": sym
        }

    # ---- 7) волатильностная SL-дистанция (fallback к min_stop_ticks и спреду) ----
    try:
        sl_distance_px = float(w._compute_vol_stop_distance_px(sym, mid_px=price))
    except Exception:
        try:
            min_stop_ticks = int(exec_cfg.get("min_stop_ticks", 12))
        except Exception:
            min_stop_ticks = 12
        spread = max(0.0, ask - bid)
        sl_distance_px = max(
            price_step * max(6, min_stop_ticks),
            spread * 2.0,
            price_step * 6,
        )

    if sl_distance_px <= 0:
        return {"ok": False, "reason": "bad_sl_distance_px", "symbol": sym, "side": side}

    # ---- 8) qty: ручной или авто, затем квантование и min_notional ----
    if req.qty is not None and req.qty > 0:
        qty = float(req.qty)
    else:
        try:
            qty = float(w._compute_auto_qty(sym, side, sl_distance_px))
        except Exception as e:
            return {"ok": False, "reason": "auto_qty_failed", "symbol": sym, "side": side, "error": str(e)}

    qty = float(_quantize(qty, qty_step))
    if qty <= 0.0:
        return {
            "ok": False, "reason": "qty_rounded_to_zero", "symbol": sym, "side": side,
            "hint": f"try qty ≥ {max(qty_step, 2 * qty_step)} for {sym}",
        }
    if qty * price < acc["min_notional"]:
        min_qty = _quantize((acc["min_notional"] / price) + qty_step, qty_step)
        return {
            "ok": False, "reason": "below_min_notional", "symbol": sym, "side": side, "qty": qty,
            "hint": f"min notional ${acc['min_notional']} → try qty ≥ {min_qty}",
        }

    # ---- 9) превью лимит-цены (maker friendly) ----
    def _preview_limit_px(bid_val: float, ask_val: float, side_val: str, tick: float, off_ticks: int) -> float:
        try:
            off = max(0, int(off_ticks)) * float(max(tick, 0.0))
        except Exception:
            off = 0.0
        px = 0.0
        if side_val == "BUY" and ask_val > 0:
            px = ask_val - off if off > 0 else ask_val
        elif side_val == "SELL" and bid_val > 0:
            px = bid_val + off if off > 0 else bid_val
        if px <= 0.0 and bid_val > 0 and ask_val > 0:
            px = (bid_val + ask_val) / 2.0
        return float(px or 0.0)

    preview_px = _preview_limit_px(bid, ask, side, price_step, limit_offset_ticks)
    if preview_px <= 0.0:
        preview_px = price

    # ---- 10) SL/TP план с RR ----
    try:
        plan = compute_sltp(
            side=side,
            entry_px=float(preview_px),
            qty=float(qty),
            price_tick=float(price_step),
            sl_distance_px=float(sl_distance_px),
            rr=float(rr_target),
        )
        sl_px = float(plan.sl_px) if getattr(plan, "sl_px", None) is not None else None
        tp_px = float(plan.tp_px) if getattr(plan, "tp_px", None) is not None else None
    except Exception:
        plan = None
        sl_px, tp_px = None, None

    # ---- 11) safety-check (pre/post) ----
    def _spread_ticks(bid_val: float, ask_val: float, tick: float) -> float:
        if bid_val > 0 and ask_val > 0 and tick > 0:
            return max(0.0, (ask_val - bid_val) / tick)
        return 0.0

    spr_ticks = _spread_ticks(bid, ask, price_step)
    micro = MicroCtx(spread_ticks=spr_ticks, top5_liq_usd=1e12)
    tctx = TimeCtx(ts_ms=w._now_ms(), server_time_offset_ms=getattr(w, "_server_time_offset_ms", 0))

    pre_ok = check_entry_safety(side, micro, tctx, pos_ctx=None, safety=w._safety_cfg)
    post_ok = pre_ok
    if sl_px is not None:
        pos_ctx = PositionalCtx(
            entry_px=float(preview_px),
            sl_px=float(sl_px),
            leverage=float(getattr(w, "_leverage", 10.0)),
        )
        post_ok = check_entry_safety(side, micro=micro, time_ctx=tctx, pos_ctx=pos_ctx, safety=w._safety_cfg)

    # ---- 12) риск/комиссии/EV ----
    notional = price * qty
    expected_risk_usd = sl_distance_px * qty

    def _estimate_fees_usd(notional_entry: float, notional_exit: float, maker_bps: float, taker_bps: float, taker_like: bool) -> float:
        bps_entry = taker_bps if taker_like else maker_bps
        bps_exit = taker_bps
        return (notional_entry * bps_entry + notional_exit * bps_exit) / 10_000.0

    est_fees_usd_conservative = _estimate_fees_usd(notional, notional, fee_maker_bps, fee_taker_bps, taker_like_entry)
    est_fees_usd_maker_only = (notional * (fee_maker_bps / 10_000.0)) * 2.0

    warnings: List[str] = []
    if expected_risk_usd < rsk["min_risk_floor"]:
        warnings.append(f"expected_risk_usd({expected_risk_usd:.4f}) < min_risk_floor({rsk['min_risk_floor']:.4f})")
    if est_fees_usd_maker_only > 0 and expected_risk_usd < (0.8 * est_fees_usd_maker_only):
        warnings.append(f"risk({expected_risk_usd:.4f}) < 0.8×maker_only_fees({est_fees_usd_maker_only:.4f})")
    fees_ratio = est_fees_usd_conservative / max(expected_risk_usd, 1e-9)

    if fees_ratio > 0.4:  # комиссии > 40% от риска — запретить
        return {
            "ok": False,
            "reason": "fees_vs_risk_too_high",
            "symbol": sym,
            "side": side,
            "expected_risk_usd": round(float(expected_risk_usd), 6),
            "fees_usd_conservative": round(float(est_fees_usd_conservative), 6),
            "fees_ratio": round(float(fees_ratio), 3),
        }

    rr_used = rr_target
    expected_profit_usd_est = expected_risk_usd * rr_used
    net_profit_usd_est = expected_profit_usd_est - est_fees_usd_conservative
    ev_ok = (net_profit_usd_est >= (ev_guard_kR * expected_risk_usd)) and (rr_used >= 1.8)

    safety_block = (not pre_ok.allow) or (not post_ok.allow)

    # ---- 13) dry превью ----
    if req.dry:
        return {
            "ok": (not safety_block) and ev_ok,
            "dry": True,
            "symbol": sym,
            "side": side,
            "qty": float(qty),
            "price": float(price),
            "preview_px": float(preview_px),
            "sl_distance_px": round(float(sl_distance_px), 6),
            "expected_risk_usd": round(float(expected_risk_usd), 6),
            "fees_usd": {
                "maker_only": round(float(est_fees_usd_maker_only), 6),
                "conservative_maker_plus_taker": round(float(est_fees_usd_conservative), 6),
                "entry_taker_like": bool(taker_like_entry),
            },
            "tif": time_in_force,
            "post_only": bool(post_only),
            "on_timeout": on_timeout,
            "safety": {
                "pre": {"allow": bool(pre_ok.allow), "reasons": pre_ok.reasons},
                "post": {"allow": bool(post_ok.allow), "reasons": post_ok.reasons},
            },
            "plan": {"sl_px": sl_px, "tp_px": tp_px, "rr": float(rr_used)},
            "guards": {
                "ev_ok": bool(ev_ok),
                "net_profit_usd_est": round(float(net_profit_usd_est), 6),
                "ev_floor_kR": float(ev_guard_kR),
                "rr_ok": bool(rr_used >= 1.8),
            },
            "warnings": warnings or None,
        }

    try:
        report = await w.place_entry(sym, side, float(qty), int(limit_offset_ticks), sl_distance_px=float(sl_distance_px))
    except TypeError:
        try:
            report = await w.place_entry(sym, side, float(qty), int(limit_offset_ticks))
        except TypeError:
            if req.qty is None:
                report = await w.place_entry_auto(sym, side)
            else:
                report = await w.place_entry(sym, side, float(qty))

    out: Dict[str, Any] = {
        "ok": True,
        "symbol": sym,
        "side": side,
        "qty": float(qty),
        "price": float(price),
        "preview_px": float(preview_px),
        "sl_distance_px": round(float(sl_distance_px), 6),
        "expected_risk_usd": round(float(expected_risk_usd), 6),
        "fees_usd": {
            "maker_only": round(float(est_fees_usd_maker_only), 6),
            "conservative_maker_plus_taker": round(float(est_fees_usd_conservative), 6),
            "entry_taker_like": bool(taker_like_entry),
        },
        "tif": time_in_force,
        "post_only": bool(post_only),
        "on_timeout": on_timeout,
        "safety": {
            "pre": {"allow": bool(pre_ok.allow), "reasons": pre_ok.reasons},
            "post": {"allow": bool(post_ok.allow), "reasons": post_ok.reasons},
        },
        "plan": {"sl_px": sl_px, "tp_px": tp_px, "rr": float(rr_used)},
        "guards": {
            "ev_ok": bool(ev_ok),
            "net_profit_usd_est": round(float(net_profit_usd_est), 6),
            "ev_floor_kR": float(ev_guard_kR),
            "rr_ok": bool(rr_used >= 1.8),
        },
        "entry_mode": ("taker_like" if taker_like_entry else "maker_pref"),
        "report": report,
        "warnings": warnings or None,
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

@app.get("/control/auto")
def control_auto_get() -> Dict[str, Any]:
    w = _worker_required()
    d = w.diag() or {}
    enabled = bool(d.get("auto_signal_enabled", False))
    return {"ok": True, "enabled": enabled}


@app.post("/control/auto")
def control_auto_set(payload: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    """
    Надёжно включает/выключает авто-сигналы, не ломая текущие cooldown/min_flat:
    используем Worker.set_strategy(enabled, cooldown_ms, min_flat_ms) если он есть.
    
    Accepts either {"enabled": true/false} or {"auto": true/false}.
    If both are provided, "enabled" takes precedence.
    """
    log = logging.getLogger("uvicorn.error")
    w = _worker_required()
    
    # Extract enabled value from payload - accept both "enabled" and "auto" fields
    enabled_val = payload.get("enabled")
    auto_val = payload.get("auto")
    
    # Determine the enabled value: use enabled if set, otherwise use auto
    if enabled_val is not None:
        enabled = bool(enabled_val)
    elif auto_val is not None:
        enabled = bool(auto_val)
    else:
        raise HTTPException(
            status_code=422,
            detail="Either 'enabled' or 'auto' field is required"
        )
    
    # Get current config to preserve cooldown/min_flat
    d = w.diag() or {}
    cfg = (d.get("strategy_cfg") or {}) if isinstance(d, dict) else {}
    cooldown = int(cfg.get("cooldown_ms", 500))
    min_flat = int(cfg.get("min_flat_ms", 200))

    # Set the strategy state
    set_strategy = getattr(w, "set_strategy", None)
    if callable(set_strategy):
        res = set_strategy(enabled=enabled, cooldown_ms=cooldown, min_flat_ms=min_flat)
        if isinstance(res, dict):
            # Log the state change
            if enabled:
                log.info("[CONTROL] auto mode enabled")
            else:
                log.info("[CONTROL] auto mode disabled")
            return {"ok": True, "enabled": enabled, **res}

    # фолбэк — выставим очевидное поле; diag всё равно прочитает актуальный флаг
    try:
        setattr(w, "_auto_enabled", enabled)
    except Exception:
        pass
    
    # Log the state change
    if enabled:
        log.info("[CONTROL] auto mode enabled")
    else:
        log.info("[CONTROL] auto mode disabled")
    
    # Verify the state was set correctly
    nd = w.diag() or {}
    final_enabled = bool(nd.get("auto_signal_enabled", enabled))
    return {"ok": True, "enabled": final_enabled}


# ------------------------------------------------------------------------------
# Positions / metrics / reasons
# ------------------------------------------------------------------------------

@app.get("/positions")
async def positions() -> Dict[str, Any]:
    w = _worker_required()
    d = w.diag()
    positions = d.get("positions", {})
    return {"symbols": list(w.symbols), "positions": positions}

@app.get("/fees")
def fees() -> Dict[str, Any]:
    """
    Быстрый просмотр комиссий: maker/taker BPS из настроек и то, что вернёт воркер через _fees_for_symbol.
    """
    s = get_settings()
    exec_fees = {
        "maker_bps": getattr(getattr(s, "execution", object()), "fee_bps_maker", 2.0),
        "taker_bps": getattr(getattr(s, "execution", object()), "fee_bps_taker", 4.0),
    }
    fees_cfg = getattr(s, "fees", None)
    fees_binance = None
    if fees_cfg and hasattr(fees_cfg, "binance_usdtm"):
        try:
            fees_binance = dict(getattr(fees_cfg, "binance_usdtm"))
        except Exception:
            pass

    w = _worker_required()
    per_symbol = {}
    for sym in w.symbols:
        try:
            maker, taker = w._fees_for_symbol(sym)  # твоя функция
            per_symbol[sym] = {"maker_bps": float(maker), "taker_bps": float(taker)}
        except Exception as e:
            per_symbol[sym] = {"error": str(e)}

    return {"execution": exec_fees, "fees.binance_usdtm": fees_binance, "per_symbol_effective": per_symbol}


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
    """
    Лёгкие метрики для health/графиков:
    - ws / coalescer counters
    - exec counters
    - количество открытых позиций
    - дневной PnL
    """
    w = _worker_required()
    try:
        d = w.diag() or {}
    except Exception:
        d = {}

    ws = d.get("ws_detail", {}) or {}
    coal = d.get("coal", {}) or {}
    exec_counters = d.get("exec_counters", {}) or {}
    pnl_day = d.get("pnl_day", {}) or {}
    unrealized = d.get("unrealized", {}) or {}
    positions = d.get("positions", {}) or {}

    open_positions = sum(
        1 for p in (positions or {}).values()
        if (p or {}).get("state") == "OPEN"
    )

    return {
        "ws_messages": int(ws.get("messages", 0)),
        "ws_last_rx_ms": int(ws.get("last_rx_ms", 0)),
        "ws_reconnects": int(ws.get("reconnects", 0)),
        "coalesce_window_ms": coal.get("window_ms"),
        "coalesce_emit_count": int(coal.get("emit_count", 0)),
        "coalesce_emits_per_sec": float(coal.get("emits_per_sec", 0.0)),
        "coalesce_queue_max": int(coal.get("max_queue_depth", 0)),
        "orders_limit_total": int(exec_counters.get("limit_total", 0)),
        "orders_market_total": int(exec_counters.get("market_total", 0)),
        "orders_cancel_total": int(exec_counters.get("cancel_total", 0)),
        "orders_reject_total": int(exec_counters.get("reject_total", 0)),
        "open_positions": open_positions,
        "symbols": d.get("symbols", []),
        "pnl_r_day": float(pnl_day.get("pnl_r", 0.0) or 0.0),
        "pnl_usd_day": float(pnl_day.get("pnl_usd", 0.0) or 0.0),
        "winrate_day": pnl_day.get("winrate"),
        "trades_day": int(pnl_day.get("trades", 0) or 0),
        "max_dd_r_day": pnl_day.get("max_dd_r"),
        "unrealized_total_usd": float(unrealized.get("total_usd", 0.0) or 0.0),
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
async def get_trades(limit: int = 50, offset: int = 0):
    eng = get_engine()
    with eng.begin() as conn:
        rows = conn.execute(
            text(
                """
                SELECT
                  t.id,
                  t.symbol,
                  t.side,
                  t.qty,
                  t.entry_px,
                  t.exit_px,
                  t.pnl_usd,
                  t.pnl_r,
                  t.entry_ts_ms,
                  t.exit_ts_ms,
                  t.reason_open,
                  t.reason_close,
                  COALESCE(t.fees_usd, 0.0) AS fees_usd
                FROM trades t
                ORDER BY t.exit_ts_ms DESC, t.entry_ts_ms DESC
                LIMIT :limit OFFSET :offset
                """
            ),
            {"limit": limit, "offset": offset},
        ).fetchall()

        total = conn.execute(
            text("SELECT COUNT(*) FROM trades")
        ).scalar() or 0

    items = []
    for row in rows:
        m = row._mapping if hasattr(row, "_mapping") else None

        def col(name, idx, default=None):
            if m is not None:
                return m.get(name, default)
            return row[idx] if row[idx] is not None else default

        items.append(
            {
                "id": col("id", 0),
                "symbol": col("symbol", 1),
                "side": col("side", 2),
                "qty": float(col("qty", 3, 0.0) or 0.0),
                "entry_px": float(col("entry_px", 4, 0.0) or 0.0),
                "exit_px": float(col("exit_px", 5, 0.0) or 0.0),
                "pnl_usd": float(col("pnl_usd", 6, 0.0) or 0.0),
                "pnl_r": float(col("pnl_r", 7, 0.0) or 0.0),
                "opened_ts": int(col("entry_ts_ms", 8, 0) or 0),
                "closed_ts": int(col("exit_ts_ms", 9, 0) or 0),
                "reason_open": col("reason_open", 10),
                "reason_close": col("reason_close", 11),
                "fees_usd": float(col("fees_usd", 12, 0.0) or 0.0),
            }
        )

    return {"items": items, "count": int(total)}


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


@app.get("/trades/recent")
async def trades_recent(
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of trades to return"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
) -> Dict[str, Any]:
    """
    Clean trade history with aggregated performance statistics.
    
    Returns:
    - trades: list of recent trades with all required fields
    - stats: aggregated statistics (total_trades, wins, losses, winrate, total_realized_pnl_usd, average_r)
    
    Uses stored trades from database, NOT recomputed from raw candles.
    """
    log = logging.getLogger("uvicorn.error")
    eng = get_engine()
    
    # Fetch recent trades
    with eng.begin() as conn:
        rows = conn.execute(
            text(
                """
                SELECT
                  t.id,
                  t.symbol,
                  t.side,
                  t.qty,
                  t.entry_px,
                  t.exit_px,
                  t.pnl_usd,
                  t.r,
                  t.pnl_r,
                  t.entry_ts_ms,
                  t.exit_ts_ms,
                  t.reason_open,
                  t.reason_close,
                  COALESCE(t.fees_usd, 0.0) AS fees_usd
                FROM trades t
                WHERE t.exit_ts_ms > 0
                ORDER BY t.exit_ts_ms DESC, t.entry_ts_ms DESC
                LIMIT :limit OFFSET :offset
                """
            ),
            {"limit": limit, "offset": offset},
        ).fetchall()

        # Get total count of closed trades
        total_row = conn.execute(
            text("SELECT COUNT(*) FROM trades WHERE exit_ts_ms > 0")
        ).fetchone()
        total_count = int(total_row[0] if total_row else 0)

        # Get aggregated stats for ALL closed trades (not just the page)
        stats_row = conn.execute(
            text(
                """
                SELECT
                  COUNT(*) AS total_trades,
                  COALESCE(SUM(CASE WHEN pnl_usd > 0.01 THEN 1 ELSE 0 END), 0) AS wins,
                  COALESCE(SUM(CASE WHEN pnl_usd < -0.01 THEN 1 ELSE 0 END), 0) AS losses,
                  COALESCE(SUM(pnl_usd), 0.0) AS total_realized_pnl_usd,
                  COALESCE(AVG(r), 0.0) AS average_r
                FROM trades
                WHERE exit_ts_ms > 0
                """
            )
        ).fetchone()

    # Parse trades
    trades = []
    for row in rows:
        m = row._mapping if hasattr(row, "_mapping") else None

        def col(name, idx, default=None):
            if m is not None:
                return m.get(name, default)
            return row[idx] if row[idx] is not None else default

        # Calculate r_value = realized_pnl_usd / risk_per_trade_usd
        # For now, r_value is the same as r (stored in DB)
        # If you need to recalculate based on equity at time of trade, that would require more complex logic
        r_value = float(col("r", 7, 0.0) or 0.0)
        
        trades.append({
            "id": col("id", 0),
            "symbol": col("symbol", 1),
            "side": col("side", 2),
            "qty": float(col("qty", 3, 0.0) or 0.0),
            "entry_price": float(col("entry_px", 4, 0.0) or 0.0),
            "exit_price": float(col("exit_px", 5, 0.0) or 0.0),
            "realized_pnl_usd": float(col("pnl_usd", 6, 0.0) or 0.0),
            "r_value": r_value,
            "opened_at": int(col("entry_ts_ms", 9, 0) or 0),
            "closed_at": int(col("exit_ts_ms", 10, 0) or 0),
            "reason_open": col("reason_open", 11),
            "reason_close": col("reason_close", 12),
            "fees_usd": float(col("fees_usd", 13, 0.0) or 0.0),
        })

    # Parse aggregated stats
    stats = {
        "total_trades": 0,
        "wins": 0,
        "losses": 0,
        "winrate": None,
        "total_realized_pnl_usd": 0.0,
        "average_r": 0.0,
    }
    
    if stats_row is not None:
        try:
            m = stats_row._mapping if hasattr(stats_row, "_mapping") else None
            
            def stat_col(name, idx, default=0):
                if m is not None:
                    return m.get(name, default) or default
                return stats_row[idx] if stats_row[idx] is not None else default
            
            total_trades = int(stat_col("total_trades", 0, 0))
            wins = int(stat_col("wins", 1, 0))
            losses = int(stat_col("losses", 2, 0))
            total_realized_pnl_usd = float(stat_col("total_realized_pnl_usd", 3, 0.0))
            average_r = float(stat_col("average_r", 4, 0.0))
            
            # Calculate winrate
            winrate = None
            if total_trades > 0:
                winrate = round(wins / total_trades, 4) if wins + losses > 0 else None
            
            stats = {
                "total_trades": total_trades,
                "wins": wins,
                "losses": losses,
                "winrate": winrate,
                "total_realized_pnl_usd": round(total_realized_pnl_usd, 6),
                "average_r": round(average_r, 4),
            }
        except Exception as e:
            log.warning(f"[/trades/recent] Failed to parse stats: {e}")

    return {
        "trades": trades,
        "stats": stats,
        "pagination": {
            "total": total_count,
            "limit": limit,
            "offset": offset,
        },
    }


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
    Clean PnL snapshot from AccountState and stored trades (single source of truth).
    
    Returns:
    - equity: total wallet balance from AccountState
    - realized_pnl_today: realized PnL for today (from daily_stats)
    - unrealized_pnl: total unrealized PnL for open positions (from AccountState)
    - open_positions_count: number of open positions
    
    Uses stored trades and AccountState, NOT recomputed from raw candles.
    """
    log = logging.getLogger("uvicorn.error")
    w = _worker_required()
    
    # Get AccountState service from worker
    acc_state = getattr(w, "_account_state", None)
    if acc_state is None:
        log.error("[/pnl/now] AccountState not configured in worker")
        raise HTTPException(
            status_code=503,
            detail="AccountState not configured - cannot compute PnL"
        )
    
    # Refresh account snapshot (this computes unrealized PnL and fetches daily stats)
    try:
        snap = await acc_state.refresh()
    except Exception as e:
        log.error(f"[/pnl/now] AccountState.refresh() failed: {e}", exc_info=True)
        raise HTTPException(
            status_code=503,
            detail=f"Failed to refresh account state: {e}"
        )
    
    if snap is None:
        log.error("[/pnl/now] AccountState.refresh() returned None")
        raise HTTPException(
            status_code=503,
            detail="AccountState refresh returned no snapshot"
        )
    
    # Check if snapshot is stale or has errors
    age_ms = acc_state.snapshot_age_ms()
    consecutive_errors = acc_state.consecutive_errors
    
    if consecutive_errors > 0:
        log.warning(
            f"[/pnl/now] AccountState has {consecutive_errors} consecutive errors, "
            f"snapshot may be degraded (age: {age_ms}ms)"
        )
    
    # Get current day string
    try:
        day_str = snap.ts.strftime("%Y-%m-%d")
    except Exception:
        day_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    
    trades_day = 0
    pnl_usd_day = 0.0
    pnl_r_day = 0.0
    fees_usd_day = 0.0
    winrate = None
    avg_r = None
    max_dd_r = None  # можем позже посчитать отдельно

    try:
        eng = get_engine()
        with eng.begin() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT
                    COUNT(*) AS trades,
                    COALESCE(SUM(pnl_usd), 0.0) AS pnl_usd_sum,
                    COALESCE(SUM(pnl_r), 0.0) AS pnl_r_sum,
                    COALESCE(SUM(fees_usd), 0.0) AS fees_usd_sum,
                    COALESCE(SUM(CASE WHEN pnl_usd > 0 THEN 1 ELSE 0 END), 0) AS wins,
                    COALESCE(SUM(CASE WHEN pnl_usd < 0 THEN 1 ELSE 0 END), 0) AS losses
                    FROM trades
                    WHERE DATE(exit_ts_ms / 1000.0, 'unixepoch') = :day
                    AND exit_ts_ms > 0
                    """
                ),
                {"day": day_str},
            ).fetchone()

            if row is not None:
                m = row._mapping if hasattr(row, "_mapping") else None

                def col(name, idx, default=0.0):
                    if m is not None:
                        return m.get(name, default) or default
                    return row[idx] if row[idx] is not None else default

                trades_day = int(col("trades", 0, 0))
                pnl_usd_day = float(col("pnl_usd_sum", 1, 0.0))
                pnl_r_day = float(col("pnl_r_sum", 2, 0.0))
                fees_usd_day = float(col("fees_usd_sum", 3, 0.0))
                wins = int(col("wins", 4, 0))
                losses = int(col("losses", 5, 0))

                total_decided = wins + losses
                if total_decided > 0:
                    winrate = wins / total_decided

                if trades_day > 0:
                    avg_r = pnl_r_day / trades_day

    except Exception as e:
        log.warning("[/pnl/now] failed to aggregate trades for %s: %s", day_str, e)
    
    # Build per_symbol unrealized PnL breakdown
    per_symbol: Dict[str, float] = {}
    open_positions_count = 0
    
    for sym, pos_snap in snap.open_positions.items():
        if pos_snap.unrealized_pnl is not None:
            per_symbol[sym] = round(float(pos_snap.unrealized_pnl), 6)
            open_positions_count += 1
    
    # Compute realized PnL R-multiple (if we have equity)
    pnl_r = 0.0
    if snap.equity > 0:
        try:
            s = get_settings()
            risk_pct = float(getattr(getattr(s, "risk", object()), "risk_per_trade_pct", 0.25))
            if risk_pct > 0:
                # realized_pnl_today / (equity * risk_pct / 100)
                pnl_r = snap.realized_pnl_today / (snap.equity * risk_pct / 100.0)
        except Exception:
            pnl_r = 0.0
    
    # Query daily_stats from DB for today (winrate, avg_r, max_dd_r, trades count)
    winrate = None
    avg_r = None
    max_dd_r = None
    trades_count_from_stats = 0
    pnl_r_from_stats = 0.0
    pnl_usd_from_stats = 0.0
    
    try:
        eng = get_engine()
        with eng.begin() as conn:
            # Get daily_stats for today
            stats_row = conn.execute(
                text(
                    """
                    SELECT trades, wins, losses, pnl_usd, pnl_r, max_dd_r
                      FROM daily_stats
                     WHERE day_utc = :day
                    """
                ),
                {"day": day_str},
            ).fetchone()
            
            if stats_row is not None:
                try:
                    if hasattr(stats_row, '_mapping'):
                        mapping = stats_row._mapping
                        trades_count_from_stats = int(mapping.get("trades", 0) or 0)
                        wins = int(mapping.get("wins", 0) or 0)
                        losses = int(mapping.get("losses", 0) or 0)
                        pnl_usd_from_stats = float(mapping.get("pnl_usd", 0.0) or 0.0)
                        pnl_r_from_stats = float(mapping.get("pnl_r", 0.0) or 0.0)
                        max_dd_r = float(mapping.get("max_dd_r", 0.0) or 0.0)
                    else:
                        # Fallback to positional access
                        trades_count_from_stats = int(stats_row[0] or 0)
                        wins = int(stats_row[1] or 0)
                        losses = int(stats_row[2] or 0)
                        pnl_usd_from_stats = float(stats_row[3] or 0.0)
                        pnl_r_from_stats = float(stats_row[4] or 0.0)
                        max_dd_r = float(stats_row[5] or 0.0)
                    
                    # Calculate winrate: wins / (wins + losses), but only if there are decided trades
                    total_decided = wins + losses
                    if total_decided > 0:
                        winrate = round(wins / total_decided, 4)
                    
                    # Calculate avg_r: pnl_r / trades (if trades > 0)
                    if trades_count_from_stats > 0:
                        avg_r = round(pnl_r_from_stats / trades_count_from_stats, 4)
                        
                except Exception as e_parse:
                    log.warning(f"[/pnl/now] Failed to parse daily_stats for {day_str}: {e_parse}")
                    
    except Exception as e:
        log.warning(f"[/pnl/now] Failed to query daily_stats for {day_str}: {e}")
    
    # Compute daily fees from DB (total fees for all closed trades today)
    fees_today = 0.0
    try:
        eng = get_engine()
        with eng.begin() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT COALESCE(SUM(f.fee_usd), 0.0) AS fees_today
                      FROM fills f
                      JOIN orders o ON o.id = f.order_id
                      JOIN trades t ON t.id = o.trade_id
                     WHERE DATE(t.exit_ts_ms/1000, 'unixepoch') = :day
                    """
                ),
                {"day": day_str},
            ).fetchone()
            
            if row is not None:
                if hasattr(row, '_mapping'):
                    fees_today = float(row._mapping.get("fees_today", 0.0) or 0.0)
                else:
                    fees_today = float(row[0] or 0.0)
    except Exception as e:
        log.warning(f"[/pnl/now] Failed to query daily fees for {day_str}: {e}. Using 0.0")
        fees_today = 0.0
    
    # Build response
    # Use daily_stats values if available, otherwise fall back to AccountState snapshot
    # daily_stats should be authoritative since it's updated by close_trade()
    trades_final = trades_count_from_stats if trades_count_from_stats > 0 else int(trades_day)
    pnl_usd_final = pnl_usd_from_stats if trades_count_from_stats > 0 else float(pnl_usd_day)
    pnl_r_final = pnl_r_from_stats if trades_count_from_stats > 0 else float(pnl_r_day)
    
    resp = {
        "equity": round(float(snap.equity), 6),
        "realized_pnl_today": round(float(pnl_usd_final), 6),
        "unrealized_pnl": round(float(snap.unrealized_pnl_now), 6),
        "open_positions_count": open_positions_count,
        "pnl_day": {
            "day": day_str,
            "trades": trades_final,
            "pnl_usd": round(float(pnl_usd_final), 6),
            "pnl_r": round(float(pnl_r_final), 4),
            "fees_usd": round(float(fees_today), 6),
            "winrate": winrate,
            "avg_r": avg_r,
            "max_dd_r": max_dd_r,
        },
        "unrealized_breakdown": per_symbol if per_symbol else {},
        "available_margin": round(float(snap.available_margin), 6),
        "meta": {
            "snapshot_ts": snap.ts.isoformat(),
            "snapshot_age_ms": age_ms,
            "consecutive_errors": consecutive_errors,
        },
    }
    
    return JSONResponse(resp)

# ------------------------------------------------------------------------------
# Stats management endpoints
# ------------------------------------------------------------------------------

@app.post("/paper/reset_metrics")
async def paper_reset_metrics(request: Request) -> Dict[str, Any]:
    """
    DEPRECATED: Use /stats/clear instead.
    Reset daily metrics for paper trading.
    Accepts optional 'baseline' parameter in request body: "now" or "today"
    """
    # Redirect to clear_stats for backward compatibility
    return await clear_stats()

@app.post("/metrics/reset")
async def metrics_reset() -> Dict[str, Any]:
    """
    Сброс дневных метрик / pnl / счётчиков.
    Реализация зависит от того, как у тебя сделан Worker,
    но MVP-идея примерно такая: вызвать метод reset_metrics() если он есть.
    """
    w = _worker_required()
    reset = getattr(w, "reset_metrics", None)
    if callable(reset):
        res = reset()
        if isinstance(res, dict):
            return {"ok": True, **res}
    return {"ok": True}

@app.post("/stats/clear")
async def clear_stats() -> Dict[str, Any]:
    """
    Clear all trades and reset daily_stats.
    This deletes all trades from the database and resets all daily_stats to zero.
    WARNING: This is irreversible!
    """
    log = logging.getLogger("uvicorn.error")
    eng = get_engine()
    
    try:
        with eng.begin() as conn:
            # Delete all trades
            trades_deleted = conn.execute(
                text("DELETE FROM trades")
            ).rowcount
            
            # Delete all orders (cascade should handle fills)
            orders_deleted = conn.execute(
                text("DELETE FROM orders")
            ).rowcount
            
            # Delete all fills
            fills_deleted = conn.execute(
                text("DELETE FROM fills")
            ).rowcount
            
            # Delete all daily_stats
            stats_deleted = conn.execute(
                text("DELETE FROM daily_stats")
            ).rowcount
            
            log.info(
                f"[/stats/clear] Cleared: {trades_deleted} trades, "
                f"{orders_deleted} orders, {fills_deleted} fills, "
                f"{stats_deleted} daily_stats"
            )
            
            return {
                "ok": True,
                "trades_deleted": trades_deleted,
                "orders_deleted": orders_deleted,
                "fills_deleted": fills_deleted,
                "stats_deleted": stats_deleted,
            }
    except Exception as e:
        log.error(f"[/stats/clear] Failed to clear stats: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to clear stats: {e}"
        )

# ------------------------------------------------------------------------------
# SSE stream (compact snapshot each second)
# ------------------------------------------------------------------------------

@app.get("/stream/sse")
async def stream_sse():
    """
    Server-Sent Events:
    1 раз/сек отдаём компактный снапшот на основе Worker.diag():
      - pnl_day
      - unrealized
      - positions
      - best
      - по одному последнему трейду на символ
      - auto_signal_enabled, day_limits, block_reasons (для UI)
    """
    w = _worker_required()

    async def eventgen():
        # hint для клиента по reconnect
        yield "retry: 2000\n\n"
        try:
            while True:
                try:
                    d = w.diag() or {}
                    symbols = d.get("symbols", []) or []
                    trades = d.get("trades", {}) or {}

                    trades_last: Dict[str, Any] = {}
                    if isinstance(trades, dict):
                        for sym, arr in trades.items():
                            if not arr:
                                continue
                            last = max(
                                arr,
                                key=lambda x: x.get("closed_ts", x.get("opened_ts", 0)),
                            )
                            trades_last[sym] = {
                                "symbol": last.get("symbol"),
                                "side": last.get("side"),
                                "qty": last.get("qty"),
                                "entry_px": last.get("entry_px"),
                                "exit_px": last.get("exit_px"),
                                "pnl_usd": last.get("pnl_usd"),
                                "pnl_r": last.get("pnl_r"),
                                "reason": last.get("reason"),
                                "opened_ts": last.get("opened_ts"),
                                "closed_ts": last.get("closed_ts"),
                            }

                    payload = {
                        "ts_ms": int(time.time() * 1000),
                        "symbols": symbols,
                        "pnl_day": d.get("pnl_day", {}),
                        "unrealized": d.get("unrealized", {}),
                        "positions": d.get("positions", {}),
                        "best": d.get("best", {}),
                        "trades_last": trades_last,
                        "auto_signal_enabled": bool(d.get("auto_signal_enabled", False)),
                        "day_limits": d.get("day_limits", {}),
                        "block_reasons": d.get("block_reasons", {}),
                    }

                    data = json.dumps(
                        payload,
                        separators=(",", ":"),
                        ensure_ascii=False,
                    )
                    yield f"event: tick\ndata: {data}\n\n"

                except Exception as e:
                    err = json.dumps(
                        {"error": str(e)},
                        separators=(",", ":"),
                        ensure_ascii=False,
                    )
                    yield f"event: error\ndata: {err}\n\n"

                # heartbeat
                yield ":keep-alive\n\n"
                await asyncio.sleep(1.0)

        except asyncio.CancelledError:
            return

    return StreamingResponse(
        eventgen(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )