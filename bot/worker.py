# bot/worker.py
from __future__ import annotations

import asyncio
import contextlib
import logging
import time
import math
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Literal
from collections import deque
from typing import Deque
from contextlib import contextmanager

from exec.fsm import PositionFSM
from bot.core.types import Side as CoreSide  # (может быть неиспользован — ок)

from adapters.binance_ws import BinanceWS
from stream.coalescer import Coalescer, TickerTick
from adapters.binance_rest import PaperAdapter
from exec.executor import Executor, ExecCfg, ExecutionReport
# SL/TP планировщик: сначала пытаемся взять fee-aware, при неудаче — простой
try:
    from exec.sltp import compute_sltp_fee_aware, compute_sltp, SLTPPlan  # type: ignore
except Exception:
    from exec.sltp import compute_sltp, SLTPPlan  # type: ignore
    compute_sltp_fee_aware = None  # type: ignore
try:
    from features.microstructure import MicroFeatureEngine  # type: ignore
except Exception:
    MicroFeatureEngine = None  # type: ignore

try:
    from features.indicators import IndiEngine  # type: ignore
except Exception:
    IndiEngine = None  # type: ignore

# риск-фильтры
from risk.filters import (
    SafetyCfg, RiskCfg,
    MicroCtx, TimeCtx, PositionalCtx, DayState,
    check_entry_safety, check_day_limits,
)

# кандидаты (лёгкая стратегия)
from strategy.candidates import CandidateEngine, Decision

# мягкий импорт настроек — чтобы smoke работал без падений
try:
    from bot.core.config import get_settings  # type: ignore
except Exception:
    get_settings = None  # type: ignore

logger = logging.getLogger(__name__)

Side = Literal["BUY", "SELL"]


# --- вспомогательные структуры ---

@dataclass
class SymbolSpec:
    symbol: str
    price_tick: float
    qty_step: float


class MarketHub:
    """Хранит последнюю лучшую пару bid/ask по символу (потокобезопасно)."""

    def __init__(self, symbols: List[str]) -> None:
        self._best: Dict[str, Tuple[float, float]] = {s: (0.0, 0.0) for s in symbols}
        self._lock = asyncio.Lock()

    async def update(self, t: TickerTick) -> None:
        async with self._lock:
            self._best[t.symbol] = (t.bid, t.ask)

    def best_bid_ask(self, symbol: str) -> Tuple[float, float]:
        # чтение без лока — приемлемо, т.к. обновление атомарно заменяет кортеж
        return self._best.get(symbol, (0.0, 0.0))


@dataclass
class PositionState:
    state: str            # "FLAT" | "ENTERING" | "OPEN" | "EXITING"
    side: Optional[Side]  # BUY/SELL
    qty: float
    entry_px: float
    sl_px: Optional[float]
    tp_px: Optional[float]
    opened_ts_ms: int
    timeout_ms: int       # 3–5 мин


class Worker:
    """
    Оркестратор:
      WS (BinanceWS) -> Coalescer -> MarketHub(best bid/ask) -> Executors(per-symbol, paper)
    Плюс:
      - история тиков (deque per-symbol),
      - FSM позиции per-symbol (PositionFSM),
      - SL/TP план + watchdog (таймаут и защита),
      - учёт сделок и дневной PnL (best-effort),
      - счётчики исполнения (для /metrics),
      - комиссии (maker/taker) в paper-режиме,
      - риск-фильтры (safety + дневные лимиты),
      - авто-сайзинг позиции от процента риска,
      - лёгкие авто-сигналы (кандидаты) с анти-спамом.
    """

    def __init__(
        self,
        symbols: List[str],
        *,
        futures: bool = True,
        coalesce_ms: int = 75,
        history_maxlen: int = 4000,
    ) -> None:
        self.symbols = [s.upper() for s in symbols]
        self.ws = BinanceWS(self.symbols, futures=futures, mark_interval_1s=True)
        self.coal = Coalescer(coalesce_ms=coalesce_ms)
        self.hub = MarketHub(self.symbols)

        self._tp_tol_ticks = getattr(self, "_tp_tol_ticks", 1)               # допуск по тикам
        self._tp_grace_ms  = getattr(self, "_tp_grace_ms", 350)              # сколько ждём лимит перед market
        self._tp_force     = getattr(self, "_tp_force", True)                # включить форс-закрытие

        # история тиков (последние N по символу)
        self._hist: Dict[str, Deque[Dict[str, Any]]] = {s: deque(maxlen=history_maxlen) for s in self.symbols}
        self._history_maxlen = history_maxlen
        self._hist_lock = asyncio.Lock()

        # спецификации шагов (дефолты для перпов)
        self.specs: Dict[str, SymbolSpec] = {
            "BTCUSDT": SymbolSpec("BTCUSDT", price_tick=0.1, qty_step=0.001),
            "ETHUSDT": SymbolSpec("ETHUSDT", price_tick=0.01, qty_step=0.001),
            "SOLUSDT": SymbolSpec("SOLUSDT", price_tick=0.001, qty_step=0.1),
        }
        for s in self.symbols:
            if s not in self.specs:
                self.specs[s] = SymbolSpec(s, price_tick=0.01, qty_step=0.001)

        # executors per symbol
        self.execs: Dict[str, Executor] = {}
        # FSM per symbol
        self._fsm: Dict[str, PositionFSM] = {s: PositionFSM() for s in self.symbols}

        # локи и рантайм-позиции
        self._locks: Dict[str, asyncio.Lock] = {s: asyncio.Lock() for s in self.symbols}
        self._pos: Dict[str, PositionState] = {
            s: PositionState(
                state="FLAT", side=None, qty=0.0, entry_px=0.0,
                sl_px=None, tp_px=None, opened_ts_ms=0, timeout_ms=180_000
            )
            for s in self.symbols
        }

        # счётчики исполнения (для /metrics)
        self._exec_counters: Dict[str, int] = {
            "limit_total": 0,
            "market_total": 0,
            "cancel_total": 0,
            "reject_total": 0,
        }

        # Учёт сделок (in-memory) и дневная сводка (best-effort)
        self._trades: Dict[str, Deque[Dict[str, Any]]] = {s: deque(maxlen=1000) for s in self.symbols}
        self._pnl_day: Dict[str, Any] = {
            "day": self._day_str(),
            "trades": 0,
            "winrate": None,
            "avg_r": None,
            "pnl_r": 0.0,
            "pnl_usd": 0.0,
            "max_dd_r": None,
        }
        self._pnl_r_equity: float = 0.0
        self._consec_losses: int = 0

        # ts последнего стоп-лосса (для локального cooldown_after_sl_s)
        self._last_sl_ts_ms: int = 0

        # причины блокировок входа
        self._block_reasons: Dict[str, int] = {}

        # для оценки комиссий входа: сохраняем steps открытия
        self._entry_steps: Dict[str, List[str]] = {}

        # риск-конфиги
        self._safety_cfg = self._load_safety_cfg()
        self._risk_cfg = self._load_risk_cfg()
        self._server_time_offset_ms: int = 0

        # account-конфиги (для авто-сайзинга)
        self._starting_equity_usd = self._load_starting_equity_usd()
        self._leverage = self._load_leverage()
        self._min_notional_usd = self._load_min_notional_usd()

        # --- strategy / auto-signal runtime (driven by settings.json) ---
        self._auto_enabled: bool = False
        self._auto_cooldown_ms: int = 2000   # пауза между авто-входами (ms)
        self._auto_min_flat_ms: int = 750    # минимум FLAT до нового входа (ms)

        # пер-символьные таймштампы для анти-спама сигналов
        self._last_signal_ms: Dict[str, int] = {s: 0 for s in self.symbols}
        self._last_flat_ms: Dict[str, int] = {s: 0 for s in self.symbols}
        # метка первого «касания TP» (per-symbol), чтобы дать лимиту шанс и затем форс-закрыть
        self._tp_first_hit_ms: Dict[str, int] = {s: 0 for s in self.symbols}

        # движок кандидатов (пер-симв.), инициализируем позже
        self._cand: Dict[str, CandidateEngine] = {}

        # фичи микроструктуры и индикаторов per-symbol
        self._micro_eng: Dict[str, MicroFeatureEngine] = {}
        self._indi_eng: Dict[str, IndiEngine] = {}
        self._last_micro: Dict[str, Dict[str, Any]] = {}
        self._last_indi: Dict[str, Dict[str, Any]] = {}

        # подгрузка начальных значений из settings.strategy
        self._load_strategy_cfg()

        # таски жизненного цикла
        self._tasks: List[asyncio.Task] = []
        self._wd_task: Optional[asyncio.Task] = None
        self._strat_task: Optional[asyncio.Task] = None
        self._started = False

    # ---------- lifecycle ----------

    async def start(self) -> None:
        if self._started:
            return
        self._started = True

        def _best(sym: str) -> tuple[float, float]:
            return self.hub.best_bid_ask(sym)

        # --- base defaults ---
        limit_offset_ticks = 1
        limit_timeout_ms = 500
        time_in_force = "GTC"
        max_slippage_bp = 6.0
        poll_ms = 50
        fee_bps_maker = 2.0
        fee_bps_taker = 4.0
        prefer_maker = False

        # --- read settings once (if available) ---
        s = None
        exec_cfg = None
        if get_settings:
            try:
                s = get_settings()
            except Exception:
                s = None

        if s is not None:
            exec_cfg = getattr(s, "execution", None)
            if exec_cfg is not None:
                limit_offset_ticks = int(getattr(exec_cfg, "limit_offset_ticks", limit_offset_ticks))
                limit_timeout_ms   = int(getattr(exec_cfg, "limit_timeout_ms",   limit_timeout_ms))
                max_slippage_bp    = float(getattr(exec_cfg, "max_slippage_bp",  max_slippage_bp))
                time_in_force      = str(getattr(exec_cfg, "time_in_force",      time_in_force))
                poll_ms            = int(getattr(exec_cfg, "poll_ms",            poll_ms))
                fee_bps_maker      = float(getattr(exec_cfg, "fee_bps_maker",    fee_bps_maker))
                fee_bps_taker      = float(getattr(exec_cfg, "fee_bps_taker",    fee_bps_taker))
                prefer_maker       = bool(getattr(exec_cfg, "prefer_maker",      prefer_maker))

            # авто-сигналы (обратная совместимость)
            strat_cfg = getattr(s, "strategy", None)
            if strat_cfg is not None:
                self._auto_enabled     = bool(getattr(strat_cfg, "auto_signal_enabled", self._auto_enabled))
                self._auto_cooldown_ms = int(getattr(strat_cfg, "auto_cooldown_ms",     self._auto_cooldown_ms))
                self._auto_min_flat_ms = int(getattr(strat_cfg, "auto_min_flat_ms",     self._auto_min_flat_ms))

        # prefer maker if post-only TIF requested
        tif_upper = (time_in_force or "").upper()
        if tif_upper in ("GTX", "PO", "POST_ONLY"):
            prefer_maker = True

        # --- persist flat exec cfg used by helpers (after settings are applied) ---
        try:
            min_stop_ticks_cfg = int(getattr(exec_cfg, "min_stop_ticks", self._min_stop_ticks_default())) if exec_cfg else self._min_stop_ticks_default()
        except Exception:
            min_stop_ticks_cfg = self._min_stop_ticks_default()

        self._exec_cfg = {
            "min_stop_ticks": int(min_stop_ticks_cfg),
            "limit_timeout_ms": int(limit_timeout_ms),
            "time_in_force": str(time_in_force),
        }

        # --- init executors & per-symbol engines ---
        for sym in self.symbols:
            spec = self.specs[sym]

            cfg = ExecCfg(
                price_tick=spec.price_tick,
                qty_step=spec.qty_step,
                limit_offset_ticks=limit_offset_ticks,
                limit_timeout_ms=limit_timeout_ms,
                time_in_force=time_in_force,
                poll_ms=poll_ms,
                prefer_maker=prefer_maker,
                fee_bps_maker=fee_bps_maker,
                fee_bps_taker=fee_bps_taker,
            )

            adapter = PaperAdapter(_best, max_slippage_bp=max_slippage_bp)
            self.execs[sym] = Executor(adapter=adapter, cfg=cfg, get_best=_best)

            # стратегия и фичи по символу
            self._cand[sym] = self._make_candidate_engine(sym, spec)

            if MicroFeatureEngine is not None:
                self._micro_eng[sym] = MicroFeatureEngine(
                    tick_size=spec.price_tick,
                    lot_usd=50_000.0,  # можно вынести в конфиг
                )
            if IndiEngine is not None:
                self._indi_eng[sym] = IndiEngine(price_step=spec.price_tick)

        # --- start streams/tasks ---
        await self.ws.connect(self._on_ws_raw)
        self._tasks.append(asyncio.create_task(self.coal.run(self._on_tick), name="coal.run"))

        self._wd_task = asyncio.create_task(self._watchdog_loop(), name="watchdog")
        self._strat_task = asyncio.create_task(self._strategy_loop(), name="strategy")

    async def stop(self) -> None:
        if not self._started:
            return
        self._started = False

        # Остановить стратегию
        if self._strat_task:
            self._strat_task.cancel()
            with contextlib.suppress(Exception):
                await self._strat_task
            self._strat_task = None

        # Остановить watchdog
        if self._wd_task:
            self._wd_task.cancel()
            with contextlib.suppress(Exception):
                await self._wd_task
            self._wd_task = None

        # Остановить коалесцер и WS
        with contextlib.suppress(Exception):
            await self.coal.close()
        with contextlib.suppress(Exception):
            await self.ws.close()

        # Погасить фоновые таски
        for t in self._tasks:
            t.cancel()
        with contextlib.suppress(Exception):
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

    # ---------- pipeline handlers ----------

    async def _on_ws_raw(self, raw: Dict[str, Any]) -> None:
        await self.coal.on_event(raw)

    async def _on_tick(self, tick: TickerTick) -> None:
        # 1) обновляем best bid/ask
        await self.hub.update(tick)
        # 2) сохраняем в историю (компактный dict)
        item = {
            "symbol": tick.symbol,
            "ts_ms": tick.ts_ms,
            "price": tick.price,
            "bid": tick.bid,
            "ask": tick.ask,
            "bid_size": tick.bid_size,
            "ask_size": tick.ask_size,
            "mark_price": tick.mark_price,
        }
        async with self._hist_lock:
            dq = self._hist.get(tick.symbol)
            if dq is not None:
                dq.append(item)
            else:
                self._hist[tick.symbol] = deque([item], maxlen=self._history_maxlen)

        # фиксируем время последнего FLAT — нужно для анти-спама стратегии
        pos = self._pos[tick.symbol]
        if pos.state == "FLAT":
            self._last_flat_ms[tick.symbol] = tick.ts_ms

        # обогащаем микроструктурой и индикаторами (best-effort)
        try:
            me = self._micro_eng.get(tick.symbol)
            ie = self._indi_eng.get(tick.symbol)

            if me is not None:
                mf = me.update(
                    price=float(tick.price or 0.0),
                    bid=float(tick.bid or 0.0),
                    ask=float(tick.ask or 0.0),
                    bid_sz=float(tick.bid_size or 0.0),
                    ask_sz=float(tick.ask_size or 0.0),
                )
                self._last_micro[tick.symbol] = {
                    "spread_ticks": float(mf.spread_ticks),
                    "mid": float(mf.mid),
                    "top_liq_usd": float(mf.top_liq_usd),
                    "microprice_drift": float(mf.microprice_drift),
                    "tick_velocity": float(mf.tick_velocity) if mf.tick_velocity is not None else None,
                    "obi": float(mf.obi),
                }

            if ie is not None:
                ii = ie.update(price=float(tick.price or 0.0))
                self._last_indi[tick.symbol] = {
                    "ema9": float(ii.ema9),
                    "ema21": float(ii.ema21),
                    "ema_slope_ticks": float(ii.ema_slope_ticks),
                    "vwap_drift": float(ii.vwap_drift),
                    "bb_z": float(ii.bb_z),
                    "rsi": float(ii.rsi),
                    "realized_vol_bp": float(ii.realized_vol_bp),
                }
        except Exception:
            # индикаторы не критичны — не роняем пайплайн
            pass

    # ---------- strategy loop (авто-сигналы) ----------

    async def _strategy_loop(self) -> None:
        """
        Auto scalper (time-based windows, sane anti-chop):

        - Windows based on time (robust to coalescer rate): long≈7s, short≈2s
        - Anti-chop passes if EITHER absolute 1 tick move OR ~0.6 bps drift exists
        - Momentum breakout and gentle reversion bounce
        - Safety filters (spread/liquidity/funding/liq-buffer) preserved
        """
        import asyncio, logging, math, time as _t
        log = logging.getLogger(__name__ + "._strategy")

        symbols = list(self.symbols)
        if not symbols:
            log.warning("[strategy] no symbols configured, exiting _strategy_loop")
            return

        # timings / basic pacing
        cooldown_ms = max(250, int(getattr(self, "_auto_cooldown_ms", 1200)))
        min_flat_ms = max(0, int(getattr(self, "_auto_min_flat_ms", 300)))
        base_delay_s = max(0.09, cooldown_ms / 4000.0)

        # time-based windows & thresholds
        warmup_ms = 15_000
        stale_ms  = 1_500
        long_ms   = 5_000      # было 7s → 5s
        short_ms  = 1_400       # ~2s short MA

        # momentum thresholds (slightly looser so signals occur)
        ma_edge_mom     = 0.00006   # ~0.6 bps
        slope_edge_mom  = 0.000006   # short MA slope

        # reversion thresholds (gentle edge)
        ma_edge_rev     = 0.00003  # ~0.5 bps

        # anti-chop: pass if EITHER absolute tick range or small bps drift is present
        min_range_bps    = 0.4
        min_range_ticks  = 1

        last_entry_ts = {sym: 0 for sym in symbols}

        def _now_ms() -> int:
            try:
                return int(self._now_ms())
            except Exception:
                return int(_t.time() * 1000)

        def _inc_block(reason: str) -> None:
            try:
                self._inc_block_reason(reason)
            except Exception:
                pass

        def _price_step(sym: str) -> float:
            try:
                spec = (self.specs or {}).get(sym)
                if not spec:
                    return 0.1
                for name in ("price_step", "price_tick", "tick_size", "tickSize"):
                    v = getattr(spec, name, None)
                    if isinstance(v, (int, float)) and v > 0:
                        return float(v)
            except Exception:
                pass
            return 0.1

        def _load_best(sym: str, best_all) -> tuple[float, float]:
            bid = ask = 0.0
            b = best_all.get(sym) if isinstance(best_all, dict) else None
            if isinstance(b, (list, tuple)) and len(b) >= 2:
                bid, ask = float(b[0] or 0.0), float(b[1] or 0.0)
            elif isinstance(b, dict):
                bid, ask = float(b.get("bid") or 0.0), float(b.get("ask") or 0.0)
            if bid <= 0 or ask <= 0:
                try:
                    bid, ask = self.best_bid_ask(sym)
                except Exception:
                    bid = ask = 0.0
            return float(bid or 0.0), float(ask or 0.0)

        # risk filters (optional soft import already available module-wide)
        try:
            from risk.filters import MicroCtx, TimeCtx, PositionalCtx, check_entry_safety
            _HAS_FILTERS = True
        except Exception:
            MicroCtx = TimeCtx = PositionalCtx = check_entry_safety = None  # type: ignore
            _HAS_FILTERS = False

        # spread limit default (prefer strict unless cfg present)
        try:
            cfg_spread_ticks = float(getattr(self._safety_cfg, "max_spread_ticks", 4.0))
            max_spread_ticks_default = 4.0 if not math.isfinite(cfg_spread_ticks) or cfg_spread_ticks <= 0 else cfg_spread_ticks
        except Exception:
            max_spread_ticks_default = 4.0
        # адресное послабление спред-порога для SOL (чуть ступенчатый актив)
        per_symbol_spread_max = {
            "SOLUSDT": max_spread_ticks_default + 2.0
        }

        try:
            while True:
                if not bool(getattr(self, "_auto_enabled", False)):
                    await asyncio.sleep(0.3)
                    continue

                now = _now_ms()
                safety_cfg = getattr(self, "_safety_cfg", None)
                # global cooldown after SL
                try:
                    cfg = self._risk_cfg
                    if cfg.cooldown_after_sl_s > 0 and self._last_sl_ts_ms:
                        if now - self._last_sl_ts_ms < cfg.cooldown_after_sl_s * 1000:
                            _inc_block("cooldown_after_sl")
                            await asyncio.sleep(base_delay_s)
                            continue
                except Exception:
                    pass

                # diag snapshot
                try:
                    d = self.diag() or {}
                except Exception:
                    d = {}

                # day limits
                day_limits = d.get("day_limits") or {}
                if not bool(day_limits.get("can_trade", True)):
                    reasons = day_limits.get("reasons") or []
                    if reasons:
                        _inc_block("day_limits:" + ",".join(map(str, reasons)))
                    await asyncio.sleep(1.0)
                    continue

                positions    = d.get("positions") or {}
                best_all     = d.get("best") or {}
                history_meta = d.get("history") or {}

                for sym in symbols:
                    try:
                        # skip if not flat enough
                        pos = positions.get(sym) or {}
                        st = str(pos.get("state", "")).upper()
                        if st in ("OPEN", "ENTERING", "EXITING"):
                            continue

                        if now - last_entry_ts[sym] < cooldown_ms:
                            continue

                        last_flat = int(self._last_flat_ms.get(sym, 0) or 0)
                        if last_flat and now - last_flat < min_flat_ms:
                            continue

                        # best bid/ask & spread gate
                        bid, ask = _load_best(sym, best_all)
                        if bid <= 0 or ask <= 0:
                            _inc_block(f"{sym}:no_best")
                            continue

                        mid = (bid + ask) / 2.0
                        if mid <= 0:
                            _inc_block(f"{sym}:bad_mid")
                            continue

                        step = _price_step(sym)
                        spread = max(0.0, ask - bid)
                        sprticks = (spread / step) if step > 0 else 0.0
                        spread_bps = (spread / mid * 1e4) if mid > 0 else 0.0

                        max_spread_ticks = max_spread_ticks_default
                        max_spread_ticks_sym = per_symbol_spread_max.get(sym, max_spread_ticks_default)
                        max_spread_ticks_sym = per_symbol_spread_max.get(sym, max_spread_ticks_default)
                        if sprticks > max_spread_ticks_sym and spread_bps > 5.0:
                            _inc_block(f"{sym}:spread>{max_spread_ticks:.1f}t")
                            continue

                        # history freshness
                        hmeta = history_meta.get(sym) or {}
                        latest_ts = int(hmeta.get("latest_ts_ms") or 0)
                        if not latest_ts or now - latest_ts > stale_ms:
                            _inc_block(f"{sym}:stale_history_meta")
                            continue

                        try:
                            ticks = self.history_ticks(sym, since_ms=now - warmup_ms, limit=800)
                        except Exception:
                            _inc_block(f"{sym}:no_history")
                            continue

                        # build aligned arrays
                        prices: list[float] = []
                        ts_list: list[int] = []
                        vols: list[float] = []
                        for t in (ticks or []):
                            if isinstance(t, dict):
                                ts = int(t.get("ts_ms") or t.get("ts") or 0)
                                px = t.get("price")
                                if px is None:
                                    b_ = t.get("bid"); a_ = t.get("ask")
                                    if b_ and a_:
                                        px = (float(b_) + float(a_)) / 2.0
                                vol = t.get("volume") or t.get("qty") or t.get("size") or 0.0
                            else:
                                ts = int(getattr(t, "ts_ms", getattr(t, "ts", 0)) or 0)
                                px = getattr(t, "price", None)
                                if px is None:
                                    b_ = getattr(t, "bid", None); a_ = getattr(t, "ask", None)
                                    if b_ and a_:
                                        px = (float(b_) + float(a_)) / 2.0
                                vol = getattr(t, "volume", getattr(t, "qty", getattr(t, "size", 0.0)))
                            if ts > 0 and px is not None:
                                ts_list.append(ts)
                                prices.append(float(px))
                                try:
                                    vols.append(float(vol) if vol is not None else 0.0)
                                except Exception:
                                    vols.append(0.0)

                        if len(prices) < 24:
                            _inc_block(f"{sym}:skip_warmup")
                            continue
                        if ts_list and now - ts_list[-1] > stale_ms:
                            _inc_block(f"{sym}:stale_tick")
                            continue

                        # time-slicing helpers
                        def slice_since(ms: int) -> list[float]:
                            if not ts_list:
                                return []
                            lo = ts_list[-1] - ms
                            return [p for p, ts in zip(prices, ts_list) if ts >= lo]

                        _short = slice_since(short_ms)
                        _long  = slice_since(long_ms)

                        if len(_short) < 6 or len(_long) < 12:
                            _inc_block(f"{sym}:skip_warmup")
                            continue

                        short_ma = sum(_short) / len(_short)
                        long_ma  = sum(_long) / len(_long)
                        if long_ma <= 0:
                            _inc_block(f"{sym}:skip_no_baseline")
                            continue

                        # local range from long window
                        local_high = max(_long)
                        local_low  = min(_long)
                        range_px   = max(0.0, local_high - local_low)

                        # anti-chop: require either ≥1 tick OR ≥0.6 bps
                        min_range_px_ticks = step * float(min_range_ticks)
                        min_range_px_bps   = mid * (min_range_bps / 10_000.0)
                        if not (range_px >= min_range_px_ticks or range_px >= min_range_px_bps):
                            _inc_block(f"{sym}:skip_chop_range")
                            continue

                        # slope via older vs newer chunk inside short window
                        k = max(4, int(0.4 * len(_short)))
                        prev_short_ma = sum(_short[:-k]) / max(1, len(_short) - k) if len(_short) > k else short_ma

                        last_px   = prices[-1]
                        diff_rel  = (short_ma - long_ma) / long_ma
                        slope_rel = (short_ma - prev_short_ma) / prev_short_ma if prev_short_ma > 0 else 0.0

                        # stop distance (vol-aware)
                        try:
                            sl_distance_px = float(self._compute_vol_stop_distance_px(sym, mid_px=mid))
                        except Exception:
                            # sigma fallback
                            if len(_long) >= 20:
                                m = sum(_long[-20:]) / 20.0
                                var = sum((p - m) ** 2 for p in _long[-20:]) / 20.0
                                sigma = math.sqrt(var)
                            else:
                                sigma = range_px / 6.0 if range_px > 0 else step * min_range_ticks
                            sl_distance_px = max(step * float(min_range_ticks), 1.2 * sigma, spread * 2.0)

                        if sl_distance_px <= 0:
                            _inc_block(f"{sym}:bad_sl_distance")
                            continue

                        # trigger zones within range
                        upper_trigger = local_high - 0.5 * sl_distance_px
                        lower_trigger = local_low  + 0.5 * sl_distance_px

                        side: str | None = None
                        signal_kind: str | None = None  # "momentum" | "reversion"

                        # MOMENTUM breakout
                        if (
                            last_px >= upper_trigger
                            and diff_rel > ma_edge_mom
                            and slope_rel > slope_edge_mom
                        ):
                            side = "BUY"; signal_kind = "momentum"
                        elif (
                            last_px <= lower_trigger
                            and diff_rel < -ma_edge_mom
                            and slope_rel < -slope_edge_mom
                        ):
                            side = "SELL"; signal_kind = "momentum"

                        # REVERSION bounce (only if trend not strong)
                        if side is None:
                            trend_strong = abs(diff_rel) > ma_edge_mom * 2.0
                            if not trend_strong:
                                edge_zone = 0.25 * range_px
                                if (
                                    last_px <= local_low + edge_zone
                                    and diff_rel > ma_edge_rev
                                    and slope_rel > 0.0
                                ):
                                    side = "BUY"; signal_kind = "reversion"
                                elif (
                                    last_px >= local_high - edge_zone
                                    and diff_rel < -ma_edge_rev
                                    and slope_rel < 0.0
                                ):
                                    side = "SELL"; signal_kind = "reversion"
                        
                        # --- Fast micro-momentum (если фичи доступны) ---
                        if side is None and self._last_micro.get(sym):
                            lm = self._last_micro.get(sym) or {}
                            drift = float(lm.get("microprice_drift") or 0.0)
                            tvel  = float(lm.get("tick_velocity") or 0.0)
                            spread_ticks_now = sprticks

                            # условия: маленький спред, явный дрейф и тиковая скорость
                            if spread_ticks_now <= max_spread_ticks_default and abs(drift) > 0.35 and abs(tvel) > 0.6:
                                if drift > 0 and tvel > 0:
                                    side = "BUY";  signal_kind = "momentum"
                                elif drift < 0 and tvel < 0:
                                    side = "SELL"; signal_kind = "momentum"

                        if side is None:
                            _inc_block(f"{sym}:skip_no_signal")
                            continue

                        # light relative volume filter
                        if vols and sum(vols) > 0:
                            base_slice = vols[:-5] if len(vols) > 12 else vols
                            recent_vol = sum(vols[-5:])
                            base_vol   = (sum(base_slice) / max(1, len(base_slice))) if base_slice else 0.0
                            if base_vol > 0 and recent_vol < 0.3 * base_vol:
                                _inc_block(f"{sym}:skip_low_volume")
                                continue

                        # risk.filters safety pre-check
                        if _HAS_FILTERS and safety_cfg is not None:
                            try:
                                lm = (self._last_micro.get(sym, {}) or {})
                                eff_liq = float(lm.get("top5_liq_usd") or 0.0)
                                if eff_liq <= 0:
                                    tl = float(lm.get("top_liq_usd") or 0.0)
                                    eff_liq = tl * 3.0 if tl > 0 else 0.0

                                micro_ctx = MicroCtx(
                                    spread_ticks=float(sprticks),
                                    top5_liq_usd=float(eff_liq if eff_liq and eff_liq > 0 else 1e9),
                                )
                                tctx = TimeCtx(
                                    ts_ms=now,
                                    server_time_offset_ms=getattr(self, "_server_time_offset_ms", 0),
                                )
                                sl_px = (mid - sl_distance_px) if side == "BUY" else (mid + sl_distance_px)
                                pos_ctx = PositionalCtx(
                                    entry_px=float(mid),
                                    sl_px=float(sl_px),
                                    leverage=float(getattr(self, "_leverage", 10.0)),
                                )
                                dec = check_entry_safety(side, micro=micro_ctx, time_ctx=tctx, pos_ctx=pos_ctx, safety=safety_cfg)
                                if not dec.allow:
                                    _inc_block(f"{sym}:safety_pre:" + ",".join(dec.reasons))
                                    continue
                            except Exception:
                                log.exception("[strategy] safety check failed for %s", sym)

                        # ENTRY: route by kind (momentum→IOC, reversion→maker)
                        try:
                            rep = await self._place_entry_with_kind(sym, side, signal_kind, sl_distance_px, mid, bid, ask)
                        except Exception as e:
                            log.exception("[strategy] place_entry failed for %s %s: %s", sym, side, e)
                            _inc_block(f"{sym}:entry_error")
                            continue

                        if not isinstance(rep, dict) or not rep.get("ok", False):
                            reason = (rep or {}).get("reason", "rejected")
                            _inc_block(f"{sym}:entry_rejected_{reason}")
                            continue

                        last_entry_ts[sym] = now

                    except Exception:
                        log.exception("[strategy] symbol-loop error for %s", sym)
                        _inc_block(f"{sym}:loop_error")
                        continue

                await asyncio.sleep(base_delay_s)

        except asyncio.CancelledError:
            log.info("[strategy] _strategy_loop canceled")
            return
        except Exception:
            log.exception("[strategy] outer-loop error")
            await asyncio.sleep(1.0)
    
    def _compute_sl_distance_px(self, symbol: str, spread_ticks: float) -> float:
        """
        Плановая дистанция до SL для сайзинга и нормализации:
        - не ставим стоп внутри спреда/шума,
        - учитываем тики, спред, волу и минимум в bps.
        """
        spec = self.specs[symbol]
        tick = float(spec.price_tick) or 0.1

        # минимум по тикам
        base_min_ticks = max(self._min_stop_ticks_default(), 8.0)
        min_ticks_px = base_min_ticks * tick

        # от спреда: не меньше 1.5 * спреда и 2 тиков
        if spread_ticks > 0.0:
            spread_px = max(spread_ticks * tick * 1.5, 2.0 * tick)
        else:
            spread_px = 2.0 * tick

        # волатильность за 60s
        vol_px = 0.0
        sigma = self._recent_volatility(symbol, window_ms=60_000)
        if sigma > 0.0:
            vol_px = 0.6 * float(sigma)

        # минимум в bps: 0.12% (12 bps)
        last = self.latest_tick(symbol) or {}
        price = float(last.get("price") or 0.0)
        min_sl_bps = 12.0  # можно вынести в конфиг
        bps_px = price * (min_sl_bps / 10_000.0) if price > 0.0 else 0.0

        sl_px = max(min_ticks_px, spread_px, vol_px, bps_px)
        if sl_px <= 0.0:
            sl_px = 10.0 * tick

        return float(sl_px)
    
    def _build_sltp_plan(
        self,
        symbol: str,
        side: Side,
        entry_px: float,
        qty: float,
        *,
        micro: Optional[Dict[str, Any]] = None,
        indi: Optional[Dict[str, Any]] = None,
        decision: Optional[Any] = None,
    ) -> SLTPPlan:
        """
        Единая точка расчёта SL/TP (fee-aware, с безопасными фолбэками).

        Шаги:
        - считаем базовую дистанцию до SL через _compute_sl_distance_px (тик/спред/вола/bps);
        - выбираем целевой RR по типу сигнала (_tp_rr_from_decision);
        - строим план через compute_sltp_fee_aware (если доступен), иначе compute_sltp;
        - нормализуем финальные уровни через _normalize_sltp (округления, минимумы).
        """
        symbol = symbol.upper()
        spec = self.specs[symbol]
        tick = float(spec.price_tick) or 0.1

        # best bid/ask -> спред (в bps и в px)
        bid, ask = self.hub.best_bid_ask(symbol)
        spread_px = max(0.0, (ask - bid))
        spread_bps = 0.0
        if bid > 0.0 and ask > 0.0 and (bid + ask) > 0.0:
            mid = (bid + ask) / 2.0
            if mid > 0.0:
                spread_bps = (spread_px / mid) * 1e4

        # базовая дистанция до SL
        # (учитывает тик, спред, волу и минимум в bps)
        # используем spread_ticks только как хинт, реальная логика внутри метода
        sl_distance_px = float(self._compute_sl_distance_px(symbol, spread_px / max(tick, 1e-12)))
        if sl_distance_px <= 0.0:
            sl_distance_px = max(tick, abs(entry_px) * 0.0008)  # резерв

        # целевой RR
        rr = float(self._tp_rr_from_decision(decision))

        # комиссии
        maker_bps, taker_bps = self._fees_for_symbol(symbol)
        # консервативная добавка к выходу: половина спреда в bps
        addl_exit_bps = 0.5 * spread_bps

        # --- построение плана ---
        try:
            if compute_sltp_fee_aware:
                base_plan = compute_sltp_fee_aware(
                    side=side,
                    entry_px=float(entry_px),
                    qty=float(qty),
                    price_tick=tick,
                    sl_distance_px=sl_distance_px,
                    rr_target=rr,
                    maker_bps=maker_bps,
                    taker_bps=taker_bps,
                    entry_taker_like=True,   # вход считаем как taker
                    exit_taker_like=True,    # выход считаем как taker
                    addl_exit_bps=float(addl_exit_bps),
                    # полы/минимумы
                    min_stop_ticks=max(int(self._min_stop_ticks_default()), 8),
                    spread_px=spread_px,
                    spread_mult=1.5,
                    min_sl_bps=12.0,
                    # требуем минимальный net-RR (после комиссий/спреда)
                    min_net_rr=1.2,
                    allow_expand_tp=True,
                    sl_px_hint=None,
                )
            else:
                base_plan = compute_sltp(
                    side=side,
                    entry_px=float(entry_px),
                    qty=float(qty),
                    price_tick=tick,
                    sl_distance_px=sl_distance_px,
                    rr=rr,
                )
        except Exception as e:
            logger.warning("compute_sltp failed for %s (%s), using fallback", symbol, e)
            if side == "BUY":
                sl_px = entry_px - sl_distance_px
                tp_px = entry_px + rr * sl_distance_px
            else:
                sl_px = entry_px + sl_distance_px
                tp_px = entry_px - rr * sl_distance_px
            base_plan = SLTPPlan(
                sl_px=float(sl_px),
                tp_px=float(tp_px),
                sl_qty=float(qty),
                tp_qty=float(qty),
            )

        # финальная нормализация (округления/минимумы/стороны)
        plan = self._normalize_sltp(
            symbol=symbol,
            side=side,
            entry_px=float(entry_px),
            plan=base_plan,
            decision=decision,
        )
        return plan
    
    def _tp_rr_from_decision(self, decision: Optional[Decision]) -> float:
        """
        Выбор целевого RR для TP по типу сигнала.
        Без информации возвращает дефолт 1.5R.
        """
        try:
            kind = (getattr(decision, "kind", "") or "").lower()
        except Exception:
            kind = ""

        if "rev" in kind or "mean" in kind or "revert" in kind or "bounce" in kind:
            return 1.3
        if "mom" in kind or "break" in kind or "trend" in kind:
            return 1.6
        return 1.5

    def _normalize_sltp(
        self,
        symbol: str,
        side: Side,
        entry_px: float,
        plan: SLTPPlan,
        decision: Optional[Decision] = None,
    ) -> SLTPPlan:
        """
        Нормализуем SL/TP:

        - SL на адекватном расстоянии (через _compute_sl_distance_px),
        - SL с правильной стороны,
        - TP минимум с заданным RR,
        - приводим к шагу цены,
        - сохраняем sl_qty/tp_qty из исходного плана, если они есть.
        """
        spec = self.specs[symbol]
        tick = float(spec.price_tick)

        # исходные qty из плана, если заданы
        sl_qty = getattr(plan, "sl_qty", None)
        tp_qty = getattr(plan, "tp_qty", None)

        bid, ask = self.hub.best_bid_ask(symbol)
        spread_ticks = 0.0
        if tick > 0 and bid > 0 and ask > 0:
            spread_ticks = max(0.0, (ask - bid) / tick)

        min_sl_px = self._compute_sl_distance_px(symbol, spread_ticks)

        sl = plan.sl_px
        tp = plan.tp_px

        def round_down(x: float) -> float:
            return math.floor(x / tick) * tick

        def round_up(x: float) -> float:
            return math.ceil(x / tick) * tick

        rr = self._tp_rr_from_decision(decision)

        if side == "BUY":
            # SL: ниже входа и не ближе, чем min_sl_px
            if sl is None or sl >= entry_px or (entry_px - sl) < min_sl_px:
                sl = entry_px - min_sl_px
            sl = round_down(sl)
            if sl <= 0.0:
                sl = round_down(entry_px - min_sl_px)

            # TP: если нет или неправильный — ставим по RR
            if tp is None or tp <= entry_px:
                dist = max(entry_px - sl, min_sl_px)
                tp = entry_px + rr * dist
            tp = round_up(tp)

        else:  # SELL
            # SL: выше входа и не ближе, чем min_sl_px
            if sl is None or sl <= entry_px or (sl - entry_px) < min_sl_px:
                sl = entry_px + min_sl_px
            sl = round_up(sl)

            # TP: если нет или неправильный — ставим по RR
            if tp is None or tp >= entry_px:
                dist = max(sl - entry_px, min_sl_px)
                tp = entry_px - rr * dist
            tp = round_down(tp)

        return SLTPPlan(
            sl_px=float(sl),
            tp_px=float(tp),
            sl_qty=sl_qty,
            tp_qty=tp_qty,
        )
    
    # --- TP detection & force close ----------------------------------------------

    def _tp_hit(self, side: str, tp_px: float, bid: float, ask: float, price_tick: float, tol_ticks: int = 1) -> bool:
        """
        Считаем TP-хит по исполнимой стороне:
        LONG → можем выйти по bid >= tp;  SHORT → по ask <= tp.
        Добавляем допуск tol_ticks*tick, чтобы не зависеть от квантования цены.
        """
        if not tp_px or tp_px <= 0:
            return False
        tol = max(0.0, float(tol_ticks) * float(max(price_tick, 0.0)))
        if side == "BUY":
            return (bid or 0.0) >= (tp_px - tol)
        else:
            return (ask or 0.0) <= (tp_px + tol)

    async def _force_flatten_after_tp(self, symbol: str, side: str, qty: float) -> dict:
        """
        Аварийное закрытие позиции рыночным reduceOnly (если лимит не исполнился быстро).
        """
        try:
            # если есть специальная ручка — используй её; иначе fallback на flatten()
            close_side = "SELL" if side == "BUY" else "BUY"
            # предпочтительно reduceOnly MARKET:
            place_reduce = getattr(self, "place_reduce_only_market", None)
            if callable(place_reduce):
                return await place_reduce(symbol, close_side, float(qty))
        except Exception:
            pass
        # фолбэк
        return await self.flatten(symbol)
    
    # ---------- public ops ----------

    def best_bid_ask(self, symbol: str) -> Tuple[float, float]:
        return self.hub.best_bid_ask(symbol)

    def latest_tick(self, symbol: str) -> Optional[Dict[str, Any]]:
        dq = self._hist.get(symbol.upper())
        if not dq:
            return None
        return dq[-1] if len(dq) > 0 else None

    def history_ticks(self, symbol: str, since_ms: int, limit: int) -> List[Dict[str, Any]]:
        sym = symbol.upper()
        dq = self._hist.get(sym)
        if not dq:
            return []
        result: List[Dict[str, Any]] = [t for t in dq if t.get("ts_ms", 0) >= since_ms]
        if len(result) > limit:
            result = result[-limit:]
        return result

    # --- ВХОДЫ ---
    @contextmanager
    def _temporary_exec_overrides(
        self,
        sym: str,
        *,
        time_in_force: Optional[str] = None,
        prefer_maker: Optional[bool] = None,
        limit_offset_ticks: Optional[int] = None,
    ):
        """
        Temporarily override per-symbol Executor config fields for one call.
        Safe for single-threaded worker context.
        """
        ex = self.execs.get(sym)
        if ex is None or not hasattr(ex, "c"):
            yield
            return

        c = ex.c
        old_tif = getattr(c, "time_in_force", None)
        old_pm  = getattr(c, "prefer_maker", None)
        old_lo  = getattr(c, "limit_offset_ticks", None)

        try:
            if time_in_force is not None:
                c.time_in_force = str(time_in_force)
            if prefer_maker is not None:
                c.prefer_maker = bool(prefer_maker)
            if limit_offset_ticks is not None:
                try:
                    c.limit_offset_ticks = int(limit_offset_ticks)
                except Exception:
                    pass
            yield
        finally:
            with contextlib.suppress(Exception):
                if time_in_force is not None and old_tif is not None:
                    c.time_in_force = old_tif
                if prefer_maker is not None and old_pm is not None:
                    c.prefer_maker = old_pm
                if limit_offset_ticks is not None and old_lo is not None:
                    c.limit_offset_ticks = old_lo
    
    async def place_entry(
        self,
        symbol: str,
        side: Side,
        qty: float,
        limit_offset_ticks: Optional[int] = None,
        *,
        sl_distance_px: Optional[float] = None,
        sl_px: Optional[float] = None,
        tp_px: Optional[float] = None,
    ) -> Dict[str, Any]:
        """
        Unified entry:
        - Accepts optional limit_offset_ticks (temporary override for this call).
        - If sl_px/tp_px are not given, computes them.
            If sl_distance_px is provided → use it for planning; else use volatility-based distance.
        - Enforces qty step & min notional.
        Compatible with older callers that passed (symbol, side, qty) or (symbol, side, qty, sl_px, tp_px).
        """
        sym = symbol.upper()
        s = side.upper()
        if s not in ("BUY", "SELL"):
            return {"ok": False, "reason": "bad_side"}

        ex = self.execs.get(sym)
        if ex is None:
            return {"ok": False, "reason": "no_executor"}

        # steps for later fee accounting
        self._entry_steps.pop(sym, None)

        # best bid/ask for mid/guards
        bid, ask = self.best_bid_ask(sym)
        if bid <= 0 or ask <= 0:
            return {"ok": False, "reason": "no_best_bid_ask"}
        entry_px = (bid + ask) / 2.0
        if entry_px <= 0:
            return {"ok": False, "reason": "bad_entry_px"}

        # qty step & min notional
        spec = self.specs[sym]
        step = max(float(spec.qty_step), 1e-12)
        qty = (int(qty / step)) * step
        if qty <= 0:
            return {"ok": False, "reason": "qty_rounded_to_zero"}

        notional = qty * entry_px
        if notional < float(self._min_notional_usd):
            return {"ok": False, "reason": "below_min_notional", "hint": f"need ≥ ${self._min_notional_usd}"}

        # Build SL/TP if needed
        if sl_px is None or tp_px is None:
            # choose stop distance: explicit → volatility-based
            if sl_distance_px is None or sl_distance_px <= 0:
                try:
                    sl_distance_px = float(self._compute_vol_stop_distance_px(sym, mid_px=entry_px))
                except Exception:
                    sl_distance_px = None

            if sl_distance_px is not None and sl_distance_px > 0:
                # direct plan using provided distance
                rr = self._tp_rr_from_decision(None)
                try:
                    plan = compute_sltp(
                        side=s,
                        entry_px=float(entry_px),
                        qty=float(qty),
                        price_tick=float(spec.price_tick),
                        sl_distance_px=float(sl_distance_px),
                        rr=float(rr),
                    )
                    sl_px = float(plan.sl_px)
                    tp_px = float(plan.tp_px)
                except Exception:
                    # fallback math
                    if s == "BUY":
                        sl_px = entry_px - sl_distance_px
                        tp_px = entry_px + rr * sl_distance_px
                    else:
                        sl_px = entry_px + sl_distance_px
                        tp_px = entry_px - rr * sl_distance_px
                # normalize to internal rules (rounding, floors, sanity)
                plan_norm = self._normalize_sltp(sym, s, entry_px, SLTPPlan(sl_px=sl_px, tp_px=tp_px, sl_qty=qty, tp_qty=qty))
                sl_px, tp_px = float(plan_norm.sl_px), float(plan_norm.tp_px)
            else:
                # use standard builder (tick/spread/vol/min-bps aware)
                plan = self._build_sltp_plan(
                    symbol=sym,
                    side=s,
                    entry_px=float(entry_px),
                    qty=float(qty),
                    micro=self._last_micro.get(sym),
                    indi=self._last_indi.get(sym),
                    decision=None,
                )
                sl_px = float(plan.sl_px)
                tp_px = float(plan.tp_px)

        # Submit through executor with optional per-call overrides
        try:
            ctx_mgr = self._temporary_exec_overrides(
                sym,
                limit_offset_ticks=limit_offset_ticks if limit_offset_ticks is not None else None,
            )
        except Exception:
            # fallback if override helper fails for any reason
            @contextlib.contextmanager
            def _dummy():  # type: ignore
                yield
            ctx_mgr = _dummy()

        try:
            with ctx_mgr:
                # NEW: не передаём sl_px/tp_px в executor — у executora их больше нет в сигнатуре
                rep: ExecutionReport = await ex.place_entry(sym, s, float(qty))
                self._accumulate_exec_counters(rep.steps)
                self._entry_steps[sym] = list(rep.steps or [])
        except Exception as e:
            logger.exception("[place_entry] executor error for %s %s: %s", sym, s, e)
            return {"ok": False, "reason": "exec_error", "error": str(e)}

        # update runtime position
        lock = self._locks[sym]
        async with lock:
            self._pos[sym] = PositionState(
                state="OPEN",
                side=s,
                qty=float(qty),
                entry_px=float(entry_px),
                sl_px=float(sl_px) if sl_px is not None else None,
                tp_px=float(tp_px) if tp_px is not None else None,
                opened_ts_ms=self._now_ms(),
                timeout_ms=self._pos[sym].timeout_ms,
            )
            with contextlib.suppress(Exception):
                await self._fsm[sym].on_open()

        return {
            "ok": True,
            "symbol": sym,
            "side": s,
            "qty": float(qty),
            "entry_px": float(entry_px),
            "sl_px": float(sl_px) if sl_px is not None else None,
            "tp_px": float(tp_px) if tp_px is not None else None,
            "steps": self._entry_steps.get(sym, []),
        }

    async def place_entry_auto(self, symbol: str, side: str) -> dict:
        """
        Auto entry with risk-based sizing and SL/TP planning (fee-aware).

        - Вычисляет дистанцию SL (волатильность/тик/спред/bps);
        - Проверяет, что TP в bps >= комиссий+спреда+slippage+min_edge;
        - Сайзит позицию по заданному риску;
        - Строит SL/TP через fee-aware планировщик (fallback на простой);
        - Делает unified place_entry с готовыми SL/TP.
        """
        import math

        log = logging.getLogger(__name__ + ".place_entry_auto")

        # normalize inputs
        sym = str(symbol).upper()
        s = str(side).upper()
        if s == "LONG":
            s = "BUY"
        if s == "SHORT":
            s = "SELL"
        if s not in ("BUY", "SELL"):
            return {"ok": False, "reason": "bad_side"}

        # best bid/ask
        try:
            bid, ask = self.best_bid_ask(sym)
        except Exception:
            bid = ask = 0.0
        if bid <= 0 or ask <= 0:
            try:
                d = self.diag() or {}
                bi = (d.get("best") or {}).get(sym) or {}
                bid = float(bi.get("bid") or 0.0)
                ask = float(bi.get("ask") or 0.0)
            except Exception:
                bid = ask = 0.0
        if bid <= 0 or ask <= 0:
            self._inc_block_reason(f"{sym}:no_best_bid_ask")
            return {"ok": False, "reason": "no_best_bid_ask"}

        mid = (bid + ask) / 2.0
        if mid <= 0:
            return {"ok": False, "reason": "bad_mid"}

        # symbol steps
        price_step = self._get_price_step(sym)
        if price_step <= 0:
            price_step = 0.1
        qty_step = 0.001
        try:
            spec = (self.specs or {}).get(sym)
            if spec is not None and getattr(spec, "qty_step", None) is not None:
                qty_step = float(spec.qty_step)
        except Exception:
            pass
        if qty_step <= 0:
            qty_step = 0.001

        # exec cfg (минимум по тикам для стопа)
        exec_flat_cfg = getattr(self, "_exec_cfg", {}) or {}
        min_stop_ticks = int(exec_flat_cfg.get("min_stop_ticks", round(self._min_stop_ticks_default())))
        if min_stop_ticks <= 0:
            min_stop_ticks = int(round(self._min_stop_ticks_default()))

        spread = max(0.0, ask - bid)

        # stop distance (вола + полы)
        sl_distance_px = 0.0
        if hasattr(self, "_compute_vol_stop_distance_px"):
            try:
                sl_distance_px = float(self._compute_vol_stop_distance_px(
                    sym, mid_px=mid, min_stop_ticks_cfg=min_stop_ticks
                ))
            except Exception:
                sl_distance_px = 0.0
        if sl_distance_px <= 0:
            sl_distance_px = max(
                min_stop_ticks * price_step,  # config floor
                spread * 2.0,
                price_step * 6.0,
            )
        if sl_distance_px <= 0:
            return {"ok": False, "reason": "bad_sl_distance"}

        # требуемый RR (гросс), edge-проверка в bps
        rr = 1.6
        ok_edge, info_edge = self._entry_edge_ok(sym, mid, bid, ask, sl_distance_px, rr)
        if not ok_edge:
            self._inc_block_reason(f"{sym}:edge_too_small")
            return {"ok": False, "reason": "edge_too_small", "edge_diag": info_edge}

        # risk & account cfg
        def _load_cfg():
            risk = getattr(self, "_risk_cfg", None)
            acc = getattr(self, "_account_cfg", None)
            if not isinstance(risk, dict) or not isinstance(acc, dict):
                try:
                    d = self.diag() or {}
                    if not isinstance(risk, dict):
                        risk = d.get("risk_cfg") or {}
                    if not isinstance(acc, dict):
                        acc = d.get("account_cfg") or {}
                except Exception:
                    risk = risk or {}
                    acc = acc or {}
            return risk or {}, acc or {}

        risk_cfg, acc_cfg = _load_cfg()
        equity = float(acc_cfg.get("equity", acc_cfg.get("starting_equity_usd", 1000.0)))
        lev = float(acc_cfg.get("leverage", 15.0))
        min_notional = float(acc_cfg.get("min_notional_usd", 5.0))
        risk_pct = float(risk_cfg.get("risk_per_trade_pct", 0.15))
        min_risk_usd_floor = float(risk_cfg.get("min_risk_usd_floor", 1.0))

        desired_risk_usd = max(min_risk_usd_floor, equity * (risk_pct / 100.0))
        if desired_risk_usd <= 0:
            return {"ok": False, "reason": "bad_risk_cfg"}

        # auto sizing
        raw_qty = desired_risk_usd / sl_distance_px
        max_qty_by_lev = (equity * lev) / mid
        qty = min(raw_qty, max_qty_by_lev)

        if qty_step > 0:
            qty = math.floor(qty / qty_step) * qty_step
        if qty <= 0:
            self._inc_block_reason(f"{sym}:qty_rounded_to_zero")
            return {
                "ok": False,
                "reason": "qty_rounded_to_zero",
                "hint": f"increase risk_per_trade_pct or equity; desired_risk_usd={desired_risk_usd:.4f}",
            }

        notional = qty * mid
        if notional < min_notional:
            min_qty = math.ceil((min_notional / mid) / qty_step) * qty_step
            self._inc_block_reason(f"{sym}:below_min_notional")
            return {
                "ok": False,
                "reason": "below_min_notional",
                "symbol": sym,
                "side": s,
                "qty": float(qty),
                "hint": f"min notional ${min_notional} → try qty ≥ {min_qty}",
            }

        # --- план SL/TP (fee-aware с фолбэком) ---
        maker_bps, taker_bps = self._fees_for_symbol(sym)
        spread_bps = (spread / mid) * 1e4 if mid > 0 else 0.0
        addl_exit_bps = 0.5 * spread_bps

        try:
            if compute_sltp_fee_aware:
                plan = compute_sltp_fee_aware(
                    side=s,
                    entry_px=float(mid),
                    qty=float(qty),
                    price_tick=float(price_step),
                    sl_distance_px=float(sl_distance_px),
                    rr_target=float(rr),
                    maker_bps=maker_bps,
                    taker_bps=taker_bps,
                    entry_taker_like=True,
                    exit_taker_like=True,
                    addl_exit_bps=float(addl_exit_bps),
                    min_stop_ticks=int(min_stop_ticks),
                    spread_px=spread,
                    spread_mult=1.5,
                    min_sl_bps=12.0,
                    min_net_rr=1.2,     # требуемый net-RR
                    allow_expand_tp=True,
                )
            else:
                plan = compute_sltp(
                    side=s,
                    entry_px=float(mid),
                    qty=float(qty),
                    price_tick=float(price_step),
                    sl_distance_px=float(sl_distance_px),
                    rr=float(rr),
                )
            sl_px = float(plan.sl_px)
            tp_px = float(plan.tp_px)
        except Exception:
            if s == "BUY":
                sl_px = mid - sl_distance_px
                tp_px = mid + sl_distance_px * rr
            else:
                sl_px = mid + sl_distance_px
                tp_px = mid - sl_distance_px * rr

        # expected risk & re-size qty to match target (no hard reject)
        risk_px = abs(sl_px - mid)
        if risk_px <= 0:
            self._inc_block_reason(f"{sym}:plan_bad_risk_px")
            return {"ok": False, "reason": "bad_sl_distance"}

        # 1) первичный риск от чернового qty
        expected_risk_usd = risk_px * qty

        # 2) подгоняем qty под target_risk_usd и ограничения (плечо, нотионал, шаг)
        target_risk_usd = desired_risk_usd
        adj_qty = target_risk_usd / risk_px

        # лимит по плечу
        max_qty_by_lev = (equity * lev) / mid
        adj_qty = min(adj_qty, max_qty_by_lev)

        # минимальная нотиональ
        min_qty_by_notional = min_notional / mid
        adj_qty = max(adj_qty, min_qty_by_notional)

        # к шагу лота
        if qty_step > 0:
            adj_qty = math.floor(adj_qty / qty_step) * qty_step

        if adj_qty <= 0 or (adj_qty * mid) < min_notional:
            self._inc_block_reason(f"{sym}:qty_rounded_to_zero")
            return {"ok": False, "reason": "qty_rounded_to_zero",
                    "hint": f"min notional ${min_notional} → increase risk or equity"}

        qty = float(adj_qty)
        expected_risk_usd = round(risk_px * qty, 6)

        # submit (без жёсткого отклонения по mismatch)
        try:
            report = await self.place_entry(
                sym, s, float(qty),
                sl_px=float(sl_px),
                tp_px=float(tp_px),
            )
        except Exception as e:
            log.exception("[place_entry_auto] failed for %s %s: %s", sym, s, e)
            self._inc_block_reason(f"{sym}:entry_error")
            return {"ok": False, "reason": "entry_error", "error": str(e)}

        if isinstance(report, dict) and not report.get("ok", True):
            reason = (report.get("reason") or "rejected")
            self._inc_block_reason(f"{sym}:entry_rejected_{reason}")
            return {"ok": False, "reason": reason, "report": report}

        return {
            "ok": True,
            "symbol": sym, "side": s, "qty": float(qty),
            "entry_px": float(mid), "sl_px": float(sl_px), "tp_px": float(tp_px),
            "expected_risk_usd": round(expected_risk_usd, 6),
            "target_risk_usd": round(target_risk_usd, 6),
            "report": report,
        }

        # submit
        try:
            report = await self.place_entry(
                sym,
                s,
                float(qty),
                sl_px=float(sl_px),
                tp_px=float(tp_px),
            )
        except Exception as e:
            log.exception("[place_entry_auto] failed for %s %s: %s", sym, s, e)
            self._inc_block_reason(f"{sym}:entry_error")
            return {"ok": False, "reason": "entry_error", "error": str(e)}

        if isinstance(report, dict) and not report.get("ok", True):
            reason = (report.get("reason") or "rejected")
            self._inc_block_reason(f"{sym}:entry_rejected_{reason}")
            return {"ok": False, "reason": reason, "report": report}

        return {
            "ok": True,
            "symbol": sym,
            "side": s,
            "qty": float(qty),
            "entry_px": float(mid),
            "sl_px": float(sl_px),
            "tp_px": float(tp_px),
            "expected_risk_usd": round(expected_risk_usd, 6),
            "target_risk_usd": round(desired_risk_usd, 6),
            "report": report,
        }
    
    async def _place_entry_with_kind(
        self,
        sym: str,
        side: str,
        signal_kind: Optional[str],
        sl_distance_px: float,
        mid: float,
        bid: float,
        ask: float,
    ) -> dict:
        """
        Исполнение под тип сигнала:
        - momentum → быстрый вход (IOC / taker-like);
        - reversion → выставляем лимит (maker/post-only).
        Использует _compute_auto_qty и place_entry() с передачей sl_distance_px.
        """
        s = str(side).upper()
        if s == "LONG":  s = "BUY"
        if s == "SHORT": s = "SELL"
        if s not in ("BUY", "SELL"):
            return {"ok": False, "reason": "bad_side"}

        # qty от риска
        qty = float(self._compute_auto_qty(sym, s, float(sl_distance_px)))
        if qty <= 0:
            self._inc_block_reason(f"{sym}:qty_rounded_to_zero")
            return {"ok": False, "reason": "qty_rounded_to_zero"}

        # роутинг по типу
        kind = (signal_kind or "").lower()
        is_momentum  = ("mom" in kind) or ("break" in kind) or ("trend" in kind)
        is_reversion = ("rev" in kind) or ("mean" in kind) or ("bounce" in kind)

        # дефолтные override параметры (на один вызов)
        tif = None
        prefer_maker = None
        limit_offset_ticks = None

        if is_momentum:
            # Быстрый вход → IOC, берём ликвидность
            tif = "IOC"
            prefer_maker = False
            limit_offset_ticks = 1
        elif is_reversion:
            # Откат → стараемся стать мейкером
            tif = "GTC"
            prefer_maker = True
            limit_offset_ticks = 2
        else:
            # Если не распознали — умеренный по умолчанию
            tif = "IOC"
            prefer_maker = False
            limit_offset_ticks = 1

        # Временный override конфигурации Executor на один вызов
        try:
            ctx_mgr = self._temporary_exec_overrides(
                sym,
                time_in_force=tif,
                prefer_maker=prefer_maker,
                limit_offset_ticks=limit_offset_ticks,
            )
        except Exception:
            @contextlib.contextmanager
            def _dummy():
                yield
            ctx_mgr = _dummy()

        # вызываем unified place_entry с нашей sl_distance_px (SL/TP построятся внутри)
        with ctx_mgr:
            rep = await self.place_entry(
                sym,
                s,
                qty,
                limit_offset_ticks=limit_offset_ticks,
                sl_distance_px=float(sl_distance_px),
            )
        return rep if isinstance(rep, dict) else {"ok": False, "reason": "bad_report"}
    
    # ---------- watchdog / closing / trades ----------

    async def _watchdog_loop(self) -> None:
        """Watchdog with strict SL/TP triggering (dynamic protection disabled for MVP)."""
        # soft imports (оставляем, но использовать не будем)
        try:
            from bot.core.protection import ProtectionCfg, PositionSnap, plan_next_protection  # type: ignore
        except Exception:
            ProtectionCfg = PositionSnap = plan_next_protection = None  # type: ignore

        try:
            while True:
                await asyncio.sleep(0.25)
                now = self._now_ms()
                for sym in self.symbols:
                    lock = self._locks[sym]
                    async with lock:
                        pos = self._pos[sym]
                        if pos.state != "OPEN":
                            continue

                        # 1) fail-safe timeout if no SL/TP at all
                        if pos.sl_px is None and pos.tp_px is None:
                            if pos.opened_ts_ms and (now - pos.opened_ts_ms) >= pos.timeout_ms:
                                await self._paper_close(sym, pos, reason="no_protection")
                            continue

                        # 2) latest price + best bid/ask (для исполнимости)
                        last = self.latest_tick(sym)
                        if not last:
                            continue
                        try:
                            p = float(last.get("price") or 0.0)
                            mp = float(last.get("mark_price") or 0.0)
                            px = p if p > 0 else mp
                        except Exception:
                            px = 0.0

                        try:
                            bid, ask = self.best_bid_ask(sym)
                            bid = float(bid or 0.0); ask = float(ask or 0.0)
                        except Exception:
                            bid = ask = 0.0

                        if px <= 0.0 and (bid <= 0.0 or ask <= 0.0):
                            continue

                        # шаг цены для допуска по тикам
                        try:
                            spec = (self.specs or {}).get(sym)
                            price_tick = float(getattr(spec, "price_step", getattr(spec, "price_tick", 0.1)))
                        except Exception:
                            price_tick = 0.1

                        # 3) (DISABLED) dynamic protection — выключено для стабильности MVP
                        # if (False and ProtectionCfg and PositionSnap and plan_next_protection
                        #     and pos.sl_px is not None and px > 0.0):
                        #     ... (оставлено намеренно выключенным)

                        # 3.5) FAST TP: детект по исполнимой стороне + grace + форс-закрытие
                        try:
                            side_now = (pos.side or "BUY").upper()
                            tp_px = float(pos.tp_px) if pos.tp_px is not None else None
                            qty_now = float(pos.qty or 0.0)

                            # если есть TP и есть котировка bid/ask — проверяем исполнимость
                            if tp_px and qty_now > 0.0 and bid > 0.0 and ask > 0.0:
                                if self._tp_hit(side_now, tp_px, bid, ask, price_tick, tol_ticks=self._tp_tol_ticks):
                                    # зафиксировать момент первого касания, если ещё нет
                                    first = int(self._tp_first_hit_ms.get(sym, 0) or 0)
                                    now_ms = self._now_ms()
                                    if first <= 0:
                                        self._tp_first_hit_ms[sym] = now_ms
                                        # убедиться, что reduceOnly лимит на TP стоит (best-effort)
                                        try:
                                            place_take = getattr(self, "ensure_take_order", None)
                                            if callable(place_take):
                                                await place_take(sym, side_now, qty_now, tp_px)
                                        except Exception:
                                            pass
                                    else:
                                        # если grace истёк — форсируем рыночным reduceOnly
                                        if self._tp_force and (now_ms - first >= int(self._tp_grace_ms)):
                                            await self._force_flatten_after_tp(sym, side_now, qty_now)
                                            self._tp_first_hit_ms[sym] = 0
                                            self._inc_block_reason(f"{sym}:tp_force_close")
                                            # после форс-закрытия позиция станет FLAT в _paper_close; продолжать нечего
                                            continue
                                else:
                                    # ушли от TP — сбросить маркер
                                    if int(self._tp_first_hit_ms.get(sym, 0) or 0) > 0:
                                        self._tp_first_hit_ms[sym] = 0
                        except Exception:
                            # не ломаем сторож
                            pass

                        # 4) strict SL/TP triggering
                        side = pos.side or "BUY"
                        if side == "BUY":
                            if pos.sl_px is not None and px <= pos.sl_px:
                                await self._paper_close(sym, pos, reason="sl_hit")
                                continue
                            if pos.tp_px is not None and px >= pos.tp_px:
                                await self._paper_close(sym, pos, reason="tp_hit")
                                continue
                        else:  # SELL
                            if pos.sl_px is not None and px >= pos.sl_px:
                                await self._paper_close(sym, pos, reason="sl_hit")
                                continue
                            if pos.tp_px is not None and px <= pos.tp_px:
                                await self._paper_close(sym, pos, reason="tp_hit")
                                continue
        except asyncio.CancelledError:
            return
        except Exception:
            logger.exception("watchdog loop error")

    async def flatten(self, symbol: str) -> Dict[str, Any]:
        symbol = symbol.upper()
        lock = self._locks[symbol]
        async with lock:
            pos = self._pos[symbol]
            if pos.state != "OPEN":
                self._pos[symbol] = PositionState(
                    state="FLAT", side=None, qty=0.0, entry_px=0.0,
                    sl_px=None, tp_px=None, opened_ts_ms=0, timeout_ms=pos.timeout_ms
                )
                self._last_flat_ms[symbol] = self._now_ms()
                with contextlib.suppress(Exception):
                    await self._fsm[symbol].on_flat()
                return {"ok": True, "symbol": symbol, "state": "FLAT", "reason": "no_open_position"}

            await self._paper_close(symbol, pos, reason="flatten_forced")
            return {"ok": True, "symbol": symbol, "state": "FLAT", "reason": "flatten_forced"}

    def set_timeout_ms(self, symbol: str, timeout_ms: int) -> Dict[str, Any]:
        symbol = symbol.upper()
        self._pos[symbol].timeout_ms = max(5_000, int(timeout_ms))
        return {"ok": True, "symbol": symbol, "timeout_ms": self._pos[symbol].timeout_ms}

    async def _paper_close(self, sym: str, pos: PositionState, *, reason: str) -> None:
        """
        Безопасное закрытие позиции в paper-режиме.

        Правила:
        - Для sl_hit / tp_hit выходим по уровню SL/TP (строго, без переоценки в -30R).
        - Для flatten/timeout/no_protection — по консервативной рыночной цене.
        - Executor.place_exit используем только для получения steps/аудита,
          но не даём ему ломать целевой уровень SL/TP.
        - В любом случае в конце приводим состояние к FLAT и обновляем last_flat_ms.
        """
        side = pos.side
        qty = float(pos.qty or 0.0)
        entry_px = float(pos.entry_px or 0.0)
        sl_px = float(pos.sl_px) if pos.sl_px is not None else None
        tp_px = float(pos.tp_px) if pos.tp_px is not None else None
        timeout_ms = int(pos.timeout_ms or 180_000)

        # Если по факту позиции нет — просто приводим к FLAT.
        if qty <= 0.0 or not side or entry_px <= 0.0:
            self._pos[sym] = PositionState(
                state="FLAT",
                side=None,
                qty=0.0,
                entry_px=0.0,
                sl_px=None,
                tp_px=None,
                opened_ts_ms=0,
                timeout_ms=timeout_ms,
            )
            self._entry_steps.pop(sym, None)
            self._last_flat_ms[sym] = self._now_ms()
            with contextlib.suppress(Exception):
                await self._fsm[sym].on_flat()
            return

        close_side: Side = "SELL" if side == "BUY" else "BUY"

        # Целевой уровень для SL/TP (только для sl_hit / tp_hit).
        level_px: Optional[float] = None
        if reason == "sl_hit" and sl_px is not None:
            level_px = sl_px
        elif reason == "tp_hit" and tp_px is not None:
            level_px = tp_px

        exit_px: Optional[float] = None
        exit_steps: List[str] = []

        try:
            # FSM: переходим в EXITING (best-effort)
            with contextlib.suppress(Exception):
                await self._fsm[sym].on_exiting()

            ex = self.execs.get(sym)

            # 1) Пытаемся формально вызвать Executor.place_exit (reduce_only) ради steps/аудита.
            if ex is not None:
                try:
                    rep: ExecutionReport = await ex.place_exit(sym, close_side, qty)
                    self._accumulate_exec_counters(rep.steps)
                    exit_steps = list(rep.steps or [])
                except Exception:
                    # Не роняем закрытие, просто помечаем.
                    exit_steps = ["exec_exit_error"]

            # 2) Определяем финальную цену выхода.
            if level_px is not None:
                # Для sl_hit/tp_hit в paper-режиме ЖЁСТКО используем плановый уровень.
                exit_px = float(level_px)
            else:
                # Для остальных причин (timeout, flatten, no_protection, etc.)
                # берём консервативную доступную цену.
                exit_px = self._exit_price_conservative(sym, close_side, entry_px)

            # 3) Регистрируем трейд, если цена вменяема.
            if exit_px is not None and exit_px > 0.0:
                entry_steps = self._entry_steps.get(sym, [])
                trade = self._build_trade_record(
                    sym,
                    pos,
                    float(exit_px),
                    reason=reason,
                    entry_steps=entry_steps,
                    exit_steps=exit_steps or None,
                )
                self._register_trade(trade)
                # Фоновая пересборка истории (если есть хранилище).
                asyncio.create_task(self._rebuild_trades_safe())

        finally:
            # 4) Всегда приводим состояние к FLAT и фиксируем момент FLAT для анти-спама.
            self._pos[sym] = PositionState(
                state="FLAT",
                side=None,
                qty=0.0,
                entry_px=0.0,
                sl_px=None,
                tp_px=None,
                opened_ts_ms=0,
                timeout_ms=timeout_ms,
            )
            self._entry_steps.pop(sym, None)
            self._last_flat_ms[sym] = self._now_ms()
            with contextlib.suppress(Exception):
                await self._fsm[sym].on_flat()

    # ---------- учёт/PNL ----------

    def _build_trade_record(
        self,
        sym: str,
        pos: PositionState,
        exit_px: float,
        *,
        reason: str,
        entry_steps: Optional[List[str]] = None,
        exit_steps: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        side = (pos.side or "BUY")
        qty = float(pos.qty or 0.0)
        entry = float(pos.entry_px or 0.0)
        sl_px = float(pos.sl_px) if pos.sl_px is not None else None
        tp_px = float(pos.tp_px) if pos.tp_px is not None else None

        now = self._now_ms()

        if qty <= 0.0 or entry <= 0.0:
            return {
                "symbol": sym,
                "side": side,
                "opened_ts": int(pos.opened_ts_ms or 0),
                "closed_ts": now,
                "qty": qty,
                "entry_px": entry,
                "exit_px": float(exit_px),
                "sl_px": sl_px,
                "tp_px": tp_px,
                "pnl_usd": 0.0,
                "pnl_r": 0.0,
                "risk_usd": 0.0,
                "reason": f"{reason}|invalid_pos",
                "fees": 0.0,
            }

        # gross PnL и номинальный риск по SL
        if side == "BUY":
            pnl_usd_gross = (exit_px - entry) * qty
            sl_ref = sl_px if sl_px is not None and sl_px < entry else entry
            risk_usd_nominal = abs(entry - sl_ref) * qty
        else:
            pnl_usd_gross = (entry - exit_px) * qty
            sl_ref = sl_px if sl_px is not None and sl_px > entry else entry
            risk_usd_nominal = abs(sl_ref - entry) * qty

        # комиссии
        entry_bps = self._fee_bps_for_steps(entry_steps or [])
        exit_bps = self._fee_bps_for_steps(exit_steps or [])
        fees_usd = (entry * qty * entry_bps + exit_px * qty * exit_bps) / 10_000.0

        pnl_usd = pnl_usd_gross - fees_usd

        # плановый риск
        try:
            equity = float(self._starting_equity_usd + float(self._pnl_day.get("pnl_usd", 0.0) or 0.0))
        except Exception:
            equity = float(self._starting_equity_usd)

        risk_pct = float(getattr(self._risk_cfg, "risk_per_trade_pct", 0.0) or 0.0)
        risk_target = equity * (risk_pct / 100.0) if risk_pct > 0.0 else 0.0

        cfg_floor = float(getattr(self._risk_cfg, "min_risk_usd_floor", 0.0) or 0.0)
        if cfg_floor <= 0.0:
            cfg_floor = 1.0

        risk_usd = max(risk_usd_nominal, risk_target, cfg_floor, 1e-9)
        pnl_r = pnl_usd / risk_usd

        return {
            "symbol": sym,
            "side": side,
            "opened_ts": int(pos.opened_ts_ms or 0),
            "closed_ts": now,
            "qty": qty,
            "entry_px": entry,
            "exit_px": float(exit_px),
            "sl_px": sl_px,
            "tp_px": tp_px,
            "pnl_usd": round(pnl_usd, 6),
            "pnl_r": round(pnl_r, 6),
            "risk_usd": round(risk_usd, 6),
            "reason": reason,
            "fees": round(fees_usd, 6),
        }

    def _register_trade(self, trade: Dict[str, Any]) -> None:
        sym = trade["symbol"]
        dq = self._trades.get(sym)
        if dq is None:
            dq = self._trades[sym] = deque(maxlen=1000)
        dq.appendleft(trade)

        now = self._now_ms()

        # смена дня
        day = self._day_str()
        if self._pnl_day.get("day") != day:
            self._pnl_day = {
                "day": day,
                "trades": 0,
                "winrate": None,
                "avg_r": None,
                "pnl_r": 0.0,
                "pnl_usd": 0.0,
                "max_dd_r": None,
            }
            self._pnl_r_equity = 0.0
            self._consec_losses = 0

        pnl_usd = float(trade.get("pnl_usd", 0.0) or 0.0)
        pnl_r = float(trade.get("pnl_r", 0.0) or 0.0)
        risk_usd = float(trade.get("risk_usd", 0.0) or 0.0)
        reason = str(trade.get("reason", "")).lower()

        # агрегаты
        self._pnl_day["trades"] += 1
        self._pnl_day["pnl_usd"] += pnl_usd
        self._pnl_day["pnl_r"] += pnl_r

        wins_prev = int(round((self._pnl_day.get("winrate_raw", 0.0) or 0.0)
                              * max(self._pnl_day["trades"] - 1, 0)))
        if pnl_r > 0:
            wins_now = wins_prev + 1
        else:
            wins_now = wins_prev

        self._pnl_day["winrate_raw"] = wins_now / max(self._pnl_day["trades"], 1)
        self._pnl_day["winrate"] = round(self._pnl_day["winrate_raw"], 4)
        self._pnl_day["avg_r"] = round(self._pnl_day["pnl_r"] / max(self._pnl_day["trades"], 1), 6)

        # DD по R
        self._pnl_r_equity += pnl_r
        peak = self._pnl_day.get("peak_r", 0.0)
        trough = self._pnl_day.get("trough_r", 0.0)
        if self._pnl_r_equity > peak:
            peak = self._pnl_r_equity
        if self._pnl_r_equity < trough:
            trough = self._pnl_r_equity
        self._pnl_day["peak_r"] = peak
        self._pnl_day["trough_r"] = trough
        self._pnl_day["max_dd_r"] = round(max(peak - trough, 0.0), 6)

        # логика последовательных стопов
        # реальный стоп: SL-триггер, риск не микроскопический, R <= -0.8
        cfg_floor = float(getattr(self._risk_cfg, "min_risk_usd_floor", 1.0) or 1.0)
        try:
            risk_pct = float(getattr(self._risk_cfg, "risk_per_trade_pct", 0.0) or 0.0)
            equity = float(self._starting_equity_usd + float(self._pnl_day.get("pnl_usd", 0.0) or 0.0))
            risk_target = equity * (risk_pct / 100.0) if risk_pct > 0.0 else cfg_floor
        except Exception:
            risk_target = cfg_floor

        is_sl_reason = ("sl_hit" in reason) or (" sl" in reason) or ("stop" in reason)
        is_meaningful_risk = risk_usd >= 0.5 * risk_target
        is_real_stop = is_sl_reason and is_meaningful_risk and (pnl_r <= -0.8)

        if pnl_r > 0:
            self._consec_losses = 0
        elif is_real_stop:
            self._consec_losses += 1
            self._last_sl_ts_ms = now
        # мелкие минуса не двигают серию

    # Вспомогательное: отдаём positions как dict (без лишней математики)
    def diag_positions(self) -> Dict[str, Dict[str, Any]]:
        return {
            s: {
                "state": self._pos[s].state,
                "side": self._pos[s].side,
                "qty": self._pos[s].qty,
                "entry_px": self._pos[s].entry_px,
                "sl_px": self._pos[s].sl_px,
                "tp_px": self._pos[s].tp_px,
                "opened_ts_ms": self._pos[s].opened_ts_ms,
                "timeout_ms": self._pos[s].timeout_ms,
            }
            for s in self.symbols
        }

    def diag(self) -> Dict[str, Any]:
        """
        Стабильный снапшот состояния для API/фронта.

        Без выброса исключений наружу:
        - ws/stream статус
        - best bid/ask
        - история тиков по символам (meta)
        - позиции и uPnL
        - сделки и дневной PnL
        - риск, безопасники, дневные лимиты
        - авто-сигналы/стратегия
        """
        symbols = list(self.symbols)

        # --- WS / coalescer ---
        try:
            ws_detail = self.ws.diag()
        except Exception:
            ws_detail = {}

        try:
            coal_stats = self.coal.stats()
        except Exception:
            coal_stats = {}

        # --- best bid/ask ---
        best: Dict[str, Tuple[float, float]] = {}
        for s in symbols:
            try:
                bid, ask = self.hub.best_bid_ask(s)
            except Exception:
                bid, ask = 0.0, 0.0
            best[s] = (float(bid), float(ask))

        # --- history meta ---
        history: Dict[str, Dict[str, Any]] = {}
        for s in symbols:
            dq = self._hist.get(s) or []
            latest_ts = 0
            if dq:
                try:
                    latest_ts = int(dq[-1].get("ts_ms", 0))
                except Exception:
                    latest_ts = 0
            history[s] = {
                "len": len(dq),
                "maxlen": self._history_maxlen,
                "latest_ts_ms": latest_ts,
            }

        # --- positions snapshot ---
        positions = self.diag_positions()

        # --- unrealized snapshot ---
        try:
            unrealized = self._unrealized_snapshot()
        except Exception:
            unrealized = {
                "total_usd": 0.0,
                "per_symbol": None,
                "open_positions": 0,
            }

        # --- pnl_day (safe/rounded) ---
        try:
            pnl_raw = dict(self._pnl_day or {})
        except Exception:
            pnl_raw = {}

        pnl_day: Dict[str, Any] = {}
        if not pnl_raw:
            pnl_day = {
                "day": self._day_str(),
                "trades": 0,
                "winrate": None,
                "avg_r": None,
                "pnl_r": 0.0,
                "pnl_usd": 0.0,
                "max_dd_r": None,
            }
        else:
            for k, v in pnl_raw.items():
                if k.endswith("_raw"):
                    continue
                if isinstance(v, float):
                    pnl_day[k] = round(v, 6)
                else:
                    pnl_day[k] = v

        # --- trades snapshot ---
        trades: Dict[str, Any] = {}
        for s in symbols:
            try:
                trades[s] = list(self._trades.get(s, []))
            except Exception:
                trades[s] = []

        # --- risk cfg ---
        try:
            risk_cfg = {
                "risk_per_trade_pct": float(self._risk_cfg.risk_per_trade_pct),
                "daily_stop_r": float(self._risk_cfg.daily_stop_r),
                "daily_target_r": float(self._risk_cfg.daily_target_r),
                "max_consec_losses": int(self._risk_cfg.max_consec_losses),
                "cooldown_after_sl_s": int(self._risk_cfg.cooldown_after_sl_s),
                "min_risk_usd_floor": float(getattr(self._risk_cfg, "min_risk_usd_floor", 0.0)),
            }
        except Exception:
            risk_cfg = {}

        # --- protection cfg (subset risk) ---
        try:
            protection_cfg = {
                "daily_stop_r": float(self._risk_cfg.daily_stop_r),
                "daily_target_r": float(self._risk_cfg.daily_target_r),
                "max_consec_losses": int(self._risk_cfg.max_consec_losses),
            }
        except Exception:
            protection_cfg = {}

        # --- safety cfg ---
        try:
            safety_cfg = {
                "max_spread_ticks": int(self._safety_cfg.max_spread_ticks),
                "min_top5_liquidity_usd": float(self._safety_cfg.min_top5_liquidity_usd),
                "skip_funding_minute": bool(self._safety_cfg.skip_funding_minute),
                "skip_minute_zero": bool(self._safety_cfg.skip_minute_zero),
                "min_liq_buffer_sl_mult": float(self._safety_cfg.min_liq_buffer_sl_mult),
            }
        except Exception:
            safety_cfg = {}

        # --- account cfg ---
        try:
            equity_now = float(
                self._starting_equity_usd
                + float(self._pnl_day.get("pnl_usd", 0.0) or 0.0)
            )
        except Exception:
            equity_now = float(self._starting_equity_usd)

        account_cfg = {
            "starting_equity_usd": float(self._starting_equity_usd),
            "leverage": float(self._leverage),
            "min_notional_usd": float(self._min_notional_usd),
            "equity_now_usd": round(equity_now, 6),
        }

        # --- day limits snapshot ---
        try:
            day_state = DayState(
                pnl_r_day=float(pnl_day.get("pnl_r", 0.0) or 0.0),
                consec_losses=int(self._consec_losses),
                trading_disabled=False,
            )
            day_dec = check_day_limits(day_state, self._risk_cfg)
            day_limits = {
                "can_trade": bool(day_dec.allow),
                "reasons": list(day_dec.reasons),
            }
        except Exception:
            day_limits = {
                "can_trade": True,
                "reasons": [],
            }

        # --- exec cfg per symbol ---
        exec_cfg: Dict[str, Dict[str, Any]] = {}
        for s in symbols:
            try:
                c = self.execs[s].c
                exec_cfg[s] = {
                    "price_tick": float(c.price_tick),
                    "qty_step": float(c.qty_step),
                    "limit_offset_ticks": int(c.limit_offset_ticks),
                    "limit_timeout_ms": int(c.limit_timeout_ms),
                    "time_in_force": str(c.time_in_force),
                    "poll_ms": int(c.poll_ms),
                    "prefer_maker": bool(c.prefer_maker),
                    "fee_bps_maker": float(c.fee_bps_maker),
                    "fee_bps_taker": float(c.fee_bps_taker),
                }
            except Exception:
                exec_cfg[s] = {}

        # --- counters / блокировки ---
        exec_counters = dict(self._exec_counters or {})
        block_reasons = dict(self._block_reasons or {})

        # --- strategy / auto ---
        strategy_cfg = {
            "cooldown_ms": int(self._auto_cooldown_ms),
            "min_flat_ms": int(self._auto_min_flat_ms),
        }

        return {
            "symbols": symbols,
            "ws_detail": ws_detail,
            "coal": coal_stats,
            "best": best,
            "history": history,
            "positions": positions,
            "unrealized": unrealized,
            "exec_cfg": exec_cfg,
            "exec_counters": exec_counters,
            "pnl_day": pnl_day,
            "trades": trades,
            "block_reasons": block_reasons,
            "risk_cfg": risk_cfg,
            "protection_cfg": protection_cfg,
            "safety_cfg": safety_cfg,
            "consec_losses": int(self._consec_losses),
            "account_cfg": account_cfg,
            "auto_signal_enabled": bool(self._auto_enabled),
            "strategy_cfg": strategy_cfg,
            "day_limits": day_limits,
        }

    # ---------- utils / sizing / config loaders ----------

    @staticmethod
    def _now_ms() -> int:
        return int(time.time() * 1000)

    @staticmethod
    def _day_str() -> Optional[str]:
        try:
            return time.strftime("%Y-%m-%d", time.gmtime())
        except Exception:
            return None
    
    def _unrealized_snapshot(self) -> Dict[str, Any]:
        """
        Compute unrealized PnL snapshot in USD:
        - per_symbol uPnL for OPEN positions only,
        - total_usd across symbols,
        - open_positions count.
        Price source priority: last.trade price -> mark_price -> mid(bid/ask) -> entry_px.
        Safe and exception-free.
        """
        total = 0.0
        per_symbol: Dict[str, float] = {}
        open_cnt = 0

        for sym in self.symbols:
            try:
                pos = self._pos.get(sym)
                if not pos or pos.state != "OPEN":
                    continue

                side = (pos.side or "BUY").upper()
                qty = float(pos.qty or 0.0)
                entry = float(pos.entry_px or 0.0)
                if qty <= 0.0 or entry <= 0.0:
                    continue

                # latest price heuristic
                last = self.latest_tick(sym) or {}
                px = 0.0
                try:
                    p = float(last.get("price") or 0.0)
                    mp = float(last.get("mark_price") or 0.0)
                    px = p if p > 0 else mp
                except Exception:
                    px = 0.0

                if px <= 0.0:
                    try:
                        bid, ask = self.best_bid_ask(sym)
                        if bid > 0 and ask > 0:
                            px = (bid + ask) / 2.0
                    except Exception:
                        px = 0.0

                if px <= 0.0:
                    px = float(entry)  # ultimate fallback

                upnl = (px - entry) * qty if side == "BUY" else (entry - px) * qty
                per_symbol[sym] = round(float(upnl), 6)
                total += upnl
                open_cnt += 1
            except Exception:
                # keep going; snapshot must never fail
                continue

        return {
            "total_usd": round(float(total), 6),
            "per_symbol": per_symbol if per_symbol else None,
            "open_positions": int(open_cnt),
        }

    async def _rebuild_trades_safe(self) -> None:
        """
        Пересобирает таблицу сделок из orders/fills (если доступно).
        Делается фоном, чтобы не блокировать watchdog/UI.
        """
        try:
            from storage.repo import rebuild_trades  # type: ignore
        except ModuleNotFoundError:
            return
        except Exception:
            return

        try:
            # rebuild_trades — синхронный, унесём в thread-пул
            await asyncio.to_thread(rebuild_trades)
        except Exception as e:
            logger.warning("rebuild_trades failed: %s", e)
    
    def _pick_side_from_decision(self, sym: str, dec: Decision) -> Optional[Side]:
        """
        Извлекаем направление из Decision:
        - сначала используем dec.side, если оно валидно,
        - если side нет, но есть reason с weak_edge / ema_* / rsi_* / momentum_* / reversion_*,
          и там явно есть 'buy' или 'sell' — используем это как направление.
        """
        # 1) прямой side
        raw = (getattr(dec, "side", None) or "").upper()
        if raw in ("BUY", "SELL"):
            return raw  # type: ignore[return-value]

        # 2) пытаемся вытащить из reason
        reason = (getattr(dec, "reason", "") or "").lower()

        # типичные форматы: momentum_sell_weak_edge, reversion_buy_weak_edge, ema_sell, rsi_buy и т.п.
        if "buy" in reason:
            return "BUY"
        if "sell" in reason:
            return "SELL"

        return None


    def _recent_volatility(self, symbol: str, window_ms: int = 60_000) -> float:
        """
        Простейшая оценка волатильности по тикам за окно:
        stddev(last_trade_price) за window_ms.
        Используется только для калибровки SL и фильтра сигналов.
        """
        sym = symbol.upper()
        dq = self._hist.get(sym)
        if not dq:
            return 0.0

        now = self._now_ms()
        prices: List[float] = []

        # идём с конца, пока тики в окне
        for t in reversed(dq):
            try:
                ts = int(t.get("ts_ms", 0))
            except Exception:
                continue
            if ts <= 0 or now - ts > window_ms:
                break

            try:
                p = float(t.get("price") or 0.0)
            except Exception:
                p = 0.0
            if p > 0.0:
                prices.append(p)

        n = len(prices)
        if n < 4:
            return 0.0

        mean = sum(prices) / n
        var = 0.0
        for p in prices:
            diff = p - mean
            var += diff * diff
        var /= (n - 1)
        if var <= 0.0:
            return 0.0

        return float(var ** 0.5)
    
    def _accumulate_exec_counters(self, steps: Optional[List[str]]) -> None:
        if not steps:
            return
        for s in steps:
            if s.startswith("limit_submit"):
                self._exec_counters["limit_total"] += 1
            elif s.startswith("market_submit") or s.startswith("market_filled"):
                self._exec_counters["market_total"] += 1
            elif s.startswith("limit_cancel"):
                self._exec_counters["cancel_total"] += 1
            elif "rejected" in s:
                self._exec_counters["reject_total"] += 1

    def _inc_block_reason(self, key: str) -> None:
        self._block_reasons[key] = self._block_reasons.get(key, 0) + 1

    def _min_stop_ticks_default(self) -> float:
        """
        System-wide minimal SL distance, in ticks, when no setting is available.
        We keep this modest (8–12 ticks) to avoid qty rounding to zero and
        min-notional rejections on thin symbols.
        """
        default = 10.0  # <<< baseline 10 ticks (safe middle of 8–12 window)
        if get_settings:
            try:
                s = get_settings()
                val = getattr(getattr(s, "execution", object()), "min_stop_ticks", None)
                if isinstance(val, (int, float)) and val > 0:
                    return float(val)
            except Exception:
                pass
        return default
    
    def _exit_price_conservative(self, symbol: str, close_side: Side, entry_px: float) -> float:
        """
        Консервативная мгновенная цена выхода:
        - если выходим SELL (закрываем long) — берём bid,
        - если выходим BUY  (закрываем short) — берём ask,
        фолбэки: mark_price -> mid(bid/ask) -> entry_px.
        """
        try:
            bid, ask = self.hub.best_bid_ask(symbol)
        except Exception:
            bid, ask = 0.0, 0.0

        px = 0.0
        if close_side == "SELL" and bid > 0:
            px = bid
        elif close_side == "BUY" and ask > 0:
            px = ask

        if px <= 0.0:
            try:
                lt = self.latest_tick(symbol) or {}
                mp = float(lt.get("mark_price") or 0.0)
                if mp > 0:
                    px = mp
            except Exception:
                pass

        if px <= 0.0 and bid > 0 and ask > 0:
            px = (bid + ask) / 2.0

        if px <= 0.0:
            px = float(entry_px or 0.0)

        return float(px or 0.0)


    def _fee_bps_for_steps(self, steps: List[str]) -> float:
        """
        Выравниваем дефолты с app/status: maker=2.0 bps, taker=4.0 bps.
        Если в steps есть market_* или limit_filled_immediate — считаем как taker.
        """
        taker_like = any(
            s.startswith("market_") or s.startswith("limit_filled_immediate")
            for s in (steps or [])
        )
        maker_bps = 2.0
        taker_bps = 4.0
        if get_settings:
            try:
                s = get_settings()
                exe = getattr(s, "execution", object())
                maker_bps = float(getattr(exe, "fee_bps_maker", maker_bps))
                taker_bps = float(getattr(exe, "fee_bps_taker", taker_bps))
            except Exception:
                pass
        return taker_bps if taker_like else maker_bps
    
    def _get_price_step(self, sym: str) -> float:
        try:
            specs = getattr(self, "specs", {}) or {}
            spec = specs.get(sym)
            if spec is None:
                return 0.1
            for name in ("price_step", "price_tick", "tick_size", "tickSize"):
                v = getattr(spec, name, None)
                if v:
                    v = float(v)
                    if v > 0:
                        return v
        except Exception:
            pass
        return 0.1

    def _compute_vol_stop_distance_px(
        self,
        sym: str,
        mid_px: float,
        *,
        min_stop_ticks_cfg: int | None = None,
        window_ms: int = 60_000,
        max_lookback: int = 512,
        k_sigma: float = 1.8,
        spread_mult: float = 2.0,
    ) -> float:
        """
        Глобальный источник правды для стоп-дистанции.

        Гарантирует:
        - не меньше min_stop_ticks * price_step;
        - не меньше spread * spread_mult;
        - не меньше k_sigma * sigma(дельты за окно);
        - NEW: не меньше USD-пола по символу (чтобы риск не проигрывал комиссиям).
        """
        step = self._get_price_step(sym)
        if mid_px <= 0 or step <= 0:
            return step * max(8, int(min_stop_ticks_cfg or 8))

        # min_stop_ticks из exec_cfg / аргумента
        try:
            msticks = int(min_stop_ticks_cfg) if min_stop_ticks_cfg is not None else int((getattr(self, "_exec_cfg", {}) or {}).get("min_stop_ticks", 8))
        except Exception:
            msticks = 8
        base_min = max(step, step * max(4, msticks))

        # история для оценки sigma
        now_ms = int(getattr(self, "_now_ms", lambda: time.time() * 1000)())
        since = now_ms - int(window_ms)
        try:
            ticks = self.history_ticks(sym, since_ms=since, limit=max_lookback)  # type: ignore[attr-defined]
        except Exception:
            ticks = []

        prices: list[float] = []
        last_ts = 0
        for t in ticks or []:
            if isinstance(t, dict):
                ts = int(t.get("ts_ms") or t.get("ts") or 0)
                px = t.get("price")
                if px is None:
                    b = t.get("bid"); a = t.get("ask")
                    if b and a: px = (float(b) + float(a)) / 2.0
            else:
                ts = int(getattr(t, "ts_ms", getattr(t, "ts", 0)) or 0)
                px = getattr(t, "price", None)
                if px is None:
                    b = getattr(t, "bid", None); a = getattr(t, "ask", None)
                    if b and a: px = (float(b) + float(a)) / 2.0
            if ts: last_ts = max(last_ts, ts)
            if px: prices.append(float(px))

        if len(prices) < 20 or (last_ts and now_ms - last_ts > 3_000):
            # данных мало — вернём минимум в тиках
            dist = base_min
        else:
            diffs = [prices[i + 1] - prices[i] for i in range(len(prices) - 1)]
            if diffs:
                mean = sum(diffs) / len(diffs)
                var = sum((d - mean) ** 2 for d in diffs) / len(diffs)
                sigma = (var ** 0.5) if var > 0 else 0.0
            else:
                sigma = 0.0

            vol_stop = max(k_sigma * sigma, step)
            try:
                bid, ask = self.best_bid_ask(sym)
                spread = max(0.0, (ask - bid))
            except Exception:
                spread = 0.0
            spread_floor = spread * spread_mult if spread > 0 else 0.0

            dist = max(base_min, vol_stop, spread_floor)

        # --- NEW: жёсткий пол в USD по символам (чтобы expected_risk_usd >= fees)
        usd_floor_by_sym = {
            "BTCUSDT": 80.0,   # ~0.075% при ~106k
            "ETHUSDT": 10.0,   # ~0.28% при ~3.6k
            "SOLUSDT": 0.35,   # под шум/спред
        }
        try:
            usd_floor = float(usd_floor_by_sym.get(str(sym).upper(), 0.0))
            if usd_floor > 0:
                dist = max(dist, usd_floor)
        except Exception:
            pass

        # квантуем по шагу и уважаем min_stop_ticks
        if dist < step:
            dist = step
        dist_ticks = int(dist / step)
        if dist_ticks < msticks:
            dist_ticks = msticks
        return dist_ticks * step
    
    def _fees_for_symbol(self, sym: str) -> tuple[float, float]:
        """
        Вернёт (maker_bps, taker_bps) для символа из Executor.c
        с безопасными дефолтами 2.0 / 4.0 bps.
        """
        try:
            ex = self.execs.get(sym)
            if ex and hasattr(ex, "c"):
                return float(ex.c.fee_bps_maker), float(ex.c.fee_bps_taker)
        except Exception:
            pass
        return 2.0, 4.0
    
    def _entry_edge_ok(
        self,
        sym: str,
        mid: float,
        bid: float,
        ask: float,
        sl_distance_px: float,
        rr: float,
    ) -> tuple[bool, dict]:
        """
        Проверяем, что TP в бипсах >= (комиссии + спред + минимальный edge).
        Берём консервативно: обе стороны taker.
        """
        if mid <= 0 or sl_distance_px <= 0 or rr <= 0:
            return False, {"why": "bad_params"}

        tp_bps = (rr * sl_distance_px / mid) * 1e4

        try:
            ex = self.execs.get(sym)
            maker = float(getattr(ex.c, "fee_bps_maker", 2.0)) if ex else 2.0
            taker = float(getattr(ex.c, "fee_bps_taker", 4.0)) if ex else 4.0
        except Exception:
            maker, taker = 2.0, 4.0
        fees_bps_total = 2.0 * taker  # taker in + taker out

        spread = max(0.0, (ask - bid))
        spread_bps = (spread / mid) * 1e4 if mid > 0 else 0.0

        # ↓↓↓ fallback looser (settings.strategy.min_edge_bps still overrides) ↓↓↓
        min_edge_bps = 1.0
        if get_settings:
            try:
                s = get_settings()
                strat = getattr(s, "strategy", object())
                v = getattr(strat, "min_edge_bps", None)
                if isinstance(v, (int, float)) and v >= 0:
                    min_edge_bps = float(v)
            except Exception:
                pass

        slip_bps = 0.35 * spread_bps
        need_bps = fees_bps_total + 0.8 * spread_bps + slip_bps + min_edge_bps
        return (tp_bps >= need_bps), {
            "tp_bps": round(tp_bps, 3),
            "need_bps": round(need_bps, 3),
            "fees_bps_total": round(fees_bps_total, 3),
            "spread_bps": round(spread_bps, 3),
            "min_edge_bps": round(min_edge_bps, 3),
        }

    # --- strategy runtime api (исправлено: без несуществующих приватных полей) ---

    @property
    def auto_signal_enabled(self) -> bool:
        return self._auto_enabled

    @property
    def strategy_cfg(self) -> Dict[str, int]:
        return {
            "cooldown_ms": self._auto_cooldown_ms,
            "min_flat_ms": self._auto_min_flat_ms,
        }

    def set_strategy(self, *, enabled: bool, cooldown_ms: int, min_flat_ms: int) -> Dict[str, Any]:
        self._auto_enabled = bool(enabled)
        self._auto_cooldown_ms = max(0, int(cooldown_ms))
        self._auto_min_flat_ms = max(0, int(min_flat_ms))
        return {
            "auto_signal_enabled": self._auto_enabled,
            "strategy_cfg": {
                "cooldown_ms": self._auto_cooldown_ms,
                "min_flat_ms": self._auto_min_flat_ms,
            },
        }

    # --- sizing helpers ---

    def _compute_auto_qty(self, symbol: str, side: Side, sl_distance_px: float) -> float:
        """Размер позиции от процента риска и SL-дистанции. Учёт шага лота, плеча, минимальной нотионали."""
        spec = self.specs[symbol]
        risk_pct = float(self._risk_cfg.risk_per_trade_pct)  # трактуем как процент
        equity = float(self._starting_equity_usd + float(self._pnl_day.get("pnl_usd", 0.0) or 0.0))
        risk_usd_target = max(self._risk_cfg.min_risk_usd_floor, equity * (risk_pct / 100.0))

        if sl_distance_px <= 0:
            return 0.0

        qty_raw = risk_usd_target / sl_distance_px

        # ограничение по плечу (max notional)
        bid, ask = self.hub.best_bid_ask(symbol)
        px = ask if side == "BUY" else bid
        if px <= 0:
            return 0.0

        max_notional = equity * max(self._leverage, 1.0)
        max_qty_by_lev = max_notional / px
        qty = min(qty_raw, max_qty_by_lev)

        # минимальная нотиональ
        min_qty_by_notional = self._min_notional_usd / px

        # привести к шагу лота (вниз)
        step = max(spec.qty_step, 1e-12)

        def floor_to_step(x: float, st: float) -> float:
            return (int(x / st)) * st

        qty = max(min_qty_by_notional, qty)
        qty = floor_to_step(qty, step)

        # если после округления получилось 0 — попробуем хотя бы шаг, если укладывается в лимиты
        if qty <= 0:
            qty = step
            if qty * px < self._min_notional_usd or qty > max_qty_by_lev:
                return 0.0

        return float(qty)

    # --- cfg loaders ---

    def _make_candidate_engine(self, sym: str, spec: SymbolSpec) -> CandidateEngine:
        """
        Создаём CandidateEngine с параметрами из конфига (если есть),
        но без жёсткой привязки — безопасно для smoke.
        """
        # дефолты (соответствуют тому, что зашито в CandidateEngine)
        mom_thr_ticks = 5.0
        rev_thr_ticks = 8.0
        obi_mom_thr = 0.35
        obi_rev_thr = 0.30
        spread_max_ticks = 6.0

        if get_settings:
            try:
                s = get_settings()
                strat = getattr(s, "strategy", None)
                if strat is not None:
                    # можно подкрутить пороги из strategy.momentum / strategy.reversion
                    mom = getattr(strat, "momentum", None)
                    if mom is not None:
                        # если есть кастомное поле thr_ticks — используем
                        thr = getattr(mom, "thr_ticks", None)
                        if isinstance(thr, (int, float)) and thr > 0:
                            mom_thr_ticks = float(thr)

                    rev = getattr(strat, "reversion", None)
                    if rev is not None:
                        thr = getattr(rev, "thr_ticks", None)
                        if isinstance(thr, (int, float)) and thr > 0:
                            rev_thr_ticks = float(thr)

                    spread = getattr(strat, "max_spread_ticks", None)
                    if isinstance(spread, (int, float)) and spread > 0:
                        spread_max_ticks = float(spread)
            except Exception:
                # тихий фолбэк на дефолты
                pass

        return CandidateEngine(
            price_tick=spec.price_tick,
            max_history=256,
            mom_lookback=6,
            mom_thr_ticks=mom_thr_ticks,
            rev_window=48,
            rev_thr_ticks=rev_thr_ticks,
            obi_mom_thr=obi_mom_thr,
            obi_rev_thr=obi_rev_thr,
            spread_max_ticks=spread_max_ticks,
        )
    
    def _load_safety_cfg(self) -> SafetyCfg:
        cfg = SafetyCfg()
        if get_settings:
            try:
                s = get_settings()
                cfg = SafetyCfg(
                    max_spread_ticks=int(getattr(s.safety, "max_spread_ticks", cfg.max_spread_ticks)),
                    min_top5_liquidity_usd=float(getattr(s.safety, "min_top5_liquidity_usd", cfg.min_top5_liquidity_usd)),
                    skip_funding_minute=bool(getattr(s.safety, "skip_funding_minute", cfg.skip_funding_minute)),
                    skip_minute_zero=bool(getattr(s.safety, "skip_minute_zero", cfg.skip_minute_zero)),
                    min_liq_buffer_sl_mult=float(getattr(s.safety, "min_liq_buffer_sl_mult", cfg.min_liq_buffer_sl_mult)),
                )
            except Exception:
                logger.debug("safety cfg: using defaults")
            # мягко увеличим допуск по спреду, если в конфиге его не задали
        try:
            if not isinstance(cfg.max_spread_ticks, (int, float)) or cfg.max_spread_ticks <= 0:
                cfg.max_spread_ticks = 10
        except Exception:
            pass

        # --- MVP override: разрешаем торговать на минуте ':00'
        try:
            cfg.skip_minute_zero = False
        except Exception:
            pass

        return cfg

    def _load_risk_cfg(self) -> RiskCfg:
        cfg = RiskCfg()
        if get_settings:
            try:
                s = get_settings()
                cfg = RiskCfg(
                    risk_per_trade_pct=float(getattr(s.risk, "risk_per_trade_pct", cfg.risk_per_trade_pct)),
                    daily_stop_r=float(getattr(s.risk, "daily_stop_r", cfg.daily_stop_r)),
                    daily_target_r=float(getattr(s.risk, "daily_target_r", cfg.daily_target_r)),
                    max_consec_losses=int(getattr(s.risk, "max_consec_losses", cfg.max_consec_losses)),
                    cooldown_after_sl_s=int(getattr(s.risk, "cooldown_after_sl_s", cfg.cooldown_after_sl_s)),
                    min_risk_usd_floor=float(getattr(getattr(s, "risk", object()), "min_risk_usd_floor", cfg.min_risk_usd_floor)),
                )
            except Exception:
                logger.debug("risk cfg: using defaults")
        return cfg

    def _load_starting_equity_usd(self) -> float:
        default = 1000.0
        if get_settings:
            try:
                s = get_settings()
                val = getattr(getattr(s, "account", object()), "starting_equity_usd", default)
                return float(val)
            except Exception:
                pass
        return default

    def _load_leverage(self) -> float:
        default = 15.0
        if get_settings:
            try:
                s = get_settings()
                val = getattr(getattr(s, "account", object()), "leverage", default)
                return max(1.0, float(val))
            except Exception:
                pass
        return default

    def _load_min_notional_usd(self) -> float:
        default = 5.0  # для тестов/бумаги; в бою подставим реальные минимумы по символам
        if get_settings:
            try:
                s = get_settings()
                val = getattr(getattr(s, "account", object()), "min_notional_usd", default)
                return max(0.0, float(val))
            except Exception:
                pass
        return default

    # --- strategy cfg loader (robust: pydantic OR raw JSON fallback) ---

    def _load_strategy_cfg(self) -> None:
        """
        Загружает strategy.auto_* из настроек.

        Поддерживает:
          - Pydantic v2: .model_dump()
          - Pydantic v1: .dict()
          - Обычный объект с атрибутами
          - raw JSON из source_path

        Понимает ключи:
          - auto_signal_enabled / autoSignalEnabled
          - auto_cooldown_ms  / autoCooldownMs
          - auto_min_flat_ms  / autoMinFlatMs
        """
        auto_enabled = bool(self._auto_enabled)
        auto_cd = int(self._auto_cooldown_ms)
        auto_minflat = int(self._auto_min_flat_ms)

        def _apply_from_dict(d: Dict[str, Any]) -> None:
            nonlocal auto_enabled, auto_cd, auto_minflat
            if not isinstance(d, dict):
                return

            if "auto_signal_enabled" in d or "autoSignalEnabled" in d:
                auto_enabled = bool(
                    d.get("auto_signal_enabled", d.get("autoSignalEnabled", auto_enabled))
                )

            if "auto_cooldown_ms" in d or "autoCooldownMs" in d:
                try:
                    auto_cd = int(
                        d.get("auto_cooldown_ms", d.get("autoCooldownMs", auto_cd))
                    )
                except Exception:
                    pass

            if "auto_min_flat_ms" in d or "autoMinFlatMs" in d:
                try:
                    auto_minflat = int(
                        d.get("auto_min_flat_ms", d.get("autoMinFlatMs", auto_minflat))
                    )
                except Exception:
                    pass

        # 1) Пытаемся прочитать из get_settings()
        s = None
        if get_settings:
            try:
                s = get_settings()
            except Exception:
                s = None

        if s is not None:
            strat = getattr(s, "strategy", None)
            if strat is not None:
                # Pydantic v2
                if hasattr(strat, "model_dump"):
                    try:
                        _apply_from_dict(strat.model_dump())
                    except Exception:
                        pass
                # Pydantic v1
                elif hasattr(strat, "dict"):
                    try:
                        _apply_from_dict(strat.dict())
                    except Exception:
                        pass
                # уже dict
                elif isinstance(strat, dict):
                    _apply_from_dict(strat)
                # generic-объект
                else:
                    _apply_from_dict({
                        "auto_signal_enabled": getattr(strat, "auto_signal_enabled", auto_enabled),
                        "auto_cooldown_ms": getattr(strat, "auto_cooldown_ms", auto_cd),
                        "auto_min_flat_ms": getattr(strat, "auto_min_flat_ms", auto_minflat),
                    })

        # 2) Fallback: читаем raw JSON, если есть source_path
        if s is not None:
            try:
                import json
                import os

                source_path = getattr(s, "source_path", None)
                if source_path and os.path.exists(source_path):
                    with open(source_path, "r", encoding="utf-8") as f:
                        raw = json.load(f)
                    strat_raw = raw.get("strategy") or {}
                    if isinstance(strat_raw, dict):
                        _apply_from_dict(strat_raw)
            except Exception:
                # тихо: не ломаем старт
                pass

        # 3) Фиксируем значения (с защитой от мусора)
        self._auto_enabled = bool(auto_enabled)
        self._auto_cooldown_ms = max(0, int(auto_cd))
        self._auto_min_flat_ms = max(0, int(auto_minflat))

    def reload_strategy_cfg(self) -> Dict[str, Any]:
        """Горячая перечитка только strategy.* без перезапуска воркера."""
        self._load_strategy_cfg()
        return {
            "ok": True,
            "auto_signal_enabled": self._auto_enabled,
            "strategy_cfg": {
                "cooldown_ms": self._auto_cooldown_ms,
                "min_flat_ms": self._auto_min_flat_ms,
            },
        }
    def reload_runtime_cfg(self) -> dict:
        """
        Горячая перечитка settings.* без рестарта:
        - execution (limit_offset_ticks, fees, tif, poll_ms, prefer_maker)
        - safety/risk/account
        - strategy (auto_*), через reload_strategy_cfg()
        Обновляет конфиги у всех Executor'ов.
        """
        if not get_settings:
            return {"ok": False, "reason": "no_settings_provider"}

        # 1) читаем настройки
        try:
            s = get_settings()
        except Exception as e:
            return {"ok": False, "reason": "get_settings_failed", "error": str(e)}

        # 2) execution → обновить per-executor cfg
        exec_cfg = getattr(s, "execution", None)
        if exec_cfg is not None:
            try:
                limit_offset_ticks = int(getattr(exec_cfg, "limit_offset_ticks", 1))
                limit_timeout_ms   = int(getattr(exec_cfg, "limit_timeout_ms", 500))
                time_in_force      = str(getattr(exec_cfg, "time_in_force", "GTC"))
                poll_ms            = int(getattr(exec_cfg, "poll_ms", 50))
                fee_bps_maker      = float(getattr(exec_cfg, "fee_bps_maker", 2.0))
                fee_bps_taker      = float(getattr(exec_cfg, "fee_bps_taker", 4.0))
                prefer_maker       = bool(getattr(exec_cfg, "post_only", False))
            except Exception:
                limit_offset_ticks, limit_timeout_ms = 1, 500
                time_in_force, poll_ms = "GTC", 50
                fee_bps_maker, fee_bps_taker = 2.0, 4.0
                prefer_maker = False

            # store flat copy for helpers
            try:
                min_stop_ticks_cfg = int(getattr(exec_cfg, "min_stop_ticks", self._min_stop_ticks_default()))
            except Exception:
                min_stop_ticks_cfg = self._min_stop_ticks_default()

            self._exec_cfg = {
                "min_stop_ticks": int(min_stop_ticks_cfg),
                "limit_timeout_ms": int(limit_timeout_ms),
                "time_in_force": str(time_in_force),
            }

            # обновить всех исполнителей
            for sym, ex in (self.execs or {}).items():
                try:
                    c = ex.c
                    c.limit_offset_ticks = int(limit_offset_ticks)
                    c.limit_timeout_ms   = int(limit_timeout_ms)
                    c.time_in_force      = str(time_in_force)
                    c.poll_ms            = int(poll_ms)
                    c.fee_bps_maker      = float(fee_bps_maker)
                    c.fee_bps_taker      = float(fee_bps_taker)
                    c.prefer_maker       = bool(prefer_maker)
                except Exception:
                    continue

        # 3) safety / risk / account
        try:
            self._safety_cfg = self._load_safety_cfg()
        except Exception:
            pass
        try:
            self._risk_cfg = self._load_risk_cfg()
        except Exception:
            pass
        try:
            # account поля
            self._starting_equity_usd = self._load_starting_equity_usd()
            self._leverage            = self._load_leverage()
            self._min_notional_usd    = self._load_min_notional_usd()
        except Exception:
            pass

        # 4) strategy auto_* (без рестарта)
        self.reload_strategy_cfg()

        # 5) вернуть актуальный снапшот
        try:
            snap = self.diag()
        except Exception:
            snap = {"ok": True}
        return {"ok": True, "diag": snap}

    # совместимость с /status фолбэком
    @property
    def coalescer(self) -> Coalescer:
        return self.coal

    

# ---------- модульный smoke-тест ----------

async def _smoke() -> None:
    syms = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    w = Worker(syms, futures=True, coalesce_ms=75, history_maxlen=2000)
    await w.start()
    print("[smoke] started, warming up ticks...")
    await asyncio.sleep(2.5)

    bid, ask = w.best_bid_ask("BTCUSDT")
    print(f"[smoke] BTC best bid/ask = {bid} / {ask}")
    if bid == 0.0 or ask == 0.0:
        print("[smoke] no best price yet, wait a bit more...")
        await asyncio.sleep(2.0)

    rep = await w.place_entry_auto("BTCUSDT", "BUY")
    print("[smoke] AUTO EXEC:", rep)

    await w.flatten("BTCUSDT")
    diag = w.diag()
    print("[smoke] trades recorded:", len(diag.get("trades", {}).get("BTCUSDT", [])))
    print("[smoke] pnl_day:", diag.get("pnl_day"))

    hist = w.history_ticks("BTCUSDT", since_ms=0, limit=5)
    print("[smoke] last ticks:", len(hist), "example:", hist[-1] if hist else None)

    await w.stop()


def _run_smoke() -> None:
    try:
        asyncio.run(_smoke())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    _run_smoke()
