# bot/worker.py
from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Literal
from collections import deque
from typing import Deque

from exec.fsm import PositionFSM
from bot.core.types import Side as CoreSide  # (может быть неиспользован — ок)

from adapters.binance_ws import BinanceWS
from stream.coalescer import Coalescer, TickerTick
from adapters.binance_rest import PaperAdapter
from exec.executor import Executor, ExecCfg, ExecutionReport
from exec.sltp import compute_sltp, SLTPPlan  # план цен SL/TP (paper)

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

        # движок кандидатов (если используешь)
        self._cand: Dict[str, CandidateEngine] = {}

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

        # базовые дефолты
        limit_offset_ticks = 1
        limit_timeout_ms = 500
        time_in_force = "GTC"
        max_slippage_bp = 6.0
        poll_ms = 50
        fee_bps_maker = 2.0
        fee_bps_taker = 4.0
        prefer_maker = False

        # один раз читаем settings (если есть)
        s = None
        if get_settings:
            try:
                s = get_settings()
            except Exception:
                s = None

        if s is not None:
            exec_cfg = getattr(s, "execution", None)
            if exec_cfg is not None:
                limit_offset_ticks = int(getattr(exec_cfg, "limit_offset_ticks", limit_offset_ticks))
                limit_timeout_ms = int(getattr(exec_cfg, "limit_timeout_ms", limit_timeout_ms))
                max_slippage_bp = float(getattr(exec_cfg, "max_slippage_bp", max_slippage_bp))
                time_in_force = str(getattr(exec_cfg, "time_in_force", time_in_force))
                poll_ms = int(getattr(exec_cfg, "poll_ms", poll_ms))
                fee_bps_maker = float(getattr(exec_cfg, "fee_bps_maker", fee_bps_maker))
                fee_bps_taker = float(getattr(exec_cfg, "fee_bps_taker", fee_bps_taker))
                prefer_maker = bool(getattr(exec_cfg, "prefer_maker", prefer_maker))

            # авто-сигналы — для обратной совместимости
            strat_cfg = getattr(s, "strategy", None)
            if strat_cfg is not None:
                self._auto_enabled = bool(getattr(strat_cfg, "auto_signal_enabled", self._auto_enabled))
                self._auto_cooldown_ms = int(getattr(strat_cfg, "auto_cooldown_ms", self._auto_cooldown_ms))
                self._auto_min_flat_ms = int(getattr(strat_cfg, "auto_min_flat_ms", self._auto_min_flat_ms))

        # если TIF пост-онли — включаем prefer_maker
        tif_upper = (time_in_force or "").upper()
        if tif_upper in ("GTX", "PO", "POST_ONLY"):
            prefer_maker = True

        # инициализируем executors + кандидаты по всем символам
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

            self._cand[sym] = CandidateEngine(price_tick=spec.price_tick)

        # запускаем потоки
        await self.ws.connect(self._on_ws_raw)
        self._tasks.append(asyncio.create_task(self.coal.run(self._on_tick), name="coal.run"))

        # фоновые циклы
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

        # (здесь позже может быть enrichment для фич, если потребуется)

    # ---------- strategy loop (авто-сигналы) ----------

    async def _strategy_loop(self) -> None:
        """
        Лёгкий авто-ордеринг поверх CandidateEngine.

        Принципы:
        - работаем ТОЛЬКО если _auto_enabled = True;
        - дневные лимиты проверяем один раз на цикл;
        - по символу входим только из FLAT;
        - соблюдаем cooldown и min_flat;
        - используем только свежие тики;
        - кандидат → check_entry_safety → place_entry_auto (единый пайплайн);
        - все фейлы логируем в _block_reasons (для /debug/diag);
        - не роняем цикл ни при каких исключениях.
        """
        tick_stale_ms = 2500  # максимум "возраст" тика для авто-входа (ms)

        try:
            while True:
                await asyncio.sleep(0.12)  # ~8 Гц опроса

                if not self._auto_enabled:
                    continue

                now = self._now_ms()

                # ---- 1) Дневные лимиты (общие) ----
                day_dec = None
                try:
                    day_state = DayState(
                        pnl_r_day=float(self._pnl_day.get("pnl_r", 0.0) or 0.0),
                        consec_losses=int(self._consec_losses),
                        trading_disabled=False,
                    )
                    day_dec = check_day_limits(day_state, self._risk_cfg)
                except Exception:
                    # если фильтр сломался — не блокируем торговлю целиком, но помечаем
                    self._inc_block_reason("day_limits_error")

                if day_dec is not None and not day_dec.allow:
                    for r in (day_dec.reasons or []):
                        self._inc_block_reason("day_" + str(r))
                    # дневной стоп: выходим из цикла итерации, ждём следующего тика/дня
                    continue

                # ---- 2) Перебор символов ----
                for sym in self.symbols:
                    pos = self._pos[sym]

                    # 2.1. Только из FLAT
                    if pos.state != "FLAT":
                        continue

                    # 2.2. Anti-spam: cooldown между сигналами
                    last_sig = int(self._last_signal_ms.get(sym, 0))
                    if now - last_sig < int(self._auto_cooldown_ms):
                        continue

                    # 2.3. Минимальное FLAT-время после предыдущей позиции
                    last_flat = int(self._last_flat_ms.get(sym, 0))
                    if now - last_flat < int(self._auto_min_flat_ms):
                        continue

                    # 2.4. Свежий тик
                    last = self.latest_tick(sym)
                    if not last:
                        self._inc_block_reason(f"{sym}:no_tick")
                        continue

                    ts_ms = int(last.get("ts_ms") or 0)
                    if ts_ms > 0 and now - ts_ms > tick_stale_ms:
                        self._inc_block_reason(f"{sym}:stale_tick")
                        continue

                    # 2.5. Кандидат от стратегии
                    ce = self._cand.get(sym)
                    if not ce:
                        self._inc_block_reason(f"{sym}:no_candidate_engine")
                        continue

                    try:
                        dec: Decision = ce.update(
                            price=float(last.get("price") or 0.0),
                            bid=float(last.get("bid") or 0.0),
                            ask=float(last.get("ask") or 0.0),
                            bid_sz=float(last.get("bid_size") or 0.0),
                            ask_sz=float(last.get("ask_size") or 0.0),
                        )
                    except Exception:
                        self._inc_block_reason(f"{sym}:cand_error")
                        continue

                    side_raw = (getattr(dec, "side", None) or "").upper()
                    if side_raw not in ("BUY", "SELL"):
                        reason = getattr(dec, "reason", None) or "no_side"
                        self._inc_block_reason(f"{sym}:skip_{reason}")
                        continue

                    side: Side = "BUY" if side_raw == "BUY" else "SELL"

                    # 2.6. Safety перед входом (спред, время, базовые фильтры)
                    bid, ask = self.hub.best_bid_ask(sym)
                    spec = self.specs.get(sym)
                    spread_ticks = 0.0
                    if spec and bid > 0 and ask > 0 and spec.price_tick > 0:
                        spread_ticks = max(0.0, (ask - bid) / spec.price_tick)

                    micro = MicroCtx(
                        spread_ticks=spread_ticks,
                        top5_liq_usd=1e12,  # для paper MVP считаем ликвидность достаточной
                    )
                    tctx = TimeCtx(
                        ts_ms=now,
                        server_time_offset_ms=self._server_time_offset_ms,
                    )

                    pre_dec = check_entry_safety(
                        side,
                        micro=micro,
                        time_ctx=tctx,
                        pos_ctx=None,
                        safety=self._safety_cfg,
                    )
                    if not pre_dec.allow:
                        for r in (pre_dec.reasons or []):
                            self._inc_block_reason(f"{sym}:{r}")
                        continue

                    # 2.7. Унифицированный авто-вход (risk sizing + SL/TP внутри)
                    rep = await self.place_entry_auto(sym, side)

                    if not rep.get("ok", True):
                        reason = rep.get("reason", "auto_entry_fail")
                        self._inc_block_reason(f"{sym}:{reason}")

                    # Даже если вход не удался — не спамим сигналами чаще cooldown.
                    self._last_signal_ms[sym] = now

        except asyncio.CancelledError:
            # штатное завершение при остановке воркера
            return
        except Exception:
            logger.exception("strategy loop error")

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

    async def place_entry_auto(self, symbol: str, side: Side) -> Dict[str, Any]:
        """
        Авто-сайзинг по risk_per_trade_pct и текущей SL-дистанции.
        Условия и SL/TP — как в place_entry().
        """
        symbol = symbol.upper()
        if symbol not in self.execs:
            raise ValueError(f"Unsupported symbol: {symbol}")

        # быстрый чек дневных лимитов (до локов)
        day_dec = check_day_limits(
            DayState(
                pnl_r_day=float(self._pnl_day.get("pnl_r", 0.0) or 0.0),
                consec_losses=int(self._consec_losses),
                trading_disabled=False,
            ),
            self._risk_cfg,
        )
        if not day_dec.allow:
            for r in day_dec.reasons:
                self._inc_block_reason("day_" + r)
            return {"ok": False, "reason": ",".join(["day_" + r for r in day_dec.reasons]), "symbol": symbol, "side": side}

        lock = self._locks[symbol]
        async with lock:
            pos = self._pos[symbol]
            if pos.state != "FLAT":
                self._inc_block_reason("busy_" + pos.state)
                return {"ok": False, "reason": f"busy:{pos.state}", "symbol": symbol, "side": side}

            # safety pre-check
            bid, ask = self.hub.best_bid_ask(symbol)
            spec = self.specs[symbol]
            spread_ticks = 0.0
            if bid > 0 and ask > 0 and spec.price_tick > 0:
                spread_ticks = max(0.0, (ask - bid) / spec.price_tick)

            micro = MicroCtx(spread_ticks=spread_ticks, top5_liq_usd=1e12)
            tctx = TimeCtx(ts_ms=self._now_ms(), server_time_offset_ms=self._server_time_offset_ms)
            pre_dec = check_entry_safety(side, micro, tctx, pos_ctx=None, safety=self._safety_cfg)
            if not pre_dec.allow:
                for r in pre_dec.reasons:
                    self._inc_block_reason(r)
                return {"ok": False, "reason": ",".join(pre_dec.reasons), "symbol": symbol, "side": side}

            # оценим SL-дистанцию как в обычном входе
            min_stop_ticks = self._min_stop_ticks_default()
            sl_distance_px = max(min_stop_ticks * spec.price_tick, max(spread_ticks, 1.0) * spec.price_tick)

            qty = self._compute_auto_qty(symbol, side, sl_distance_px)
            if qty <= 0.0:
                self._inc_block_reason("qty_too_small")
                return {"ok": False, "reason": "qty_too_small", "symbol": symbol, "side": side}

        # ВНИМАНИЕ: выходим из локов и используем стандартный путь входа
        return await self.place_entry(symbol, side, qty)

    async def place_entry(self, symbol: str, side: Side, qty: float) -> Dict[str, Any]:
        """
        Унифицированный вход (ручной / авто):

          - дневные лимиты,
          - только из FLAT,
          - pre-safety,
          - исполнение через Executor,
          - расчёт SL/TP (paper),
          - post-safety,
          - обновление FSM и in-memory позиции.
        """
        symbol = symbol.upper()
        if symbol not in self.execs:
            raise ValueError(f"Unsupported symbol: {symbol}")

        # --- 0) дневные лимиты до локов ---
        day_dec = check_day_limits(
            DayState(
                pnl_r_day=float(self._pnl_day.get("pnl_r", 0.0) or 0.0),
                consec_losses=int(self._consec_losses),
                trading_disabled=False,
            ),
            self._risk_cfg,
        )
        if not day_dec.allow:
            reasons = [f"day_{r}" for r in day_dec.reasons]
            for r in reasons:
                self._inc_block_reason(r)
            return {
                "ok": False,
                "reason": ",".join(reasons),
                "symbol": symbol,
                "side": side,
                "qty": qty,
            }

        lock = self._locks[symbol]
        async with lock:
            pos = self._pos[symbol]
            if pos.state != "FLAT":
                reason = f"busy:{pos.state}"
                self._inc_block_reason("busy_" + pos.state)
                return {
                    "ok": False,
                    "reason": reason,
                    "symbol": symbol,
                    "side": side,
                    "qty": qty,
                }

            # --- 1) pre-safety ---
            bid, ask = self.hub.best_bid_ask(symbol)
            spec = self.specs[symbol]

            spread_ticks = 0.0
            if bid > 0 and ask > 0 and spec.price_tick > 0:
                spread_ticks = max(0.0, (ask - bid) / spec.price_tick)

            micro = MicroCtx(spread_ticks=spread_ticks, top5_liq_usd=1e12)
            tctx = TimeCtx(ts_ms=self._now_ms(), server_time_offset_ms=self._server_time_offset_ms)

            pre_dec = check_entry_safety(
                side,
                micro=micro,
                time_ctx=tctx,
                pos_ctx=None,
                safety=self._safety_cfg,
            )
            if not pre_dec.allow:
                for r in pre_dec.reasons:
                    self._inc_block_reason(r)
                return {
                    "ok": False,
                    "reason": ",".join(pre_dec.reasons),
                    "symbol": symbol,
                    "side": side,
                    "qty": qty,
                }

            # --- 2) FSM → ENTERING ---
            with contextlib.suppress(Exception):
                await self._fsm[symbol].on_entering()
            pos.state = "ENTERING"
            pos.side = side

            # --- 3) исполнение через Executor ---
            ex = self.execs[symbol]
            rep: ExecutionReport = await ex.place_entry(symbol, side, qty)
            self._accumulate_exec_counters(rep.steps)

            if rep.status not in ("FILLED", "PARTIAL") or rep.filled_qty <= 0.0 or rep.avg_px is None:
                # нет fill → откат к FLAT
                self._pos[symbol] = PositionState(
                    state="FLAT",
                    side=None,
                    qty=0.0,
                    entry_px=0.0,
                    sl_px=None,
                    tp_px=None,
                    opened_ts_ms=0,
                    timeout_ms=pos.timeout_ms,
                )
                with contextlib.suppress(Exception):
                    await self._fsm[symbol].on_flat()

                return {
                    "ok": False,
                    "symbol": symbol,
                    "side": side,
                    "qty": float(qty),
                    "report": {
                        "status": rep.status,
                        "filled_qty": float(rep.filled_qty),
                        "avg_px": rep.avg_px,
                        "limit_oid": rep.limit_oid,
                        "market_oid": rep.market_oid,
                        "steps": list(rep.steps or []),
                        "ts": rep.ts,
                    },
                }

            # --- 4) SL/TP + post-safety ---
            entry_px = float(rep.avg_px)
            qty_open = float(rep.filled_qty)

            # пересчёт спреда уже после входа
            bid, ask = self.hub.best_bid_ask(symbol)
            spread_ticks = 0.0
            if bid > 0 and ask > 0 and spec.price_tick > 0:
                spread_ticks = max(0.0, (ask - bid) / spec.price_tick)

            min_stop_ticks = self._min_stop_ticks_default()
            sl_distance_px = max(
                min_stop_ticks * spec.price_tick,
                max(spread_ticks, 1.0) * spec.price_tick,
            )

            plan: SLTPPlan = compute_sltp(
                side=side,
                entry_px=entry_px,
                qty=qty_open,
                price_tick=spec.price_tick,
                sl_distance_px=sl_distance_px,
                rr=1.8,
            )

            post_dec = check_entry_safety(
                side,
                micro=micro,
                time_ctx=tctx,
                pos_ctx=PositionalCtx(
                    entry_px=entry_px,
                    sl_px=plan.sl_px,
                    leverage=self._leverage,
                ),
                safety=self._safety_cfg,
            )

            if not post_dec.allow:
                for r in post_dec.reasons:
                    self._inc_block_reason(r)

                # откат к FLAT, позицию не держим
                self._pos[symbol] = PositionState(
                    state="FLAT",
                    side=None,
                    qty=0.0,
                    entry_px=0.0,
                    sl_px=None,
                    tp_px=None,
                    opened_ts_ms=0,
                    timeout_ms=pos.timeout_ms,
                )
                with contextlib.suppress(Exception):
                    await self._fsm[symbol].on_flat()

                return {
                    "ok": False,
                    "reason": ",".join(post_dec.reasons),
                    "symbol": symbol,
                    "side": side,
                    "qty": float(qty),
                    "report": {
                        "status": rep.status,
                        "filled_qty": float(qty_open),
                        "avg_px": entry_px,
                        "limit_oid": rep.limit_oid,
                        "market_oid": rep.market_oid,
                        "steps": list(rep.steps or [])
                        + [f"blocked_after_plan:{'|'.join(post_dec.reasons)}"],
                        "ts": rep.ts,
                    },
                }

            # --- 5) фиксация открытой позиции ---
            opened_ts_ms = self._now_ms()
            self._pos[symbol] = PositionState(
                state="OPEN",
                side=side,
                qty=qty_open,
                entry_px=entry_px,
                sl_px=plan.sl_px,
                tp_px=plan.tp_px,
                opened_ts_ms=opened_ts_ms,
                timeout_ms=pos.timeout_ms,
            )
            self._entry_steps[symbol] = list(rep.steps or [])

            with contextlib.suppress(Exception):
                await self._fsm[symbol].on_open(
                    entry_px=entry_px,
                    qty=qty_open,
                    sl=float(plan.sl_px) if plan.sl_px is not None else entry_px,
                    tp=float(plan.tp_px) if plan.tp_px is not None else None,
                    rr=1.8,
                    opened_ts=opened_ts_ms,
                )

            return {
                "ok": True,
                "symbol": symbol,
                "side": side,
                "qty": float(qty_open),
                "report": {
                    "status": rep.status,
                    "filled_qty": float(qty_open),
                    "avg_px": entry_px,
                    "limit_oid": rep.limit_oid,
                    "market_oid": rep.market_oid,
                    "steps": list(rep.steps or [])
                    + [f"protection_set:SL@{plan.sl_px} TP@{plan.tp_px}"],
                    "ts": self._now_ms(),
                },
            }

    # ---------- watchdog / closing / trades ----------

    async def _watchdog_loop(self) -> None:
        """
        Watchdog:
          - следим за таймаутом позиции,
          - гарантируем наличие защиты,
          - исполняем SL/TP по последней цене (paper-режим),
          - не роняем цикл при единичных ошибках.
        """
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

                        # 1) Таймаут позиции
                        if pos.opened_ts_ms and (now - pos.opened_ts_ms) >= pos.timeout_ms:
                            await self._paper_close(sym, pos, reason="timeout")
                            continue

                        # 2) Если вообще нет защитных уровней — закрываем (fail-safe)
                        if pos.sl_px is None and pos.tp_px is None:
                            await self._paper_close(sym, pos, reason="no_protection")
                            continue

                        # 3) Проверка SL/TP по последнему тику (paper-логика)
                        last = self.latest_tick(sym)
                        if not last:
                            continue

                        px = 0.0
                        try:
                            # предпочитаем mark_price, если есть; иначе trade price
                            mp = float(last.get("mark_price") or 0.0)
                            p = float(last.get("price") or 0.0)
                            px = mp if mp > 0 else p
                        except Exception:
                            px = 0.0

                        if px <= 0.0:
                            continue

                        side = pos.side or "BUY"

                        # BUY (long): SL ниже, TP выше
                        if side == "BUY":
                            if pos.sl_px is not None and px <= pos.sl_px:
                                await self._paper_close(sym, pos, reason="sl_hit")
                                continue
                            if pos.tp_px is not None and px >= pos.tp_px:
                                await self._paper_close(sym, pos, reason="tp_hit")
                                continue

                        # SELL (short): SL выше, TP ниже
                        elif side == "SELL":
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
        - Executor.place_exit используем только для получения steps/аудита, но не даём ему ломать цену SL/TP.
        - В любом случае в конце приводим состояние к FLAT.
        """
        side = pos.side
        qty = float(pos.qty or 0.0)
        entry_px = float(pos.entry_px or 0.0)
        sl_px = float(pos.sl_px) if pos.sl_px is not None else None
        tp_px = float(pos.tp_px) if pos.tp_px is not None else None
        opened_ts_ms = int(pos.opened_ts_ms or 0)
        timeout_ms = int(pos.timeout_ms or 180_000)

        # Если по факту позиции нет — просто FLAT
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
            with contextlib.suppress(Exception):
                await self._fsm[sym].on_flat()
            return

        close_side: Side = "SELL" if side == "BUY" else "BUY"

        # Целевой уровень для SL/TP
        level_px: Optional[float] = None
        if reason == "sl_hit" and sl_px is not None:
            level_px = sl_px
        elif reason == "tp_hit" and tp_px is not None:
            level_px = tp_px

        exit_px: Optional[float] = None
        exit_steps: Optional[list[str]] = None

        try:
            with contextlib.suppress(Exception):
                await self._fsm[sym].on_exiting()

            ex = self.execs.get(sym)

            # --- 1) Попробуем формально вызвать Executor (reduce_only), чтобы получить steps ---
            if ex is not None:
                try:
                    rep: ExecutionReport = await ex.place_exit(sym, close_side, qty)
                    self._accumulate_exec_counters(rep.steps)
                    exit_steps = list(rep.steps or [])
                except Exception:
                    # не роняем закрытие, просто падаем в fallback
                    exit_steps = ["exec_exit_error"]

            # --- 2) Определяем финальную цену выхода ---
            if level_px is not None:
                # Для sl_hit / tp_hit в paper-режиме ЖЁСТКО закрываем по плановому уровню.
                exit_px = float(level_px)
            else:
                # Для остальных причин берём честную консервативную цену.
                exit_px = self._exit_price_conservative(sym, close_side, entry_px)

            # --- 3) Регистрируем трейд, если цена вменяемая ---
            if exit_px and exit_px > 0.0:
                entry_steps = self._entry_steps.get(sym, [])
                trade = self._build_trade_record(
                    sym,
                    pos,
                    float(exit_px),
                    reason=reason,
                    entry_steps=entry_steps,
                    exit_steps=exit_steps,
                )
                self._register_trade(trade)
                asyncio.create_task(self._rebuild_trades_safe())

        finally:
            # --- 4) Всегда приводим к FLAT ---
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

        if qty <= 0.0 or entry <= 0.0:
            # защита от мусора, не должно сюда доходить
            return {
                "symbol": sym,
                "side": side,
                "opened_ts": int(pos.opened_ts_ms or 0),
                "closed_ts": self._now_ms(),
                "qty": qty,
                "entry_px": entry,
                "exit_px": float(exit_px),
                "sl_px": sl_px,
                "tp_px": tp_px,
                "pnl_usd": 0.0,
                "pnl_r": 0.0,
                "reason": f"{reason}|invalid_pos",
                "fees": 0.0,
            }

        # ----- PnL -----
        if side == "BUY":
            pnl_usd_gross = (exit_px - entry) * qty
            sl_ref = sl_px if sl_px is not None and sl_px < entry else entry
            pnl_risk_usd = abs(entry - sl_ref) * qty
        else:  # SELL (short)
            pnl_usd_gross = (entry - exit_px) * qty
            sl_ref = sl_px if sl_px is not None and sl_px > entry else entry
            pnl_risk_usd = abs(sl_ref - entry) * qty

        # ----- комиссии по steps (paper-аудит) -----
        entry_bps = self._fee_bps_for_steps(entry_steps or [])
        exit_bps = self._fee_bps_for_steps(exit_steps or [])
        fees_usd = (entry * qty * entry_bps + exit_px * qty * exit_bps) / 10_000.0

        pnl_usd = pnl_usd_gross - fees_usd

        # ----- корректный R -----
        min_floor = float(getattr(self._risk_cfg, "min_risk_usd_floor", 0.0) or 0.0)
        # Если SL не задан или слишком близко — всё равно не даём risk_usd стать микроскопическим
        risk_usd = max(pnl_risk_usd, min_floor, 1e-6)
        pnl_r = pnl_usd / risk_usd

        return {
            "symbol": sym,
            "side": side,
            "opened_ts": int(pos.opened_ts_ms or 0),
            "closed_ts": self._now_ms(),
            "qty": qty,
            "entry_px": entry,
            "exit_px": float(exit_px),
            "sl_px": sl_px,
            "tp_px": tp_px,
            "pnl_usd": round(pnl_usd, 6),
            "pnl_r": round(pnl_r, 6),
            "reason": reason,
            "fees": round(fees_usd, 6),
        }

    def _register_trade(self, trade: Dict[str, Any]) -> None:
        sym = trade["symbol"]
        dq = self._trades.get(sym)
        if dq is None:
            dq = self._trades[sym] = deque(maxlen=1000)
        dq.appendleft(trade)

        day = self._day_str()
        if self._pnl_day["day"] != day:
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

        self._pnl_day["trades"] += 1
        self._pnl_day["pnl_usd"] += trade["pnl_usd"]
        self._pnl_day["pnl_r"] += trade["pnl_r"]

        if trade["pnl_r"] > 0:
            self._consec_losses = 0
        else:
            self._consec_losses += 1

        wins = int(round((self._pnl_day.get("winrate_raw", 0.0) or 0.0) * max(self._pnl_day["trades"] - 1, 0)))
        if trade["pnl_r"] > 0:
            wins += 1
        self._pnl_day["winrate_raw"] = wins / max(self._pnl_day["trades"], 1)
        self._pnl_day["winrate"] = round(self._pnl_day["winrate_raw"], 4)
        self._pnl_day["avg_r"] = round(self._pnl_day["pnl_r"] / max(self._pnl_day["trades"], 1), 6)

        self._pnl_r_equity += trade["pnl_r"]
        peak = self._pnl_day.get("peak_r", 0.0)
        trough = self._pnl_day.get("trough_r", 0.0)
        if self._pnl_r_equity > peak:
            peak = self._pnl_r_equity
        if self._pnl_r_equity < trough:
            trough = self._pnl_r_equity
        self._pnl_day["peak_r"] = peak
        self._pnl_day["trough_r"] = trough
        self._pnl_day["max_dd_r"] = round(peak - trough if peak - trough > 0 else 0.0, 6)

        # ---------- diag ----------

    def _unrealized_snapshot(self) -> Dict[str, Any]:
        """
        Быстрый uPnL-снапшот:
        - считаем по консервативной цене выхода (_exit_price_conservative),
        - учитываем только taker-комиссию на выход,
        - возвращаем total_usd, per_symbol и число открытых позиций.
        """
        taker_bps = 4.0
        if get_settings:
            try:
                s = get_settings()
                taker_bps = float(getattr(getattr(s, "execution", object()), "fee_bps_taker", taker_bps))
            except Exception:
                pass

        total = 0.0
        per_symbol: Dict[str, float] = {}
        open_positions = 0

        for sym, p in self.diag_positions().items():
            if p.get("state") != "OPEN":
                continue

            side = p.get("side")
            qty = float(p.get("qty") or 0.0)
            entry = float(p.get("entry_px") or 0.0)
            if qty <= 0.0 or entry <= 0.0 or not side:
                continue

            close_side: Side = "SELL" if side == "BUY" else "BUY"
            exit_px = self._exit_price_conservative(sym, close_side, entry)
            if exit_px <= 0.0:
                continue

            if side == "BUY":
                pnl_gross = (exit_px - entry) * qty
            else:
                pnl_gross = (entry - exit_px) * qty

            fee = exit_px * qty * (taker_bps / 10_000.0)
            u = float(round(pnl_gross - fee, 6))

            per_symbol[sym] = u
            total += u
            open_positions += 1

        return {
            "total_usd": float(round(total, 6)),
            "per_symbol": per_symbol or None,
            "open_positions": open_positions,
        }

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
        default = 2.0
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
                maker_bps = float(getattr(s.execution, "fee_bps_maker", maker_bps))
                taker_bps = float(getattr(s.execution, "fee_bps_taker", taker_bps))
            except Exception:
                 pass
        return taker_bps if taker_like else maker_bps


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
