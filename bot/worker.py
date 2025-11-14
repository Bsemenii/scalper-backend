# bot/worker.py
from __future__ import annotations

import asyncio
import contextlib
import logging
import os
from dotenv import load_dotenv
import time
import math
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Literal
from collections import deque
from typing import Deque
from contextlib import contextmanager
from strategy.cross_guard import decide_cross_guard, CrossCfg, pick_best_symbol_by_speed
from adapters.binance_rest import PaperAdapter, BinanceUSDTMAdapter

from exec.fsm import PositionFSM
from bot.core.types import Side as CoreSide  # (может быть неиспользован — ок)
from bot.core.config import get_settings

from adapters.binance_ws import BinanceWS
from stream.coalescer import Coalescer, TickerTick
from adapters.binance_rest import PaperAdapter
from exec.executor import Executor, ExecCfg, ExecutionReport
from strategy.router import StrategyCfg, pick_candidate
from strategy.momentum import EntryCandidate as MomEntry
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
load_dotenv()


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
        # --- базовая инфраструктура рынка / потоков ---
        self.symbols = [s.upper() for s in symbols]
        self.ws = BinanceWS(self.symbols, futures=futures, mark_interval_1s=True)
        self.coal = Coalescer(coalesce_ms=coalesce_ms)
        self.hub = MarketHub(self.symbols)

        # --- настройки TP/форс-закрытия (оставляем как было) ---
        self._tp_tol_ticks = getattr(self, "_tp_tol_ticks", 1)     # допуск по тикам
        self._tp_grace_ms  = getattr(self, "_tp_grace_ms", 350)    # сколько ждём лимит перед market
        self._tp_force     = getattr(self, "_tp_force", True)      # включить форс-закрытие

        # --- история тиков (последние N по символу) ---
        self._hist: Dict[str, Deque[Dict[str, Any]]] = {
            s: deque(maxlen=history_maxlen) for s in self.symbols
        }
        self._history_maxlen = history_maxlen
        self._hist_lock = asyncio.Lock()

        # --- спецификации шагов (дефолты для перпов) ---
        self.specs: Dict[str, SymbolSpec] = {
            "BTCUSDT": SymbolSpec("BTCUSDT", price_tick=0.1,  qty_step=0.001),
            "ETHUSDT": SymbolSpec("ETHUSDT", price_tick=0.01, qty_step=0.001),
            "SOLUSDT": SymbolSpec("SOLUSDT", price_tick=0.001, qty_step=0.1),
        }
        for s in self.symbols:
            if s not in self.specs:
                self.specs[s] = SymbolSpec(s, price_tick=0.01, qty_step=0.001)

        # --- исполнители и FSM по символам ---
        self.execs: Dict[str, Executor] = {}
        self._fsm: Dict[str, PositionFSM] = {s: PositionFSM() for s in self.symbols}

        # --- локи и рантайм-позиции ---
        self._locks: Dict[str, asyncio.Lock] = {s: asyncio.Lock() for s in self.symbols}
        self._pos: Dict[str, PositionState] = {
            s: PositionState(
                state="FLAT",
                side=None,
                qty=0.0,
                entry_px=0.0,
                sl_px=None,
                tp_px=None,
                opened_ts_ms=0,
                timeout_ms=180_000,
            )
            for s in self.symbols
        }

        # --- счётчики исполнения (для /metrics) ---
        self._exec_counters: Dict[str, int] = {
            "limit_total": 0,
            "market_total": 0,
            "cancel_total": 0,
            "reject_total": 0,
        }

        # --- учёт сделок (in-memory) и дневная сводка ---
        self._trades: Dict[str, Deque[Dict[str, Any]]] = {
            s: deque(maxlen=1000) for s in self.symbols
        }
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

        # ts последнего стоп-лосса (для cooldown_after_sl_s)
        self._last_sl_ts_ms: int = 0

        # --- причины блокировок входа ---
        self._block_reasons: Dict[str, int] = {}

        # --- для оценки комиссий входа: сохраняем steps открытия ---
        self._entry_steps: Dict[str, List[str]] = {}

        # --- риск / safety конфиги ---
        self._safety_cfg = self._load_safety_cfg()
        self._risk_cfg = self._load_risk_cfg()
        self._server_time_offset_ms: int = 0

        # --- account-конфиги (для авто-сайзинга) ---
        self._starting_equity_usd = self._load_starting_equity_usd()
        self._leverage = self._load_leverage()
        self._min_notional_usd = self._load_min_notional_usd()

        # --- runtime стратегии / авто-сигналов (управляется settings.json и /control/auto) ---
        self._auto_enabled: bool = False
        self._auto_cooldown_ms: int = 2000   # пауза между авто-входами (ms)
        self._auto_min_flat_ms: int = 750    # минимум FLAT до нового входа (ms)
        self._last_strategy_error: Optional[str] = None
        self._strategy_heartbeat_ms: int = 0  # для диагностики живости цикла
        self._strategy_cfg: Dict[str, Any] = {}  # актуальный cfg стратегии (diag)

        # пер-символьные таймштампы для анти-спама сигналов
        self._last_signal_ms: Dict[str, int] = {s: 0 for s in self.symbols}
        self._last_flat_ms: Dict[str, int] = {s: 0 for s in self.symbols}

        # метка первого «касания TP» (per-symbol), чтобы дать лимиту шанс и затем форс-закрыть
        self._tp_first_hit_ms: Dict[str, int] = {s: 0 for s in self.symbols}

        # --- движок кандидатов, микрофич и индикаторов per-symbol ---
        self._cand: Dict[str, CandidateEngine] = {}

        self._micro_eng: Dict[str, MicroFeatureEngine] = {}
        self._indi_eng: Dict[str, IndiEngine] = {}
        self._last_micro: Dict[str, Dict[str, Any]] = {}
        self._last_indi: Dict[str, Dict[str, Any]] = {}

        # подгрузка начальных значений из settings.strategy (если есть)
        # (метод может обновить _auto_* и _strategy_cfg)
        try:
            self._load_strategy_cfg()
        except Exception:
            # не ломаем старт воркера, максимум оставим дефолты
            pass

        # --- таски жизненного цикла воркера ---
        self._tasks: List[asyncio.Task] = []
        self._wd_task: Optional[asyncio.Task] = None   # watchdog / health
        self._strat_task: Optional[asyncio.Task] = None  # авто-стратегия
        self._exchange_task: Optional[asyncio.Task] = None  # live-guard: консистентность с биржей
        self._started: bool = False

    # ---------- lifecycle ----------

    async def start(self) -> None:
        """
        Старт воркера:
        - читает settings + ENV;
        - решает, paper или live;
        - инициализирует Executor'ы с нужным адаптером (PaperAdapter / BinanceUSDTMAdapter);
        - поднимает WS, коалесер, watchdog и strategy_loop.
        """
        if self._started:
            return
        self._started = True

        # ---------------------------------------------------
        # 0. Helper: best bid/ask для Exec'уторов
        # ---------------------------------------------------
        def _best(sym: str) -> tuple[float, float]:
            return self.hub.best_bid_ask(sym)

        # ---------------------------------------------------
        # 1. Базовые дефолты исполнения
        # ---------------------------------------------------
        limit_offset_ticks = 1
        limit_timeout_ms = 500
        time_in_force = "GTC"
        max_slippage_bp = 6.0
        poll_ms = 50
        fee_bps_maker = 2.0
        fee_bps_taker = 4.0
        prefer_maker = False

       # ---------------------------------------------------
        # 2. Читаем settings + режим трейдинга (ТОЛЬКО при старте)
        # ---------------------------------------------------
        s = None
        exec_cfg = None
        account_cfg = None
        binance_cfg = None

        if get_settings:
            try:
                s = get_settings()
            except Exception:
                s = None

        if s is not None:
            account_cfg = getattr(s, "account", None)
            binance_cfg = getattr(s, "binance_usdtm", None)
            exec_cfg = getattr(s, "execution", None)

        # mode: ТОЛЬКО из settings.account.mode, fallback = "paper"
        mode = "paper"
        if account_cfg is not None:
            try:
                mode = str(getattr(account_cfg, "mode", "paper") or "paper").lower()
            except Exception:
                mode = "paper"

        # exchange: settings.account.exchange или "binance_usdtm"
        exch_name = "binance_usdtm"
        if account_cfg is not None:
            try:
                exch_name = str(getattr(account_cfg, "exchange", "binance_usdtm") or "binance_usdtm").lower()
            except Exception:
                exch_name = "binance_usdtm"

        logger.info("[worker.start] mode=%s, exchange=%s (from settings.json)", mode, exch_name)

        # ---------------------------------------------------
        # 3. execution-конфиг (лимиты, комиссии, TIF и т.д.)
        # ---------------------------------------------------
        if exec_cfg is not None:
            try:
                limit_offset_ticks = int(getattr(exec_cfg, "limit_offset_ticks", limit_offset_ticks))
                limit_timeout_ms   = int(getattr(exec_cfg, "limit_timeout_ms",   limit_timeout_ms))
                max_slippage_bp    = float(getattr(exec_cfg, "max_slippage_bp",  max_slippage_bp))
                time_in_force      = str(getattr(exec_cfg, "time_in_force",      time_in_force))
                poll_ms            = int(getattr(exec_cfg, "poll_ms",            poll_ms))
                fee_bps_maker      = float(getattr(exec_cfg, "fee_bps_maker",    fee_bps_maker))
                fee_bps_taker      = float(getattr(exec_cfg, "fee_bps_taker",    fee_bps_taker))
                prefer_maker       = bool(getattr(exec_cfg, "prefer_maker",      prefer_maker))
            except Exception:
                logger.exception("[worker.start] failed to read execution config, using defaults")

        # auto-сигналы (обратная совместимость)
        if s is not None:
            strat_cfg = getattr(s, "strategy", None)
            if strat_cfg is not None:
                self._auto_enabled     = bool(getattr(strat_cfg, "auto_signal_enabled", self._auto_enabled))
                self._auto_cooldown_ms = int(getattr(strat_cfg, "auto_cooldown_ms",     self._auto_cooldown_ms))
                self._auto_min_flat_ms = int(getattr(strat_cfg, "auto_min_flat_ms",     self._auto_min_flat_ms))

        # prefer maker, если TIF = post-only
        tif_upper = (time_in_force or "").upper()
        if tif_upper in ("GTX", "PO", "POST_ONLY"):
            prefer_maker = True

        # min_stop_ticks (для стопов / волы)
        try:
            min_stop_ticks_cfg = int(
                getattr(exec_cfg, "min_stop_ticks", self._min_stop_ticks_default())
            ) if exec_cfg else self._min_stop_ticks_default()
        except Exception:
            min_stop_ticks_cfg = self._min_stop_ticks_default()

        # сохраняем уплощённый exec_cfg для других методов
        self._exec_cfg = {
            "min_stop_ticks": int(min_stop_ticks_cfg),
            "limit_timeout_ms": int(limit_timeout_ms),
            "time_in_force": str(time_in_force),
        }

        # ---------------------------------------------------
        # 4. Настройки Binance из settings + ENV (как в test_binance_testnet)
        # ---------------------------------------------------
        # ENV-имена по умолчанию (как в твоём тесте)
        api_key_env_default = "BINANCE_API_KEY"
        api_secret_env_default = "BINANCE_API_SECRET"
        base_url_env_default = "BINANCE_FUTURES_BASE_URL"

        # Если есть binance_usdtm в settings — можно кастомизировать имена env
        if binance_cfg is not None:
            api_key_env = getattr(binance_cfg, "api_key_env", api_key_env_default)
            api_secret_env = getattr(binance_cfg, "api_secret_env", api_secret_env_default)
            base_url_env = getattr(binance_cfg, "base_url_env", base_url_env_default)
        else:
            api_key_env = api_key_env_default
            api_secret_env = api_secret_env_default
            base_url_env = base_url_env_default

        # Читаем реальные значения
        api_key = os.getenv(api_key_env)
        api_secret = os.getenv(api_secret_env)
        base_url = os.getenv(base_url_env, "https://demo-fapi.binance.com")

        # testnet-флаг (если нужен дальше в коде)
        env_testnet_raw = os.getenv("BINANCE_FUTURES_TESTNET", "")
        use_testnet = False
        if binance_cfg is not None:
            # settings.binance_usdtm.testnet имеет приоритет
            use_testnet = bool(getattr(binance_cfg, "testnet", False))
        elif env_testnet_raw.lower() in ("1", "true", "yes"):
            use_testnet = True

        logger.info(
            "[worker.start] binance cfg: api_key_env=%s, base_url_env=%s, base_url=%s, testnet=%s",
            api_key_env, base_url_env, base_url, use_testnet,
        )

        # ---------------------------------------------------
        # 5. Старт strategy_loop (до инициализации потоков ОК)
        # ---------------------------------------------------
        if self._strat_task is None or self._strat_task.done():
            loop = asyncio.get_running_loop()
            self._strat_task = loop.create_task(self._strategy_loop(), name="strategy_loop")

        # ---------------------------------------------------
        # 6. Инициализация executors и движков по символам
        # ---------------------------------------------------
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

            def _best_local(sym_local: str, _self=self):
                return _self.hub.best_bid_ask(sym_local)

            # --- выбор адаптера ---
            if mode == "live" and exch_name == "binance_usdtm":
                if not api_key or not api_secret:
                    raise RuntimeError(
                        f"Binance live mode: API key/secret not set "
                        f"(env {api_key_env} / {api_secret_env})"
                    )

                from adapters.binance_rest import BinanceUSDTMAdapter

                adapter = BinanceUSDTMAdapter(
                    api_key=api_key,
                    api_secret=api_secret,
                    base_url=base_url,
                )
                logger.info(
                    "[worker] %s executor: LIVE binance_usdtm (base_url=%s)",
                    sym, base_url,
                )
            else:
                from adapters.binance_rest import PaperAdapter
                adapter = PaperAdapter()
                logger.info("[worker] %s executor: PAPER mode", sym)

            self.execs[sym] = Executor(
                adapter=adapter,
                cfg=cfg,
                get_best=_best_local,
            )

            # стратегия / фичи по символу
            self._cand[sym] = self._make_candidate_engine(sym, spec)

            if MicroFeatureEngine is not None:
                self._micro_eng[sym] = MicroFeatureEngine(
                    tick_size=spec.price_tick,
                    lot_usd=50_000.0,
                )
            if IndiEngine is not None:
                self._indi_eng[sym] = IndiEngine(price_step=spec.price_tick)

        # ---------------------------------------------------
        # 6.5 Live Binance: sanity-check + margin/leverage + guard-loop
        # ---------------------------------------------------
        if mode == "live" and exch_name == "binance_usdtm":
            # 1) жёсткая проверка, что аккаунт пустой по нашим символам
            await self._sanity_check_live_start(account_cfg)

            # 2) проставляем marginType и leverage по каждому символу
            try:
                await self._apply_margin_and_leverage_live(account_cfg)
            except Exception as e:
                logger.warning("[worker.start] apply_margin_and_leverage_live failed: %s", e)

            # 3) запускаем фонового «охранника» консистентности
            loop = asyncio.get_running_loop()
            if self._exchange_task is None or self._exchange_task.done():
                self._exchange_task = loop.create_task(
                    self._exchange_consistency_loop(),
                    name="exchange_guard",
                )

        # ---------------------------------------------------
        # 7. Старт WS + коалесер + watchdog
        # ---------------------------------------------------
        await self.ws.connect(self._on_ws_raw)
        self._tasks.append(asyncio.create_task(self.coal.run(self._on_tick), name="coal.run"))
        self._wd_task = asyncio.create_task(self._watchdog_loop(), name="watchdog")

        logger.info("[worker.start] started with symbols=%s, mode=%s, exchange=%s", self.symbols, mode, exch_name)

    async def stop(self) -> None:
        if not self._started:
            return
        self._started = False

        # Остановить стратегию
        if self._strat_task:
            self._strat_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._strat_task
            self._strat_task = None

        # Остановить watchdog
        if self._wd_task:
            self._wd_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._wd_task
            self._wd_task = None
        
        # Остановить exchange-guard
        if self._exchange_task:
            self._exchange_task.cancel()
            with contextlib.suppress(Exception):
                await self._exchange_task
            self._exchange_task = None

        # Старое имя таска (если вдруг где-то ещё создаётся) — мягко гасим
        try:
            t = getattr(self, "_watchdog_task", None)
            if t:
                t.cancel()
        except Exception:
            pass

        # Остановить коалесер и WS
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

        # ---------- exchange helpers (live Binance integration) ----------

    def _get_live_adapter(self):
        """
        Возвращает первый попавшийся BinanceUSDTMAdapter из executors
        (если мы в live binance_usdtm режиме). Если нет — None.
        """
        from adapters.binance_rest import BinanceUSDTMAdapter  # локальный импорт, чтобы не ломать тесты

        for ex in self.execs.values():
            # в Executor адаптер обычно хранится как .a или .adapter
            a = getattr(ex, "a", None) or getattr(ex, "adapter", None)
            if isinstance(a, BinanceUSDTMAdapter):
                return a
        return None
    
    async def _sanity_check_live_start(self, account_cfg: Any) -> None:
        """
        Жёсткая проверка перед стартом live:
        - по всем self.symbols проверяем, что на Binance НЕТ открытых позиций и ордеров;
        - если что-то есть — выбрасываем RuntimeError и НЕ стартуем воркер.

        Это гарантирует, что бот никогда не начнёт торговать поверх старого мусора.
        """
        adapter = self._get_live_adapter()
        if adapter is None:
            return  # не live-binance, не о чем говорить

        # --- позиции ---
        try:
            if hasattr(adapter, "get_positions"):
                raw_positions = await adapter.get_positions()
            else:
                raw_positions = []
        except Exception as e:
            logger.error("[worker.sanity] failed to fetch positions from Binance: %s", e)
            # в проде лучше упасть, чем торговать вслепую
            raise RuntimeError(f"Failed to fetch positions from Binance: {e}") from e

        non_flat: List[str] = []
        sym_set = set(self.symbols)
        try:
            for p in raw_positions or []:
                sym = str(p.get("symbol", "")).upper()
                if sym not in sym_set:
                    continue
                try:
                    amt = float(p.get("positionAmt") or 0.0)
                except Exception:
                    amt = 0.0
                if abs(amt) > 1e-9:
                    non_flat.append(f"{sym} amt={amt}")
        except Exception as e:
            logger.warning("[worker.sanity] error parsing positions: %s", e)

        # --- открытые ордера ---
        open_orders_meta: List[str] = []
        try:
            if hasattr(adapter, "get_open_orders"):
                raw_orders = await adapter.get_open_orders()
            else:
                raw_orders = []
        except Exception as e:
            logger.error("[worker.sanity] failed to fetch open orders from Binance: %s", e)
            raise RuntimeError(f"Failed to fetch open orders from Binance: {e}") from e

        try:
            for o in raw_orders or []:
                sym = str(o.get("symbol", "")).upper()
                if sym not in sym_set:
                    continue
                status = str(o.get("status", "")).upper()
                if status not in ("FILLED", "CANCELED", "REJECTED", "EXPIRED"):
                    open_orders_meta.append(f"{sym}#{o.get('orderId')}/{status}")
        except Exception as e:
            logger.warning("[worker.sanity] error parsing open orders: %s", e)

        if non_flat or open_orders_meta:
            msg = (
                "Live start blocked: Binance account is not flat.\n"
                f"  positions: {', '.join(non_flat) or 'none'}\n"
                f"  open_orders: {', '.join(open_orders_meta) or 'none'}"
            )
            logger.error("[worker.sanity] %s", msg)
            raise RuntimeError(msg)
        
    async def _apply_margin_and_leverage_live(self, account_cfg: Any) -> None:
        """
        Для live-binance:
        - выставляет marginType (ISOLATED/CROSSED) по каждому символу;
        - выставляет leverage по каждому символу согласно конфигу.
        Любые ошибки логируются, но не останавливают бота.
        """
        adapter = self._get_live_adapter()
        if adapter is None:
            return

        # margin_mode из settings.account.margin_mode, по умолчанию ISOLATED
        try:
            margin_mode = str(getattr(account_cfg, "margin_mode", "ISOLATED") or "ISOLATED").upper()
        except Exception:
            margin_mode = "ISOLATED"

        for sym in self.symbols:
            # --- leverage: per-symbol если есть метод _leverage_for_symbol, иначе общий ---
            try:
                if hasattr(self, "_leverage_for_symbol"):
                    lev = int(self._leverage_for_symbol(sym))
                else:
                    lev = int(getattr(self, "_leverage", 10))
            except Exception:
                lev = 10

            # marginType
            try:
                if hasattr(adapter, "set_margin_type"):
                    await adapter.set_margin_type(sym, margin_mode)
            except Exception as e:
                logger.warning("[worker.live] set_margin_type(%s, %s) failed: %s", sym, margin_mode, e)

            # leverage
            try:
                if hasattr(adapter, "set_leverage"):
                    await adapter.set_leverage(sym, lev)
            except Exception as e:
                logger.warning("[worker.live] set_leverage(%s, %s) failed: %s", sym, lev, e)

    async def _exchange_consistency_loop(self) -> None:
        """
        Live-guard:
        - периодически опрашивает Binance позиции (positionRisk);
        - если по торгуемым символам есть расхождения с self._pos,
          отключает авто и добавляет блок-реизон.
        Ничего не чинит автоматически, только стопорит автоторговлю.
        """
        adapter = self._get_live_adapter()
        if adapter is None:
            return

        try:
            interval_s = 12.0  # можно вынести в конфиг
            sym_set = set(self.symbols)

            while True:
                await asyncio.sleep(interval_s)

                # если воркер уже остановлен — выходим
                if not self._started:
                    return

                try:
                    if not hasattr(adapter, "get_positions"):
                        continue
                    raw_positions = await adapter.get_positions()
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.warning("[exchange_guard] failed to fetch positions: %s", e)
                    continue

                external_amt: Dict[str, float] = {}
                try:
                    for p in raw_positions or []:
                        sym = str(p.get("symbol", "")).upper()
                        if sym not in sym_set:
                            continue
                        try:
                            amt = float(p.get("positionAmt") or 0.0)
                        except Exception:
                            amt = 0.0
                        external_amt[sym] = amt
                except Exception as e:
                    logger.warning("[exchange_guard] error parsing positions: %s", e)
                    continue

                mismatches: List[str] = []
                for sym in self.symbols:
                    ext_amt = float(external_amt.get(sym, 0.0))
                    local = self._pos.get(sym)
                    local_open = bool(local and local.state != "FLAT" and local.qty and local.qty > 0.0)

                    if abs(ext_amt) > 1e-9 and not local_open:
                        mismatches.append(f"{sym}: ext={ext_amt}, local=FLAT")
                    # при желании можно проверять и обратный случай:
                    # local_open, ext_amt ≈ 0 — но это обычно transient.

                if mismatches:
                    # выключаем авто
                    if getattr(self, "_auto_enabled", False):
                        logger.error(
                            "[exchange_guard] exchange/worker mismatch, disabling auto: %s",
                            "; ".join(mismatches),
                        )
                    self._auto_enabled = False
                    try:
                        self._inc_block_reason("GLOBAL:exchange_mismatch")
                    except Exception:
                        pass

        except asyncio.CancelledError:
            logger.info("[exchange_guard] loop cancelled")
            return
        except Exception:
            logger.exception("[exchange_guard] fatal error")


    # ---------- strategy loop (авто-сигналы) ----------

    async def _strategy_loop(self) -> None:
        """
        Боевой авто-скальпер (MVP):

        - Работает по времени: окна ~7s (long) и ~2s (short).
        - Включается только когда _auto_enabled == True.
        - Учитывает:
            * cooldown между входами по символу
            * min_flat_ms после FLAT
            * глобальный cooldown после SL (из risk_cfg)
        - Простые сигналы:
            * momentum (пробой края диапазона + локальный тренд)
            * reversion (отскок от края, когда тренд не сильный)
            * micro-momentum по микрофичам, если доступны
        - Safety:
            * спред / ликвидность через risk.filters
            * day_limits из diag()
        - Все отказы пишутся в _block_reasons через self._inc_block_reason(reason).
        """
        import asyncio
        import logging
        import math
        import time as _t
        from typing import Dict, Any, List

        log = logging.getLogger(__name__ + "._strategy")

        # --- cfg стратегии из settings (если есть) ---
        cfg: Dict[str, Any] = {}
        try:
            if hasattr(self, "_load_strategy_cfg"):
                cfg = self._load_strategy_cfg() or {}
        except Exception as e:
            log.warning("[strategy] _load_strategy_cfg failed: %s", e)
            cfg = {}
        self._strategy_cfg = dict(cfg or {})

        symbols = list(self.symbols)
        if not symbols:
            log.warning("[strategy] no symbols configured, exit.")
            return

        # --- тайминги ---
        cooldown_ms = int(cfg.get("cooldown_ms", getattr(self, "_auto_cooldown_ms", 2000)))
        cooldown_ms = max(300, cooldown_ms)

        min_flat_ms = int(cfg.get("min_flat_ms", getattr(self, "_auto_min_flat_ms", 750)))
        min_flat_ms = max(0, min_flat_ms)

        warmup_ms = int(cfg.get("warmup_ms", 15_000))
        stale_ms  = int(cfg.get("stale_ms", 3_000))

        long_ms   = int(cfg.get("long_ms", 7_000))
        short_ms  = int(cfg.get("short_ms", 2_000))

        # --- пороги логики ---
        ma_edge_mom    = float(cfg.get("ma_edge_mom", 0.00005))
        slope_edge_mom = float(cfg.get("slope_edge_mom", 0.000004))

        ma_edge_rev    = float(cfg.get("ma_edge_rev", 0.00003))

        min_range_bps   = float(cfg.get("min_range_bps", 0.3))
        min_range_ticks = int(cfg.get("min_range_ticks", 1))

        # пер-символьный cooldown
        last_entry_ts: Dict[str, int] = {s: 0 for s in symbols}

        def _now_ms() -> int:
            try:
                return int(self._now_ms())
            except Exception:
                return int(_t.time() * 1000)

        def _inc_block(reason: str) -> None:
            """
            Уважает существующую сигнатуру:
            self._inc_block_reason(reason: str), где reason уже включает символ.
            """
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

        def _load_best(sym: str, best_all: Dict[str, Any], now_ms: int) -> tuple[float, float]:
            bid = ask = 0.0

            # 1) diag.best
            try:
                b = (best_all or {}).get(sym) or {}
                bid = float(b.get("bid") or 0.0)
                ask = float(b.get("ask") or 0.0)
            except Exception:
                bid = ask = 0.0

            # 2) live best
            if bid <= 0.0 or ask <= 0.0:
                try:
                    b2, a2 = self.best_bid_ask(sym)
                    bid = float(b2 or 0.0)
                    ask = float(a2 or 0.0)
                except Exception:
                    bid = ask = 0.0

            # 3) фолбэк по последнему тику
            if bid <= 0.0 or ask <= 0.0:
                try:
                    ticks = self.history_ticks(sym, since_ms=now_ms - stale_ms, limit=1)
                except Exception:
                    ticks = []
                if ticks:
                    t = ticks[-1]
                    mid = 0.0
                    try:
                        mid = float(
                            t.get("price")
                            or t.get("mark_price")
                            or t.get("mid")
                            or 0.0
                        )
                    except Exception:
                        mid = 0.0

                    if mid <= 0.0:
                        try:
                            b_ = float(t.get("bid") or 0.0)
                            a_ = float(t.get("ask") or 0.0)
                            if b_ > 0.0 and a_ > 0.0:
                                mid = (b_ + a_) / 2.0
                        except Exception:
                            mid = 0.0

                    if mid > 0.0:
                        try:
                            tick_sz = float(self.hub.price_tick(sym))
                        except Exception:
                            tick_sz = 0.0
                        if tick_sz <= 0.0:
                            tick_sz = mid * 0.0001
                        bid = mid - tick_sz
                        ask = mid + tick_sz

            if bid <= 0.0 or ask <= 0.0:
                return 0.0, 0.0
            return float(bid), float(ask)

        # --- risk.filters (если есть) ---
        try:
            from risk.filters import MicroCtx, TimeCtx, PositionalCtx, check_entry_safety
            HAS_FILTERS = True
        except Exception:
            MicroCtx = TimeCtx = PositionalCtx = check_entry_safety = None  # type: ignore
            HAS_FILTERS = False

        # --- лимит по спреду ---
        try:
            cfg_spread_ticks = float(getattr(self._safety_cfg, "max_spread_ticks", 6.0))
            if cfg_spread_ticks <= 0.0 or not math.isfinite(cfg_spread_ticks):
                cfg_spread_ticks = 6.0
            max_spread_ticks_default = cfg_spread_ticks
        except Exception:
            max_spread_ticks_default = 6.0

        per_symbol_spread_max: Dict[str, float] = {
            "SOLUSDT": max_spread_ticks_default + 2.0,
        }

        base_delay_s = max(0.1, cooldown_ms / 4000.0)

        try:
            while True:
                try:
                    now = _now_ms()
                    self._strategy_heartbeat_ms = now

                    if not bool(getattr(self, "_auto_enabled", False)):
                        await asyncio.sleep(0.3)
                        continue

                    # глобальный cooldown после SL
                    try:
                        rcfg = self._risk_cfg
                        last_sl = int(getattr(self, "_last_sl_ts_ms", 0) or 0)
                        cd_s = int(getattr(rcfg, "cooldown_after_sl_s", 0) or 0)
                        if cd_s > 0 and last_sl and now - last_sl < cd_s * 1000:
                            _inc_block("GLOBAL:cooldown_after_sl")
                            await asyncio.sleep(base_delay_s)
                            continue
                    except Exception:
                        pass

                    # diag-снапшот
                    try:
                        d = self.diag() or {}
                    except Exception:
                        d = {}

                    day_limits = d.get("day_limits") or {}
                    if not bool(day_limits.get("can_trade", True)):
                        reasons = day_limits.get("reasons") or []
                        if reasons:
                            _inc_block("GLOBAL:day_limits:" + ",".join(map(str, reasons)))
                        await asyncio.sleep(1.0)
                        continue

                    positions = d.get("positions") or {}
                    best_all = d.get("best") or {}
                    history_meta = d.get("history") or {}

                    # --- глобальный лимит по числу позиций (MVP: максимум 1) ---
                    open_cnt = 0
                    for _ps in (positions or {}).values():
                        try:
                            st_p = str((_ps or {}).get("state", "")).upper()
                        except Exception:
                            st_p = ""
                        if st_p in ("OPEN", "ENTERING", "EXITING"):
                            open_cnt += 1

                    for sym in symbols:
                        try:
                            pos = positions.get(sym) or {}
                            st = str(pos.get("state", "")).upper()
                            if st in ("OPEN", "ENTERING", "EXITING"):
                                # по этому символу уже что-то есть
                                continue

                            # если где-то уже есть открытая/входящая/выходящая позиция —
                            # новые входы по другим символам не даём
                            if open_cnt >= 1:
                                _inc_block(f"{sym}:global_pos_limit")
                                continue

                            if now - last_entry_ts.get(sym, 0) < cooldown_ms:
                                _inc_block(f"{sym}:cooldown_symbol")
                                continue

                            last_flat_ts = int(self._last_flat_ms.get(sym) or 0)
                            if last_flat_ts > 0 and (now - last_flat_ts) < min_flat_ms:
                                _inc_block(f"{sym}:min_flat")
                                continue

                            bid, ask = _load_best(sym, best_all, now)
                            if bid <= 0.0 or ask <= 0.0:
                                _inc_block(f"{sym}:no_best")
                                continue

                            mid = (bid + ask) / 2.0
                            if mid <= 0.0:
                                _inc_block(f"{sym}:bad_mid")
                                continue

                            step = _price_step(sym)
                            spread = max(0.0, ask - bid)
                            sprticks = spread / step if step > 0 else 0.0
                            spread_bps = spread / mid * 1e4 if mid > 0 else 0.0

                            max_spread_ticks = per_symbol_spread_max.get(sym, max_spread_ticks_default)
                            if sprticks > max_spread_ticks and spread_bps > 5.0:
                                _inc_block(f"{sym}:spread_too_wide")
                                continue

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

                            prices: List[float] = []
                            ts_list: List[int] = []
                            vols: List[float] = []

                            for t in ticks or []:
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

                            def slice_since(window_ms: int) -> List[float]:
                                if not ts_list:
                                    return []
                                lo = ts_list[-1] - window_ms
                                return [p for p, ts in zip(prices, ts_list) if ts >= lo]

                            arr_short = slice_since(short_ms)
                            arr_long = slice_since(long_ms)

                            if len(arr_short) < 6 or len(arr_long) < 12:
                                _inc_block(f"{sym}:skip_warmup")
                                continue

                            short_ma = sum(arr_short) / len(arr_short)
                            long_ma  = sum(arr_long) / len(arr_long)
                            if long_ma <= 0.0:
                                _inc_block(f"{sym}:no_baseline")
                                continue

                            local_high = max(arr_long)
                            local_low  = min(arr_long)
                            range_px   = max(0.0, local_high - local_low)

                            min_range_px_ticks = step * float(min_range_ticks)
                            min_range_px_bps   = mid * (min_range_bps / 10_000.0)

                            if not (range_px >= min_range_px_ticks or range_px >= min_range_px_bps):
                                _inc_block(f"{sym}:chop_range")
                                continue

                            k = max(4, int(0.4 * len(arr_short)))
                            if len(arr_short) > k:
                                prev_short_ma = sum(arr_short[:-k]) / max(1, len(arr_short) - k)
                            else:
                                prev_short_ma = short_ma

                            last_px   = prices[-1]
                            diff_rel  = (short_ma - long_ma) / long_ma
                            slope_rel = (short_ma - prev_short_ma) / prev_short_ma if prev_short_ma > 0.0 else 0.0

                            try:
                                sl_distance_px = float(self._compute_vol_stop_distance_px(sym, mid_px=mid))
                            except Exception:
                                if len(arr_long) >= 20:
                                    m = sum(arr_long[-20:]) / 20.0
                                    var = sum((p - m) ** 2 for p in arr_long[-20:]) / 20.0
                                    sigma = math.sqrt(var)
                                else:
                                    sigma = range_px / 6.0 if range_px > 0.0 else step * max(1, min_range_ticks)
                                sl_distance_px = max(
                                    step * float(min_range_ticks),
                                    1.2 * sigma,
                                    spread * 2.0,
                                )

                            if sl_distance_px <= 0.0:
                                _inc_block(f"{sym}:bad_sl_distance")
                                continue

                            upper_trigger = local_high - 0.5 * sl_distance_px
                            lower_trigger = local_low  + 0.5 * sl_distance_px

                            side: Optional[str] = None
                            signal_kind: Optional[str] = None

                            # momentum
                            if (
                                last_px >= upper_trigger
                                and diff_rel > ma_edge_mom
                                and slope_rel > slope_edge_mom
                            ):
                                side = "BUY"
                                signal_kind = "momentum"
                            elif (
                                last_px <= lower_trigger
                                and diff_rel < -ma_edge_mom
                                and slope_rel < -slope_edge_mom
                            ):
                                side = "SELL"
                                signal_kind = "momentum"

                            # reversion
                            if side is None:
                                trend_strong = abs(diff_rel) > ma_edge_mom * 2.0
                                if not trend_strong:
                                    edge_zone = 0.25 * range_px
                                    if (
                                        last_px <= local_low + edge_zone
                                        and diff_rel > ma_edge_rev
                                        and slope_rel > 0.0
                                    ):
                                        side = "BUY"
                                        signal_kind = "reversion"
                                    elif (
                                        last_px >= local_high - edge_zone
                                        and diff_rel < -ma_edge_rev
                                        and slope_rel < 0.0
                                    ):
                                        side = "SELL"
                                        signal_kind = "reversion"

                            # micro-momentum
                            if side is None and getattr(self, "_last_micro", None):
                                lm = (self._last_micro or {}).get(sym) or {}
                                drift = float(lm.get("microprice_drift") or 0.0)
                                tvel  = float(lm.get("tick_velocity") or 0.0)
                                if sprticks <= max_spread_ticks_default and abs(drift) > 0.3 and abs(tvel) > 0.5:
                                    if drift > 0.0 and tvel > 0.0:
                                        side = "BUY"
                                        signal_kind = "momentum"
                                    elif drift < 0.0 and tvel < 0.0:
                                        side = "SELL"
                                        signal_kind = "momentum"

                            if side is None:
                                _inc_block(f"{sym}:no_signal")
                                continue
                            ok_cross, why_cross = self._cross_guard_allow(sym, side)
                            if not ok_cross:
                                self._inc_block_reason(f"{sym}:{why_cross}")
                                continue

                            # объём
                            if vols and sum(vols) > 0.0:
                                base_slice = vols[:-5] if len(vols) > 12 else vols
                                recent = sum(vols[-5:])
                                base = sum(base_slice) / max(1, len(base_slice)) if base_slice else 0.0
                                if base > 0.0 and recent < 0.25 * base:
                                    _inc_block(f"{sym}:low_volume")
                                    continue

                            # safety
                            if HAS_FILTERS and getattr(self, "_safety_cfg", None) is not None:
                                try:
                                    lm = (getattr(self, "_last_micro", {}) or {}).get(sym, {}) or {}
                                    eff_liq = float(lm.get("top5_liq_usd") or 0.0)
                                    if eff_liq <= 0.0:
                                        tl = float(lm.get("top_liq_usd") or 0.0)
                                        eff_liq = tl * 3.0 if tl > 0.0 else 0.0

                                    micro_ctx = MicroCtx(
                                        spread_ticks=float(sprticks),
                                        top5_liq_usd=float(eff_liq if eff_liq > 0.0 else 1e9),
                                    )
                                    tctx = TimeCtx(
                                        ts_ms=now,
                                        server_time_offset_ms=getattr(self, "_server_time_offset_ms", 0),
                                    )
                                    sl_px_eff = mid - sl_distance_px if side == "BUY" else mid + sl_distance_px
                                    pos_ctx = PositionalCtx(
                                        entry_px=float(mid),
                                        sl_px=float(sl_px_eff),
                                        leverage=float(getattr(self, "_leverage", 10.0)),
                                    )
                                    dec = check_entry_safety(
                                        side,
                                        micro=micro_ctx,
                                        time_ctx=tctx,
                                        pos_ctx=pos_ctx,
                                        safety=self._safety_cfg,
                                    )
                                    if not dec.allow:
                                        _inc_block(f"{sym}:safety_pre:" + ",".join(dec.reasons))
                                        continue
                                except Exception:
                                    log.exception("[strategy] safety check failed for %s", sym)

                            # фактический вход
                            try:
                                rep = await self._place_entry_with_kind(
                                    sym,
                                    side,
                                    signal_kind,
                                    sl_distance_px,
                                    mid,
                                    bid,
                                    ask,
                                )
                            except Exception as e:
                                log.exception("[strategy] place_entry failed for %s %s: %s", sym, side, e)
                                _inc_block(f"{sym}:entry_error")
                                continue

                            if not isinstance(rep, dict) or not rep.get("ok", False):
                                reason = (rep or {}).get("reason", "rejected")
                                _inc_block(f"{sym}:entry_rejected:{reason}")
                                continue

                            last_entry_ts[sym] = now

                        except Exception:
                            log.exception("[strategy] symbol-loop error for %s", sym)
                            _inc_block(f"{sym}:loop_error")
                            continue

                    await asyncio.sleep(base_delay_s)

                except asyncio.CancelledError:
                    log.info("[strategy] _strategy_loop cancelled")
                    return
                except Exception as e:
                    log.exception("[strategy] outer loop error: %s", e)
                    self._last_strategy_error = f"{type(e).__name__}: {e}"
                    await asyncio.sleep(1.0)

        except asyncio.CancelledError:
            log.info("[strategy] _strategy_loop cancelled (outer)")
        except Exception as e:
            log.exception("[strategy] fatal error: %s", e)
            self._last_strategy_error = f"{type(e).__name__}: {e}"
            await asyncio.sleep(1.0)
    
    def _compute_sl_distance_px(self, symbol: str, spread_ticks: float) -> float:
        """
        Единый источник плановой SL-дистанции:
        просто обёртка над _compute_vol_stop_distance_px с оценкой mid.
        Параметр spread_ticks оставлен для совместимости, но не критичен.
        """
        sym = symbol.upper()

        # оцениваем mid по best bid/ask, фолбэк – по последнему тику
        try:
            bid, ask = self.best_bid_ask(sym)
        except Exception:
            bid, ask = 0.0, 0.0

        mid = (bid + ask) / 2.0 if bid > 0.0 and ask > 0.0 else 0.0
        if mid <= 0.0:
            last = self.latest_tick(sym) or {}
            try:
                mid = float(last.get("price") or last.get("mark_price") or 0.0)
            except Exception:
                mid = 0.0

        # если совсем нет цены – fallback на старый минимальный тик
        if mid <= 0.0:
            step = self._get_price_step(sym)
            if step <= 0.0:
                step = 0.1
            base_min_ticks = max(self._min_stop_ticks_default(), 8.0)
            return float(base_min_ticks * step)

        # min_stop_ticks из exec_cfg или дефолта
        try:
            min_stop_ticks_cfg = int(
                (getattr(self, "_exec_cfg", {}) or {}).get(
                    "min_stop_ticks", self._min_stop_ticks_default()
                )
            )
        except Exception:
            min_stop_ticks_cfg = int(self._min_stop_ticks_default())

        dist = self._compute_vol_stop_distance_px(
            sym,
            mid_px=float(mid),
            min_stop_ticks_cfg=min_stop_ticks_cfg,
        )

        # добавляем минимум по bps: пусть будет >= 0.25% от цены
        step = self._get_price_step(sym)
        if step <= 0.0:
            step = 0.1
        min_sl_bps = getattr(self, "_min_sl_bps", 25.0)  # можно вынести в конфиг
        min_sl_px_bps = mid * (min_sl_bps / 10_000.0) if mid > 0 else 0.0
        base_min_ticks = max(self._min_stop_ticks_default(), 8.0)

        dist = max(
            float(dist),
            base_min_ticks * step,
            min_sl_px_bps,
        )
        return float(dist)
    
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
                    min_sl_bps=25.0,
                    # требуем минимальный net-RR (после комиссий/спреда)
                    min_net_rr=1.4,
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
                rr = self._rr_floor_for_symbol(symbol, rr)
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
        side: str,
        qty: float,
        limit_offset_ticks: Optional[int] = None,
        *,
        sl_distance_px: Optional[float] = None,
        sl_px: Optional[float] = None,
        tp_px: Optional[float] = None,
    ) -> Dict[str, Any]:
        """
        Unified entry (prod): сохраняем совместимость.
        - Если SL/TP не переданы — строим (вола/тик/спред-aware).
        - Соблюдаем qty step, min notional.
        - Executor возьмёт maker-first/GTX из конфига, SL reduceOnly STOP_MARKET, TP reduceOnly post-only.
        """
        import contextlib, logging
        logger = logging.getLogger(__name__ + ".place_entry")

        sym = symbol.upper()
        s = side.upper()
        if s not in ("BUY", "SELL"):
            return {"ok": False, "reason": "bad_side"}

        ex = self.execs.get(sym)
        if ex is None:
            return {"ok": False, "reason": "no_executor"}

        # steps for later fee accounting
        self._entry_steps.pop(sym, None)

        # market snapshot
        bid, ask = self.best_bid_ask(sym)
        if bid <= 0 or ask <= 0:
            return {"ok": False, "reason": "no_best_bid_ask"}
        entry_px = (bid + ask) / 2.0
        if entry_px <= 0:
            return {"ok": False, "reason": "bad_entry_px"}

        # qty step & min notional
        spec = self.specs.get(sym) if hasattr(self, "specs") else None
        step = max(float(getattr(spec, "qty_step", 0.001) if spec else 0.001), 1e-12)
        qty = (int(qty / step)) * step
        if qty <= 0:
            return {"ok": False, "reason": "qty_rounded_to_zero"}

        min_notional = float(getattr(self, "_min_notional_usd", 5.0))
        if qty * entry_px < min_notional:
            return {"ok": False, "reason": "below_min_notional", "hint": f"need ≥ ${min_notional}"}

        # Build SL/TP if needed
        if sl_px is None or tp_px is None:
            try:
                plan = self._build_sltp_plan(
                    symbol=sym,
                    side=s,           # "BUY" / "SELL"
                    entry_px=entry_px,
                    qty=qty,
                    micro=self._last_micro.get(sym),
                    indi=self._last_indi.get(sym),
                    decision=None,    # или сюда можно передавать info о типе сигнала
                )
                sl_px = float(plan.sl_px)
                tp_px = float(plan.tp_px)
            except Exception:
                # старый фолбэк, чтобы не упасть
                spec = self.specs.get(sym) if hasattr(self, "specs") else None
                price_tick = float(getattr(spec, "price_tick", 0.1) if spec else 0.1)
                spread = max(0.0, ask - bid)
                try:
                    if sl_distance_px is None or sl_distance_px <= 0:
                        sl_distance_px = float(
                            self._compute_vol_stop_distance_px(sym, mid_px=entry_px)
                        )
                except Exception:
                    sl_distance_px = max(12 * price_tick, 2.0 * spread, 6.0 * price_tick)

                rr = float(self._tp_rr_from_decision(None) or 1.6)
                if s == "BUY":
                    sl_px = entry_px - sl_distance_px
                    tp_px = entry_px + rr * sl_distance_px
                else:
                    sl_px = entry_px + sl_distance_px
                    tp_px = entry_px - rr * sl_distance_px

            # нормализация/округление (если есть утилита)
            try:
                plan_norm = self._normalize_sltp(sym, s, entry_px, SLTPPlan(sl_px=sl_px, tp_px=tp_px, sl_qty=qty, tp_qty=qty))
                sl_px, tp_px = float(plan_norm.sl_px), float(plan_norm.tp_px)
            except Exception:
                pass

        # executor overrides (безопасно, опционально)
        try:
            ctx_mgr = self._temporary_exec_overrides(sym, limit_offset_ticks=limit_offset_ticks if limit_offset_ticks is not None else None)
        except Exception:
            @contextlib.contextmanager
            def _dummy(): yield
            ctx_mgr = _dummy()

        # submit
        try:
            with ctx_mgr:
                rep: ExecutionReport = await ex.place_entry(sym, s, float(qty))
                self._accumulate_exec_counters(getattr(rep, "steps", []))
                self._entry_steps[sym] = list(getattr(rep, "steps", []) or [])
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
            "steps": self._entry_steps.get(sym, [])
        }

    async def place_entry_auto(self, symbol: str, side: str) -> dict:
        """
        Auto entry with risk-based sizing and fee-aware SL/TP planning (prod-tuned).
        Ничего не ломает: использует ваши хелперы и executor, усиливает экономику.
        """
        import math, logging
        log = logging.getLogger(__name__ + ".place_entry_auto")

        # --- normalize ---
        sym = str(symbol).upper()
        s = str(side).upper()
        if s in ("LONG", "BUY"): s = "BUY"
        elif s in ("SHORT", "SELL"): s = "SELL"
        else: return {"ok": False, "reason": "bad_side"}

        # --- market snapshot ---
        try:
            bid, ask = self.best_bid_ask(sym)
        except Exception:
            bid = ask = 0.0
        if bid <= 0 or ask <= 0:
            self._inc_block_reason(f"{sym}:no_best_bid_ask")
            return {"ok": False, "reason": "no_best_bid_ask"}

        mid = (bid + ask) / 2.0
        if mid <= 0:
            return {"ok": False, "reason": "bad_mid"}

        # --- steps/quant ---
        price_step = self._get_price_step(sym) or float(getattr(self, "_price_tick_fallback", 0.1) or 0.1)
        try:
            spec = (self.specs or {}).get(sym)
            qty_step = float(getattr(spec, "qty_step", 0.001)) if spec else float(getattr(self, "_qty_step_fallback", 0.001) or 0.001)
        except Exception:
            qty_step = 0.001
        if qty_step <= 0: qty_step = 0.001

        # --- exec minima ---
        exec_cfg = getattr(self, "_exec_cfg", {}) or {}
        min_stop_ticks = int(exec_cfg.get("min_stop_ticks", 12))
        spread = max(0.0, ask - bid)

        # --- stop distance (вола/тик/спред + полы) ---
        try:
            sl_distance_px = float(self._compute_vol_stop_distance_px(sym, mid_px=mid, min_stop_ticks_cfg=min_stop_ticks))
        except Exception:
            sl_distance_px = 0.0
        if sl_distance_px <= 0:
            sl_distance_px = max(min_stop_ticks * price_step, 2.0 * spread, 6.0 * price_step)
        if sl_distance_px <= 0:
            return {"ok": False, "reason": "bad_sl_distance"}

        # --- первичная edge-проверка (gross RR floor) ---
        rr_gross = float(self._rr_floor_for_symbol(sym, 1.6))
        ok_edge, edge_info = self._entry_edge_ok(sym, mid, bid, ask, sl_distance_px, rr_gross)
        if not ok_edge:
            self._inc_block_reason(f"{sym}:edge_too_small")
            return {"ok": False, "reason": "edge_too_small", "edge_diag": edge_info}

         # --- account snapshot / risk target ---
        try:
            equity_now = float(self._starting_equity_usd + float(self._pnl_day.get("pnl_usd", 0.0) or 0.0))
        except Exception:
            equity_now = float(getattr(self, "_starting_equity_usd", 1000.0))

        # leverage: per-symbol override через хелпер
        lev = float(self._leverage_for_symbol(sym))
        if lev < 1.0:
            lev = 1.0

        min_notional = float(getattr(self, "_min_notional_usd", 5.0))
        risk_cfg = getattr(self, "_risk_cfg", None)
        risk_pct = float(getattr(risk_cfg, "risk_per_trade_pct", 0.30))
        min_risk_usd_floor = float(getattr(risk_cfg, "min_risk_usd_floor", 3.0))
        target_risk_usd = max(min_risk_usd_floor, equity_now * (risk_pct / 100.0))
        if target_risk_usd <= 0:
            return {"ok": False, "reason": "bad_risk_cfg"}

        # --- первичный сайзинг ---
        raw_qty = target_risk_usd / sl_distance_px
        max_qty_by_lev = max(0.0, (equity_now * lev) / mid)
        qty = min(raw_qty, max_qty_by_lev)
        if qty_step > 0: qty = math.floor(qty / qty_step) * qty_step
        if qty <= 0:
            self._inc_block_reason(f"{sym}:qty_rounded_to_zero")
            return {"ok": False, "reason": "qty_rounded_to_zero"}

        if qty * mid < min_notional:
            # подсказка по минимальному размеру
            min_qty = math.ceil((min_notional / mid) / qty_step) * qty_step
            self._inc_block_reason(f"{sym}:below_min_notional")
            return {"ok": False, "reason": "below_min_notional", "hint": f"min ${min_notional} → qty ≥ {min_qty}"}

        # --- SL/TP план (fee-aware, maker-first вход/TP, SL=market) ---
        maker_bps, taker_bps = self._fees_for_symbol(sym)
        spread_bps = (spread / mid) * 1e4 if mid > 0 else 0.0
        addl_exit_bps = 0.5 * max(spread_bps, 0.1)

        # defaults
        sl_px, tp_px = None, None
        try:
            # если у тебя есть compute_sltp_fee_aware — используем
            plan = compute_sltp_fee_aware(
                side=s,
                entry_px=float(mid),
                qty=float(qty),
                price_tick=float(price_step),
                sl_distance_px=float(sl_distance_px),
                rr_target=float(rr_gross),
                maker_bps=maker_bps,
                taker_bps=taker_bps,
                entry_taker_like=False,          # maker-first
                exit_taker_like=True,            # SL будет taker
                addl_exit_bps=float(addl_exit_bps),
                min_stop_ticks=int(min_stop_ticks),
                spread_px=spread,
                spread_mult=1.5,
                min_sl_bps=float(getattr(self, "_min_sl_bps", 12.0) or 12.0),
                min_net_rr=float(getattr(self, "_min_net_rr", 1.25) or 1.25),
                allow_expand_tp=True,
            )
            sl_px = float(plan.sl_px)
            tp_px = float(getattr(plan, "tp_px_maker", getattr(plan, "tp_px", 0.0)) or 0.0)
        except Exception:
            # безопасный фоллбэк
            if s == "BUY":
                sl_px = mid - sl_distance_px
                tp_px = mid + sl_distance_px * rr_gross
            else:
                sl_px = mid + sl_distance_px
                tp_px = mid - sl_distance_px * rr_gross

        # --- точный риск и ресайз под target ---
        risk_px = abs(sl_px - mid)
        if risk_px <= 0:
            self._inc_block_reason(f"{sym}:plan_bad_risk_px")
            return {"ok": False, "reason": "bad_sl_distance"}

        adj_qty = min(target_risk_usd / risk_px, max_qty_by_lev)
        adj_qty = max(adj_qty, (min_notional / mid))
        if qty_step > 0: adj_qty = math.floor(adj_qty / qty_step) * qty_step
        if adj_qty <= 0 or (adj_qty * mid) < min_notional:
            self._inc_block_reason(f"{sym}:qty_rounded_to_zero")
            return {"ok": False, "reason": "qty_rounded_to_zero"}

        qty = float(adj_qty)
        expected_risk_usd = round(risk_px * qty, 6)

        # --- повторная edge-проверка на финале ---
        try:
            bid2, ask2 = self.best_bid_ask(sym)
            mid2 = (bid2 + ask2) / 2.0 if bid2 > 0 and ask2 > 0 else mid
        except Exception:
            bid2 = ask2 = 0.0
            mid2 = mid

        sl_dist_final = abs(mid2 - sl_px)
        ok_edge2, edge_meta2 = self._entry_edge_ok(sym, mid2, bid2, ask2, sl_dist_final, rr_gross)
        if not ok_edge2:
            self._inc_block_reason(f"{sym}:edge_insufficient")
            return {
                "ok": False, "reason": "edge_insufficient",
                "edge_diag": edge_meta2, "entry_px": float(mid2),
                "sl_px": float(sl_px), "tp_px": float(tp_px)
            }
        # --- liq-buffer: не входим, если SL слишком близко к ликвидации ---
        try:
            lb_mult = self._liq_buffer_mult(float(mid2), float(sl_px), s, lev)
            min_mult = float(
                getattr(self, "_safety_cfg", "min_liq_buffer_sl_mult")
                if hasattr(self, "_safety_cfg") else 3.0
            )
            if not math.isfinite(lb_mult) or lb_mult < max(1.5, float(min_mult or 3.0)):
                self._inc_block_reason(f"{sym}:liq_buffer_too_small")
                return {
                    "ok": False,
                    "reason": "liq_buffer_too_small",
                    "liq_buffer_mult": float(lb_mult),
                }
        except Exception:
            # диагностику liq-buffer не считаем критичной
            pass

        # --- submit (executor читает maker-first из конфига) ---
        try:
            report = await self.place_entry(sym, s, qty, sl_px=float(sl_px), tp_px=float(tp_px))
        except Exception as e:
            log.exception("[place_entry_auto] failed for %s %s: %s", sym, s, e)
            self._inc_block_reason(f"{sym}:entry_error")
            return {"ok": False, "reason": "entry_error", "error": str(e)}

        if isinstance(report, dict) and not report.get("ok", True):
            reason = report.get("reason") or "rejected"
            self._inc_block_reason(f"{sym}:entry_rejected_{reason}")
            return {"ok": False, "reason": reason, "report": report}

        return {
            "ok": True, "symbol": sym, "side": s, "qty": float(qty),
            "entry_px": float(mid), "sl_px": float(sl_px), "tp_px": float(tp_px),
            "expected_risk_usd": float(expected_risk_usd),
            "target_risk_usd": round(float(target_risk_usd), 6),
            "report": report
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
                                await self._close_position(sym, pos, reason="no_protection")
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

                        side_now = (pos.side or "BUY").upper()
                        qty_now  = float(pos.qty or 0.0)
                        tp_px    = float(pos.tp_px) if pos.tp_px is not None else None
                        sl_px    = float(pos.sl_px) if pos.sl_px is not None else None

                        # --- SL: исполнимость по противоположной стороне ---
                        # LONG → стоп исполним, когда ask <= SL; SHORT → когда bid >= SL
                        if sl_px and qty_now > 0.0 and bid > 0.0 and ask > 0.0:
                            tol = max(price_tick, self._tp_tol_ticks * price_tick)  # тот же толеранс
                            sl_hit = (ask <= sl_px + tol) if side_now == "BUY" else (bid >= sl_px - tol)
                            if sl_hit:
                                await self._close_position(sym, pos, reason="sl_hit")
                                self._tp_first_hit_ms[sym] = 0
                                continue

                        # --- TP: быстрый сценарий с grace и затем форс-закрытием ---
                        if tp_px and qty_now > 0.0 and bid > 0.0 and ask > 0.0:
                            if self._tp_hit(side_now, tp_px, bid, ask, price_tick, tol_ticks=self._tp_tol_ticks):
                                now_ms = self._now_ms()
                                first  = int(self._tp_first_hit_ms.get(sym, 0) or 0)

                                if first <= 0:
                                    # первое касание — запоминаем и (best-effort) ставим reduce-only лимит
                                    self._tp_first_hit_ms[sym] = now_ms
                                    place_reduce = getattr(self, "place_reduce_only_limit", None)
                                    if callable(place_reduce):
                                        try:
                                            px = tp_px  # ставим ровно по TP
                                            close_side = "SELL" if side_now == "BUY" else "BUY"
                                            await place_reduce(sym, close_side, float(qty_now), price=float(px))
                                        except Exception:
                                            pass
                                else:
                                    # если grace истёк — форсим reduce-only MARKET
                                    if now_ms - first >= int(getattr(self, "_tp_grace_ms", 350)):
                                        self._tp_first_hit_ms[sym] = 0
                                        if bool(getattr(self, "_tp_force", True)):
                                            rep = await self._force_flatten_after_tp(sym, side_now, float(qty_now))
                                            if isinstance(rep, dict) and rep.get("ok"):
                                                # причина закрытия — tp_hit (а не flatten_forced),
                                                # чтобы дневная метрика была «чистой»
                                                await self._close_position(sym, pos, reason="tp_hit")
                                                continue
                                        # на всякий: если не получилось — просто закрываем как tp_hit
                                        await self._close_position(sym, pos, reason="tp_hit")
                                        continue
                            else:
                                # ушли от TP — обнуляем окно grace
                                self._tp_first_hit_ms[sym] = 0
                                
                            # --- fallback: price-touch check через best bid/ask ---
                            touched, which = self._price_touch_check(sym, side_now, sl_px, tp_px)
                            if touched:
                                reason = "sl_hit" if which == "sl" else "tp_hit"
                                await self._close_position(sym, pos, reason=reason)
                                if which == "tp":
                                    self._tp_first_hit_ms[sym] = 0
                                continue
        except asyncio.CancelledError:
            return
        except Exception:
            logger.exception("watchdog loop error")
    
    async def flatten_exchange(self) -> Dict[str, Any]:
        """
        Полное аварийное закрытие всех позиций на бирже (Binance USDT-M) по нашим символам.
        Использует MARKET reduceOnly ордера через адаптер.
        Использовать осторожно: это прямой слив всех поз по аккаунту.
        """
        adapter = self._get_live_adapter()
        if adapter is None:
            return {"ok": False, "reason": "not_live_binance"}

        try:
            if not hasattr(adapter, "get_positions"):
                return {"ok": False, "reason": "adapter_has_no_get_positions"}
            raw_positions = await adapter.get_positions()
        except Exception as e:
            logger.error("[flatten_exchange] failed to fetch positions: %s", e)
            return {"ok": False, "error": f"failed_to_fetch_positions: {e}"}

        sym_set = set(self.symbols)
        results: Dict[str, Any] = {}

        for p in raw_positions or []:
            sym = str(p.get("symbol", "")).upper()
            if sym not in sym_set:
                continue

            try:
                amt = float(p.get("positionAmt") or 0.0)
            except Exception:
                amt = 0.0

            if abs(amt) <= 1e-9:
                continue

            side = "SELL" if amt > 0 else "BUY"
            qty = abs(amt)

            try:
                # сигнатуру create_order подстрой под свой адаптер:
                if hasattr(adapter, "create_order"):
                    await adapter.create_order(
                        symbol=sym,
                        side=side,
                        type="MARKET",
                        qty=qty,
                        reduce_only=True,
                    )
                    results[sym] = {"ok": True, "closed_qty": qty, "side": side}
                else:
                    results[sym] = {"ok": False, "error": "adapter_has_no_create_order"}
            except Exception as e:
                logger.error("[flatten_exchange] close failed for %s: %s", sym, e)
                results[sym] = {"ok": False, "error": str(e)}

        all_ok = all(v.get("ok", False) for v in results.values()) if results else True
        return {"ok": all_ok, "results": results}
        
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

            await self._close_position(symbol, pos, reason="flatten_forced")
            return {"ok": True, "symbol": symbol, "state": "FLAT", "reason": "flatten_forced"}

    def set_timeout_ms(self, symbol: str, timeout_ms: int) -> Dict[str, Any]:
        symbol = symbol.upper()
        self._pos[symbol].timeout_ms = max(5_000, int(timeout_ms))
        return {"ok": True, "symbol": symbol, "timeout_ms": self._pos[symbol].timeout_ms}

    async def _close_position(self, sym: str, pos: PositionState, *, reason: str) -> None:
        """
        Безопасное закрытие позиции (paper + live).

        Правила:
        - Для sl_hit / tp_hit в учёте PnL используем плановый уровень SL/TP
          (чтобы не ловить -30R из-за хвоста).
        - Для flatten/timeout/no_protection — берём консервативную рыночную цену.
        - Executor.place_exit вызывается всегда (если есть), поэтому на live реально
          отправляется ордер на выход, а эта функция отвечает за приведение
          локального стейта к FLAT и корректный учёт сделки.
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

        # gross PnL и номинальный риск по SL (без комиссий)
        if side == "BUY":
            pnl_usd_gross = (exit_px - entry) * qty
            sl_ref = sl_px if sl_px is not None and sl_px < entry else entry
            risk_usd_nominal = abs(entry - sl_ref) * qty
        else:
            pnl_usd_gross = (entry - exit_px) * qty
            sl_ref = sl_px if sl_px is not None and sl_px > entry else entry
            risk_usd_nominal = abs(sl_ref - entry) * qty

        # комиссии по фактическим шагам исполнения
        entry_bps = self._fee_bps_for_steps(entry_steps or [])
        exit_bps = self._fee_bps_for_steps(exit_steps or [])
        fees_usd = (entry * qty * entry_bps + exit_px * qty * exit_bps) / 10_000.0

        pnl_usd = pnl_usd_gross - fees_usd

        # фактический риск сделки = SL-дистанция + комиссии
        gross_risk_usd = max(risk_usd_nominal, 0.0)
        risk_usd_total = gross_risk_usd + max(fees_usd, 0.0)

        # читаем конфиг риска (нужен для порогов, но не для искажения R)
        try:
            equity = float(self._starting_equity_usd + float(self._pnl_day.get("pnl_usd", 0.0) or 0.0))
        except Exception:
            equity = float(self._starting_equity_usd)

        risk_pct = float(getattr(self._risk_cfg, "risk_per_trade_pct", 0.0) or 0.0)
        risk_target = equity * (risk_pct / 100.0) if risk_pct > 0.0 else 0.0

        cfg_floor = float(getattr(self._risk_cfg, "min_risk_usd_floor", 0.0) or 0.0)
        if cfg_floor <= 0.0:
            cfg_floor = 1.0

        # ВАЖНО: risk_usd = реальный риск сделки (SL + fees), а не max с risk_target.
        # floor нужен только чтобы не делить на 0 в аккуратных/микро-сделках.
        risk_usd = max(risk_usd_total, cfg_floor, 1e-9)
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
        Стабильный снепшот состояния воркера для API.

        Требования:
        - НИКОГДА не падает (всё в try/except).
        - Возвращает ключи, которых ждут эндпоинты:
            symbols, ws_detail, coal, history, best, positions,
            unrealized, trades, pnl_day, risk_cfg, protection_cfg,
            safety_cfg, day_limits, consec_losses, account_cfg,
            exec_cfg, exec_counters, block_reasons,
            auto_signal_enabled, strategy_cfg, strategy_last_error,
            strategy_heartbeat_ms.
        """
        import dataclasses
        from typing import Dict, Any

        out: Dict[str, Any] = {}

        # --- базовые вещи ---
        try:
            out["symbols"] = list(self.symbols)
        except Exception:
            out["symbols"] = []

        # --- ws_detail ---
        ws_detail: Dict[str, Any] = {}
        try:
            if hasattr(self.ws, "stats"):
                ws_detail = self.ws.stats() or {}
        except Exception:
            ws_detail = {}
        out["ws_detail"] = ws_detail

        # --- coalescer stats (coal) ---
        coal: Dict[str, Any] = {}
        try:
            if hasattr(self.coal, "stats"):
                coal = self.coal.stats() or {}
        except Exception:
            coal = {}
        out["coal"] = coal

        # --- history meta: last ts по _hist ---
        history_meta: Dict[str, Any] = {}
        try:
            for sym, dq in (getattr(self, "_hist", {}) or {}).items():
                latest_ts = 0
                if dq:
                    last = dq[-1]
                    try:
                        latest_ts = int(
                            (last.get("ts_ms")
                            or last.get("ts")
                            or 0)
                        )
                    except Exception:
                        latest_ts = 0
                history_meta[sym] = {"latest_ts_ms": latest_ts}
        except Exception:
            history_meta = {}
        out["history"] = history_meta

        # --- best bid/ask ---
        best: Dict[str, Any] = {}
        try:
            for sym in out.get("symbols", []):
                try:
                    bid, ask = self.best_bid_ask(sym)
                except Exception:
                    bid, ask = 0.0, 0.0
                best[sym] = {"bid": float(bid or 0.0), "ask": float(ask or 0.0)}
        except Exception:
            best = {}
        out["best"] = best

        # --- positions (из self._pos) ---
        positions: Dict[str, Any] = {}
        try:
            for sym, p in (getattr(self, "_pos", {}) or {}).items():
                if hasattr(p, "__dict__"):
                    # dataclass PositionState
                    try:
                        positions[sym] = dataclasses.asdict(p)
                    except Exception:
                        positions[sym] = dict(p.__dict__)
                else:
                    positions[sym] = dict(p or {})
        except Exception:
            positions = {}
        out["positions"] = positions

        # --- unrealized PnL (best-effort) ---
        unrealized = {
            "total_usd": 0.0,
            "per_symbol": {},
            "open_positions": 0,
        }
        try:
            per_sym = {}
            total = 0.0
            open_pos = 0
            for sym, p in (positions or {}).items():
                p = p or {}
                if str(p.get("state", "")).upper() != "OPEN":
                    continue
                open_pos += 1
                qty = float(p.get("qty") or 0.0)
                entry = float(p.get("entry_px") or 0.0)
                if qty <= 0.0 or entry <= 0.0:
                    continue
                b = best.get(sym, {}).get("bid", 0.0)
                a = best.get(sym, {}).get("ask", 0.0)
                mid = (float(b or 0.0) + float(a or 0.0)) / 2.0
                if mid <= 0.0:
                    continue
                side = (p.get("side") or "").upper()
                diff = (mid - entry) if side == "BUY" else (entry - mid)
                u = diff * qty
                per_sym[sym] = u
                total += u
            unrealized["total_usd"] = float(total)
            unrealized["per_symbol"] = per_sym if per_sym else None
            unrealized["open_positions"] = int(open_pos)
        except Exception:
            pass
        out["unrealized"] = unrealized

        # --- trades (in-memory) ---
        try:
            out["trades"] = {
                sym: list(dq) for sym, dq in (getattr(self, "_trades", {}) or {}).items()
            }
        except Exception:
            out["trades"] = {}

        # --- pnl_day ---
        try:
            out["pnl_day"] = dict(getattr(self, "_pnl_day", {}) or {})
        except Exception:
            out["pnl_day"] = {}

        # --- risk_cfg ---
        try:
            rcfg = getattr(self, "_risk_cfg", None)
            if rcfg is None:
                out["risk_cfg"] = {}
            elif isinstance(rcfg, dict):
                out["risk_cfg"] = dict(rcfg)
            elif hasattr(rcfg, "__dict__"):
                out["risk_cfg"] = dict(rcfg.__dict__)
            else:
                out["risk_cfg"] = {}
        except Exception:
            out["risk_cfg"] = {}

        # --- safety_cfg ---
        try:
            scfg = getattr(self, "_safety_cfg", None)
            if scfg is None:
                out["safety_cfg"] = {}
            elif isinstance(scfg, dict):
                out["safety_cfg"] = dict(scfg)
            elif hasattr(scfg, "__dict__"):
                out["safety_cfg"] = dict(scfg.__dict__)
            else:
                out["safety_cfg"] = {}
        except Exception:
            out["safety_cfg"] = {}

        # protection_cfg — пока пустой (если позже добавишь — просто заполни)
        out["protection_cfg"] = out.get("protection_cfg", {}) or {}

        # --- day_limits (если у тебя нет логики — дефолт) ---
        try:
            if "day_limits" not in out or not out["day_limits"]:
                out["day_limits"] = {
                    "can_trade": True,
                    "reasons": [],
                }
        except Exception:
            out["day_limits"] = {"can_trade": True, "reasons": []}

        # --- consec_losses ---
        try:
            out["consec_losses"] = int(getattr(self, "_consec_losses", 0) or 0)
        except Exception:
            out["consec_losses"] = 0

        # --- account_cfg ---
        try:
            out["account_cfg"] = {
                "starting_equity_usd": float(getattr(self, "_starting_equity_usd", 1000.0)),
                "leverage": float(getattr(self, "_leverage", 15.0)),
                "min_notional_usd": float(getattr(self, "_min_notional_usd", 5.0)),
            }
        except Exception:
            out["account_cfg"] = {}

        # --- exec_cfg ---
        exec_cfg: Dict[str, Any] = {}
        try:
            if hasattr(self, "_exec_cfg"):
                exec_cfg.update(getattr(self, "_exec_cfg") or {})
        except Exception:
            pass
        out["exec_cfg"] = exec_cfg

        # --- exec_counters ---
        try:
            out["exec_counters"] = dict(getattr(self, "_exec_counters", {}) or {})
        except Exception:
            out["exec_counters"] = {}

        # --- block_reasons ---
        try:
            out["block_reasons"] = dict(getattr(self, "_block_reasons", {}) or {})
        except Exception:
            out["block_reasons"] = {}

        # --- авто-стратегия / статус ---
        try:
            out["auto_signal_enabled"] = bool(getattr(self, "_auto_enabled", False))
        except Exception:
            out["auto_signal_enabled"] = False

        try:
            out["strategy_cfg"] = dict(getattr(self, "_strategy_cfg", {}) or {})
        except Exception:
            out["strategy_cfg"] = {}

        out["strategy_last_error"] = getattr(self, "_last_strategy_error", None)
        out["strategy_heartbeat_ms"] = int(getattr(self, "_strategy_heartbeat_ms", 0) or 0)

        # --- режим торговли / биржа ---
        try:
            if get_settings:
                s = get_settings()
                acc = getattr(s, "account", object())
                out["trading_mode"] = str(getattr(acc, "mode", "paper"))
                out["exchange"] = str(getattr(acc, "exchange", "binance_usdtm"))
        except Exception:
            # дефолты, если конфиг не читается
            out.setdefault("trading_mode", "paper")
            out.setdefault("exchange", "binance_usdtm")

        return out
    
    # === BEGIN: Worker per-symbol fees/leverage & safe sizing ===

    def update_exec_cfg(self, cfg: dict) -> None:
        """
        Обновление политик исполнения/комиссий/плеча из app.
        cfg:
        - time_in_force, post_only, on_timeout, limit_timeout_ms, max_slippage_bp
        - fee_bps_maker, fee_bps_taker (глобальные дефолты)
        - min_stop_ticks
        """
        self._exec_cfg = dict(getattr(self, "_exec_cfg", {}))
        self._exec_cfg.update({
            "time_in_force": str(cfg.get("time_in_force", self._exec_cfg.get("time_in_force", "GTX"))),
            "post_only": bool(cfg.get("post_only", self._exec_cfg.get("post_only", True))),
            "on_timeout": str(cfg.get("on_timeout", self._exec_cfg.get("on_timeout", "abort"))),
            "limit_timeout_ms": int(cfg.get("limit_timeout_ms", self._exec_cfg.get("limit_timeout_ms", 900))),
            "max_slippage_bp": float(cfg.get("max_slippage_bp", self._exec_cfg.get("max_slippage_bp", 4.0))),
            "min_stop_ticks": int(cfg.get("min_stop_ticks", self._exec_cfg.get("min_stop_ticks", 8))),
            "fee_bps_maker": float(cfg.get("fee_bps_maker", self._exec_cfg.get("fee_bps_maker", 2.0))),
            "fee_bps_taker": float(cfg.get("fee_bps_taker", self._exec_cfg.get("fee_bps_taker", 4.0))),
        })

    def _init_per_symbol_maps(self):
        if not hasattr(self, "_per_symbol_leverage"):
            self._per_symbol_leverage = {}  # { "BTCUSDT": 15.0, ... }
        if not hasattr(self, "_per_symbol_fees_bps"):
            self._per_symbol_fees_bps = {}  # { "BTCUSDT": (maker_bps, taker_bps), ... }

    def set_per_symbol_leverage(self, mapping: dict[str, float]) -> None:
        """Напр. {"BTCUSDT": 20.0, "ETHUSDT": 15.0}."""
        self._init_per_symbol_maps()
        for k, v in (mapping or {}).items():
            try:
                self._per_symbol_leverage[str(k).upper()] = float(v)
            except Exception:
                continue

    def set_per_symbol_fees_bps(self, mapping: dict[str, tuple[float, float]] | dict[str, dict]) -> None:
        """
        mapping может быть:
        - { "BTCUSDT": (2.0, 4.0), ... }  # (maker, taker)
        - { "BTCUSDT": {"maker_bps": 2.0, "taker_bps": 4.0}, ... }
        """
        self._init_per_symbol_maps()
        for k, v in (mapping or {}).items():
            try:
                sym = str(k).upper()
                if isinstance(v, (list, tuple)) and len(v) >= 2:
                    mk, tk = float(v[0]), float(v[1])
                elif isinstance(v, dict):
                    mk = float(v.get("maker_bps", v.get("maker", 2.0)))
                    tk = float(v.get("taker_bps", v.get("taker", 4.0)))
                else:
                    continue
                self._per_symbol_fees_bps[sym] = (mk, tk)
            except Exception:
                continue

    def _fees_for_symbol(self, symbol: str) -> tuple[float, float]:
        """
        Эффективные комиссии в bps (maker, taker):
        приоритет:
        1) per-symbol override (set_per_symbol_fees_bps),
        2) exec_cfg (update_exec_cfg / reload_runtime_cfg),
        3) settings().fees.binance_usdtm,
        4) дефолты 2 / 4.
        """
        sym = str(symbol).upper()
        maker = taker = None

        # 1) per-symbol map
        try:
            self._init_per_symbol_maps()
            if sym in self._per_symbol_fees_bps:
                mk, tk = self._per_symbol_fees_bps[sym]
                maker, taker = float(mk), float(tk)
        except Exception:
            pass

        # 2) exec_cfg
        if maker is None or taker is None:
            try:
                ecfg = getattr(self, "_exec_cfg", {}) or {}
                mk = ecfg.get("fee_bps_maker")
                tk = ecfg.get("fee_bps_taker")
                if mk is not None and tk is not None:
                    maker = float(mk)
                    taker = float(tk)
            except Exception:
                pass

        # 3) settings().fees.binance_usdtm
        if (maker is None or taker is None) and get_settings:
            try:
                s = get_settings()
                f = (getattr(s, "fees", None) or {}).get("binance_usdtm", {})
                mk = f.get("maker_bps")
                tk = f.get("taker_bps")
                if mk is not None and tk is not None:
                    maker = float(mk)
                    taker = float(tk)
            except Exception:
                pass

        # 4) defaults
        if maker is None:
            maker = 2.0
        if taker is None:
            taker = 4.0

        return float(maker), float(taker)

    def _leverage_for_symbol(self, symbol: str) -> float:
        """
        Эффективное плечо: сначала per-symbol, затем глобальная настройка аккаунта, затем дефолт 15.
        """
        self._init_per_symbol_maps()
        sym = str(symbol).upper()
        if sym in self._per_symbol_leverage:
            return float(self._per_symbol_leverage[sym])
        try:
            # если есть get_settings() где-то внутри воркера
            from bot.core.config import get_settings
            s = get_settings()
            acc = getattr(s, "account", object())
            per_map = getattr(acc, "per_symbol_leverage", None)
            if isinstance(per_map, dict):
                v = per_map.get(sym)
                if v:
                    return float(v)
            return float(getattr(acc, "leverage", 15.0))
        except Exception:
            return float(getattr(self, "_leverage", 15.0) or 15.0)

    def _approx_liq_price(self, entry_px: float, side: str, leverage: float) -> float:
        lev = max(1.0, float(leverage))
        if str(side).upper() == "BUY":
            return entry_px * (1.0 - 1.0 / lev)
        else:
            return entry_px * (1.0 + 1.0 / lev)

    def _liq_buffer_mult(self, entry_px: float, sl_px: float, side: str, leverage: float) -> float:
        liq = self._approx_liq_price(entry_px, side, leverage)
        dist_liq = abs(liq - entry_px)
        dist_sl = abs(sl_px - entry_px)
        if dist_sl <= 0:
            return float("inf")
        return dist_liq / dist_sl

    # === END: Worker per-symbol fees/leverage & safe sizing ===


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
    
    
    def _trend_gate(self, sym: str, side: Side) -> tuple[bool, dict]:
        """
        МЯГКИЙ тренд-фильтр входа.

        Идея: пропускаем сделку, если тренд явно НЕ против нас.
        Блокируем только когда есть УБЕДИТЕЛЬНЫЙ встречный тренд на 2 окнах.

        Источники сигнала:
        1) EMA-гейт (если доступны индикаторы): 
            BUY  -> ema9 > ema21 и ema_slope_ticks > 0
            SELL -> ema9 < ema21 и ema_slope_ticks < 0
            (мягкий: если недоступно/нейтрально — не блокируем сразу, смотрим на наклон цен)
        2) Slope по двум окнам по тикам (bps): short/long окна, тренд «за/против».
        3) (опц.) Микро-дрейф/тик-скорость: усиливаем уверенность при явном микродвижении.

        Конфиг (settings.strategy.trend.*):
        - win_s: [short_s, long_s] по умолчанию [300, 900]
        - min_slope_bps: 4.0           # порог «убедительного» встречного тренда
        - neutral_bps: 1.2             # коридор нейтрали: если |slope| ниже, не режем
        - use_ema_gate: true           # включить EMA-гейт
        - use_micro_gate: false        # учитывать microprice_drift/tick_velocity
        - micro_drift_min: 0.35
        - tick_vel_min: 0.6
        - per_symbol: { "BTCUSDT": { "min_slope_bps": 5.0, "neutral_bps": 1.5 } }

        Возвращает: (allow: bool, diag: dict)
        """
        # --------- defaults / settings ----------
        sym_u = str(sym).upper()
        win_s = [300, 900]
        min_slope_bps = 4.0
        neutral_bps = 1.2
        use_ema_gate = True
        use_micro_gate = False
        micro_drift_min = 0.35
        tick_vel_min = 0.6

        if get_settings:
            try:
                s = get_settings()
                tr = getattr(getattr(s, "strategy", object()), "trend", object())

                v = getattr(tr, "win_s", None)
                if isinstance(v, (list, tuple)) and len(v) >= 2:
                    win_s = [int(v[0]), int(v[1])]
                v = getattr(tr, "min_slope_bps", None)
                if isinstance(v, (int, float)) and v >= 0:
                    min_slope_bps = float(v)
                v = getattr(tr, "neutral_bps", None)
                if isinstance(v, (int, float)) and v >= 0:
                    neutral_bps = float(v)
                v = getattr(tr, "use_ema_gate", None)
                if isinstance(v, bool):
                    use_ema_gate = v
                v = getattr(tr, "use_micro_gate", None)
                if isinstance(v, bool):
                    use_micro_gate = v
                v = getattr(tr, "micro_drift_min", None)
                if isinstance(v, (int, float)):
                    micro_drift_min = float(v)
                v = getattr(tr, "tick_vel_min", None)
                if isinstance(v, (int, float)):
                    tick_vel_min = float(v)

                # адресные тюнинги per_symbol
                ps = getattr(tr, "per_symbol", None)
                if isinstance(ps, dict):
                    psym = ps.get(sym_u) or {}
                    if isinstance(psym, dict):
                        if isinstance(psym.get("min_slope_bps"), (int, float)):
                            min_slope_bps = float(psym["min_slope_bps"])
                        if isinstance(psym.get("neutral_bps"), (int, float)):
                            neutral_bps = float(psym["neutral_bps"])
            except Exception:
                pass

        # --------- history & prices ----------
        now = self._now_ms()
        since_ms = now - (max(win_s) * 1000) - 3000
        try:
            hist = self.history_ticks(sym_u, since_ms=since_ms, limit=4000)
        except Exception:
            hist = []

        prices: list[float] = []
        for t in hist or []:
            try:
                if isinstance(t, dict):
                    p = t.get("price")
                    if p is None:
                        b, a = t.get("bid"), t.get("ask")
                        if b and a:
                            p = (float(b) + float(a)) / 2.0
                    p = float(p or 0.0)
                else:
                    p = float(getattr(t, "price", 0.0) or 0.0)
                    if p <= 0:
                        b = getattr(t, "bid", None); a = getattr(t, "ask", None)
                        if b and a:
                            p = (float(b) + float(a)) / 2.0
            except Exception:
                p = 0.0
            if p > 0:
                prices.append(p)

        if len(prices) < 32:
            # мало истории — не режем вход
            return True, {"why": "no_history", "size": len(prices)}

        p_now = prices[-1]

        def slope_bps(win_sec: int) -> float:
            """
            Грубый slope по окну: сравниваем текущую цену с ценой «win_sec назад»
            через даунсэмпл по количеству точек (без привязки к точным ts).
            """
            n = max(int(win_sec / 2), 16)  # простой downsample-шафт
            if len(prices) <= n:
                return 0.0
            p_old = prices[-n]
            if p_old <= 0:
                return 0.0
            return float((p_now - p_old) / p_old * 1e4)

        s_short = slope_bps(int(win_s[0]))
        s_long  = slope_bps(int(win_s[1]))

        # нейтральная зона: если оба |slope| < neutral_bps — не блокируем
        neutral_short = abs(s_short) < neutral_bps
        neutral_long  = abs(s_long)  < neutral_bps
        if neutral_short and neutral_long:
            return True, {
                "why": "neutral_zone",
                "slope_bps": [round(s_short, 3), round(s_long, 3)],
                "neutral_bps": neutral_bps,
                "side": side,
            }

        # --------- EMA gate (если есть индикаторы) ----------
        ema_ok = True
        ema_diag = {}
        if use_ema_gate:
            try:
                ii = self._last_indi.get(sym_u, {}) or {}
            except Exception:
                ii = {}
            ema9  = float(ii.get("ema9") or 0.0)
            ema21 = float(ii.get("ema21") or 0.0)
            ema_slope_ticks = float(ii.get("ema_slope_ticks") or 0.0)

            if ema9 > 0 and ema21 > 0:
                if side == "BUY":
                    ema_ok = (ema9 > ema21) and (ema_slope_ticks > 0)
                else:  # SELL
                    ema_ok = (ema9 < ema21) and (ema_slope_ticks < 0)
            # если индикаторов нет — не блокируем, просто помечаем
            ema_diag = {
                "ema9": round(ema9, 6) if ema9 else None,
                "ema21": round(ema21, 6) if ema21 else None,
                "ema_slope_ticks": round(ema_slope_ticks, 6) if ema_slope_ticks else None,
                "ema_gate_used": bool(ema9 and ema21),
                "ema_ok": bool(ema_ok),
            }

        # --------- micro gate (опционально усиливаем уверенность) ----------
        micro_bias = 0  # -1/0/+1
        micro_diag = {}
        if use_micro_gate:
            try:
                lm = self._last_micro.get(sym_u, {}) or {}
            except Exception:
                lm = {}
            drift = float(lm.get("microprice_drift") or 0.0)
            tvel  = float(lm.get("tick_velocity") or 0.0)
            if abs(drift) >= micro_drift_min and abs(tvel) >= tick_vel_min:
                if drift > 0 and tvel > 0:
                    micro_bias = +1
                elif drift < 0 and tvel < 0:
                    micro_bias = -1
            micro_diag = {
                "microprice_drift": round(drift, 6),
                "tick_velocity": round(tvel, 6),
                "micro_bias": micro_bias,
                "use_micro_gate": True,
                "drift_min": micro_drift_min,
                "tvel_min": tick_vel_min,
            }

        # --------- Решение по тренду ----------
        # Блокируем ТОЛЬКО если:
        #  - EMA-гейт явно против (если он включён и индикаторы есть),
        #  - и оба slope показывают убедительный встречно-направленный тренд,
        #  - и (если включён micro) нет контраргумента в нашу сторону.
        against_slope = False
        if side == "BUY":
            against_slope = (s_short <= -min_slope_bps) and (s_long <= -min_slope_bps)
            if use_ema_gate and ema_diag.get("ema_gate_used"):
                if (ema_ok is False) and against_slope and (micro_bias <= 0):
                    return False, {
                        "why": "trend_against_buy",
                        "slope_bps": [round(s_short, 3), round(s_long, 3)],
                        "min_slope_bps": min_slope_bps,
                        **ema_diag,
                        **micro_diag,
                        "side": side,
                    }
        else:  # SELL
            against_slope = (s_short >= min_slope_bps) and (s_long >= min_slope_bps)
            if use_ema_gate and ema_diag.get("ema_gate_used"):
                if (ema_ok is False) and against_slope and (micro_bias >= 0):
                    return False, {
                        "why": "trend_against_sell",
                        "slope_bps": [round(s_short, 3), round(s_long, 3)],
                        "min_slope_bps": min_slope_bps,
                        **ema_diag,
                        **micro_diag,
                        "side": side,
                    }

        # Если сюда дошли — тренд не «убийственно против» → пускаем
        return True, {
            "why": "ok",
            "slope_bps": [round(s_short, 3), round(s_long, 3)],
            "min_slope_bps": min_slope_bps,
            "neutral_bps": neutral_bps,
            **ema_diag,
            **micro_diag,
            "side": side,
        }

    def _rr_floor_for_symbol(self, symbol: str, rr: float) -> float:
        """
        Неразрушающий RR-floor per symbol: только поднимаем слишком низкое значение,
        но не понижаем, если стратегия дала выше.
        """
        su = str(symbol).upper()
        floor = 0.0
        if su == "BTCUSDT":
            floor = 2.2    # мягкий минимум TP/SL на BTC
        elif su == "ETHUSDT":
            floor = 2.0    # ETH можно держать чуть ниже
        # SOL / прочее — без флоора

        try:
            rr = float(rr)
        except Exception:
            return rr
        return rr if rr >= floor else floor

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
            bid, ask = self.best_bid_ask(symbol)
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

    def _price_touch_check(self, symbol: str, side: Side, sl_px: float | None, tp_px: float | None) -> tuple[bool, str]:
        """
        Возвращает (touched, which), где which ∈ {"sl", "tp", ""}.
        Чекаем касание по bid/ask:
        - LONG (BUY): SL сработал, если bid <= sl; TP сработал, если ask >= tp.
        - SHORT(SELL): SL сработал, если ask >= sl; TP сработал, если bid <= tp.
        """
        if sl_px is None and tp_px is None:
            return False, ""

        try:
            bid, ask = self.best_bid_ask(symbol)
        except Exception:
            bid, ask = 0.0, 0.0

        if side == "BUY":
            if sl_px is not None and bid > 0 and bid <= float(sl_px):
                return True, "sl"
            if tp_px is not None and ask > 0 and ask >= float(tp_px):
                return True, "tp"
        else:  # SELL
            if sl_px is not None and ask > 0 and ask >= float(sl_px):
                return True, "sl"
            if tp_px is not None and bid > 0 and bid <= float(tp_px):
                return True, "tp"

        return False, ""


    def _fee_bps_for_steps(self, steps: List[str]) -> float:
        """
        Возвращает эффективные bps комиссии, исходя из реальных шагов исполнения.
        maker=2.0 bps, taker=4.0 bps по умолчанию (можно переопределить в settings.execution).
        Логика:
        - Если есть market_* или limit_filled_immediate -> это taker.
        - Если есть limit_submit_postonly/limit_filled_resting/limit_filled_maker -> это maker.
        - Если смешано (частично maker/частично taker) — берём среднее.
        """
        if not steps:
            steps = []

        taker_markers = (
            "market_submit",
            "market_filled",
            "limit_filled_immediate",  # лимит схлопнулся мгновенно, фактически taker
        )
        maker_markers = (
            "limit_submit_postonly",   # мы явно выставили post-only
            "limit_filled_resting",    # стоял в книге
            "limit_filled_maker",      # явный маркер maker-заливки
        )

        taker_like = any(any(s.startswith(m) for m in taker_markers) for s in steps)
        maker_like = any(any(m in s for m in maker_markers) for s in steps)

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

        if taker_like and not maker_like:
            return float(taker_bps)
        if maker_like and not taker_like:
            return float(maker_bps)

        # Смешанный кейс: берём среднее (дёшево и стабильно для метрик)
        return float((maker_bps + taker_bps) / 2.0)
    
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
    
    def _inc_block_reason(self, *parts: str) -> None:
        """
        Универсальный счётчик причин блокировок.

        Поддерживает оба варианта вызова:
          - self._inc_block_reason("BTCUSDT", "no_best")
          - self._inc_block_reason("BTCUSDT:no_best")
          - self._inc_block_reason("GLOBAL:cooldown_after_sl")

        Всё склеивается в один ключ и инкрементится в self._block_reasons.
        Никогда не кидает исключений.
        """
        try:
            if not parts:
                return

            # если одна строка – используем как есть
            if len(parts) == 1:
                key = str(parts[0])
            else:
                # несколько частей → склеиваем через ':'
                key = ":".join(str(p) for p in parts if p is not None)

            key = key.strip()
            if not key:
                return

            # инициализация словаря на всякий случай
            if not hasattr(self, "_block_reasons") or self._block_reasons is None:
                self._block_reasons = {}

            d = self._block_reasons
            d[key] = d.get(key, 0) + 1
        except Exception:
            # защита от любых кривых кейсов — просто не падаем
            pass

    def _compute_vol_stop_distance_px(
        self,
        sym: str,
        mid_px: float,
        *,
        min_stop_ticks_cfg: int | None = None,
        window_ms: int = 60_000,
        max_lookback: int = 512,
        k_sigma: float = 1.2,
        spread_mult: float = 1.5,
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
            # внутри _compute_vol_stop_distance_px, после расчёта dist:
            dist_bps = (dist / mid_px) * 1e4

            su = str(sym).upper()
            if su == "BTCUSDT":
                # SL 10–20 bps
                dist_bps = max(10.0, min(dist_bps, 20.0))
            elif su == "ETHUSDT":
                # SL 15–30 bps
                dist_bps = max(15.0, min(dist_bps, 30.0))

            dist = (dist_bps / 1e4) * mid_px

        # квантуем по шагу и уважаем min_stop_ticks
        if dist < step:
            dist = step
        dist_ticks = int(dist / step)
        if dist_ticks < msticks:
            dist_ticks = msticks
        return dist_ticks * step

    
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
        Консервативно считаем обе стороны как taker (taker+taker), чтобы не переоценивать EV.
        Per-symbol поднимаем min_edge_bps (особенно на BTC).
        """
        if mid <= 0 or sl_distance_px <= 0 or rr <= 0:
            return False, {"why": "bad_params"}

        # потенциальный TP в bps при данном RR и SL-дистанции
        tp_bps = (rr * sl_distance_px / mid) * 1e4

        # базовые комиссии (taker+taker)
        try:
            ex = self.execs.get(sym)
            maker = float(getattr(ex.c, "fee_bps_maker", 2.0)) if ex else 2.0
            taker = float(getattr(ex.c, "fee_bps_taker", 4.0)) if ex else 4.0
        except Exception:
            maker, taker = 2.0, 4.0

        fees_bps_total = 2.0 * taker  # консервативно считаем вход+выход как taker

        # спред в bps
        spread = max(0.0, (ask - bid))
        spread_bps = (spread / mid) * 1e4 if mid > 0 else 0.0

        # минимальный технологический edge (может быть переопределён в settings.strategy.min_edge_bps)
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

        # --- per-symbol усиление порога ---
        su = str(sym).upper()
        if su == "BTCUSDT":
            # BTC дорогой и fee-сильный → поднимем минимальный «запас» edge
            min_edge_bps = max(min_edge_bps, 18.0)
        elif su == "ETHUSDT":
            min_edge_bps = max(min_edge_bps, 12.0)
        # SOL оставим как есть — ликвидности и так хватает

        # простой учёт слипа: доля спреда
        slip_bps = 0.35 * spread_bps

        need_bps = fees_bps_total + 0.8 * spread_bps + slip_bps + min_edge_bps
        ok = (tp_bps >= need_bps)

        return ok, {
            "tp_bps": round(tp_bps, 3),
            "need_bps": round(need_bps, 3),
            "fees_bps_total": round(fees_bps_total, 3),
            "spread_bps": round(spread_bps, 3),
            "min_edge_bps": round(min_edge_bps, 3),
            "why": None if ok else "edge_insufficient",
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

    def set_strategy(self, *, enabled: bool, cooldown_ms: int, min_flat_ms: int) -> dict:
        self._auto_enabled = bool(enabled)
        self._auto_cooldown_ms = int(max(200, cooldown_ms))
        self._auto_min_flat_ms = int(max(0, min_flat_ms))

        # для diag()
        self._strategy_cfg = {
            "cooldown_ms": self._auto_cooldown_ms,
            "min_flat_ms": self._auto_min_flat_ms,
        }

        return {
            "auto_signal_enabled": self._auto_enabled,
            "strategy_cfg": dict(self._strategy_cfg),
        }

    # --- sizing helpers ---

    def _compute_auto_qty(self, symbol: str, side: Side, sl_distance_px: float) -> float:
        """
        Размер позиции от процента риска и SL-дистанции.

        NEW: учитываем комиссии (taker+taker) при расчёте qty так,
        чтобы общий убыток на стопе (цена + комиссии) ≈ risk_usd_target.
        """
        sym = symbol.upper()
        spec = self.specs[sym]

        # --- риск-таргет в USD ---
        try:
            risk_pct = float(self._risk_cfg.risk_per_trade_pct)
        except Exception:
            risk_pct = 0.0

        try:
            equity = float(self._starting_equity_usd + float(self._pnl_day.get("pnl_usd", 0.0) or 0.0))
        except Exception:
            equity = float(self._starting_equity_usd)

        try:
            min_floor = float(self._risk_cfg.min_risk_usd_floor)
        except Exception:
            min_floor = 1.0

        risk_usd_target = max(min_floor, equity * (risk_pct / 100.0)) if risk_pct > 0.0 else min_floor
        if sl_distance_px <= 0.0 or risk_usd_target <= 0.0:
            return 0.0

        # --- рынок и цена ---
        bid, ask = self.hub.best_bid_ask(sym)
        px = ask if side == "BUY" else bid
        if px <= 0.0:
            return 0.0

        # --- комиссии в долях (taker + taker, консервативно) ---
        try:
            maker_bps, taker_bps = self._fees_for_symbol(sym)
        except Exception:
            maker_bps, taker_bps = 2.0, 4.0

        fee_bps_total = 2.0 * float(taker_bps)  # вход + выход как taker
        c = fee_bps_total / 10_000.0           # доля нотиционала

        # относительное движение до SL (в доле цены)
        d = sl_distance_px / px
        if d <= 0.0:
            return 0.0

        denom = d + c
        if denom <= 0.0:
            return 0.0

        # --- целевой нотиционал с учётом комиссий ---
        notional_target = risk_usd_target / denom  # N = R / (d + c)

        # лимит по плечу
        lev = float(self._leverage_for_symbol(sym))
        if lev < 1.0:
            lev = 1.0
        max_notional = equity * lev
        if max_notional <= 0.0:
            return 0.0

        notional = min(notional_target, max_notional)
        qty_raw = notional / px

        # --- минимальная нотиональ ---
        min_notional = float(getattr(self, "_min_notional_usd", 5.0))
        min_qty_by_notional = min_notional / px if px > 0.0 else 0.0

        # --- шаг лота ---
        step = max(float(getattr(spec, "qty_step", 0.001)), 1e-12)

        def floor_to_step(x: float, st: float) -> float:
            return (int(x / st)) * st

        qty = max(min_qty_by_notional, qty_raw)
        qty = floor_to_step(qty, step)

        # если после округления всё равно 0 — пробуем минимум, если он валиден
        if qty <= 0.0:
            qty = step
            if qty * px < min_notional or qty * px > max_notional:
                return 0.0

        # --- liq-buffer: SL не должен быть рядом с ликвидацией ---
        try:
            sl_px = px - sl_distance_px if side == "BUY" else px + sl_distance_px
            lb_mult = self._liq_buffer_mult(px, sl_px, side, lev)
            min_mult = float(
                getattr(self._safety_cfg, "min_liq_buffer_sl_mult", 3.0) or 3.0
            )
            if lb_mult < max(1.5, min_mult):
                return 0.0
        except Exception:
            pass

        return float(qty)

    # --- cfg loaders ---
    def _cross_guard_allow(self, sym: str, side: Side) -> tuple[bool, str]:
        """
        Cross-guard против BTC:
        - BTC сам по себе не фильтруем (он — якорь),
        - для ETH/других смотрим, не идём ли мы против доминирующего движения BTC,
        и не проигрываем ли BTC по скорости.
        """
        su = sym.upper()
        if su == "BTCUSDT":
            return True, "root_symbol"

        lookback_s = 60
        btc_prices = self._last_mid_prices("BTCUSDT", lookback_s)
        sym_prices = self._last_mid_prices(su, lookback_s)

        if len(btc_prices) < 10 or len(sym_prices) < 10:
            return True, "no_history"

        allow, reason, btc_drift, sym_drift = decide_cross_guard(
            side=side,
            btc_prices=btc_prices,
            sym_prices=sym_prices,
            cfg=CrossCfg(
                lookback_s=lookback_s,
                min_btc_strength_bp=6.0,
                block_on_divergence=True,
                prefer_symbol_with_higher_speed=False,  # скорость разрулим ниже
            ),
        )

        if not allow:
            return False, f"cross_guard:{reason}"

        # опционально: если BTC и символ в одну сторону, берём более быстрый
        best = pick_best_symbol_by_speed(
            btc_prices=btc_prices,
            sym_a=("BTCUSDT", btc_prices),
            sym_b=(su, sym_prices),
            lookback_s=lookback_s,
        )
        if best != su:
            return False, "cross_guard:prefer_btc"

        return True, "ok"

    def _last_mid_prices(self, sym: str, lookback_s: int) -> list[float]:
        """Грубый хвост mid-цен за lookback_s секунд."""
        now = self._now_ms()
        since_ms = now - lookback_s * 1000
        try:
            ticks = self.history_ticks(sym, since_ms=since_ms, limit=2000)
        except Exception:
            ticks = []

        out: list[float] = []
        for t in ticks or []:
            try:
                if isinstance(t, dict):
                    px = t.get("price")
                    if px is None:
                        b = t.get("bid"); a = t.get("ask")
                        if b and a:
                            px = (float(b) + float(a)) / 2.0
                else:
                    px = getattr(t, "price", None)
                    if px is None:
                        b = getattr(t, "bid", None); a = getattr(t, "ask", None)
                        if b and a:
                            px = (float(b) + float(a)) / 2.0
                if px:
                    out.append(float(px))
            except Exception:
                continue
        return out

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
