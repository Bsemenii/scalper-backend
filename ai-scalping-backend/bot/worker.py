# bot/worker.py
from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import time
import math
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Literal, Deque
from collections import deque
from contextlib import contextmanager
from dotenv import load_dotenv
from strategy.cross_guard import decide_cross_guard, CrossCfg, pick_best_symbol_by_speed
from adapters.binance_rest import BinanceUSDTMAdapter

from exec.fsm import PositionFSM
from bot.core.types import Side as CoreSide  # (может быть неиспользован — ок)
from bot.core.config import get_settings
from account.state import AccountState
from exec.position_state import (
    get_position_state,
    save_position_state,
    flat_state,
    with_long,
    with_short,
    PositionSide,
)
from exec.bollinger_executor import (
    place_entry_order,
    place_exit_order,
    compute_tp_sl_levels,
    attach_sl_tp,
    check_sl_tp_hit,
)

# Type alias for Side (used throughout the code)
Side = CoreSide

from adapters.binance_ws import BinanceWS
from stream.coalescer import Coalescer, TickerTick
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
try:
    from features.bollinger import BollingerEngine  # type: ignore
except Exception:
    BollingerEngine = None  # type: ignore

# риск-фильтры
from risk.filters import (
    SafetyCfg, RiskCfg,
    MicroCtx, TimeCtx, PositionalCtx, DayState,
    check_entry_safety, check_day_limits,
)
from risk.sizing import calc_qty_from_stop_distance_usd

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
    """
    Runtime-состояние позиции по символу.

    ВАЖНО:
    - Поля и имена используются по всему Worker'у.
    - sl_order_id / tp_order_id — это clientOrderId защитных ордеров,
      которые возвращает Executor.place_bracket().
    """
    state: Literal["FLAT", "OPEN", "EXITING"]
    side: Optional[Side]
    qty: float
    entry_px: float
    sl_px: Optional[float]
    tp_px: Optional[float]
    opened_ts_ms: int
    timeout_ms: int = 180_000

    # id активной сделки в БД (если когда-то начнёшь связывать ордера/трейды)
    trade_id: Optional[str] = None

    # id защитных ордеров (clientOrderId), чтобы Worker мог их отменять
    sl_order_id: Optional[str] = None
    tp_order_id: Optional[str] = None


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
      - комиссии (maker/taker) из реальных данных Binance,
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

        self._fixed_margin_per_trade = 30.0
        self._fixed_leverage = 10.0
        self._max_notional_per_trade = 400.0


        # ts последнего стоп-лосса (для cooldown_after_sl_s)
        self._last_sl_ts_ms: int = 0

        # ts последней сверки PnL с БД (для throttling reconciliation)
        self._last_reconcile_ts_ms: int = 0

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
        self._bollinger_eng: Dict[str, BollingerEngine] = {}
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
        self._stop_event = asyncio.Event()  # для остановки exchange_consistency_loop
        
        # CRITICAL FIX: Global lock to prevent concurrent position opening
        self._global_position_lock = asyncio.Lock()
        
        # --- desync guard counter for exchange consistency ---
        self._desync_count: int = 0

    # ---------- lifecycle ----------

    async def start(self) -> None:
        """
        Старт воркера:
        - читает settings + ENV;
        - инициализирует Executor'ы с BinanceUSDTMAdapter (только live mode);
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

        # mode: ТОЛЬКО live mode поддерживается (paper trading удалён)
        mode = "live"
        if account_cfg is not None:
            try:
                mode_raw = str(getattr(account_cfg, "mode", "live") or "live").lower()
                if mode_raw == "live":
                    mode = "live"
                else:
                    logger.warning(
                        f"[worker] Invalid mode '{mode_raw}' in settings, forcing 'live' mode. "
                        "Paper trading is no longer supported."
                    )
                    mode = "live"
            except Exception:
                mode = "live"

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

        # Log API key status (without exposing secrets)
        api_key_status = "SET" if api_key else "MISSING"
        api_secret_status = "SET" if api_secret else "MISSING"
        logger.info(
            "[worker.start] binance cfg: api_key=%s, api_secret=%s, base_url=%s, testnet=%s",
            api_key_status, api_secret_status, base_url, use_testnet,
        )
        
        # If live mode but keys are missing, log warning but continue (will fail later with clear error)
        if mode == "live" and exch_name == "binance_usdtm":
            if not api_key or not api_secret:
                logger.error(
                    "[worker.start] CRITICAL: Live mode requires API keys but they are missing! "
                    "Set %s and %s environment variables. Worker will fail during executor initialization.",
                    api_key_env, api_secret_env
                )

        # ---------------------------------------------------
        # 5. Старт strategy_loop (до инициализации потоков ОК)
        # ---------------------------------------------------
        if self._strat_task is None or self._strat_task.done():
            loop = asyncio.get_running_loop()
            self._strat_task = loop.create_task(self._strategy_loop(), name="strategy_loop")
            logger.info("[worker.start] strategy_loop task created and started")

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
            # ТОЛЬКО live mode с BinanceUSDTMAdapter (paper trading удалён)
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
                raise RuntimeError(
                    f"Invalid configuration: mode={mode}, exchange={exch_name}. "
                    "Only 'live' mode with 'binance_usdtm' exchange is supported. "
                    "Paper trading has been removed."
                )

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
                # Reversion strategy for GALAUSDT uses 1-minute candles.
                # IndiEngine uses bb_win=60 (default) which represents ~60 ticks,
                # approximately 1 minute of data for active symbols.
                # All indicators (bb_z, ema21, rsi, realized_vol_bp) are computed from 1-minute timeframe.
                self._indi_eng[sym] = IndiEngine(price_step=spec.price_tick)
            if BollingerEngine is not None:
                # Bollinger Bands with default window=20, k=2.0
                # Can be customized via settings if needed
                self._bollinger_eng[sym] = BollingerEngine(window=20, k=2.0)

        # 6.5 Live Binance: sanity-check + margin/leverage
        is_live = (mode == "live" and exch_name == "binance_usdtm")
        if is_live:
            # 1) жёсткая проверка, что аккаунт пустой по нашим символам
            await self._sanity_check_live_start(account_cfg)

            # 2) проставляем marginType и leverage по каждому символу
            try:
                await self._apply_margin_and_leverage_live(account_cfg)
            except Exception as e:
                logger.warning("[worker.start] apply_margin_and_leverage_live failed: %s", e)

        # 6.55 Initialize AccountState service (for real account data in live mode)
        adapter_for_account = None
        if is_live:
            adapter_for_account = self._get_live_adapter()
        else:
            # Use first executor's adapter (BinanceUSDTMAdapter)
            if self.execs:
                first_ex = next(iter(self.execs.values()))
                adapter_for_account = getattr(first_ex, "a", None) or getattr(first_ex, "adapter", None)

        if adapter_for_account:
            try:
                # Pass hub as price_provider so AccountState can get current mid prices for uPnL calculation
                self._account_state = AccountState(
                    adapter=adapter_for_account, 
                    is_live=is_live,
                    price_provider=self.hub
                )
                # Initial refresh to get starting snapshot
                await self._account_state.refresh()
                logger.info("[worker.start] AccountState initialized (is_live=%s)", is_live)
            except Exception as e:
                logger.warning("[worker.start] AccountState initialization failed: %s", e)
                self._account_state = None
        else:
            self._account_state = None

        # 6.6 Exchange-guard: запускаем ВСЕГДА, он сам проверит, есть ли живой Binance
        loop = asyncio.get_running_loop()
        if self._exchange_task is None or self._exchange_task.done():
            self._exchange_task = loop.create_task(
                self._exchange_consistency_loop(interval_s=1.0),  # делаем 1s вместо 10s
                name="exchange_guard",
            )
        # ---------------------------------------------------
        # 7. Старт WS + коалесер + watchdog
        # ---------------------------------------------------
        logger.info("[worker.start] connecting WebSocket for symbols=%s", self.symbols)
        try:
            await self.ws.connect(self._on_ws_raw)
            logger.info("[worker.start] WebSocket connection initiated successfully")
        except Exception as e:
            logger.error("[worker.start] WebSocket connection failed: %s", e, exc_info=True)
            raise
        
        logger.info("[worker.start] starting coalescer and watchdog")
        self._tasks.append(asyncio.create_task(self.coal.run(self._on_tick), name="coal.run"))
        self._wd_task = asyncio.create_task(self._watchdog_loop(), name="watchdog")
        logger.info("[worker.start] coalescer and watchdog tasks created")

        logger.info("[worker.start] started with symbols=%s, mode=%s, exchange=%s", self.symbols, mode, exch_name)

    async def stop(self) -> None:
        if not self._started:
            return
        self._started = False

        # Сигнализируем остановку exchange_consistency_loop
        if hasattr(self, "_stop_event"):
            self._stop_event.set()

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
            be = self._bollinger_eng.get(tick.symbol)

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

            # Bollinger Bands features
            if be is not None:
                bf = be.update(price=float(tick.price or 0.0))
                # Add Bollinger features to _last_indi dict
                if tick.symbol not in self._last_indi:
                    self._last_indi[tick.symbol] = {}
                self._last_indi[tick.symbol].update({
                    "boll_mid": float(bf["boll_mid"]) if bf["boll_mid"] is not None else None,
                    "boll_up": float(bf["boll_up"]) if bf["boll_up"] is not None else None,
                    "boll_low": float(bf["boll_low"]) if bf["boll_low"] is not None else None,
                    "boll_bandwidth": float(bf["boll_bandwidth"]) if bf["boll_bandwidth"] is not None else None,
                    "boll_position": float(bf["boll_position"]) if bf["boll_position"] is not None else None,
                })
                # Log at DEBUG level (only when features are computed successfully)
                if bf["boll_mid"] is not None:
                    logger.debug(
                        "[FEATURES] bollinger computed symbol=%s mid=%.6f up=%.6f low=%.6f bandwidth=%.6f position=%.4f",
                        tick.symbol,
                        bf["boll_mid"],
                        bf["boll_up"] or 0.0,
                        bf["boll_low"] or 0.0,
                        bf["boll_bandwidth"] or 0.0,
                        bf["boll_position"] or 0.0,
                    )
        except Exception:
            # индикаторы не критичны — не роняем пайплайн
            pass

        # ---------- SL/TP monitoring (lightweight check) ----------
        try:
            pos_state = get_position_state(tick.symbol)
            if not pos_state.is_flat() and tick.price:
                hit = check_sl_tp_hit(tick.symbol, float(tick.price), pos_state)
                if hit == "exit":
                    # Determine which level was hit (SL or TP)
                    which = "UNKNOWN"
                    if pos_state.sl_price is not None and pos_state.tp_price is not None:
                        price = float(tick.price)
                        sl_dist = abs(price - float(pos_state.sl_price))
                        tp_dist = abs(price - float(pos_state.tp_price))
                        which = "SL" if sl_dist < tp_dist else "TP"
                    logger.info(
                        "[EXEC] sl/tp hit symbol=%s, side=%s, price=%.6f, which=%s",
                        tick.symbol, pos_state.to_str_side(), float(tick.price), which
                    )
                    # Trigger exit asynchronously (don't block tick processing)
                    asyncio.create_task(self._handle_sltp_exit(tick.symbol, pos_state))
        except Exception:
            # SL/TP monitoring is best-effort, don't break tick processing
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
    
    async def _sync_symbol_from_exchange(self, symbol: str) -> None:
        """
        Привести локальный snapshot self._pos[symbol] к фактическому состоянию на бирже.

        - читает /positionRisk
        - читает /openOrders по symbol
        - обновляет side / qty / entry_px
        - подтягивает sl_px / tp_px и sl_order_id / tp_order_id из reduceOnly-ордеров

        Если на бирже позиция закрылась, а локально была OPEN с trade_id — считаем это
        внешним закрытием (manual close) и регистрируем сделку.
        """
        adapter = self._get_live_adapter()
        if adapter is None:
            # paper-режим или нет live-адаптера — ничего не делаем
            return

        sym_u = symbol.upper()

        # --- 1. Читаем позиции ---
        try:
            positions = await adapter.get_positions()
        except Exception as e:
            logger.warning("[exchange_sync] get_positions failed: %s", e)
            return

        pos_row: Optional[Dict[str, Any]] = None
        for p in positions or []:
            try:
                if str(p.get("symbol", "")).upper() == sym_u:
                    pos_row = p
                    break
            except Exception:
                continue

        ext_amt = 0.0
        entry_px = 0.0
        side: Optional[str] = None  # BUY / SELL с точки зрения биржи

        if pos_row is not None:
            try:
                ext_amt = float(pos_row.get("positionAmt") or 0.0)
            except Exception:
                ext_amt = 0.0
            try:
                entry_px = float(pos_row.get("entryPrice") or 0.0)
            except Exception:
                entry_px = 0.0

            if abs(ext_amt) > 1e-9:
                side = "BUY" if ext_amt > 0 else "SELL"

        # --- 2. Читаем открытые ордера (SL/TP reduceOnly) ---
        sl_px: Optional[float] = None
        tp_px: Optional[float] = None
        sl_oid: Optional[str] = None
        tp_oid: Optional[str] = None

        open_orders: list[dict[str, Any]] = []
        try:
            open_orders = await adapter.get_open_orders(symbol=sym_u)  # type: ignore[arg-type]
        except Exception as e:
            logger.warning("[exchange_sync] get_open_orders(%s) failed: %s", sym_u, e)

        for od in open_orders or []:
            try:
                if str(od.get("symbol", "")).upper() != sym_u:
                    continue

                reduce_only = str(od.get("reduceOnly") or "").lower()
                if reduce_only not in ("true", "1"):
                    # нас интересуют только защитные (reduceOnly) ордера
                    continue

                otype = str(od.get("type") or "")
                coid = str(od.get("clientOrderId") or "")

                if otype == "STOP_MARKET":
                    sp = float(od.get("stopPrice") or 0.0)
                    if sp > 0:
                        sl_px = sp
                        sl_oid = coid
                elif otype in ("TAKE_PROFIT_MARKET", "LIMIT"):
                    price = float(od.get("stopPrice") or od.get("price") or 0.0)
                    if price > 0:
                        tp_px = price
                        tp_oid = coid
            except Exception:
                continue

        # --- 3. Берём предыдущий локальный стейт ---
        prev = self._pos.get(sym_u)
        default_timeout_ms = 180_000
        timeout_ms = prev.timeout_ms if prev is not None else default_timeout_ms
        trade_id = prev.trade_id if prev is not None else None
        opened_ts_ms = prev.opened_ts_ms if prev is not None else 0

        # --- 4. На бирже FLAT: обрабатываем возможное внешнее закрытие ---
        if side is None or abs(ext_amt) <= 1e-9:
            try:
                # WARNING if we detect external close but have no trade_id
                if (
                    prev is not None
                    and prev.state == "OPEN"
                    and float(prev.qty or 0.0) > 0.0
                    and not trade_id
                ):
                    logger.warning(
                        "[exchange_sync] %s external close detected but trade_id is None! "
                        "Trade will NOT be recorded in DB. "
                        "Position: side=%s, qty=%.6f, entry_px=%.2f, opened_ts_ms=%d",
                        sym_u,
                        prev.side,
                        float(prev.qty or 0.0),
                        float(prev.entry_px or 0.0),
                        prev.opened_ts_ms or 0,
                    )
                
                if (
                    prev is not None
                    and prev.state == "OPEN"
                    and float(prev.qty or 0.0) > 0.0
                    and trade_id
                ):
                    exit_px: Optional[float] = None

                    # 4.1. Пытаемся взять mid-цену через hub.best_bid_ask
                    with contextlib.suppress(Exception):
                        bid, ask = self.hub.best_bid_ask(sym_u)
                        if bid > 0.0 and ask > 0.0:
                            exit_px = (float(bid) + float(ask)) / 2.0
                        else:
                            exit_px = float(max(bid, ask))

                    # 4.2. Fallback: используем entry_px / prev.entry_px
                    if exit_px is None or exit_px <= 0.0:
                        exit_px = float(entry_px or prev.entry_px or 0.0)

                    if exit_px and exit_px > 0.0:
                        # 4.3. Cancel bracket orders (SL/TP) if they exist
                        sl_oid = getattr(prev, "sl_order_id", None)
                        tp_oid = getattr(prev, "tp_order_id", None)
                        if sl_oid or tp_oid:
                            ex = self.execs.get(sym_u)
                            if ex is not None:
                                cancel_bracket = getattr(ex, "cancel_bracket", None)
                                cancel_order = getattr(ex, "cancel_order", None)

                                if callable(cancel_bracket):
                                    try:
                                        result = await cancel_bracket(
                                            sym_u,
                                            sl_order_id=sl_oid,
                                            tp_order_id=tp_oid,
                                        )
                                        logger.info(
                                            "[exchange_sync] cancel_bracket for external_close on %s: sl_order_id=%s, tp_order_id=%s, result=%s",
                                            sym_u, sl_oid, tp_oid, result
                                        )
                                    except Exception as e_cancel:
                                        logger.error(
                                            "[exchange_sync] cancel_bracket failed for %s: sl_order_id=%s, tp_order_id=%s, error=%s",
                                            sym_u, sl_oid, tp_oid, e_cancel
                                        )
                                elif callable(cancel_order):
                                    for oid in (sl_oid, tp_oid):
                                        if oid:
                                            with contextlib.suppress(Exception):
                                                await cancel_order(sym_u, oid)
                                                logger.info(
                                                    "[exchange_sync] cancel_order for external_close on %s: order_id=%s",
                                                    sym_u, oid
                                                )

                        # 4.4. Compute PnL estimate
                        entry_px_val = float(prev.entry_px or 0.0)
                        qty_val = float(prev.qty or 0.0)
                        exit_px_val = float(exit_px)
                        side_val = str(prev.side or "").upper()

                        # Calculate gross PnL based on position side
                        pnl_gross = 0.0
                        if side_val == "BUY":
                            # LONG: profit when exit > entry
                            pnl_gross = (exit_px_val - entry_px_val) * qty_val
                        elif side_val == "SELL":
                            # SHORT: profit when entry > exit
                            pnl_gross = (entry_px_val - exit_px_val) * qty_val

                        # R estimate is approximate (we don't have clean risk info for external close)
                        r_est = 0.0

                        logger.info(
                            "[exchange_sync] %s external close detected: trade_id=%s, side=%s, qty=%.6f, entry_px=%s, exit_px=%s, pnl_gross=%.4f",
                            sym_u,
                            trade_id,
                            side_val,
                            qty_val,
                            entry_px_val,
                            exit_px_val,
                            pnl_gross,
                        )

                        # 4.5. Закрываем трейд в БД
                        repo_close_trade = None
                        try:
                            from storage.repo import close_trade as repo_close_trade  # type: ignore
                        except Exception as e_imp:
                            logger.warning(
                                "[exchange_sync] cannot import repo.close_trade for %s trade_id=%s: %s",
                                sym_u,
                                trade_id,
                                e_imp,
                            )

                        if repo_close_trade is not None:
                            try:
                                repo_close_trade(
                                    trade_id=trade_id,
                                    exit_px=exit_px_val,
                                    r=r_est,
                                    pnl_usd=pnl_gross,
                                    reason_close="external_close",
                                )
                            except Exception as e_close:
                                logger.warning(
                                    "[exchange_sync] repo_close_trade failed for %s trade_id=%s: %s",
                                    sym_u,
                                    trade_id,
                                    e_close,
                                )

                        # 4.6. Обновляем in-memory статистику, если есть helper'ы
                        with contextlib.suppress(Exception):
                            if hasattr(self, "_build_trade_record") and hasattr(self, "_register_trade"):
                                entry_steps = self._entry_steps.get(sym_u, [])
                                trade_rec = self._build_trade_record(  # type: ignore[attr-defined]
                                    sym_u,
                                    prev,
                                    exit_px_val,
                                    reason="external_close",
                                    entry_steps=entry_steps,
                                    exit_steps=[],
                                )
                                self._register_trade(trade_rec)  # type: ignore[attr-defined]

            except Exception as e:
                logger.warning(
                    "[exchange_sync] failed to register external close for %s trade_id=%s: %s",
                    sym_u,
                    trade_id,
                    e,
                )

            # Приводим локальный стейт к FLAT
            self._pos[sym_u] = PositionState(
                state="FLAT",
                side=None,
                qty=0.0,
                entry_px=0.0,
                sl_px=None,
                tp_px=None,
                opened_ts_ms=0,
                timeout_ms=timeout_ms,
                trade_id=None,
                sl_order_id=None,
                tp_order_id=None,
            )
            # Update unified PositionState
            self._update_unified_position_state(sym_u)
            logger.info("[exchange_sync] %s -> FLAT (no position on exchange)", sym_u)
            
            # Чистим все stray reduceOnly ордера после обновления стейта
            await self._cleanup_brackets_for_symbol(sym_u)
            return

        # --- 5. На бирже есть позиция: синхронизируем OPEN-состояние ---
        now_ms = self._now_ms()
        if prev is not None and prev.state == "OPEN":
            opened_ts_ms = prev.opened_ts_ms or now_ms
        else:
            opened_ts_ms = now_ms

        # Preserve previous SL/TP if not found in open orders (defensive: don't overwrite valid values with None)
        # This handles cases where TP order already filled, was cancelled, or parsing failed
        if sl_px is None and prev is not None and prev.sl_px is not None:
            sl_px = prev.sl_px
        if tp_px is None and prev is not None and prev.tp_px is not None:
            tp_px = prev.tp_px

        self._pos[sym_u] = PositionState(
            state="OPEN",
            side=side,  # type: ignore[arg-type]
            qty=abs(ext_amt),
            entry_px=float(entry_px),
            sl_px=sl_px,
            tp_px=tp_px,
            opened_ts_ms=opened_ts_ms,
            timeout_ms=timeout_ms,
            trade_id=trade_id,
            sl_order_id=sl_oid,
            tp_order_id=tp_oid,
        )

        logger.info(
            "[exchange_sync] %s synced from exchange: side=%s qty=%.6f entry_px=%s sl=%s tp=%s sl_oid=%s tp_oid=%s",
            sym_u,
            side,
            abs(ext_amt),
            entry_px,
            sl_px,
            tp_px,
            sl_oid,
            tp_oid,
        )
        
        # Чистим все stray reduceOnly ордера после обновления стейта
        await self._cleanup_brackets_for_symbol(sym_u)

    async def _cleanup_brackets_for_symbol(self, symbol: str) -> None:
        """
        Агрессивная очистка всех stray SL/TP (reduceOnly) ордеров для символа.

        Правила:
        - Если локальная позиция FLAT → отменяем ВСЕ reduceOnly ордера.
        - Если локальная позиция OPEN → отменяем reduceOnly ордера, чей origQty значительно
          больше текущего размера позиции (> pos.qty * 1.01), т.к. это остатки от старых сделок.

        Вызывается:
        1) После _sync_symbol_from_exchange (гарантия чистоты после обновления стейта)
        2) Перед открытием новой позиции (гарантия отсутствия ghost-ордеров)

        ВАЖНО:
        - НИКОГДА не падает наружу (все ошибки логируются и suppress'ятся).
        - Явно логирует список отменённых ордеров (INFO).
        - Логирует ошибки отмены как WARNING (не блокирует основной флоу).
        """
        adapter = self._get_live_adapter()
        if adapter is None:
            # paper-режим или нет live-адаптера — ничего не делаем
            return

        sym_u = symbol.upper()

        # --- 1. Читаем открытые ордера ---
        open_orders: list[dict[str, Any]] = []
        try:
            open_orders = await adapter.get_open_orders(symbol=sym_u)  # type: ignore[arg-type]
        except asyncio.CancelledError:
            # Shutdown in progress - don't log as error
            return
        except Exception as e:
            logger.warning(
                "[bracket_gc] get_open_orders(%s) failed: %s", sym_u, e
            )
            return

        if not open_orders:
            # Нет открытых ордеров — нечего чистить
            return

        # --- 2. Фильтруем только reduceOnly ордера ---
        reduce_only_orders: list[dict[str, Any]] = []
        for od in open_orders:
            try:
                if str(od.get("symbol", "")).upper() != sym_u:
                    continue

                # Проверяем флаг reduceOnly (может быть bool или string "true"/"false")
                reduce_only = od.get("reduceOnly")
                if isinstance(reduce_only, bool):
                    is_reduce_only = reduce_only
                else:
                    is_reduce_only = str(reduce_only or "").lower() in ("true", "1")

                if is_reduce_only:
                    reduce_only_orders.append(od)
            except Exception as e:
                logger.debug(
                    "[bracket_gc] failed to parse order %r for %s: %s",
                    od, sym_u, e
                )
                continue

        if not reduce_only_orders:
            # Нет reduceOnly ордеров — нечего чистить
            return

        # --- 3. Проверяем локальный стейт позиции ---
        pos = self._pos.get(sym_u)
        local_is_flat = (
            pos is None
            or pos.state == "FLAT"
            or abs(float(pos.qty or 0.0)) <= 1e-9
        )

        local_qty = 0.0
        if pos is not None and pos.state == "OPEN":
            local_qty = abs(float(pos.qty or 0.0))

        # --- 4. Определяем, какие ордера нужно отменить ---
        to_cancel: list[tuple[str, str]] = []  # (client_order_id, reason)

        for od in reduce_only_orders:
            try:
                coid = str(od.get("clientOrderId") or "")
                if not coid:
                    # Нет clientOrderId — пропускаем (не можем отменить)
                    continue

                # Если локально FLAT → отменяем все reduceOnly
                if local_is_flat:
                    to_cancel.append((coid, "local_flat"))
                    continue

                # Если локально OPEN → проверяем размер ордера
                try:
                    orig_qty = float(od.get("origQty") or 0.0)
                except Exception:
                    orig_qty = 0.0

                # Если origQty значительно больше текущей позиции → это stray order
                if orig_qty > local_qty * 1.01 and local_qty > 0.0:
                    to_cancel.append((coid, "qty_mismatch"))
                    continue

            except Exception as e:
                logger.debug(
                    "[bracket_gc] failed to check order %r for %s: %s",
                    od, sym_u, e
                )
                continue

        if not to_cancel:
            # Нет ордеров для отмены
            return

        # --- 5. Отменяем stray ордера ---
        canceled: list[str] = []
        failed: list[tuple[str, str]] = []  # (coid, error_msg)

        for coid, reason in to_cancel:
            try:
                await adapter.cancel_order(sym_u, client_order_id=coid)  # type: ignore[arg-type]
                canceled.append(coid)
                logger.debug(
                    "[bracket_gc] %s canceled order %s (reason=%s)",
                    sym_u, coid, reason
                )
            except Exception as e:
                failed.append((coid, str(e)))
                logger.warning(
                    "[bracket_gc] %s failed to cancel order %s (reason=%s): %s",
                    sym_u, coid, reason, e
                )

        # --- 6. Итоговый лог ---
        if canceled:
            logger.info(
                "[bracket_gc] %s cleaned up %d stray reduceOnly orders: %s",
                sym_u, len(canceled), canceled
            )

        if failed:
            logger.warning(
                "[bracket_gc] %s failed to cancel %d orders: %s",
                sym_u, len(failed), [f"{c}({e})" for c, e in failed]
            )

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
            # TEMP: single-symbol mode GALAUSDT, 10x leverage
            # --- leverage: per-symbol если есть метод _leverage_for_symbol, иначе общий ---
            # Leverage is read from config (per_symbol_leverage or account.leverage), fixed at 10x for GALAUSDT
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
                # Binance returns -4046 when margin type is already set (harmless)
                binance_code = getattr(e, "binance_code", None)
                if binance_code == -4046:
                    logger.info("[worker.live] set_margin_type(%s, %s): already set (code=-4046)", sym, margin_mode)
                else:
                    logger.warning("[worker.live] set_margin_type(%s, %s) failed: %s", sym, margin_mode, e)

            # leverage
            try:
                if hasattr(adapter, "set_leverage"):
                    await adapter.set_leverage(sym, lev)
            except Exception as e:
                logger.warning("[worker.live] set_leverage(%s, %s) failed: %s", sym, lev, e)

    async def _exchange_consistency_loop(
        self,
        *,
        interval_s: float = 10.0,
    ) -> None:
        """
        Периодическая сверка локального FSM с биржей (positionRisk).

        Логика (согласно SPEC 2.1–2.3):

        1) Биржа считается источником истины (SPEC 2.1).
        2) Для каждого символа периодически вызываем _sync_symbol_from_exchange(symbol),
           который является ЕДИНСТВЕННОЙ точкой входа для синхронизации позиций.
        3) После синхронизации проверяем, сошлось ли состояние; если нет — логируем
           и выключаем авто-режим.

        Это гарантирует:
          - Все изменения позиций на бирже автоматически подхватываются,
          - Нет конфликтующей логики, которая напрямую мутирует self._pos[sym],
          - _sync_symbol_from_exchange обрабатывает все кейсы (FLAT/OPEN, trade_id).
        """
        logger.info("[exchange_guard] consistency loop started, interval_s=%s", interval_s)

        try:
            while not self._stop_event.is_set():
                await asyncio.sleep(interval_s)

                # Refresh AccountState periodically to keep it synchronized
                try:
                    if hasattr(self, "_account_state") and self._account_state:
                        await self._account_state.refresh()
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.warning("[exchange_guard] AccountState refresh failed: %s", e)

                adapter = self._get_live_adapter()
                if adapter is None:
                    # нет live-адаптера — нечего сверять
                    continue

                # Предварительно читаем позиции для последующей валидации
                try:
                    positions = await adapter.get_positions()
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.warning("[exchange_guard] get_positions error: %s", e)
                    continue

                external_amt: dict[str, float] = {}
                for p in positions or []:
                    try:
                        sym = str(p.get("symbol", "")).upper()
                        if sym not in self.symbols:
                            continue
                        amt = float(p.get("positionAmt") or 0.0)
                        external_amt[sym] = amt
                    except Exception:
                        continue

                mismatches: list[str] = []

                # --- Синхронизируем каждый символ через _sync_symbol_from_exchange ---
                for sym in self.symbols:
                    sym_u = sym.upper()
                    ext_amt = float(external_amt.get(sym_u, 0.0))

                    # Запоминаем локальное состояние ДО синхронизации для логирования
                    local_before = self._pos.get(sym_u)
                    local_amt_before = 0.0
                    if local_before and local_before.state != "FLAT" and local_before.qty > 0:
                        local_amt_before = (
                            local_before.qty if local_before.side == "BUY" else -local_before.qty
                        )

                    # --- Вызываем _sync_symbol_from_exchange как единственную точку входа ---
                    try:
                        await self._sync_symbol_from_exchange(sym_u)
                    except asyncio.CancelledError:
                        raise
                    except Exception as e:
                        logger.warning(
                            "[exchange_guard] _sync_symbol_from_exchange failed for %s: %s", sym_u, e
                        )
                        # Если синхронизация упала — считаем это mismatch
                        mismatches.append(f"{sym_u}: sync_failed ({e})")
                        continue

                    # Проверяем локальное состояние ПОСЛЕ синхронизации
                    local_after = self._pos.get(sym_u)
                    local_amt_after = 0.0
                    if local_after and local_after.state != "FLAT" and local_after.qty > 0:
                        local_amt_after = (
                            local_after.qty if local_after.side == "BUY" else -local_after.qty
                        )

                    # Если после синхронизации локальное состояние совпадает с биржей — ок
                    if abs(ext_amt - local_amt_after) <= 1e-8:
                        # Логируем только если было изменение
                        if abs(local_amt_before - local_amt_after) > 1e-8:
                            logger.info(
                                "[exchange_guard] synced %s: ext=%.6f, local_before=%.6f, local_after=%.6f",
                                sym_u,
                                ext_amt,
                                local_amt_before,
                                local_amt_after,
                            )
                        continue

                    # Если после синхронизации всё ещё mismatch — это ошибка
                    logger.warning(
                        "[exchange_guard] mismatch after sync for %s: ext=%.6f, local_after=%.6f",
                        sym_u,
                        ext_amt,
                        local_amt_after,
                    )
                    mismatches.append(f"{sym_u}: ext={ext_amt}, local={local_amt_after}")

                if mismatches:
                    msg = ", ".join(mismatches)
                    
                    # Increment desync counter
                    self._desync_count += 1
                    logger.warning(
                        "[exchange_guard] desync detected (count=%d): %s",
                        self._desync_count, msg
                    )
                    
                    # If 3 consecutive desyncs, disable auto-trading
                    if self._desync_count >= 3:
                        if self._auto_enabled:
                            logger.error(
                                "[exchange_guard] persistent desync detected (%d consecutive iterations), disabling auto-trading: %s",
                                self._desync_count, msg
                            )
                            self._auto_enabled = False
                        else:
                            # Already disabled, just log the persistent desync
                            logger.error(
                                "[exchange_guard] persistent desync continues (%d consecutive iterations): %s",
                                self._desync_count, msg
                            )

                    try:
                        self._block_reasons["GLOBAL:exchange_mismatch"] = (
                            self._block_reasons.get("GLOBAL:exchange_mismatch", 0) + 1
                        )
                    except Exception:
                        pass
                else:
                    # No mismatches, reset desync counter
                    if self._desync_count > 0:
                        logger.info("[exchange_guard] desync cleared, resetting counter from %d to 0", self._desync_count)
                    self._desync_count = 0
        except asyncio.CancelledError:
            logger.info("[exchange_guard] consistency loop cancelled, exiting")
            raise
        except Exception as e:
            logger.exception("[exchange_guard] consistency loop error: %s", e)


    # ---------- strategy loop (авто-сигналы) ----------

    async def _strategy_loop(self) -> None:
        """
        Боевой авто-скальпер (Bollinger reversion MVP):

        - Работает по времени: одно «длинное» окно для расчёта Bollinger.
        - Включается только когда _auto_enabled == True.
        - Учитывает:
            * cooldown между входами по символу
            * min_flat_ms после FLAT
            * глобальный cooldown после SL (из risk_cfg)
            * day_limits из diag()
            * максимум 1 активная позиция суммарно
        - Простые сигналы:
            * reversion по Bollinger: LONG от нижней полосы к центру,
            SHORT от верхней полосы к центру.
        - Safety:
            * спред / ликвидность через risk.filters (если настроены)
            * лимиты по дню (day_limits)
        - Все отказы пишутся в _block_reasons через self._inc_block_reason(reason).
        """
        import asyncio
        import logging
        import math
        import time as _t
        from typing import Dict, Any, List

        log = logging.getLogger(__name__ + "._strategy")
        logger.info("[WORKER] strategy loop started")
        
        # Get strategy mode from settings
        strategy_mode = "reversion_only"  # default
        try:
            if get_settings:
                s = get_settings()
                strat_cfg = getattr(s, "strategy", None)
                if strat_cfg:
                    strategy_mode = str(getattr(strat_cfg, "mode", strategy_mode))
        except Exception:
            pass
        
        log.info("[strategy] _strategy_loop started: symbols=%s, mode=%s", list(self.symbols), strategy_mode)

        # --- cfg стратегии из settings (если есть) ---
        cfg: Dict[str, Any] = {}
        try:
            if hasattr(self, "_load_strategy_cfg"):
                cfg = self._load_strategy_cfg() or {}
        except Exception as e:
            log.warning("[strategy] _load_strategy_cfg failed: %s", e)
            cfg = {}
        self._strategy_cfg = dict(cfg or {})
        
        # Load StrategyCfg for router
        router_cfg = None
        try:
            if get_settings:
                s = get_settings()
                strat_cfg = getattr(s, "strategy", None)
                if strat_cfg:
                    router_cfg = StrategyCfg.from_pydantic(strat_cfg)
        except Exception as e:
            log.warning("[strategy] failed to load StrategyCfg for router: %s", e)
            router_cfg = StrategyCfg()  # use defaults

        symbols = list(self.symbols)
        if not symbols:
            log.warning("[WORKER] strategy loop: no symbols configured, exit.")
            return
        logger.info(f"[WORKER] strategy loop: monitoring symbols={symbols}")

        # --- тайминги ---
        cooldown_ms = int(cfg.get("cooldown_ms", getattr(self, "_auto_cooldown_ms", 2000)))
        cooldown_ms = max(300, cooldown_ms)

        min_flat_ms = int(cfg.get("min_flat_ms", getattr(self, "_auto_min_flat_ms", 750)))
        min_flat_ms = max(0, min_flat_ms)

        warmup_ms = int(cfg.get("warmup_ms", 15_000))
        stale_ms  = int(cfg.get("stale_ms", 3_000))

        # окно для Bollinger (по умолчанию 7s как «long_ms» раньше)
        bb_window_ms = int(cfg.get("bb_window_ms", cfg.get("long_ms", 7_000)))
        bb_window_ms = max(3_000, bb_window_ms)

        # --- пороги Bollinger ---
        bb_k          = float(cfg.get("bb_k", 2.0))          # ширина полос в сигмах
        bb_entry_z    = float(cfg.get("bb_entry_z", 1.5))    # сколько сигм от центра для входа
        bb_stop_k     = float(cfg.get("bb_stop_k", 1.0))     # стоп в сигмах за полосой
        min_sigma_rel = float(cfg.get("min_sigma_rel", 0.0001))  # минимальная относительная вола

        min_range_bps   = float(cfg.get("min_range_bps", 0.3))
        min_range_ticks = int(cfg.get("min_range_ticks", 1))
        bb_min_points   = int(cfg.get("bb_min_points", 32))

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
                            spec = self.specs.get(sym)
                            if spec:
                                tick_sz = float(getattr(spec, "price_tick", 0.01))
                            else:
                                tick_sz = 0.01
                        except Exception:
                            tick_sz = 0.01
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

        # Track last heartbeat log time for periodic logging
        last_heartbeat_log_ms = 0
        heartbeat_log_interval_ms = 5000  # 5 seconds
        
        try:
            while True:
                try:
                    now = _now_ms()
                    self._strategy_heartbeat_ms = now
                    auto_enabled = bool(getattr(self, "_auto_enabled", False))

                    # Periodic strategy heartbeat log (every 5 seconds) - log even when disabled
                    if now - last_heartbeat_log_ms >= heartbeat_log_interval_ms:
                        last_heartbeat_log_ms = now
                        # Collect last price for each symbol
                        heartbeat_info = []
                        for sym in symbols:
                            try:
                                bid, ask = _load_best(sym, {}, now)
                                if bid > 0 and ask > 0:
                                    price = (bid + ask) / 2.0
                                    heartbeat_info.append(f"{sym}: price={price:.6f}")
                            except Exception:
                                pass
                        if heartbeat_info:
                            logger.info(f"[WORKER] strategy heartbeat: auto_enabled={auto_enabled}, symbols: {', '.join(heartbeat_info)}")
                        else:
                            logger.info(f"[WORKER] strategy heartbeat: auto_enabled={auto_enabled}, no price data yet")

                    if not auto_enabled:
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

                    positions    = d.get("positions") or {}
                    best_all     = d.get("best") or {}
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
                            log.debug(f"[WORKER] evaluating strategy for symbol={sym}")
                            pos = positions.get(sym) or {}
                            st = str(pos.get("state", "")).upper()
                            if st in ("OPEN", "ENTERING", "EXITING"):
                                # по этому символу уже что-то есть
                                log.debug(f"[WORKER] symbol={sym} already has position state={st}, skipping")
                                continue

                            # если где-то уже есть открытая/входящая/выходящая позиция —
                            # новые входы по другим символам не даём
                            if open_cnt >= 1:
                                _inc_block(f"{sym}:global_pos_limit")
                                continue

                            # пер-символьный cooldown
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

                            # Get current position side for router
                            current_pos_side = None
                            try:
                                pos_state = get_position_state(sym)
                                if not pos_state.is_flat():
                                    current_pos_side = pos_state.to_str_side()
                            except Exception:
                                pass

                            # If using bollinger_basic mode, use pick_candidate from router
                            if router_cfg and router_cfg.mode == "bollinger_basic":
                                try:
                                    micro = self._last_micro.get(sym, {}) or {}
                                    indi = self._last_indi.get(sym, {}) or {}
                                    
                                    # Safety and limits checks
                                    safety_ok = True
                                    limits_ok = bool(day_limits.get("can_trade", True))
                                    
                                    # Check safety filters
                                    if HAS_FILTERS and getattr(self, "_safety_cfg", None) is not None:
                                        try:
                                            lm = micro or {}
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
                                            # For bollinger_basic, we don't have entry_px/sl_px yet, so pass None
                                            dec = check_entry_safety(
                                                "BUY",  # dummy side for pre-check
                                                micro=micro_ctx,
                                                time_ctx=tctx,
                                                pos_ctx=None,
                                                safety=self._safety_cfg,
                                            )
                                            safety_ok = dec.allow
                                        except Exception:
                                            safety_ok = True  # allow if check fails
                                    
                                    # Call pick_candidate
                                    candidate = pick_candidate(
                                        symbol=sym,
                                        micro=micro,
                                        indi=indi,
                                        safety_ok=safety_ok,
                                        limits_ok=limits_ok,
                                        cfg=router_cfg,
                                        current_position_side=current_pos_side,
                                    )
                                    
                                    if candidate:
                                        # Execute Bollinger entry
                                        logger.info(
                                            "[BOLL] candidate symbol=%s, side=%s, tp_pct=%.4f, sl_pct=%.4f, reason=%s",
                                            sym, candidate.side, 
                                            router_cfg.bollinger_basic.tp_pct if router_cfg.bollinger_basic else 0.004,
                                            router_cfg.bollinger_basic.sl_pct if router_cfg.bollinger_basic else 0.0025,
                                            getattr(candidate, "reason_tags", [])
                                        )
                                        rep = await self.execute_bollinger_entry(candidate)
                                        if isinstance(rep, dict) and rep.get("ok", False):
                                            last_entry_ts[sym] = now
                                            logger.info("[strategy] bollinger_basic entry executed successfully for %s", sym)
                                        else:
                                            reason = (rep or {}).get("reason", "unknown")
                                            _inc_block(f"{sym}:bollinger_entry_failed:{reason}")
                                    else:
                                        _inc_block(f"{sym}:no_bollinger_candidate")
                                    
                                    continue  # Skip hardcoded logic when using bollinger_basic
                                except Exception as e:
                                    log.exception("[strategy] pick_candidate failed for %s: %s", sym, e)
                                    _inc_block(f"{sym}:pick_candidate_error")
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
                                            px = (float(b_) + float(a_) ) / 2.0
                                    vol = getattr(t, "volume", getattr(t, "qty", getattr(t, "size", 0.0)))

                                if ts > 0 and px is not None:
                                    ts_list.append(ts)
                                    prices.append(float(px))
                                    try:
                                        vols.append(float(vol) if vol is not None else 0.0)
                                    except Exception:
                                        vols.append(0.0)

                            if len(prices) < bb_min_points:
                                _inc_block(f"{sym}:skip_warmup")
                                continue
                            if ts_list and now - ts_list[-1] > stale_ms:
                                _inc_block(f"{sym}:stale_tick")
                                continue

                            # --- окно для Bollinger ---
                            def slice_since(window_ms: int) -> List[float]:
                                if not ts_list:
                                    return []
                                lo = ts_list[-1] - window_ms
                                return [p for p, ts in zip(prices, ts_list) if ts >= lo]

                            arr_win = slice_since(bb_window_ms)
                            if len(arr_win) < bb_min_points:
                                _inc_block(f"{sym}:skip_warmup_bb")
                                continue

                            local_high = max(arr_win)
                            local_low  = min(arr_win)
                            range_px   = max(0.0, local_high - local_low)

                            min_range_px_ticks = step * float(min_range_ticks)
                            min_range_px_bps   = mid * (min_range_bps / 10_000.0)

                            if not (range_px >= min_range_px_ticks or range_px >= min_range_px_bps):
                                _inc_block(f"{sym}:chop_range")
                                continue

                            # --- расчёт Bollinger: mid, sigma, полосы ---
                            mean_px = sum(arr_win) / len(arr_win)
                            var = sum((p - mean_px) ** 2 for p in arr_win) / max(1, len(arr_win))
                            sigma = math.sqrt(var)

                            if mean_px <= 0.0 or not math.isfinite(sigma):
                                _inc_block(f"{sym}:bad_bb_stats")
                                continue

                            # минимальная волатильность
                            if sigma / mean_px < min_sigma_rel:
                                _inc_block(f"{sym}:sigma_too_small")
                                continue

                            bb_mid = mean_px
                            bb_hi  = bb_mid + bb_k * sigma
                            bb_lo  = bb_mid - bb_k * sigma

                            last_px = prices[-1]
                            z = (last_px - bb_mid) / (sigma if sigma > 0.0 else 1e-9)

                            side = None
                            signal_kind = "bb_reversion"

                            # --- сигналы: от нижней полосы к центру (LONG), от верхней к центру (SHORT) ---
                            if z <= -bb_entry_z:
                                side = "BUY"
                            elif z >= bb_entry_z:
                                side = "SELL"

                            if side is None:
                                _inc_block(f"{sym}:no_signal_bb")
                                continue

                            # --- стоп: за полосой на bb_stop_k * sigma ---
                            if side == "BUY":
                                sl_ref = bb_lo - bb_stop_k * sigma
                                sl_distance_px = max(step * float(min_range_ticks), last_px - sl_ref, spread * 2.0)
                            else:  # SELL
                                sl_ref = bb_hi + bb_stop_k * sigma
                                sl_distance_px = max(step * float(min_range_ticks), sl_ref - last_px, spread * 2.0)

                            if sl_distance_px <= 0.0 or not math.isfinite(sl_distance_px):
                                _inc_block(f"{sym}:bad_sl_distance")
                                continue

                            # --- простой sanity-чек RR (опционально) ---
                            try:
                                # TP условно около bb_mid
                                if side == "BUY":
                                    tp_px = bb_mid
                                    rr = (tp_px - last_px) / sl_distance_px if sl_distance_px > 0 else 0.0
                                else:
                                    tp_px = bb_mid
                                    rr = (last_px - tp_px) / sl_distance_px if sl_distance_px > 0 else 0.0
                            except Exception:
                                rr = 1.0

                            min_rr = float(cfg.get("min_rr", 1.1))
                            if rr < min_rr:
                                _inc_block(f"{sym}:rr_too_low")
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
                                    sl_px_eff = last_px - sl_distance_px if side == "BUY" else last_px + sl_distance_px
                                    pos_ctx = PositionalCtx(
                                        entry_px=float(last_px),
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
                                    last_px,
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
            log.exception("[WORKER] strategy loop fatal error: %s", e)
            self._last_strategy_error = f"{type(e).__name__}: {e}"
            await asyncio.sleep(1.0)
    
    def _update_unified_position_state(self, symbol: str) -> None:
        """
        Update unified PositionState from Worker's _pos dict.
        
        This keeps the unified PositionState in sync with Worker._pos[symbol].
        Called whenever Worker._pos[symbol] is updated.
        """
        try:
            pos = self._pos.get(symbol)
            if pos is None:
                # No position, set to FLAT
                save_position_state(flat_state(symbol))
                return
            
            state_str = getattr(pos, "state", "FLAT")
            side_raw = getattr(pos, "side", None)
            qty = getattr(pos, "qty", 0.0)
            entry_px = getattr(pos, "entry_px", 0.0)
            
            # Convert Worker PositionState to unified PositionState
            if state_str == "FLAT" or qty <= 1e-9:
                new_state = flat_state(symbol)
            elif side_raw == "BUY" or side_raw == Side.BUY:
                new_state = with_long(symbol, entry_px, qty)
            elif side_raw == "SELL" or side_raw == Side.SELL:
                new_state = with_short(symbol, entry_px, qty)
            else:
                new_state = flat_state(symbol)
            
            save_position_state(new_state)
        except Exception as e:
            logger.debug("[worker] _update_unified_position_state failed for %s: %s", symbol, e)
    
    def get_current_position_side(self, symbol: str) -> Optional[Literal["LONG", "SHORT"]]:
        """
        Get current position side for a symbol.
        
        This is the unified way to get the current position side for use by strategies/routers.
        
        Args:
            symbol: Trading symbol
        
        Returns:
            "LONG", "SHORT", or None (for FLAT)
        """
        try:
            pos_state = get_position_state(symbol)
            return pos_state.to_str_side()
        except Exception as e:
            logger.debug("[worker] get_current_position_side failed for %s: %s", symbol, e)
            return None
    
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
        Unified entry (LIVE + PAPER).

        Что делает:
        - нормализует side/qty;
        - проверяет best bid/ask и min notional;
        - при необходимости строит SL/TP-план через self._build_sltp_plan();
        - вызывает Executor.place_entry() (лимит + маркет догон);
        - при наличии live-адаптера перепроверяет факт входа по Binance
          через /positionRisk и корректирует filled/price;
        - навешивает реальные SL/TP-брекеты через Executor.place_bracket();
        - обновляет runtime-позицию self._pos[symbol] и FSM.

        Возвращает dict:
        {
            "ok": bool,
            "reason": str | None,
            "symbol": str,
            "side": "BUY"/"SELL",
            "qty": float,          # фактически исполненный объём
            "entry_px": float|None,
            "sl_px": float|None,
            "tp_px": float|None,
            "status": str,         # ExecutionReport.status
            "steps": list[str],
        }
        """
        import logging, contextlib as _ctx, time
        from typing import Tuple, Optional, Dict, Any

        log = logging.getLogger(__name__ + ".place_entry")

        sym = str(symbol).upper()
        s = str(side).upper()
        if s not in ("BUY", "SELL"):
            return {"ok": False, "reason": "bad_side", "side": side}

        # --- sanity qty ---
        try:
            qty = float(qty)
        except Exception:
            return {"ok": False, "reason": "bad_qty", "qty": qty}
        if qty <= 0.0:
            return {"ok": False, "reason": "qty_le_zero", "qty": qty}

        # CRITICAL FIX: Global lock to prevent concurrent position opening
        # This ensures only ONE position can be opened at a time across ALL symbols
        async with self._global_position_lock:
            # Check if ANY position is already open BEFORE attempting entry
            any_open = False
            for check_sym in self.symbols:
                check_pos = self._pos.get(check_sym)
                if check_pos and check_pos.state in ("OPEN", "ENTERING", "EXITING"):
                    any_open = True
                    break
            
            if any_open:
                logger.warning(
                    "[place_entry] BLOCKED: Another position is already open. Cannot open %s %s",
                    sym, s
                )
                self._inc_block_reason(f"{sym}:global_position_lock_active")
                return {"ok": False, "reason": "global_position_lock_active", "symbol": sym, "side": s, "qty": qty}
            
            # --- account state / daily limits (ТОЛЬКО из Binance данных) ---
            try:
                # по умолчанию — локальные счётчики (используются в paper / fallback)
                num_trades_today = int(self._pnl_day.get("trades", 0) or 0)
                pnl_r_day = float(self._pnl_day.get("pnl_r", 0.0) or 0.0)

                if hasattr(self, "_account_state") and self._account_state:
                    # если есть сервис аккаунта — опираемся на него
                    snapshot = self._account_state.get_snapshot()

                    # если снапшота нет или он старый – обновляем
                    if snapshot is None or not self._account_state.is_fresh(max_age_s=10):
                        await self._account_state.refresh()
                        snapshot = self._account_state.get_snapshot()

                        # Reconciliation: проверяем соответствие PnL памяти и БД раз в минуту
                        from storage.repo import now_ms, day_utc_from_ms
                        current_ts_ms = now_ms()
                        if current_ts_ms - self._last_reconcile_ts_ms >= 60_000:
                            self._last_reconcile_ts_ms = current_ts_ms
                            try:
                                day_str = day_utc_from_ms(current_ts_ms)
                                self._account_state.reconcile_realized_pnl_with_db(day_str)
                            except Exception as e:
                                logger.warning("[worker] PnL reconciliation failed: %s", e)

                    # если после refresh всё равно нет данных или много ошибок — просто не входим
                    if snapshot is None or self._account_state.consecutive_errors >= 3:
                        self._inc_block_reason("account_state_stale")
                        return {
                            "ok": False,
                            "reason": "account_state_stale",
                            "symbol": sym,
                            "side": s,
                            "qty": qty,
                        }

                    # Block new entries if daily realized PnL is below the max_daily_loss_usd threshold (daily stop)
                    from risk.filters import check_daily_loss_usd
                    realized_pnl_today = float(snapshot.realized_pnl_today or 0.0)
                    daily_loss_dec = check_daily_loss_usd(realized_pnl_today, self._risk_cfg)
                    if not daily_loss_dec.allow:
                        reasons = [f"daily_loss_usd_{r}" for r in daily_loss_dec.reasons]
                        for r in reasons:
                            self._inc_block_reason(r)
                        return {
                            "ok": False,
                            "reason": ",".join(reasons),
                            "symbol": sym,
                            "side": s,
                            "qty": qty,
                            "realized_pnl_today": realized_pnl_today,
                        }

                    # 1) количество сделок за день — из AccountState (userTrades+DB)
                    num_trades_today = int(snapshot.num_trades_today)

                    # 2) дневный PnL в R: используем realized_pnl_today (USD) и текущий equity
                    risk_pct = float(getattr(self._risk_cfg, "risk_per_trade_pct", 0.0) or 0.0)
                    equity_ref = float(snapshot.equity or 0.0)
                    if equity_ref <= 0.0:
                        equity_ref = float(getattr(self, "_starting_equity_usd", 0.0) or 0.0)

                    if risk_pct > 0.0 and equity_ref > 0.0:
                        r_usd = equity_ref * (risk_pct / 100.0)
                        pnl_r_day = float(snapshot.realized_pnl_today) / r_usd
                    else:
                        pnl_r_day = 0.0

                # Проверяем дневные лимиты в R
                day_state = DayState(
                    pnl_r_day=float(pnl_r_day),
                    consec_losses=int(getattr(self, "_consec_losses", 0) or 0),
                    trading_disabled=False,
                )
                day_dec = check_day_limits(day_state, self._risk_cfg)
                if not day_dec.allow:
                    reasons = [f"day_{r}" for r in day_dec.reasons]
                    for r in reasons:
                        self._inc_block_reason(r)
                    return {
                        "ok": False,
                        "reason": ",".join(reasons),
                        "symbol": sym,
                        "side": s,
                        "qty": qty,
                    }

                # Лимит на количество сделок в день — тоже на основе реальных данных
                max_trades_per_day = int(getattr(self._risk_cfg, "max_trades_per_day", 0) or 0)
                if max_trades_per_day > 0 and num_trades_today >= max_trades_per_day:
                    self._inc_block_reason("day_max_trades_reached")
                    return {
                        "ok": False,
                        "reason": "day_max_trades_reached",
                        "symbol": sym,
                        "side": s,
                        "qty": qty,
                        "num_trades_today": num_trades_today,
                        "max_trades": max_trades_per_day,
                    }
            except Exception as e:
                logger.warning("[place_entry] Error checking daily limits: %s", e)
                # на ошибке лимитов не входим, но и не падаем
                # Don't block on error, but log it
            
            # --- executor ---
            ex = self.execs.get(sym)
            if ex is None:
                return {"ok": False, "reason": "no_executor", "symbol": sym}

            # --- best bid/ask и last для приблизительной цены входа ---
            try:
                bid, ask = self.best_bid_ask(sym)
                bid = float(bid or 0.0)
                ask = float(ask or 0.0)
            except Exception:
                bid = ask = 0.0

            last = self.latest_tick(sym) or {}
            try:
                last_px = float(last.get("price") or 0.0)
                last_mark = float(last.get("mark_price") or 0.0)
            except Exception:
                last_px = last_mark = 0.0

            mid = 0.0
            if bid > 0.0 and ask > 0.0:
                mid = (bid + ask) / 2.0
            elif last_px > 0.0:
                mid = last_px
            elif last_mark > 0.0:
                mid = last_mark

            if mid <= 0.0:
                # вообще нет цены — лучше безопасно не входить
                self._inc_block_reason(f"{sym}:no_best")
                return {"ok": False, "reason": "no_best"}

            # --- проверка minimal notional (в USD) ---
            min_notional = float(getattr(self, "_min_notional_usd", 5.0))
            notional = abs(qty * mid)
            if notional < min_notional:
                return {
                    "ok": False,
                    "reason": "below_min_notional",
                    "notional": float(notional),
                    "min_notional": float(min_notional),
                }

            # --- если SL/TP не заданы явно — строим план через _build_sltp_plan() ---
            entry_px_plan = mid
            if (sl_px is None or tp_px is None) and hasattr(self, "_build_sltp_plan"):
                try:
                    plan = self._build_sltp_plan(
                        symbol=sym,
                        side=s,
                        entry_px=entry_px_plan,
                        qty=float(qty),
                        micro=self._last_micro.get(sym),
                        indi=self._last_indi.get(sym),
                        decision=None,
                    )
                    if sl_px is None:
                        sl_px = float(plan.sl_px)
                    if tp_px is None:
                        tp_px = float(plan.tp_px)
                except Exception as e:
                    log.exception("[place_entry] _build_sltp_plan failed for %s %s: %s", sym, s, e)
                    return {"ok": False, "reason": "sltp_plan_failed", "error": str(e)}

            # --- helper: snapshot позиции на бирже (если мы в live-режиме) ---
            async def _exchange_snapshot() -> Tuple[float, Optional[float]]:
                """
                Возвращает (positionAmt, entryPrice) для sym по Binance.
                Если адаптер не live — (0.0, None).
                """
                adapter = self._get_live_adapter()
                if adapter is None:
                    return 0.0, None
                try:
                    raw_positions = await adapter.get_positions()
                except Exception as e:
                    logger.warning("[place_entry] get_positions failed for %s: %s", sym, e)
                    return 0.0, None

                try:
                    for p in raw_positions or []:
                        psym = str(p.get("symbol", "")).upper()
                        if psym != sym:
                            continue
                        amt = float(p.get("positionAmt") or 0.0)
                        entry_price = float(p.get("entryPrice") or 0.0)
                        return amt, (entry_price if entry_price > 0.0 else None)
                except Exception as e:
                    logger.warning("[place_entry] error parsing positionRisk for %s: %s", sym, e)
                return 0.0, None

            # состояние позиции до входа (для live-режима)
            ext_amt_before, _ = await _exchange_snapshot()

            # --- КРИТИЧЕСКИ ВАЖНО: создаём trade_id ДО вызова executor ---
            # это гарантирует, что все ордера (entry, SL, TP) будут привязаны к одному trade
            trade_id: Optional[str] = None
            prev = self._pos.get(sym)
            trade_id_existing = getattr(prev, "trade_id", None)
            
            # Если позиции нет или у неё нет trade_id, создаём новый trade
            if not trade_id_existing:
                try:
                    from storage.repo import TradeOpenCtx, open_trade  # lazy import

                    side_pos = "LONG" if s == "BUY" else "SHORT"
                    # Используем планируемую цену входа для создания записи
                    entry_px_for_trade = entry_px_plan or mid
                    ctx = TradeOpenCtx(
                        symbol=sym,
                        side=side_pos,
                        qty=qty,
                        entry_px=entry_px_for_trade,
                        sl_px=float(sl_px) if sl_px is not None else None,
                        tp_px=float(tp_px) if tp_px is not None else None,
                        reason_open="auto_entry",
                        meta={
                            "source": "worker.place_entry",
                            "ts_ms": int(time.time() * 1000),
                        },
                    )
                    trade_id = open_trade(ctx)
                    logger.info(
                        "[place_entry] Created new trade_id=%s for %s %s",
                        trade_id, sym, s
                    )
                except Exception as e:
                    logger.warning(
                        "[place_entry] open_trade failed for %s %s: %s",
                        sym,
                        s,
                        e,
                    )
                    # Если не получилось создать trade_id, продолжаем без него
                    trade_id = None
            else:
                trade_id = trade_id_existing

            # --- Safety margin check: ensure required margin for this trade fits comfortably within available margin (with safety factor) ---
            if hasattr(self, "_account_state") and self._account_state:
                try:
                    snapshot = self._account_state.get_snapshot()
                    if snapshot is not None:
                        # Get leverage for this symbol
                        try:
                            if hasattr(self, "_leverage_for_symbol"):
                                leverage = float(self._leverage_for_symbol(sym))
                            else:
                                leverage = float(getattr(self, "_leverage", 10.0))
                        except Exception:
                            leverage = 10.0
                        
                        # Estimate initial margin required for the planned position
                        # For linear USDT-M: required_margin ≈ entry_price * qty / leverage
                        entry_px_est = entry_px_plan if entry_px_plan > 0.0 else mid
                        if entry_px_est > 0.0 and leverage > 0.0:
                            required_margin = abs(entry_px_est * qty) / leverage
                            available_margin = float(snapshot.available_margin or 0.0)
                            safety_factor = 0.9
                            
                            # Safety margin check: ensure required margin for this trade fits comfortably within available margin (with safety factor)
                            if required_margin > available_margin * safety_factor:
                                self._inc_block_reason(f"{sym}:insufficient_margin")
                                logger.warning(
                                    "[place_entry] Margin check failed: required=%.2f, available=%.2f, safety_factor=%.2f",
                                    required_margin, available_margin, safety_factor
                                )
                                return {
                                    "ok": False,
                                    "reason": "insufficient_margin",
                                    "symbol": sym,
                                    "side": s,
                                    "qty": qty,
                                    "required_margin": float(required_margin),
                                    "available_margin": float(available_margin),
                                    "safety_factor": safety_factor,
                                }
                except Exception as e:
                    logger.warning("[place_entry] Margin check failed with error: %s", e)
                    # Don't block on margin check error, but log it

            # --- вызываем Executor.place_entry с временными override-ами конфига ---
            with self._temporary_exec_overrides(
                sym,
                limit_offset_ticks=limit_offset_ticks,
            ):
                try:
                    rep = await ex.place_entry(sym, s, qty, reduce_only=False, trade_id=trade_id)
                except Exception as e:
                    logger.exception("[place_entry] executor failed for %s %s: %s", sym, s, e)
                    self._inc_block_reason(f"{sym}:entry_error")
                    return {"ok": False, "reason": "entry_error", "error": str(e)}

            # --- steps для метрик ---
            steps = list(getattr(rep, "steps", []) or [])
            self._entry_steps[sym] = steps
            with _ctx.suppress(Exception):
                self._accumulate_exec_counters(steps)

            # --- разбор ExecutionReport ---
            filled = float(getattr(rep, "filled_qty", 0.0) or 0.0)
            status = str(getattr(rep, "status", "CANCELED") or "CANCELED")
            avg_px = getattr(rep, "avg_px", None)

            limit_oid = getattr(rep, "limit_oid", None)
            market_oid = getattr(rep, "market_oid", None)

            logger.debug(
                "[place_entry] exec report %s %s: status=%s filled=%.6f avg_px=%s limit_oid=%s market_oid=%s steps=%s",
                sym, s, status, filled, avg_px, limit_oid, market_oid, steps,
            )

            # --- дополнительная проверка по Binance: берём правду с биржи, если есть ---
            ext_amt_after, ext_entry_px = await _exchange_snapshot()
            delta_ext = abs(ext_amt_after) - abs(ext_amt_before)

            if abs(ext_amt_after) > 1e-9 and abs(delta_ext) > 1e-9:
                # Биржа говорит, что позиция открыта / увеличена — доверяем ей.
                real_side = "BUY" if ext_amt_after > 0 else "SELL"
                real_qty = abs(ext_amt_after)
                filled = real_qty
                status = "FILLED"
                if ext_entry_px is not None and ext_entry_px > 0.0:
                    avg_px = ext_entry_px
                s = real_side

                logger.info(
                    "[place_entry] corrected from exchange for %s: side=%s qty=%.6f entry_px=%s",
                    sym, s, real_qty, avg_px,
                )

            # если реально ничего не исполнилось — считаем вход отменённым
            if filled <= 0.0 and status == "CANCELED":
                self._inc_block_reason(f"{sym}:entry_canceled_no_fill")
                return {
                    "ok": False,
                    "reason": "entry_canceled_no_fill",
                    "status": status,
                    "steps": steps,
                }

            # фактически исполненный объём и цена входа
            filled_qty = float(filled)
            entry_px_effective = float(avg_px or entry_px_plan or mid)

            # --- CRITICAL: Validate strategy-provided SL/TP prices before proceeding ---
            # After entry fill, immediately place exchange-level SL and TP orders based on strategy levels (reduce_only).
            # These SL/TP orders define the actual exit and are required for enforcing risk-reward.
            if filled_qty > 0.0:
                if sl_px is None or sl_px <= 0.0:
                    logger.error(
                        "[place_entry] CRITICAL: Entry filled for %s but sl_px is missing/invalid (sl_px=%s). "
                        "Cannot place protective orders. Rejecting trade.",
                        sym, sl_px
                    )
                    self._inc_block_reason(f"{sym}:missing_sl_px")
                    return {
                        "ok": False,
                        "reason": "missing_sl_px",
                        "symbol": sym,
                        "side": s,
                        "filled_qty": filled_qty,
                        "status": status,
                    }
                
                if tp_px is None or tp_px <= 0.0:
                    logger.warning(
                        "[place_entry] WARNING: Entry filled for %s but tp_px is missing/invalid (tp_px=%s). "
                        "Proceeding without TP order (SL only).",
                        sym, tp_px
                    )
                    # Allow SL-only, but log warning
                    tp_px = None

            # --- обновляем runtime-позицию и навешиваем SL/TP-брекеты ---
            now_ms = int(time.time() * 1000)
            lock = self._locks[sym]
            async with lock:
                prev = self._pos.get(sym)
                timeout_ms = int(getattr(prev, "timeout_ms", 180_000))

                sl_oid: Optional[str] = None
                tp_oid: Optional[str] = None

                # --- After entry fill, immediately place exchange-level SL and TP orders based on strategy levels (reduce_only) ---
                # These SL/TP orders define the actual exit and are required for enforcing risk-reward.
                if filled_qty > 0.0 and sl_px is not None and sl_px > 0.0:
                    try:
                        # Place SL/TP orders immediately after entry fill using strategy-provided prices.
                        # All SL/TP orders must be reduce_only=True to prevent position size increases.
                        logger.info(
                            "[place_entry] Placing SL/TP orders for %s %s: sl_px=%.8f tp_px=%s qty=%.6f",
                            sym, s, float(sl_px), (f"{tp_px:.8f}" if tp_px else "None"), filled_qty
                        )
                        br = await ex.place_bracket(
                            symbol=sym,
                            side=s,
                            qty=filled_qty,
                            sl_px=float(sl_px),
                            tp_px=float(tp_px) if tp_px is not None and tp_px > 0.0 else None,
                            trade_id=trade_id,
                        )
                        if isinstance(br, dict) and not br.get("ok", True):
                            logger.warning(
                                "[place_entry] place_bracket failed for %s %s: %s",
                                sym,
                                s,
                                br,
                            )
                        sl_oid = br.get("sl_order_id") or None
                        tp_oid = br.get("tp_order_id") or None

                        # best-effort проверка reduce_only и наличия SL/TP, как было у тебя
                        try:
                            if hasattr(ex, "a") or hasattr(ex, "adapter"):
                                adapter = getattr(ex, "a", None) or getattr(ex, "adapter", None)
                                if adapter and hasattr(adapter, "get_open_orders"):
                                    open_orders = await adapter.get_open_orders(symbol=sym)
                                    sl_found = False
                                    tp_found = False
                                    sl_is_reduce_only = False
                                    tp_is_reduce_only = False

                                    for o in open_orders or []:
                                        oid = str(o.get("clientOrderId") or o.get("orderId") or "")
                                        reduce_only = str(o.get("reduceOnly") or "").lower() in ("true", "1")

                                        if sl_oid and oid == str(sl_oid):
                                            sl_found = True
                                            sl_is_reduce_only = reduce_only
                                        if tp_oid and oid == str(tp_oid):
                                            tp_found = True
                                            tp_is_reduce_only = reduce_only

                                    if sl_oid and sl_found and not sl_is_reduce_only:
                                        logger.error(
                                            "[place_entry] CRITICAL: SL order %s for %s is NOT reduce_only.",
                                            sl_oid,
                                            sym,
                                        )
                                    if tp_oid and tp_found and not tp_is_reduce_only:
                                        logger.error(
                                            "[place_entry] CRITICAL: TP order %s for %s is NOT reduce_only.",
                                            tp_oid,
                                            sym,
                                        )
                        except Exception as verify_err:
                            logger.warning("[place_entry] Failed to verify SL/TP orders: %s", verify_err)

                    except Exception as e:
                        logger.warning(
                            "[place_entry] place_bracket error for %s %s: %s",
                            sym,
                            s,
                            e,
                        )

                # --- trade_id уже создан выше (BEFORE executor call) ---
                # Если фактический filled_qty отличается от запланированного qty,
                # обновим запись в БД (это нужно для точности учёта)
                if filled_qty > 0.0 and trade_id and abs(filled_qty - qty) > 1e-6:
                    try:
                        from storage.repo import get_trade  # lazy import
                        # Пробуем обновить qty/entry_px в trade (опционально)
                        # Но это не критично, т.к. close_trade пересчитает PnL по фактическим fills
                        logger.debug(
                            "[place_entry] trade_id=%s: planned qty=%.6f, actual filled=%.6f",
                            trade_id, qty, filled_qty
                        )
                    except Exception as e:
                        logger.debug("[place_entry] Could not log trade update: %s", e)

                # --- обновляем runtime-позицию ---
                self._pos[sym] = PositionState(
                    state="OPEN",
                    side=s,
                    qty=filled_qty,
                    entry_px=entry_px_effective,
                    sl_px=float(sl_px) if sl_px is not None else None,
                    tp_px=float(tp_px) if tp_px is not None else None,
                    opened_ts_ms=now_ms,
                    timeout_ms=timeout_ms,
                    trade_id=trade_id,
                    sl_order_id=sl_oid,
                    tp_order_id=tp_oid,
                )

                # Update unified PositionState
                self._update_unified_position_state(sym)

                # FSM: переводим в OPEN (best-effort)
                with _ctx.suppress(Exception):
                    fsm = self._fsm.get(sym)
                    if fsm is not None:
                        await fsm.on_open()

        return {
            "ok": True,
            "symbol": sym,
            "side": s,
            "qty": filled_qty,
            "entry_px": entry_px_effective,
            "sl_px": float(sl_px) if sl_px is not None else None,
            "tp_px": float(tp_px) if tp_px is not None else None,
            "status": status,
            "steps": steps,
        }

    async def place_entry_auto(self, symbol: str, side: str) -> dict:
        """
        Auto entry with risk-based sizing and fee-aware SL/TP planning.

        Ключевые свойства:
        - Размер позиции считает _compute_auto_qty (risk + комиссии + плечо + liq-buffer).
        - SL/TP планируются через compute_sltp_fee_aware (если доступен) или безопасный fallback.
        - Для входа применяются edge-проверки (RR/edge) до и после планирования.
        - На live не лезем в сделку, если размер не проходит по марже/плечу/liq-buffer.
        """

        import math
        import logging

        logger = logging.getLogger(__name__ + ".place_entry_auto")

        # === 1. Нормализация символа и стороны ===
        sym = str(symbol).upper()
        s_raw = str(side).upper()
        if s_raw in ("LONG", "BUY"):
            s = "BUY"
        elif s_raw in ("SHORT", "SELL"):
            s = "SELL"
        else:
            self._inc_block_reason(f"{sym}:bad_side")
            return {"ok": False, "reason": "bad_side"}

        # === 1.5. Cleanup stray brackets before opening new position ===
        # Это предотвращает конфликты с ghost SL/TP ордерами от предыдущих сделок
        await self._cleanup_brackets_for_symbol(sym)

        # === 2. Снэпшот рынка ===
        try:
            bid, ask = self.best_bid_ask(sym)
        except Exception:
            bid = ask = 0.0

        if bid <= 0.0 or ask <= 0.0:
            self._inc_block_reason(f"{sym}:no_best_bid_ask")
            return {"ok": False, "reason": "no_best_bid_ask"}

        mid = (bid + ask) / 2.0
        if mid <= 0.0:
            self._inc_block_reason(f"{sym}:bad_mid")
            return {"ok": False, "reason": "bad_mid"}

        spread = max(0.0, ask - bid)

        # === 3. Шаг цены / тиковая геометрия ===
        try:
            price_step = float(self._get_price_step(sym) or 0.0)
        except Exception:
            price_step = 0.0
        if price_step <= 0.0:
            price_step = float(getattr(self, "_price_tick_fallback", 0.1) or 0.1)

        exec_cfg = getattr(self, "_exec_cfg", {}) or {}
        min_stop_ticks = int(exec_cfg.get("min_stop_ticks", 12))

        # === 4. Дистанция до SL (вола + тики + спред) ===
        try:
            sl_distance_px = float(
                self._compute_vol_stop_distance_px(
                    sym,
                    mid_px=mid,
                    min_stop_ticks_cfg=min_stop_ticks,
                )
            )
        except Exception:
            sl_distance_px = 0.0

        if sl_distance_px <= 0.0:
            sl_distance_px = max(
                min_stop_ticks * price_step,
                2.0 * spread,
                6.0 * price_step,
            )

        if sl_distance_px <= 0.0:
            self._inc_block_reason(f"{sym}:bad_sl_distance")
            return {"ok": False, "reason": "bad_sl_distance"}

        # === 5. Первичная edge-проверка (грубая RR) ===
        rr_gross = float(self._rr_floor_for_symbol(sym, 1.6))
        ok_edge, edge_info = self._entry_edge_ok(sym, mid, bid, ask, sl_distance_px, rr_gross)
        if not ok_edge:
            self._inc_block_reason(f"{sym}:edge_too_small")
            return {
                "ok": False,
                "reason": "edge_too_small",
                "edge_diag": edge_info,
            }

        # === 6. Risk-based sizing через _compute_auto_qty ===
        qty = float(self._compute_auto_qty(sym, s, sl_distance_px) or 0.0)
        if qty <= 0.0:
            self._inc_block_reason(f"{sym}:qty_rounded_to_zero")
            return {"ok": False, "reason": "qty_rounded_to_zero"}

        # min_notional safety check (на всякий случай, _compute_auto_qty уже учитывает)
        min_notional = float(getattr(self, "_min_notional_usd", 5.0) or 5.0)
        notional = qty * mid
        if notional < min_notional:
            # Посчитаем hint для пользователя
            try:
                # шаг объёма
                spec = (self.specs or {}).get(sym)
                qty_step = float(getattr(spec, "qty_step", 0.001)) if spec else float(
                    getattr(self, "_qty_step_fallback", 0.001) or 0.001
                )
            except Exception:
                qty_step = 0.001
            if qty_step <= 0:
                qty_step = 0.001

            min_qty = math.ceil((min_notional / mid) / qty_step) * qty_step
            self._inc_block_reason(f"{sym}:below_min_notional")
            return {
                "ok": False,
                "reason": "below_min_notional",
                "hint": f"min ${min_notional} → qty ≥ {min_qty}",
            }

        # === 7. SL/TP план (fee-aware) ===
        # maker-first вход, SL как taker, TP maker где возможно
        try:
            maker_bps, taker_bps = self._fees_for_symbol(sym)
        except Exception:
            maker_bps, taker_bps = 2.0, 4.0

        spread_bps = (spread / mid) * 1e4 if mid > 0.0 else 0.0
        addl_exit_bps = 0.5 * max(spread_bps, 0.1)

        sl_px = None
        tp_px = None

        try:
            # compute_sltp_fee_aware должен быть доступен в модуле
            plan = compute_sltp_fee_aware(
                side=s,
                entry_px=float(mid),
                qty=float(qty),
                price_tick=float(price_step),
                sl_distance_px=float(sl_distance_px),
                rr_target=float(rr_gross),
                maker_bps=float(maker_bps),
                taker_bps=float(taker_bps),
                entry_taker_like=False,          # в идеале maker-вход
                exit_taker_like=True,            # SL будет taker
                addl_exit_bps=float(addl_exit_bps),
                min_stop_ticks=int(min_stop_ticks),
                spread_px=float(spread),
                spread_mult=1.5,
                min_sl_bps=float(getattr(self, "_min_sl_bps", 12.0) or 12.0),
                min_net_rr=float(getattr(self, "_min_net_rr", 1.25) or 1.25),
                allow_expand_tp=True,
            )

            sl_px = float(plan.sl_px)
            tp_px = float(
                getattr(plan, "tp_px_maker", getattr(plan, "tp_px", 0.0)) or 0.0
            )
        except Exception:
            # безопасный fallback (geometric RR)
            if s == "BUY":
                sl_px = mid - sl_distance_px
                tp_px = mid + sl_distance_px * rr_gross
            else:
                sl_px = mid + sl_distance_px
                tp_px = mid - sl_distance_px * rr_gross

        if sl_px is None or float(sl_px) <= 0.0:
            self._inc_block_reason(f"{sym}:plan_bad_risk_px")
            return {"ok": False, "reason": "bad_sl_distance"}

        sl_px = float(sl_px)
        tp_px = float(tp_px or 0.0)

        # === 8. Точный риск по финальному SL ===
        risk_px = abs(sl_px - mid)
        if risk_px <= 0.0:
            self._inc_block_reason(f"{sym}:plan_bad_risk_px")
            return {"ok": False, "reason": "bad_sl_distance"}

        expected_risk_usd = round(risk_px * qty, 6)

        # === 9. Финальная edge-проверка по обновлённому рынку ===
        try:
            bid2, ask2 = self.best_bid_ask(sym)
            if bid2 > 0.0 and ask2 > 0.0:
                mid2 = (bid2 + ask2) / 2.0
            else:
                mid2 = mid
        except Exception:
            bid2 = ask2 = 0.0
            mid2 = mid

        sl_dist_final = abs(mid2 - sl_px)
        ok_edge2, edge_info2 = self._entry_edge_ok(sym, mid2, bid2, ask2, sl_dist_final, rr_gross)
        if not ok_edge2:
            self._inc_block_reason(f"{sym}:edge_insufficient")
            return {
                "ok": False,
                "reason": "edge_insufficient",
                "edge_diag": edge_info2,
                "entry_px": float(mid2),
                "sl_px": float(sl_px),
                "tp_px": float(tp_px),
            }

        # === 10. Liq-buffer: SL не должен быть близко к ликвидации ===
        try:
            lev = float(self._leverage_for_symbol(sym))
            if lev < 1.0:
                lev = 1.0

            lb_mult = self._liq_buffer_mult(float(mid2), float(sl_px), s, lev)
            min_mult_cfg = 3.0
            try:
                min_mult_cfg = float(
                    getattr(self._safety_cfg, "min_liq_buffer_sl_mult", 3.0) or 3.0
                )
            except Exception:
                pass

            if not math.isfinite(lb_mult) or lb_mult < max(1.5, min_mult_cfg):
                self._inc_block_reason(f"{sym}:liq_buffer_too_small")
                return {
                    "ok": False,
                    "reason": "liq_buffer_too_small",
                    "liq_buffer_mult": float(lb_mult),
                }
        except Exception:
            # диагностику liq-buffer считаем best-effort
            pass

        # === 11. Отправка заявки (Executor.place_entry под капотом) ===
        try:
            report = await self.place_entry(
                sym,
                s,
                float(qty),
                sl_px=float(sl_px),
                tp_px=float(tp_px),
            )
        except Exception as e:
            logger.exception("[place_entry_auto] failed for %s %s: %s", sym, s, e)
            self._inc_block_reason(f"{sym}:entry_error")
            return {"ok": False, "reason": "entry_error", "error": str(e)}

        # Executor/adapter может вернуть dict с {ok: False, reason: ...}
        if isinstance(report, dict) and not report.get("ok", True):
            r = str(report.get("reason") or "rejected")
            self._inc_block_reason(f"{sym}:entry_rejected_{r}")
            return {"ok": False, "reason": r, "report": report}

        # === 12. Успешный результат ===
        # target_risk_usd мы можем оценить по expected_risk_usd, так как sizing уже учёл комиссии.
        return {
            "ok": True,
            "symbol": sym,
            "side": s,
            "qty": float(qty),
            "entry_px": float(mid),
            "sl_px": float(sl_px),
            "tp_px": float(tp_px),
            "expected_risk_usd": float(expected_risk_usd),
            "report": report,
        }
    
    async def _handle_sltp_exit(self, symbol: str, position_state: PositionState) -> None:
        """
        Handle SL/TP exit triggered by price monitoring.
        
        Args:
            symbol: Trading symbol
            position_state: Current position state
        """
        sym = str(symbol).upper()
        try:
            if position_state.is_flat() or position_state.size is None or position_state.size <= 0.0:
                return
            
            side_str = position_state.to_str_side()
            if side_str is None:
                return
            
            executor = self.execs.get(sym)
            if executor is None:
                logger.warning("[EXEC] sl/tp-exit %s: no executor", sym)
                return
            
            # Place reduceOnly exit order
            avg_px, filled = await place_exit_order(
                symbol=sym,
                side=side_str,
                size=float(position_state.size),
                executor=executor,
            )
            
            if avg_px is not None and filled > 0.0:
                # Update PositionState to FLAT
                new_state = flat_state(sym)
                save_position_state(new_state)
                
                # Update Worker's internal position state
                self._pos[sym] = PositionState(
                    state="FLAT",
                    side=None,
                    qty=0.0,
                    entry_px=0.0,
                    sl_px=None,
                    tp_px=None,
                    opened_ts_ms=0,
                )
                
                logger.info(
                    "[EXEC] exit-fill symbol=%s, side=%s, avg_price=%.6f, size=%.6f",
                    sym, side_str, avg_px, filled
                )
                logger.info(
                    "[POS] transition symbol=%s, from=%s to=FLAT, exit_price=%.6f, size=%.6f",
                    sym, side_str, avg_px, filled
                )
            else:
                logger.warning("[EXEC] sl/tp-exit %s: exit failed", sym)
        
        except Exception as e:
            logger.exception("[EXEC] sl/tp-exit %s failed: %s", sym, e)
    
    async def execute_bollinger_entry(
        self,
        candidate,
        *,
        entry_usd: Optional[float] = None,
        leverage: Optional[float] = None,
    ) -> Dict[str, Any]:
        """
        Execute EntryCandidate using Bollinger execution helpers.
        
        This method:
        1. Computes size from entry_usd and leverage
        2. Places entry order (limit → timeout → market)
        3. Updates PositionState
        4. Computes and attaches SL/TP levels
        
        Args:
            candidate: EntryCandidate from pick_candidate
            entry_usd: Entry risk in USD (if None, reads from config)
            leverage: Leverage multiplier (if None, reads from config)
        
        Returns:
            Dict with execution results
        """
        from strategy.router import EntryCandidate
        
        if not isinstance(candidate, EntryCandidate):
            return {"ok": False, "reason": "invalid_candidate"}
        
        sym = str(candidate.symbol).upper()
        side_str = str(candidate.side).upper()
        
        # Convert side to LONG/SHORT
        if side_str == "BUY":
            side = "LONG"
        elif side_str == "SELL":
            side = "SHORT"
        else:
            return {"ok": False, "reason": "bad_side"}
        
        # Get config values (with defaults)
        try:
            settings = get_settings()
            bb_cfg = getattr(settings.strategy, "bollinger_basic", None)
            if entry_usd is None:
                entry_usd = float(getattr(bb_cfg, "entry_usd", 25.0)) if bb_cfg else 25.0
            if leverage is None:
                leverage = float(getattr(bb_cfg, "leverage", 10.0)) if bb_cfg else 10.0
        except Exception:
            entry_usd = entry_usd or 25.0
            leverage = leverage or 10.0
        
        # Get executor
        executor = self.execs.get(sym)
        if executor is None:
            return {"ok": False, "reason": "no_executor"}
        
        # Get current price
        try:
            bid, ask = self.best_bid_ask(sym)
            entry_price = (bid + ask) / 2.0 if bid > 0 and ask > 0 else bid or ask or 0.0
        except Exception:
            return {"ok": False, "reason": "no_price"}
        
        if entry_price <= 0.0:
            return {"ok": False, "reason": "invalid_price"}
        
        # Get qty_step from spec
        spec = self.specs.get(sym)
        qty_step = float(getattr(spec, "qty_step", 0.001)) if spec else 0.001
        
        # Place entry order
        logger.info(
            "[BOLL] candidate symbol=%s, side=%s, entry_usd=%.2f, leverage=%.1f, entry_price=%.6f",
            sym, side, entry_usd, leverage, entry_price
        )
        
        cfg = {
            "leverage": float(leverage),
            "qty_step": qty_step,
        }
        
        logger.info("[EXEC] bollinger entry-request symbol=%s, side=%s, entry_usd=%.2f", sym, side, entry_usd)
        avg_px, filled = await place_entry_order(
            symbol=sym,
            side=side,
            nominal_size_usd=float(entry_usd),
            price=float(entry_price),
            executor=executor,
            cfg=cfg,
        )
        
        if avg_px is None or filled <= 0.0:
            logger.warning("[EXEC] bollinger entry-failed symbol=%s, side=%s, filled=%.6f", sym, side, filled)
            return {"ok": False, "reason": "entry_failed", "filled": filled}
        
        # Compute SL/TP from candidate or percentages
        if candidate.tp_price is not None and candidate.sl_price is not None:
            tp_price = float(candidate.tp_price)
            sl_price = float(candidate.sl_price)
        else:
            # Fallback: use percentages from strategy config
            try:
                settings = get_settings()
                bb_cfg = getattr(settings.strategy, "bollinger_basic", None)
                tp_pct = float(getattr(bb_cfg, "tp_pct", 0.004)) if bb_cfg else 0.004
                sl_pct = float(getattr(bb_cfg, "sl_pct", 0.0025)) if bb_cfg else 0.0025
            except Exception:
                tp_pct = 0.004
                sl_pct = 0.0025
            
            tp_price, sl_price = compute_tp_sl_levels(avg_px, side, tp_pct, sl_pct)
        
        # Update PositionState
        if side == "LONG":
            pos_state = with_long(sym, avg_px, filled)
        else:
            pos_state = with_short(sym, avg_px, filled)
        
        # Attach SL/TP
        pos_state = attach_sl_tp(sym, pos_state, tp_price, sl_price)
        save_position_state(pos_state)
        
        # Update Worker's internal position state
        self._pos[sym] = PositionState(
            state="OPEN",
            side=Side.BUY if side == "LONG" else Side.SELL,
            qty=filled,
            entry_px=avg_px,
            sl_px=sl_price,
            tp_px=tp_price,
            opened_ts_ms=self._now_ms(),
        )
        
        logger.info(
            "[EXEC] bollinger-entry %s %s: filled=%.6f entry=%.6f sl=%.6f tp=%.6f",
            sym, side, filled, avg_px, sl_price, tp_price
        )
        logger.info(
            "[POS] transition symbol=%s, from=FLAT to=%s, entry_price=%.6f, size=%.6f",
            sym, side, avg_px, filled
        )
        logger.info(
            "[EXEC] sl/tp set symbol=%s, side=%s, tp=%.6f, sl=%.6f",
            sym, side, tp_price, sl_price
        )
        
        return {
            "ok": True,
            "symbol": sym,
            "side": side,
            "filled": filled,
            "entry_price": avg_px,
            "sl_price": sl_price,
            "tp_price": tp_price,
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
    
    async def _enter_with_bracket(
        self,
        sym: str,
        side: Side,
        qty: float,
        entry_px: float,
        sl_px: Optional[float],
        tp_px: Optional[float],
    ) -> tuple[ExecutionReport, Optional[str], Optional[str]]:
        """
        Унифицированный вход:
        1) всегда сначала делаем фактический вход через Executor.place_entry();
        2) затем, если доступен Executor.place_bracket(), пытаемся выставить
           reduce-only SL/TP ордера на бирже;
        3) возвращаем ExecutionReport по входу + clientOrderId защитных ордеров.
        """
        ex = self.execs.get(sym)
        if ex is None:
            raise RuntimeError(f"no executor for {sym}")

        # --- 1. Фактический вход (лимит + догон маркетом внутри Executor) ---
        rep: ExecutionReport = await ex.place_entry(sym, side, float(qty))

        sl_oid: Optional[str] = None
        tp_oid: Optional[str] = None

        # --- 2. Брекеты reduce-only, если есть API у Executor ---
        place_bracket = getattr(ex, "place_bracket", None)
        if callable(place_bracket) and (sl_px or tp_px):
            try:
                br = await place_bracket(
                    sym,
                    side,
                    float(qty),
                    sl_px=float(sl_px) if sl_px else None,
                    tp_px=float(tp_px) if tp_px else None,
                )
                # Executor.place_bracket возвращает dict
                if isinstance(br, dict) and br.get("ok", False):
                    sl_oid = br.get("sl_order_id")
                    tp_oid = br.get("tp_order_id")
                else:
                    logger.warning(
                        "[worker._enter_with_bracket] bracket not ok for %s %s: %s",
                        sym,
                        side,
                        br,
                    )
            except Exception as e:
                logger.warning(
                    "[worker._enter_with_bracket] place_bracket failed for %s %s: %s",
                    sym,
                    side,
                    e,
                )

        # --- 3. Возвращаем отчёт по входу и id защитных ордеров ---
        return rep, sl_oid, tp_oid
    
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

                        # CRITICAL FIX #2: Watchdog to detect and restore missing SL/TP orders
                        # Check if position has SL/TP prices but missing order IDs
                        if pos.sl_px is not None and not pos.sl_order_id:
                            logger.error(
                                "[watchdog] CRITICAL: %s has SL price (%.2f) but NO SL order ID! Restoring...",
                                sym, pos.sl_px
                            )
                            try:
                                ex = self.execs.get(sym)
                                if ex and hasattr(ex, "place_bracket"):
                                    # Try to restore SL order
                                    br = await ex.place_bracket(
                                        symbol=sym,
                                        side=pos.side or "BUY",
                                        qty=pos.qty,
                                        sl_px=pos.sl_px,
                                        tp_px=pos.tp_px,
                                    )
                                    if isinstance(br, dict) and br.get("ok"):
                                        sl_oid = br.get("sl_order_id")
                                        tp_oid = br.get("tp_order_id")
                                        if sl_oid:
                                            pos.sl_order_id = sl_oid
                                            logger.warning("[watchdog] Restored SL order ID for %s: %s", sym, sl_oid)
                                        if tp_oid and not pos.tp_order_id:
                                            pos.tp_order_id = tp_oid
                                            logger.warning("[watchdog] Restored TP order ID for %s: %s", sym, tp_oid)
                            except Exception as restore_err:
                                logger.error("[watchdog] Failed to restore SL/TP for %s: %s", sym, restore_err)
                        
                        if pos.tp_px is not None and not pos.tp_order_id:
                            logger.warning(
                                "[watchdog] %s has TP price (%.2f) but NO TP order ID -> clearing stale TP",
                                sym,
                                float(pos.tp_px),
                            )
                            # Чистим неконсистентное состояние, чтобы не спамить и не мешать логике
                            pos.tp_px = None
                            pos.tp_order_id = None
                            # дальше не продолжаем для этой ветки
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

                                    ex = self.execs.get(sym)
                                    place_reduce = getattr(ex, "place_reduce_only_limit", None) if ex is not None else None
                                    if callable(place_reduce):
                                        try:
                                            px = float(tp_px)
                                            close_side = "SELL" if side_now == "BUY" else "BUY"
                                            await place_reduce(sym, close_side, float(qty_now), price=px)
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
            logger.info("[watchdog] loop cancelled, exiting")
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
        (чтобы не ловить -30R из-за хвоста на paper).
        - Для flatten / timeout / no_protection и прочих причин берём
        консервативную рыночную цену через _exit_price_conservative.
        - Executor.place_exit вызывается, когда доступен, чтобы:
            * на live реально отдать reduce_only-ордер на выход;
            * собрать steps/exec-метрики.
        - Если у позиции есть sl_order_id / tp_order_id, best-effort отменяем
        защитные ордера через Executor (cancel_bracket / cancel_order), если они есть.
        - Локально переводим позицию в FLAT только когда:
            * позиция изначально некорректна (qty<=0 или нет side/entry),
            * либо трейд зарегистрирован (exit_committed=True).
        На live, если place_exit провалился — позицию в локальном стейте НЕ закрываем.
        """
        import contextlib
        import asyncio
        import logging

        logger = logging.getLogger(__name__)

        side = pos.side
        qty = float(pos.qty or 0.0)
        entry_px = float(pos.entry_px or 0.0)
        sl_px = float(pos.sl_px) if pos.sl_px is not None else None
        tp_px = float(pos.tp_px) if pos.tp_px is not None else None
        timeout_ms = int(pos.timeout_ms or 180_000)

        # --- 0. Если по факту позиции нет — просто приводим к FLAT и выходим ---
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
                sl_order_id=None,
                tp_order_id=None,
            )
            # Update unified PositionState
            self._update_unified_position_state(sym)
            self._entry_steps.pop(sym, None)
            self._last_flat_ms[sym] = self._now_ms()
            with contextlib.suppress(Exception):
                await self._fsm[sym].on_flat()
            return

        # --- 1. Базовые параметры выхода ---
        side = side.upper()
        close_side: Side = "SELL" if side == "BUY" else "BUY"

        # Целевой уровень для SL/TP (только для sl_hit / tp_hit) — для paper/оценки PnL
        level_px: Optional[float] = None
        if reason == "sl_hit" and sl_px is not None:
            level_px = float(sl_px)
        elif reason == "tp_hit" and tp_px is not None:
            level_px = float(tp_px)

        exit_px: Optional[float] = None
        exit_steps: List[str] = []
        exit_committed = False
        exec_failed = False
        is_live = self._get_live_adapter() is not None

        try:
            # --- 2. FSM: переводим символ в состояние EXITING (best-effort) ---
            with contextlib.suppress(Exception):
                await self._fsm[sym].on_exiting()

            ex = self.execs.get(sym)

            # --- 3. Best-effort отмена защитных ордеров SL/TP на бирже ---
            sl_oid = getattr(pos, "sl_order_id", None)
            tp_oid = getattr(pos, "tp_order_id", None)
            if ex is not None and (sl_oid or tp_oid):
                cancel_bracket = getattr(ex, "cancel_bracket", None)
                cancel_order = getattr(ex, "cancel_order", None)

                if callable(cancel_bracket):
                    # Retry logic: up to 2 attempts with exponential backoff
                    max_attempts = 2
                    cancel_success = False
                    for attempt in range(1, max_attempts + 1):
                        try:
                            result = await cancel_bracket(
                                sym,
                                sl_order_id=sl_oid,
                                tp_order_id=tp_oid,
                            )
                            # Check if result is ok (assuming it has an 'ok' attribute or is truthy)
                            if result and (getattr(result, "ok", False) or result):
                                logger.info(
                                    "[close_position] cancel_bracket succeeded for %s: sl_order_id=%s, tp_order_id=%s, result=%s",
                                    sym, sl_oid, tp_oid, result
                                )
                                cancel_success = True
                                break
                            else:
                                logger.warning(
                                    "[close_position] cancel_bracket returned not-ok for %s (attempt %d/%d): sl_order_id=%s, tp_order_id=%s, result=%s",
                                    sym, attempt, max_attempts, sl_oid, tp_oid, result
                                )
                        except Exception as e:
                            logger.warning(
                                "[close_position] cancel_bracket failed for %s (attempt %d/%d): sl_order_id=%s, tp_order_id=%s, error=%s",
                                sym, attempt, max_attempts, sl_oid, tp_oid, e
                            )
                        
                        # Exponential backoff before next attempt
                        if attempt < max_attempts:
                            await asyncio.sleep(0.5 * attempt)
                    
                    if not cancel_success:
                        logger.error(
                            "[close_position] cancel_bracket failed after %d attempts for %s: sl_order_id=%s, tp_order_id=%s",
                            max_attempts, sym, sl_oid, tp_oid
                        )
                elif callable(cancel_order):
                    for oid in (sl_oid, tp_oid):
                        if oid:
                            with contextlib.suppress(Exception):
                                await cancel_order(sym, oid)

            # --- 4. Пытаемся формально вызвать Executor.place_exit() ---
            # Извлекаем trade_id из позиции для привязки exit-ордеров
            trade_id = getattr(pos, "trade_id", None)
            actual_exit_px: Optional[float] = None
            if ex is not None:
                try:
                    rep: ExecutionReport = await ex.place_exit(sym, close_side, qty, trade_id=trade_id)
                    steps = list(getattr(rep, "steps", []) or [])
                    if steps:
                        exit_steps = steps
                        self._accumulate_exec_counters(steps)
                    
                    # Use actual average fill price from Binance if available
                    if rep.avg_px is not None and rep.avg_px > 0.0:
                        actual_exit_px = float(rep.avg_px)
                        logger.info(
                            "[close_position] Using actual exit price from Binance for %s: %.8f (filled_qty=%.6f)",
                            sym, actual_exit_px, rep.filled_qty
                        )
                    
                    # Wait a bit for fills to be saved to database (especially for live trading)
                    if is_live and rep.status in ("FILLED", "PARTIAL") and rep.filled_qty > 0:
                        await asyncio.sleep(0.2)  # Give time for fills to be saved
                        
                except Exception as e:
                    exec_failed = True
                    exit_steps = ["exec_exit_error"]
                    logger.warning(
                        "[close_position] place_exit failed for %s: %s",
                        sym,
                        e,
                    )

            # --- 5. Определяем финальную цену выхода для учёта PnL ---
            if actual_exit_px is not None:
                # Priority 1: Use actual fill price from Binance (most accurate)
                exit_px = actual_exit_px
                logger.info("[close_position] Using actual Binance fill price: %.8f", exit_px)
            elif level_px is not None:
                # Priority 2: For sl_hit/tp_hit use planned level
                exit_px = float(level_px)
                logger.info("[close_position] Using planned level price: %.8f", exit_px)
            else:
                # Priority 3: Fallback to conservative market price
                exit_px = self._exit_price_conservative(sym, close_side, entry_px)
                logger.info("[close_position] Using conservative market price: %.8f", exit_px)

            # Если на live не удалось отправить exit-ордер — не считаем позицию закрытой локально.
            # Пусть верхний уровень/оператор разрулит ситуацию.
            # Если на live не удалось отправить exit-ордер — не считаем позицию закрытой локально.
            if exec_failed and is_live:
                return

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

                # --- синхронизация с БД: закрываем trade и подтягиваем real PnL ---
                trade_id = getattr(pos, "trade_id", None)
                if trade_id:
                    try:
                        from storage.repo import close_trade as repo_close_trade, get_trade as repo_get_trade  # lazy import

                        # закрываем сделку в базе: pnl_usd в БД будет пересчитан с учётом fees
                        try:
                            repo_close_trade(
                                trade_id=trade_id,
                                exit_px=float(exit_px),
                                r=float(trade.get("pnl_r", 0.0) or 0.0),
                                pnl_usd=float(trade.get("pnl_usd", 0.0) or 0.0),
                                reason_close=reason,
                            )
                            
                            # Small delay to ensure fills are saved and daily_stats is updated
                            await asyncio.sleep(0.1)
                            
                        except Exception as e:
                            logging.getLogger(__name__).warning(
                                "[close_position] repo_close_trade failed for %s (%s): %s",
                                trade_id,
                                sym,
                                e,
                            )

                        # пробуем прочитать сделку обратно и заменить pnl_usd на real (net) с retry
                        db_trade = None
                        for retry in range(3):
                            try:
                                db_trade = repo_get_trade(trade_id)
                                if db_trade is not None:
                                    break
                            except Exception as e:
                                if retry < 2:
                                    await asyncio.sleep(0.1)  # Wait a bit before retry
                                else:
                                    logging.getLogger(__name__).warning(
                                        "[close_position] repo_get_trade failed for %s (%s) after retries: %s",
                                        trade_id,
                                        sym,
                                        e,
                                    )

                        if db_trade is not None:
                            try:
                                # Use actual values from database (calculated from real fills)
                                trade["pnl_usd"] = float(db_trade.get("pnl_usd", trade.get("pnl_usd", 0.0)))
                                trade["fees_usd"] = float(db_trade.get("fees_usd", 0.0))
                                trade["exit_px"] = float(db_trade.get("exit_px", exit_px))
                                trade["pnl_r"] = float(db_trade.get("pnl_r", trade.get("pnl_r", 0.0)))
                                logger.info(
                                    "[close_position] Updated trade with DB values for %s: pnl_usd=%.4f, fees_usd=%.4f, exit_px=%.8f",
                                    sym, trade["pnl_usd"], trade["fees_usd"], trade["exit_px"]
                                )
                            except Exception as e:
                                logger.warning("[close_position] Failed to update trade with DB values: %s", e)
                            trade["trade_id"] = trade_id
                            trade["source"] = "db"
                        else:
                            trade["trade_id"] = trade_id
                            trade.setdefault("source", "worker")
                    except Exception as e:
                        logging.getLogger(__name__).warning(
                            "[close_position] DB sync block failed for %s (%s): %s",
                            trade_id,
                            sym,
                            e,
                        )
                        trade.setdefault("source", "worker")
                        if trade_id:
                            trade["trade_id"] = trade_id
                else:
                    # WARNING: trade_id is None — this trade won't be recorded in DB!
                    logging.getLogger(__name__).warning(
                        "[close_position] trade_id is None for %s - trade will NOT be saved to DB. "
                        "Position state: side=%s, qty=%.6f, entry_px=%.2f, exit_px=%.2f, pnl_usd=%.4f, reason=%s. "
                        "This means the position was opened without DB tracking.",
                        sym,
                        pos.side if pos else None,
                        pos.qty if pos else 0.0,
                        pos.entry_px if pos else 0.0,
                        float(exit_px) if exit_px else 0.0,
                        float(trade.get("pnl_usd", 0.0) or 0.0),
                        reason,
                    )
                    # нет trade_id — работаем как раньше (paper / старый режим)
                    trade.setdefault("source", "worker")
                    trade["trade_id"] = None

                self._register_trade(trade)
                # Фоновая пересборка истории (если есть хранилище).
                asyncio.create_task(self._rebuild_trades_safe())
                exit_committed = True
                
                # Immediately sync position from exchange to ensure frontend shows correct state
                if is_live:
                    try:
                        # Trigger immediate sync to get actual position state from Binance
                        asyncio.create_task(self._sync_symbol_from_exchange(sym))
                        logger.info("[close_position] Triggered immediate position sync for %s", sym)
                    except Exception as e:
                        logger.warning("[close_position] Failed to trigger position sync for %s: %s", sym, e)
            else:
                logger.warning(
                    "[close_position] cannot compute valid exit price for %s "
                    "(reason=%s, level_px=%r), keeping position state as-is",
                    sym,
                    reason,
                    level_px,
                )

        finally:
            # --- 7. Приводим локальный стейт к FLAT, если сделка учтена или позиция битая ---
            # На live при провале place_exit и без exit_committed мы НЕ трогаем позицию.
            if exit_committed or qty <= 0.0 or not side or entry_px <= 0.0:
                self._pos[sym] = PositionState(
                    state="FLAT",
                    side=None,
                    qty=0.0,
                    entry_px=0.0,
                    sl_px=None,
                    tp_px=None,
                    opened_ts_ms=0,
                    timeout_ms=timeout_ms,
                    sl_order_id=None,
                    tp_order_id=None,
                )
                # Update unified PositionState
                self._update_unified_position_state(sym)
                self._entry_steps.pop(sym, None)
                self._last_flat_ms[sym] = self._now_ms()
                with contextlib.suppress(Exception):
                    await self._fsm[sym].on_flat()
            else:
                # сюда попадём в основном на live, если exit не был закоммичен
                logger.warning(
                    "[close_position] exit not committed for %s, keeping position OPEN (reason=%s)",
                    sym,
                    reason,
                )

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
        """
        Собирает локальный "черновой" рекорд сделки для UI/diag.

        ВАЖНО:
        - Это ОЦЕНКА на основе локальных данных (SL/TP, fee_bps и т.д.).
        Реальная net-PnL с учётом комиссий биржи считается в БД (trades + fills).
        - Ключи "pnl_usd"/"fees"/"risk_usd" сохраняем, чтобы не ломать фронт.
        """

        def _f(v: Any, default: float = 0.0) -> float:
            try:
                if v is None:
                    return float(default)
                return float(v)
            except Exception:
                return float(default)

        side = (pos.side or "BUY").upper()
        # Нормализуем на BUY/SELL, чтобы не ловить "LONG"/"SHORT" и прочий мусор
        if side not in ("BUY", "SELL"):
            side = "BUY"

        qty = _f(pos.qty, 0.0)
        entry = _f(pos.entry_px, 0.0)
        sl_px = _f(pos.sl_px, 0.0) if pos.sl_px is not None else None
        tp_px = _f(pos.tp_px, 0.0) if pos.tp_px is not None else None
        exit_px = _f(exit_px, 0.0)

        now = self._now_ms()

        # Если позиция "битая" — возвращаем нулевой PnL, чтобы не сломать агрегации
        if qty <= 0.0 or entry <= 0.0 or exit_px <= 0.0:
            return {
                "symbol": sym,
                "side": side,
                "opened_ts": int(getattr(pos, "opened_ts_ms", 0) or 0),
                "closed_ts": now,
                "qty": float(qty),
                "entry_px": float(entry),
                "exit_px": float(exit_px),
                "sl_px": sl_px,
                "tp_px": tp_px,
                "pnl_usd": 0.0,
                "pnl_r": 0.0,
                "risk_usd": 0.0,
                "reason": f"{reason}|invalid_pos",
                "fees": 0.0,
                "fees_usd": 0.0,
                # дополнительные флажки для дебага, не ломают фронт
                "pnl_source": "local_estimate",
                "fees_estimated": True,
            }

        # --- Gross PnL (без комиссий) и номинальный риск по SL ---
        if side == "BUY":
            pnl_usd_gross = (exit_px - entry) * qty
            # для лонга SL должен быть ниже entry; иначе используем entry как ref
            sl_ref = sl_px if (sl_px is not None and sl_px < entry) else entry
            risk_usd_nominal = abs(entry - sl_ref) * qty
        else:  # SELL (шорт)
            pnl_usd_gross = (entry - exit_px) * qty
            # для шорта SL должен быть выше entry; иначе используем entry как ref
            sl_ref = sl_px if (sl_px is not None and sl_px > entry) else entry
            risk_usd_nominal = abs(sl_ref - entry) * qty

        # --- Комиссии: попытка получить из БД или fallback на оценку через fee_bps ---
        fees_usd = 0.0
        fees_estimated = True
        
        # Пытаемся достать реальные комиссии из БД по trade_id
        trade_id = getattr(pos, "trade_id", None)
        if trade_id:
            try:
                from sqlalchemy import text
                from storage.db import get_engine
                
                eng = get_engine()
                with eng.begin() as conn:
                    row = conn.execute(
                        text(
                            """
                            SELECT COALESCE(SUM(f.fee_usd), 0.0) AS fee_sum
                              FROM fills f
                              JOIN orders o ON o.id = f.order_id
                             WHERE o.trade_id = :tid
                            """
                        ),
                        {"tid": trade_id},
                    ).fetchone()
                    
                    if row is not None:
                        if hasattr(row, '_mapping'):
                            fees_usd = float(row._mapping.get("fee_sum", 0.0) or 0.0)
                        else:
                            fees_usd = float(row[0] or 0.0)
                        fees_estimated = False
            except Exception as e:
                # Логируем, но не роняем приложение
                import logging
                logging.getLogger(__name__).warning(
                    f"[_build_trade_record] Failed to query fees for trade_id={trade_id}: {e}. "
                    f"Falling back to estimate."
                )
        
        # Fallback: если не удалось получить из БД, используем оценку
        if fees_estimated:
            entry_bps = self._fee_bps_for_steps(entry_steps or [])
            exit_bps = self._fee_bps_for_steps(exit_steps or [])
            # fee = notional * (bps / 10_000)
            entry_fee_usd = (entry * qty * entry_bps) / 10_000.0
            exit_fee_usd = (exit_px * qty * exit_bps) / 10_000.0
            fees_usd = entry_fee_usd + exit_fee_usd

        # --- Net PnL (оценка) ---
        pnl_usd = pnl_usd_gross - fees_usd

        # --- Реальный риск сделки: SL-дистанция + комиссии ---
        gross_risk_usd = max(risk_usd_nominal, 0.0)
        risk_usd_total = gross_risk_usd + max(fees_usd, 0.0)

        # --- Конфиг риска (для порогов, но не для искажения R) ---
        try:
            # ВАЖНО: здесь используем in-memory pnl_day, а не AccountState,
            # чтобы не тянуть лишние зависимости и не ломать текущую логику.
            equity = float(
                self._starting_equity_usd
                + float(self._pnl_day.get("pnl_usd", 0.0) or 0.0)
            )
        except Exception:
            equity = float(self._starting_equity_usd)

        risk_pct = float(getattr(self._risk_cfg, "risk_per_trade_pct", 0.0) or 0.0)
        risk_target = equity * (risk_pct / 100.0) if risk_pct > 0.0 else 0.0

        cfg_floor = float(getattr(self._risk_cfg, "min_risk_usd_floor", 0.0) or 0.0)
        if cfg_floor <= 0.0:
            cfg_floor = 1.0

        # ВАЖНО: risk_usd = реальный риск сделки (SL + fees), floor только чтобы
        # не делить на 0 при вычислении R, а не чтобы "подтягивать" риск к целевому.
        risk_usd = max(risk_usd_total, cfg_floor, 1e-9)
        pnl_r = pnl_usd / risk_usd

        return {
            "symbol": sym,
            "side": side,
            "opened_ts": int(getattr(pos, "opened_ts_ms", 0) or 0),
            "closed_ts": now,
            "qty": float(qty),
            "entry_px": float(entry),
            "exit_px": float(exit_px),
            "sl_px": sl_px,
            "tp_px": tp_px,
            "pnl_usd": round(float(pnl_usd), 6),
            "pnl_r": round(float(pnl_r), 6),
            "risk_usd": round(float(risk_usd), 6),
            "reason": str(reason),
            "fees": round(float(fees_usd), 6),
            "fees_usd": round(float(fees_usd), 6),  # expose fees_usd for API
            # новые поля для прозрачности (не ломают фронт)
            "pnl_source": "db" if not fees_estimated else "local_estimate",
            "fees_estimated": fees_estimated,
        }

    def _register_trade(self, trade: Dict[str, Any]) -> None:
        """
        Регистрирует закрытую сделку в in-memory очереди и обновляет дневную статистику.

        ВАЖНО:
        - Работает только с тем, что пришло в trade (локальная оценка).
        AccountState / БД могут потом показать "истинный" PnL — мы это уже
        подхватываем в diag() через snapshot.
        - Здесь мы обновляем:
            _trades[sym], _pnl_day, _pnl_r_equity, _consec_losses, _last_sl_ts_ms.
        """
        from collections import deque

        sym = trade.get("symbol") or ""
        if not sym:
            # без символа не знаем, куда класть — просто выходим, чтобы не ломать структуру
            return

        dq = self._trades.get(sym)
        if dq is None:
            dq = self._trades[sym] = deque(maxlen=1000)
        dq.appendleft(trade)

        now = self._now_ms()

        # --- смена дня ---
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
                # внутренние служебные поля
                "winrate_raw": 0.0,
                "peak_r": 0.0,
                "trough_r": 0.0,
            }
            self._pnl_r_equity = 0.0
            self._consec_losses = 0

        # --- извлекаем метрики сделки ---
        try:
            pnl_usd = float(trade.get("pnl_usd", 0.0) or 0.0)
        except Exception:
            pnl_usd = 0.0

        try:
            pnl_r = float(trade.get("pnl_r", 0.0) or 0.0)
        except Exception:
            pnl_r = 0.0

        try:
            risk_usd = float(trade.get("risk_usd", 0.0) or 0.0)
        except Exception:
            risk_usd = 0.0

        reason = str(trade.get("reason", "") or "").lower()

        # --- агрегаты по дню (in-memory) ---
        self._pnl_day["trades"] = int(self._pnl_day.get("trades", 0) or 0) + 1
        self._pnl_day["pnl_usd"] = float(self._pnl_day.get("pnl_usd", 0.0) or 0.0) + pnl_usd
        self._pnl_day["pnl_r"] = float(self._pnl_day.get("pnl_r", 0.0) or 0.0) + pnl_r

        # winrate_raw = wins / trades
        prev_trades = max(int(self._pnl_day["trades"]) - 1, 0)
        prev_winrate_raw = float(self._pnl_day.get("winrate_raw", 0.0) or 0.0)
        wins_prev = int(round(prev_winrate_raw * prev_trades))

        if pnl_r > 0:
            wins_now = wins_prev + 1
        else:
            wins_now = wins_prev

        trades_now = int(self._pnl_day["trades"])
        winrate_raw = wins_now / max(trades_now, 1)

        self._pnl_day["winrate_raw"] = winrate_raw
        self._pnl_day["winrate"] = round(winrate_raw, 4)
        self._pnl_day["avg_r"] = round(
            float(self._pnl_day["pnl_r"]) / max(trades_now, 1),
            6,
        )

        # --- DD по R (equity в R-пространстве) ---
        self._pnl_r_equity = float(self._pnl_r_equity) + pnl_r

        peak = float(self._pnl_day.get("peak_r", 0.0) or 0.0)
        trough = float(self._pnl_day.get("trough_r", 0.0) or 0.0)

        if self._pnl_r_equity > peak:
            peak = self._pnl_r_equity
        if self._pnl_r_equity < trough:
            trough = self._pnl_r_equity

        self._pnl_day["peak_r"] = peak
        self._pnl_day["trough_r"] = trough
        # max_dd_r — это (peak - trough) в абсолюте
        self._pnl_day["max_dd_r"] = round(max(peak - trough, 0.0), 6)

        # --- логика последовательных стопов ---
        # реальный стоп: SL-триггер, риск не микроскопический, R <= -0.8

        cfg_floor = float(getattr(self._risk_cfg, "min_risk_usd_floor", 1.0) or 1.0)
        try:
            risk_pct = float(getattr(self._risk_cfg, "risk_per_trade_pct", 0.0) or 0.0)
            equity = float(
                self._starting_equity_usd
                + float(self._pnl_day.get("pnl_usd", 0.0) or 0.0)
            )
            risk_target = equity * (risk_pct / 100.0) if risk_pct > 0.0 else cfg_floor
        except Exception:
            risk_target = cfg_floor

        is_sl_reason = ("sl_hit" in reason) or (" sl" in reason) or ("stop" in reason)
        is_meaningful_risk = risk_usd >= 0.5 * risk_target
        is_real_stop = is_sl_reason and is_meaningful_risk and (pnl_r <= -0.8)

        if pnl_r > 0:
            # любая положительная сделка полностью сбрасывает серию
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
        + Дополнительно:
            account_state — компактный снапшот AccountState (equity, margin, PnL, позиции).
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
                        if isinstance(last, dict):
                            latest_ts = int(
                                (last.get("ts_ms") or last.get("ts") or 0)
                            )
                        else:
                            # если это датакласс/объект — пробуем через __dict__
                            last_dict = getattr(last, "__dict__", {}) or {}
                            latest_ts = int(
                                (last_dict.get("ts_ms") or last_dict.get("ts") or 0)
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
                best[sym] = {
                    "bid": float(bid or 0.0),
                    "ask": float(ask or 0.0),
                }
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

        # --- account_state (compact snapshot из AccountState) ---
        try:
            acc_state = getattr(self, "_account_state", None)
            acc_out: Dict[str, Any] = {}
            if acc_state is not None:
                snap = None
                try:
                    snap = acc_state.get_snapshot()
                except Exception:
                    snap = None

                if snap is not None:
                    # базовые поля аккаунта
                    try:
                        equity = float(getattr(snap, "equity", 0.0) or 0.0)
                    except Exception:
                        equity = 0.0
                    try:
                        avail = float(getattr(snap, "available_margin", 0.0) or 0.0)
                    except Exception:
                        avail = 0.0
                    try:
                        realized_today = float(getattr(snap, "realized_pnl_today", 0.0) or 0.0)
                    except Exception:
                        realized_today = 0.0
                    try:
                        n_trades = int(getattr(snap, "num_trades_today", 0) or 0)
                    except Exception:
                        n_trades = 0
                    try:
                        age_ms = int(getattr(acc_state, "snapshot_age_ms", lambda: 0)() or 0)
                    except Exception:
                        age_ms = 0

                    # открытые позиции с точки зрения биржи
                    open_pos_out: Dict[str, Any] = {}
                    try:
                        snap_positions = getattr(snap, "open_positions", {}) or {}
                        for psym, ps in snap_positions.items():
                            try:
                                oqty = float(getattr(ps, "qty", 0.0) or 0.0)
                                oentry = float(getattr(ps, "entry_px", 0.0) or 0.0)
                                oupnl = float(getattr(ps, "unrealized_pnl", 0.0) or 0.0)
                                lev = float(getattr(ps, "leverage", 0.0) or 0.0)
                                margin = float(getattr(ps, "position_margin", 0.0) or 0.0)
                            except Exception:
                                continue
                            if abs(oqty) <= 0.0 and oupnl == 0.0:
                                continue
                            open_pos_out[psym] = {
                                "qty": oqty,
                                "entry_px": oentry,
                                "unrealized_pnl": oupnl,
                                "leverage": lev,
                                "margin": margin,
                            }
                    except Exception:
                        open_pos_out = {}

                    acc_out = {
                        "equity": equity,
                        "available_margin": avail,
                        "realized_pnl_today": realized_today,
                        "num_trades_today": n_trades,
                        "snapshot_age_ms": age_ms,
                        "open_positions": open_pos_out,
                    }
            out["account_state"] = acc_out
        except Exception:
            out["account_state"] = {}

        # --- unrealized PnL (best-effort, с использованием AccountState, если доступен) ---
        unrealized: Dict[str, Any] = {
            "total_usd": 0.0,
            "per_symbol": {},
            "open_positions": 0,
            "source": "none",
        }
        try:
            # 1) Пробуем взять uPnL напрямую из AccountState (Binance positionRisk)
            acc_state = getattr(self, "_account_state", None)
            snap = None
            if acc_state is not None:
                try:
                    snap = acc_state.get_snapshot()
                except Exception:
                    snap = None

            used_account_state = False
            if snap is not None:
                try:
                    is_fresh = acc_state.is_fresh(max_age_s=15)  # type: ignore[union-attr]
                except Exception:
                    is_fresh = True

                if is_fresh and getattr(snap, "open_positions", None):
                    per_sym: Dict[str, float] = {}
                    total = 0.0
                    open_cnt = 0

                    for sym, ps in (snap.open_positions or {}).items():  # type: ignore[union-attr]
                        try:
                            upnl = float(getattr(ps, "unrealized_pnl", 0.0) or 0.0)
                            qty = float(getattr(ps, "qty", 0.0) or 0.0)
                        except Exception:
                            continue

                        per_sym[sym] = upnl
                        total += upnl
                        if abs(qty) > 0.0:
                            open_cnt += 1

                    unrealized["total_usd"] = float(total)
                    unrealized["per_symbol"] = per_sym if per_sym else None
                    unrealized["open_positions"] = int(open_cnt)
                    unrealized["source"] = "account_state"
                    used_account_state = True

            # 2) Fallback: если AccountState нет/старый — считаем по локальным позициям + best bid/ask
            if not used_account_state:
                per_sym = {}
                total = 0.0
                open_pos = 0

                positions_loc = out.get("positions", {}) or {}
                best_loc = out.get("best", {}) or {}

                for sym, p in (positions_loc or {}).items():
                    p = p or {}
                    if str(p.get("state", "")).upper() != "OPEN":
                        continue
                    open_pos += 1
                    qty = float(p.get("qty") or 0.0)
                    entry = float(p.get("entry_px") or 0.0)
                    if qty <= 0.0 or entry <= 0.0:
                        continue

                    b = best_loc.get(sym, {}).get("bid", 0.0)
                    a = best_loc.get(sym, {}).get("ask", 0.0)
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
                unrealized["source"] = "local"
        except Exception:
            # Диагностика никогда не должна падать
            pass
        out["unrealized"] = unrealized

        # --- trades (in-memory) ---
        try:
            out["trades"] = {
                sym: list(dq)
                for sym, dq in (getattr(self, "_trades", {}) or {}).items()
            }
        except Exception:
            out["trades"] = {}

        # --- trades_db (реальные сделки из SQLite) ---
        try:
            try:
                from storage.repo import load_recent_trades  # lazy import
            except Exception:
                load_recent_trades = None  # type: ignore[assignment]

            trades_db = []
            if load_recent_trades is not None:
                try:
                    trades_db = load_recent_trades(limit=100) or []
                except Exception:
                    trades_db = []

            out["trades_db"] = trades_db
        except Exception:
            out["trades_db"] = []

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

        # --- pnl_day (align with AccountState when possible) ---
        try:
            # базово — то, что накапливает сам воркер
            pnl_day = dict(getattr(self, "_pnl_day", {}) or {})

            acc_state = getattr(self, "_account_state", None)
            snapshot = None
            is_fresh = False

            if acc_state is not None:
                try:
                    snapshot = acc_state.get_snapshot()
                    # для дневной статистики достаточно, чтобы снапшот был не старее пары минут
                    is_fresh = acc_state.is_fresh(max_age_s=120)
                except Exception:
                    snapshot = None
                    is_fresh = False

            # Если есть живой снапшот аккаунта — переопределяем деньги/кол-во сделок
            if snapshot is not None and is_fresh:
                try:
                    trades_today = getattr(snapshot, "num_trades_today", None)
                    if trades_today is not None:
                        pnl_day["trades"] = int(trades_today or 0)
                except Exception:
                    pass

                try:
                    realized_today = getattr(snapshot, "realized_pnl_today", None)
                    if realized_today is not None:
                        pnl_day["pnl_usd"] = float(realized_today or 0.0)
                except Exception:
                    pass

            out["pnl_day"] = pnl_day
        except Exception:
            out["pnl_day"] = {}

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
                out["trading_mode"] = str(getattr(acc, "mode", "live"))
                out["exchange"] = str(getattr(acc, "exchange", "binance_usdtm"))
        except Exception:
            # дефолты, если конфиг не читается
            out.setdefault("trading_mode", "live")
            out.setdefault("exchange", "binance_usdtm")

        return out

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
        TEMP: single-symbol mode GALAUSDT, 10x leverage (configured in settings.json per_symbol_leverage).
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

    def _price_touch_check(
        self,
        symbol: str,
        side: str,
        sl_px: float | None,
        tp_px: float | None,
    ) -> tuple[bool, Optional[str]]:
        """
        Проверка, был ли *исполняемый* триггер по SL/TP с учётом bid/ask.

        Возвращает:
          (touched: bool, which: "sl" | "tp" | None)
        """
        try:
            bid, ask = self.best_bid_ask(symbol)
        except Exception:
            bid, ask = 0.0, 0.0

        bid = float(bid or 0.0)
        ask = float(ask or 0.0)

        touched: bool = False
        which: Optional[str] = None

        # если вообще нет цен – ничего не трогаем
        if bid <= 0.0 and ask <= 0.0:
            return False, None

        if side == "BUY":
            # LONG: стоп исполним, когда ask <= SL; тейк – когда bid >= TP
            if sl_px is not None and ask <= float(sl_px):
                touched, which = True, "sl"
            elif tp_px is not None and bid >= float(tp_px):
                touched, which = True, "tp"
        elif side == "SELL":
            # SHORT: стоп исполним, когда bid >= SL; тейк – когда ask <= TP
            if sl_px is not None and bid >= float(sl_px):
                touched, which = True, "sl"
            elif tp_px is not None and ask <= float(tp_px):
                touched, which = True, "tp"

        logger.debug(
            "[price_touch] %s side=%s bid=%.2f ask=%.2f sl=%s tp=%s touched=%s which=%s",
            symbol, side, bid, ask,
            (None if sl_px is None else float(sl_px)),
            (None if tp_px is None else float(tp_px)),
            touched, which,
        )

        return touched, which

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

    def _compute_auto_qty(self, symbol: str, side: Side, sl_distance_px: float, stop_distance_usd: float | None = None) -> float:
        """
        Position sizing based on fixed USD risk per trade and strategy-provided stop distance (R-scaling).
        
        Uses the new R-scaling formula: qty = risk_per_trade_usd / stop_distance_usd
        
        Args:
            symbol: Trading symbol
            side: Trade side (BUY/SELL)
            sl_distance_px: Stop distance in price units (legacy, for backward compatibility)
            stop_distance_usd: Stop distance in USD (from EntryCandidate.stop_distance_usd, preferred)
        
        Returns:
            Calculated quantity, or 0.0 if sizing fails
        """
        sym = symbol.upper()

        # Get current price
        try:
            bid, ask = self.hub.best_bid_ask(sym)
        except Exception:
            return 0.0

        try:
            if bid > 0.0 and ask > 0.0:
                px = (float(bid) + float(ask)) / 2.0
            else:
                px = float(max(bid, ask))
        except Exception:
            px = 0.0

        if px <= 0.0:
            return 0.0

        # Get stop_distance_usd: prefer provided value, otherwise convert sl_distance_px to USD
        # For linear USDT-M futures, multiplier = 1, so stop_distance_usd = sl_distance_px
        if stop_distance_usd is None or stop_distance_usd <= 0:
            if sl_distance_px is None or sl_distance_px <= 0:
                return 0.0
            # Convert price units to USD (for linear contracts, 1:1)
            stop_distance_usd = float(sl_distance_px)

        # Get risk_per_trade_usd from config
        risk_per_trade_usd = 20.0  # default
        try:
            risk_cfg = getattr(self, "_risk_cfg", None)
            if risk_cfg and hasattr(risk_cfg, "risk_per_trade_usd"):
                risk_per_trade_usd = float(risk_cfg.risk_per_trade_usd or 20.0)
            else:
                # Fallback: try to get from settings
                try:
                    settings = get_settings()
                    if settings and settings.risk:
                        risk_per_trade_usd = float(settings.risk.risk_per_trade_usd or 20.0)
                except Exception:
                    pass
        except Exception:
            pass

        if risk_per_trade_usd <= 0:
            return 0.0

        # Get symbol specifications
        spec = getattr(self, "specs", {}).get(sym) if hasattr(self, "specs") else None
        try:
            if spec is not None:
                qty_step = float(getattr(spec, "qty_step", 0.001) or 0.001)
                min_qty = float(getattr(spec, "min_qty", 0.0) or 0.0)
            else:
                qty_step = float(getattr(self, "_qty_step_fallback", 0.001) or 0.001)
                min_qty = 0.0
        except Exception:
            qty_step = 0.001
            min_qty = 0.0

        if qty_step <= 0.0:
            qty_step = 0.001

        # Get min_notional
        min_notional = 0.0
        try:
            if hasattr(self, "_min_notional_for_symbol"):
                min_notional = float(self._min_notional_for_symbol(sym) or 0.0)
            else:
                # Fallback: try to get from account config
                try:
                    settings = get_settings()
                    if settings and settings.account:
                        min_notional = float(settings.account.min_notional_usd or 0.0)
                except Exception:
                    pass
        except Exception:
            min_notional = 0.0

        # Use the new R-scaling sizing function
        qty = calc_qty_from_stop_distance_usd(
            risk_per_trade_usd=risk_per_trade_usd,
            stop_distance_usd=stop_distance_usd,
            qty_step=qty_step,
            min_qty=min_qty,
            max_qty=None,  # No max_qty constraint for now
            min_notional_usd=min_notional,
            price=px,
        )

        if qty is None or qty <= 0.0:
            return 0.0

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
