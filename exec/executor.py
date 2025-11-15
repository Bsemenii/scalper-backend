from __future__ import annotations

import asyncio
import contextlib
import logging
import math
import time
import uuid
from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN, ROUND_UP, ROUND_HALF_UP
from typing import Any, Callable, Dict, Literal, Optional, Protocol, Tuple

logger = logging.getLogger(__name__)

Side = Literal["BUY", "SELL"]


# ==============================================================================
# Public DTOs / Protocols (стабильные, не ломаем контракт)
# ==============================================================================


@dataclass
class OrderReq:
    """
    Унифицированный запрос на ордер.

    Поля совместимы с adapters.binance_rest.OrderReq,
    чтобы PaperAdapter / реальный адаптер работали без правок.
    """

    symbol: str
    side: Side
    type: Literal["LIMIT", "MARKET", "STOP_MARKET", "TAKE_PROFIT_MARKET"]
    qty: float
    price: Optional[float] = None
    time_in_force: Optional[str] = None
    reduce_only: bool = False
    client_order_id: Optional[str] = None

    # Для STOP/TP ордеров
    stop_price: Optional[float] = None
    close_position: bool = False

    @property
    def stop_px(self) -> Optional[float]:
        """Мягкий алиас, если где-то в коде используют stop_px."""
        return self.stop_price


@dataclass
class OrderResp:
    order_id: str
    status: Literal[
        "NEW",
        "PARTIALLY_FILLED",
        "FILLED",
        "CANCELED",
        "REJECTED",
        "UNKNOWN",
    ]
    filled_qty: float
    avg_px: Optional[float]
    ts: int


class ExecAdapter(Protocol):
    """
    Минимальный контракт адаптера.

    Важно: это только type-hint, в рантайме используется duck-typing.
    BinanceUSDTMAdapter и PaperAdapter удовлетворяют этому контракту.
    """

    async def create_order(self, req: Any) -> Any:
        ...

    async def cancel_order(self, symbol: str, client_order_id: str) -> Any:
        """
        Для BinanceUSDTMAdapter фактическая сигнатура:
          cancel_order(symbol, order_id=None, client_order_id=None)
        Мы вызываем её как cancel_order(symbol, client_order_id=coid).
        """
        ...

    async def get_order(self, symbol: str, client_order_id: str) -> Any:
        """
        Для BinanceUSDTMAdapter фактическая сигнатура:
          get_order(symbol, order_id=None, client_order_id=None)
        Мы вызываем её как get_order(symbol, client_order_id=coid).
        """
        ...

    # НЕ обязательный, но если есть — используем реальные трейды/комиссии:
    # async def get_trades_for_order(self, symbol: str, order_id: int) -> Any: ...


@dataclass
class ExecCfg:
    """
    Конфигурация исполнителя.

    Прокидывается из Worker, имена полей не трогаем.
    """
    price_tick: float = 0.1
    qty_step: float = 0.001
    limit_offset_ticks: int = 1
    limit_timeout_ms: int = 500
    time_in_force: str = "GTC"
    poll_ms: int = 50
    prefer_maker: bool = False  # post-only эмуляция
    fee_bps_maker: float = 2.0  # только для paper-аудита
    fee_bps_taker: float = 4.0  # только для paper-аудита


@dataclass
class ExecutionReport:
    """
    Унифицированный отчёт для Worker и API.

    status:
      - FILLED   — заявка исполнена полностью (лимит+маркет)
      - PARTIAL  — исполнена частично
      - CANCELED — нет исполнения
    """
    status: Literal["FILLED", "PARTIAL", "CANCELED"]
    filled_qty: float
    avg_px: Optional[float]
    limit_oid: Optional[str]  # clientOrderId лимитки
    market_oid: Optional[str]  # clientOrderId маркета
    steps: list[str]
    ts: int


# ==============================================================================
# Helpers
# ==============================================================================


def _now_ms() -> int:
    return int(time.time() * 1000)


def _round_to_step(
    val: float,
    step: float,
    mode: Literal["down", "up", "nearest"] = "down",
) -> float:
    """
    Аккуратное квантование до шага step с помощью Decimal, чтобы не ловить
    0.12300000000000001 и ошибки precision на Binance.
    """
    if step is None or step <= 0:
        return float(val)

    d = Decimal(str(val))
    s = Decimal(str(step))

    if mode == "down":
        q = (d / s).to_integral_value(rounding=ROUND_DOWN) * s
    elif mode == "up":
        q = (d / s).to_integral_value(rounding=ROUND_UP) * s
    else:  # "nearest"
        q = (d / s).to_integral_value(rounding=ROUND_HALF_UP) * s

    q = q.quantize(s)
    return float(q)


def _round_price_to_tick(px: float, tick: float, side: Side) -> float:
    """
    BUY  — вниз (не дороже нужного),
    SELL — вверх (не дешевле нужного).
    """
    mode: Literal["down", "up"] = "down" if side == "BUY" else "up"
    v = _round_to_step(px, tick, mode)
    return max(tick, v)


def _coid(prefix: str, symbol: str) -> str:
    # читаемый clientOrderId для дебага + cancel/get.
    return f"{prefix}-{symbol}-{_now_ms()}"


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        v = float(x)
        if math.isnan(v) or math.isinf(v):
            return default
        return v
    except Exception:
        return default


# ==============================================================================
# Executor
# ==============================================================================


class Executor:
    """
    Исполнитель для одного символа.

    Алгоритм place_entry:
      1) qty → шаг инструмента.
      2) лимитка с limit_offset_ticks от bid/ask.
      3) при need post-only (GTX/PO) — не пересекаем спред.
      4) ждём до limit_timeout_ms (если get_order есть — опрашиваем).
      5) отменяем лимитку, остаток добираем MARKET.
      6) всё логируем в steps + сохраняем в БД, если доступна.

    Дополнения:
      - place_bracket: выставление брекетов SL/TP.
      - cancel_bracket: отмена SL/TP по clientOrderId.
      - Если адаптер умеет get_trades_for_order() (наш BinanceUSDTMAdapter),
        то fills и комиссии берём из реальных трейдов, а не из bps.
    """

    def __init__(
        self,
        adapter: ExecAdapter,
        cfg: ExecCfg,
        get_best: Callable[[str], Tuple[float, float]],
    ) -> None:
        self.a = adapter
        self.c = cfg
        self.get_best = get_best

        # Если TIF пост-онли — автоматически включаем prefer_maker.
        tif = (self.c.time_in_force or "").upper()
        if tif in ("GTX", "PO", "POST_ONLY") and not self.c.prefer_maker:
            self.c.prefer_maker = True

        # Есть ли у адаптера userTrades API:
        self._has_trades_api: bool = callable(getattr(self.a, "get_trades_for_order", None))

    # ---------------------------------------------------------- Resp normalizer --

    def _to_order_resp(self, raw: Any, *, coid: Optional[str] = None) -> OrderResp:
        """
        Унифицируем ответ адаптера:
        - если уже OrderResp — возвращаем как есть;
        - если dict — вытаскиваем нужные поля с безопасными фолбэками.
        Поддерживает и paper, и реальные binance-стайловые ответы.
        """
        if isinstance(raw, OrderResp):
            return raw

        if isinstance(raw, dict):
            oid = (
                raw.get("order_id")
                or raw.get("orderId")
                or raw.get("client_order_id")
                or raw.get("clientOrderId")
                or coid
                or ""
            )

            status = str(raw.get("status") or raw.get("origStatus") or "NEW")

            filled = _safe_float(
                raw.get("filled_qty")
                or raw.get("executedQty")
                or raw.get("cumQty")
                or 0.0
            )

            avg_px_val = raw.get("avg_px")
            if avg_px_val is None:
                quote_qty = _safe_float(
                    raw.get("cummulativeQuoteQty")
                    or raw.get("cumQuote")
                    or raw.get("quoteQty")
                    or 0.0
                )
                if filled > 0 and quote_qty > 0:
                    avg_px_val = quote_qty / filled

            if avg_px_val is None:
                avg_px: Optional[float] = None
            else:
                avg_px = _safe_float(avg_px_val, default=0.0)

            ts = int(
                raw.get("ts")
                or raw.get("transactTime")
                or raw.get("updateTime")
                or _now_ms()
            )

            return OrderResp(
                order_id=str(oid),
                status=status,  # type: ignore[arg-type]
                filled_qty=float(filled),
                avg_px=avg_px,
                ts=ts,
            )

        raise TypeError(f"Unsupported order resp type: {type(raw)}")

    # ------------------------------------------------------------------ Repo I/O

    def _repo_save_order(
        self,
        *,
        order_id: str,
        symbol: str,
        side: Side,
        otype: str,
        reduce_only: bool,
        px: Optional[float],
        qty: float,
        status: str,
        created_ms: Optional[int] = None,
        updated_ms: Optional[int] = None,
        trade_id: Optional[str] = None,
    ) -> None:
        """Безопасно пишет orders; если storage.repo нет — тихо выходим."""
        try:
            from storage.repo import save_order  # type: ignore
        except ModuleNotFoundError:
            return
        except Exception as e:
            logger.warning("import save_order failed: %s", e)
            return

        try:
            save_order(
                {
                    "id": order_id,
                    "trade_id": trade_id,
                    "symbol": symbol,
                    "side": side,
                    "type": otype,
                    "reduce_only": 1 if reduce_only else 0,
                    "px": (None if px is None else float(px)),
                    "qty": float(qty),
                    "status": status,
                    "created_ms": int(created_ms or _now_ms()),
                    "updated_ms": int(updated_ms or _now_ms()),
                }
            )
        except Exception as e:
            logger.warning("save_order failed: %s", e)

    def _repo_save_fill(
        self,
        *,
        order_id: str,
        px: float,
        qty: float,
        fee_usd: float = 0.0,
        ts_ms: Optional[int] = None,
    ) -> None:
        """Безопасно пишет fills; если storage.repo нет — тихо выходим."""
        try:
            from storage.repo import save_fill  # type: ignore
        except ModuleNotFoundError:
            return
        except Exception as e:
            logger.warning("import save_fill failed: %s", e)
            return

        try:
            fill_id = str(uuid.uuid4())
            save_fill(
                {
                    "id": fill_id,
                    "order_id": order_id,
                    "ts_ms": int(ts_ms or _now_ms()),
                    "px": float(px),
                    "qty": float(qty),
                    "fee_usd": float(fee_usd),
                }
            )
        except Exception as e:
            logger.warning("save_fill failed: %s", e)

    async def _fetch_and_save_trades(
        self,
        *,
        symbol: str,
        client_order_id: str,
        exch_order_id: Optional[int],
    ) -> None:
        """
        Если адаптер умеет userTrades, вытаскиваем реальные трейды по orderId
        и сохраняем их как fills в нашей БД.

        ВАЖНО:
          - связка делается по exchange orderId, который пришёл из Binance;
          - в нашу БД мы привязываем к внутреннему clientOrderId, чтобы
            все отчёты/логика смотрели на знакомый ID.
        """
        if not self._has_trades_api:
            return
        if exch_order_id is None:
            return

        get_trades = getattr(self.a, "get_trades_for_order", None)
        if not callable(get_trades):
            return

        try:
            trades = await get_trades(symbol, order_id=exch_order_id)  # type: ignore[misc]
        except Exception as e:
            logger.warning(
                "[exec] get_trades_for_order failed symbol=%s exch_oid=%s: %s",
                symbol,
                exch_order_id,
                e,
            )
            return

        if not trades:
            return

        for t in trades:
            try:
                px = _safe_float(t.get("price"))
                qty = _safe_float(t.get("qty"))
                if px <= 0.0 or abs(qty) <= 0.0:
                    continue

                # Комиссия в USDT (для USDT-M фьючей комиссияAsset почти всегда USDT)
                commission = _safe_float(t.get("commission"), 0.0)
                commission_asset = str(t.get("commissionAsset") or "").upper()
                fee_usd = abs(commission) if commission_asset == "USDT" else 0.0

                ts_ms = int(t.get("time") or _now_ms())

                self._repo_save_fill(
                    order_id=client_order_id,
                    px=px,
                    qty=qty,
                    fee_usd=fee_usd,
                    ts_ms=ts_ms,
                )
            except Exception as e:
                logger.warning(
                    "[exec] failed to process trade for coid=%s exch_oid=%s: %s",
                    client_order_id,
                    exch_order_id,
                    e,
                )

    # ---------------------------------------------------------- Price helpers --

    def _price_from_signed_offset(
        self,
        side: Side,
        bid: float,
        ask: float,
        ticks: int,
        tick: float,
    ) -> float:
        """
        Смещение от лучшей цены.

        ticks >= 0:
            BUY  → от ask вверх
            SELL → от bid вниз
        ticks < 0:
            BUY  → от bid вниз
            SELL → от ask вверх
        """
        ticks = int(ticks)
        tick = max(float(tick), 0.0)

        if bid <= 0 and ask <= 0:
            return 0.0

        if side == "BUY":
            if ticks >= 0:
                base = ask if ask > 0 else bid
                return base + ticks * tick
            else:
                base = bid if bid > 0 else ask
                return base - abs(ticks) * tick
        else:  # SELL
            if ticks >= 0:
                base = bid if bid > 0 else ask
                return base - ticks * tick
            else:
                base = ask if ask > 0 else bid
                return base + abs(ticks) * tick

    def _enforce_post_only(
        self,
        side: Side,
        bid: float,
        ask: float,
        px: float,
        tick: float,
    ) -> Tuple[float, Optional[str]]:
        """
        prefer_maker=True: не даём пересечь спред.

        BUY  с px > bid  → прижимаем к bid
        SELL с px < ask  → прижимаем к ask
        """
        note: Optional[str] = None
        if not self.c.prefer_maker:
            return px, note
        if bid <= 0 or ask <= 0:
            return px, note

        if side == "BUY" and px > bid:
            px = _round_price_to_tick(bid, tick, "BUY")
            note = "post_only_clamped_to_bid"
        elif side == "SELL" and px < ask:
            px = _round_price_to_tick(ask, tick, "SELL")
            note = "post_only_clamped_to_ask"
        return px, note

    # ----------------------------------------------------------------- Core API

    async def place_entry(
        self,
        symbol: str,
        side: Side,
        qty: float,
        *,
        reduce_only: bool = False,
    ) -> ExecutionReport:
        """
        Лимитный вход с догоном маркетом.

        - Совместим по сигнатуре с текущим Worker.
        - steps оставлены в формате, который уже используется метриками.
        """
        steps: list[str] = []
        t_start = time.monotonic()

        # --- 1. Квантование количества ---
        qty_raw = _safe_float(qty)
        qty_rounded = max(0.0, _round_to_step(qty_raw, self.c.qty_step, "down"))

        if qty_rounded <= 0.0:
            steps.append("qty_rounded_to_zero")
            return ExecutionReport(
                status="CANCELED",
                filled_qty=0.0,
                avg_px=None,
                limit_oid=None,
                market_oid=None,
                steps=steps,
                ts=_now_ms(),
            )

        # --- 2. Лучшая цена & лимитный прайс ---
        bid, ask = self.get_best(symbol)
        price_tick = max(float(self.c.price_tick), 1e-9)

        raw_px = self._price_from_signed_offset(
            side,
            bid,
            ask,
            self.c.limit_offset_ticks,
            price_tick,
        )

        if raw_px <= 0.0:
            if side == "BUY":
                raw_px = ask or bid or price_tick
            else:
                raw_px = bid or ask or price_tick

        limit_px = _round_price_to_tick(raw_px, price_tick, side)
        limit_px, note = self._enforce_post_only(side, bid, ask, limit_px, price_tick)
        if limit_px <= 0.0:
            limit_px = price_tick

        # --- 3. Создаём лимитку ---
        limit_coid = _coid("lim", symbol)
        limit_req = OrderReq(
            symbol=symbol,
            side=side,
            type="LIMIT",
            qty=float(qty_rounded),
            price=float(limit_px),
            time_in_force=self.c.time_in_force or "GTC",
            reduce_only=bool(reduce_only),
            client_order_id=limit_coid,
        )

        submit_tag = "limit_submit_postonly" if note in (
            "post_only_clamped_to_bid",
            "post_only_clamped_to_ask",
        ) else "limit_submit"

        if note:
            steps.append(note)

        steps.append(
            f"{submit_tag}:{limit_px} qty:{qty_rounded}"
            + (" reduce_only" if reduce_only else "")
        )

        logger.debug(
            "[exec] create limit %s %s qty=%.6f px=%.8f coid=%s",
            symbol,
            side,
            qty_rounded,
            limit_px,
            limit_coid,
        )

        # create_order может упасть (margin, precision и т.п.) — пусть это
        # поднимется наверх как ошибка стратегии/воркера, это не создаёт
        # split-brain с биржей, т.к. позиция не откроется.
        lim_raw = await self.a.create_order(limit_req)
        lim = self._to_order_resp(lim_raw, coid=limit_coid)

        # exchange orderId для userTrades
        limit_exch_oid: Optional[int] = None
        if isinstance(lim_raw, dict):
            try:
                limit_exch_oid = int(lim_raw.get("orderId"))
            except Exception:
                limit_exch_oid = None

        limit_oid: Optional[str] = limit_coid
        market_oid: Optional[str] = None
        market_exch_oid: Optional[int] = None

        self._repo_save_order(
            order_id=limit_coid,
            symbol=symbol,
            side=side,
            otype="LIMIT",
            reduce_only=bool(reduce_only),
            px=limit_px,
            qty=qty_rounded,
            status="NEW",
        )

        filled_qty = 0.0
        vwap_cash = 0.0

        # --- 4. Обработка мгновенного исполнения лимитки ---
        if lim.status in ("PARTIALLY_FILLED", "FILLED") and lim.filled_qty > 0:
            part = _safe_float(lim.filled_qty)
            avg = _safe_float(lim.avg_px)
            if part > 0:
                filled_qty += part
                vwap_cash += part * avg
                steps.append(f"limit_filled_immediate:{part}@{avg}")

                # Синтетические комиссии только если нет userTrades:
                if not self._has_trades_api:
                    fee_bps = self.c.fee_bps_taker
                    fee_usd = abs(part * avg) * (fee_bps / 10_000.0)
                    self._repo_save_fill(
                        order_id=limit_coid,
                        px=avg,
                        qty=part,
                        fee_usd=fee_usd,
                        ts_ms=lim.ts,
                    )

                self._repo_save_order(
                    order_id=limit_coid,
                    symbol=symbol,
                    side=side,
                    otype="LIMIT",
                    reduce_only=bool(reduce_only),
                    px=avg,
                    qty=qty_rounded,
                    status=(
                        "FILLED" if lim.status == "FILLED" else "PARTIALLY_FILLED"
                    ),
                    updated_ms=lim.ts,
                )

        # --- 5. Polling до таймаута (если get_order доступен) ---
        deadline = time.monotonic() + (self.c.limit_timeout_ms / 1000.0)
        get_order = getattr(self.a, "get_order", None)

        if callable(get_order) and lim.status not in ("FILLED", "CANCELED", "REJECTED"):
            while time.monotonic() < deadline:
                await asyncio.sleep(max(0.0, (self.c.poll_ms or 50) / 1000.0))
                try:
                    state_raw = await get_order(symbol, client_order_id=limit_coid)  # type: ignore[misc]
                    state = self._to_order_resp(state_raw, coid=limit_coid)
                except Exception as e:
                    logger.debug(
                        "[exec] get_order error for %s coid=%s: %s",
                        symbol,
                        limit_coid,
                        e,
                    )
                    continue

                part = _safe_float(state.filled_qty)
                avg = _safe_float(state.avg_px)

                if part > filled_qty:
                    delta = part - filled_qty
                    filled_qty = part
                    vwap_cash += delta * avg
                    steps.append(f"limit_partial:{delta}@{avg}")

                    if not self._has_trades_api:
                        fee_bps = self.c.fee_bps_maker
                        fee_usd = abs(delta * avg) * (fee_bps / 10_000.0)
                        self._repo_save_fill(
                            order_id=limit_coid,
                            px=avg,
                            qty=delta,
                            fee_usd=fee_usd,
                            ts_ms=state.ts,
                        )

                    self._repo_save_order(
                        order_id=limit_coid,
                        symbol=symbol,
                        side=side,
                        otype="LIMIT",
                        reduce_only=bool(reduce_only),
                        px=avg,
                        qty=qty_rounded,
                        status=(
                            "FILLED"
                            if state.status == "FILLED"
                            else "PARTIALLY_FILLED"
                        ),
                        updated_ms=state.ts,
                    )

                if state.status == "FILLED":
                    avg_px_tmp = vwap_cash / max(filled_qty, 1e-12)
                    steps.append("limit_filled_resting")
                    steps.append(f"limit_filled_total:{filled_qty}@{avg_px_tmp:.8f}")
                    latency_ms = int((time.monotonic() - t_start) * 1000)
                    steps.append(f"latency_ms:{latency_ms}")
                    break

        # --- 6. Таймаут: отменяем лимитку (если остался объём) ---
        if filled_qty < qty_rounded - 1e-12:
            steps.append("limit_cancel")
            logger.debug("[exec] cancel limit %s coid=%s", symbol, limit_coid)

            cancel_resp: Optional[OrderResp] = None

            # Пытаемся отменить. Если отмена падает (например, ордер уже исполнен
            # и на бирже удалён) — НЕ замалчиваем, а явно логируем и пытаемся
            # добрать финальное состояние через get_order.
            try:
                cancel_raw = await self.a.cancel_order(symbol, client_order_id=limit_coid)  # type: ignore[arg-type]
                cancel_resp = self._to_order_resp(cancel_raw, coid=limit_coid)
                steps.append(f"limit_cancel_resp_status:{cancel_resp.status}")
            except Exception as e:
                logger.warning(
                    "[exec] cancel_order error for %s coid=%s: %s",
                    symbol,
                    limit_coid,
                    e,
                )
                steps.append(f"limit_cancel_error:{type(e).__name__}")

            # Если отмена отработала и вернула filled_qty — учитываем дофиллы
            if cancel_resp is not None:
                part = _safe_float(cancel_resp.filled_qty)
                avg = _safe_float(cancel_resp.avg_px)

                if part > filled_qty:
                    delta = part - filled_qty
                    filled_qty = part
                    vwap_cash += delta * avg
                    steps.append(f"limit_partial_after_cancel:{delta}@{avg}")

                    if not self._has_trades_api:
                        fee_bps = self.c.fee_bps_maker
                        fee_usd = abs(delta * avg) * (fee_bps / 10_000.0)
                        self._repo_save_fill(
                            order_id=limit_coid,
                            px=avg,
                            qty=delta,
                            fee_usd=fee_usd,
                            ts_ms=cancel_resp.ts,
                        )

            # Если отмена не дала информации, но есть get_order — пробуем финальный снимок
            if cancel_resp is None and callable(get_order):
                try:
                    state_raw = await get_order(symbol, client_order_id=limit_coid)  # type: ignore[misc]
                    state = self._to_order_resp(state_raw, coid=limit_coid)
                    part = _safe_float(state.filled_qty)
                    avg = _safe_float(state.avg_px)
                    steps.append(f"limit_state_after_cancel:{state.status}")

                    if part > filled_qty:
                        delta = part - filled_qty
                        filled_qty = part
                        vwap_cash += delta * avg
                        steps.append(f"limit_partial_after_cancel_state:{delta}@{avg}")

                        if not self._has_trades_api:
                            fee_bps = self.c.fee_bps_maker
                            fee_usd = abs(delta * avg) * (fee_bps / 10_000.0)
                            self._repo_save_fill(
                                order_id=limit_coid,
                                px=avg,
                                qty=delta,
                                fee_usd=fee_usd,
                                ts_ms=state.ts,
                            )
                except Exception as e:
                    logger.warning(
                        "[exec] get_order after cancel failed %s coid=%s: %s",
                        symbol,
                        limit_coid,
                        e,
                    )
                    steps.append("limit_state_after_cancel_error")

            # статус ордера в репозитории теперь зависит от фактического filled_qty
            if filled_qty <= 1e-12:
                repo_status = "CANCELED"
            elif filled_qty < qty_rounded - 1e-12:
                repo_status = "PARTIALLY_FILLED"
            else:
                repo_status = "FILLED"

            self._repo_save_order(
                order_id=limit_coid,
                symbol=symbol,
                side=side,
                otype="LIMIT",
                reduce_only=bool(reduce_only),
                px=limit_px,
                qty=qty_rounded,
                status=repo_status,
            )

        # --- 7. Остаток → MARKET ---
        remaining = max(0.0, qty_rounded - filled_qty)
        remaining = _round_to_step(remaining, self.c.qty_step, "down")

        if remaining > 0.0:
            market_coid = _coid("mkt", symbol)
            steps.append(
                f"market_submit:{remaining}"
                + (" reduce_only" if reduce_only else "")
            )

            mreq = OrderReq(
                symbol=symbol,
                side=side,
                type="MARKET",
                qty=float(remaining),
                reduce_only=bool(reduce_only),
                client_order_id=market_coid,
            )

            self._repo_save_order(
                order_id=market_coid,
                symbol=symbol,
                side=side,
                otype="MARKET",
                reduce_only=bool(reduce_only),
                px=None,
                qty=remaining,
                status="NEW",
            )

            logger.debug(
                "[exec] create market %s %s qty=%.6f coid=%s",
                symbol,
                side,
                remaining,
                market_coid,
            )

            m_raw = await self.a.create_order(mreq)
            m = self._to_order_resp(m_raw, coid=market_coid)
            market_oid = market_coid

            if isinstance(m_raw, dict):
                try:
                    market_exch_oid = int(m_raw.get("orderId"))
                except Exception:
                    market_exch_oid = None

            if m.status in ("PARTIALLY_FILLED", "FILLED") and m.filled_qty > 0:
                part = _safe_float(m.filled_qty)
                avg = _safe_float(m.avg_px)
                if part > 0:
                    filled_qty += part
                    vwap_cash += part * avg
                    steps.append(f"market_filled:{part}@{avg}")

                    if not self._has_trades_api:
                        fee_bps = self.c.fee_bps_taker
                        fee_usd = abs(part * avg) * (fee_bps / 10_000.0)
                        self._repo_save_fill(
                            order_id=market_coid,
                            px=avg,
                            qty=part,
                            fee_usd=fee_usd,
                            ts_ms=m.ts,
                        )

                    self._repo_save_order(
                        order_id=market_coid,
                        symbol=symbol,
                        side=side,
                        otype="MARKET",
                        reduce_only=bool(reduce_only),
                        px=avg,
                        qty=remaining,
                        status=(
                            "FILLED"
                            if m.status == "FILLED"
                            else "PARTIALLY_FILLED"
                        ),
                        updated_ms=m.ts,
                    )

            elif m.status == "REJECTED":
                steps.append("market_rejected")
                self._repo_save_order(
                    order_id=market_coid,
                    symbol=symbol,
                    side=side,
                    otype="MARKET",
                    reduce_only=bool(reduce_only),
                    px=None,
                    qty=remaining,
                    status="REJECTED",
                    updated_ms=m.ts,
                )

        else:
            market_oid = None

        # --- 7.1. Если есть userTrades — подтягиваем реальные трейды и комиссии ---
        if self._has_trades_api:
            # Лимитка
            if filled_qty > 0.0 and limit_exch_oid is not None:
                await self._fetch_and_save_trades(
                    symbol=symbol,
                    client_order_id=limit_coid,
                    exch_order_id=limit_exch_oid,
                )

            # Маркет
            if filled_qty > 0.0 and market_exch_oid is not None and market_oid:
                await self._fetch_and_save_trades(
                    symbol=symbol,
                    client_order_id=market_oid,
                    exch_order_id=market_exch_oid,
                )

        # --- 8. Финальный статус и отчёт ---
        if filled_qty > 1e-12:
            avg_px: Optional[float] = vwap_cash / max(filled_qty, 1e-12)
        else:
            avg_px = None

        if filled_qty <= 1e-12:
            status: Literal["FILLED", "PARTIAL", "CANCELED"] = "CANCELED"
        elif filled_qty < qty_rounded - 1e-12:
            status = "PARTIAL"
        else:
            status = "FILLED"

        latency_ms = int((time.monotonic() - t_start) * 1000)
        steps.append(f"latency_ms:{latency_ms}")

        logger.debug(
            "[exec] %s %s qty=%.6f filled=%.6f avg_px=%s status=%s steps=%s",
            symbol,
            side,
            qty_rounded,
            filled_qty,
            (None if avg_px is None else f"{avg_px:.8f}"),
            status,
            steps,
        )

        return ExecutionReport(
            status=status,
            filled_qty=float(filled_qty),
            avg_px=(float(avg_px) if avg_px is not None else None),
            limit_oid=limit_oid,
            market_oid=market_oid,
            steps=steps,
            ts=_now_ms(),
        )

    # ----------------------------------------------------------------- place_exit

    async def place_exit(self, symbol: str, side: Side, qty: float) -> ExecutionReport:
        """
        Безопасный выход:
        - reduce_only=True гарантирует, что мы не перевернём позицию.
        - Сохраняет тот же pipeline (лимит+маркет) и формат steps.
        """
        return await self.place_entry(symbol, side, qty, reduce_only=True)

    # ----------------------------------------------------------------- Brackets: helpers --

    async def _effective_bracket_qty(self, symbol: str, side: Side, qty: float) -> float:
        """
        Поджимаем qty под:
        - шаг инструмента (cfg.qty_step),
        - реальный размер позиции на бирже (если get_positions есть).

        Это снижает шанс -2022 (ReduceOnly Order is rejected) из-за лишних дольных qty.
        """
        sym_u = symbol.upper()
        q = max(0.0, _round_to_step(_safe_float(qty), self.c.qty_step, "down"))
        if q <= 0.0:
            return 0.0

        get_positions = getattr(self.a, "get_positions", None)
        if not callable(get_positions):
            return q

        try:
            positions = await get_positions()
        except Exception:
            return q

        pos_amt: Optional[float] = None
        for p in positions or []:
            try:
                if str(p.get("symbol", "")).upper() != sym_u:
                    continue
                amt = _safe_float(p.get("positionAmt"), 0.0)
                if abs(amt) <= 0.0:
                    continue
                pos_amt = abs(amt)
                break
            except Exception:
                continue

        if pos_amt is None:
            return q

        eff = min(q, pos_amt)
        eff = max(0.0, _round_to_step(eff, self.c.qty_step, "down"))
        return eff

    async def _submit_reduce_only_with_fallback(
        self,
        req: OrderReq,
        *,
        tag: str,
    ) -> tuple[Optional[OrderResp], Optional[str], Optional[str]]:
        """
        Универсальная подача reduce_only ордера с fallback.

        1) Пытаемся отправить req как есть (reduce_only=True).
        2) Если получили -2022 (ReduceOnly Order is rejected):
           - логируем,
           - повторяем ордер без reduce_only (fallback),
             с новым client_order_id.
        3) Возвращаем: (resp | None, final_client_order_id | None, fallback_reason | None)
        """
        symbol = req.symbol
        coid_initial = req.client_order_id or _coid(tag.lower(), symbol)
        req.client_order_id = coid_initial

        try:
            raw = await self.a.create_order(req)
            resp = self._to_order_resp(raw, coid=coid_initial)
            return resp, coid_initial, None
        except Exception as e:
            msg = str(e)
            # пытаемся вытащить binance_code, если это BinanceRestError
            binance_code = getattr(e, "binance_code", None)

            logger.warning(
                "[exec_bracket] %s reduce_only failed for %s coid=%s: %s",
                tag,
                symbol,
                coid_initial,
                msg,
            )

            is_reduce_only_error = False
            if binance_code == -2022:
                is_reduce_only_error = True
            elif "ReduceOnly Order is rejected" in msg or "-2022" in msg:
                is_reduce_only_error = True

            # Fallback только на специфическую ошибку reduceOnly
            if is_reduce_only_error:
                coid_fallback = _coid(f"{tag.lower()}-nr", symbol)
                fallback_req = OrderReq(
                    symbol=req.symbol,
                    side=req.side,
                    type=req.type,
                    qty=req.qty,
                    price=req.price,
                    time_in_force=req.time_in_force,
                    reduce_only=False,       # КЛЮЧЕВОЕ отличие
                    client_order_id=coid_fallback,
                    stop_price=req.stop_price,
                    close_position=req.close_position,
                )
                try:
                    raw2 = await self.a.create_order(fallback_req)
                    resp2 = self._to_order_resp(raw2, coid=coid_fallback)
                    logger.warning(
                        "[exec_bracket] %s fallback without reduce_only succeeded for %s coid=%s",
                        tag,
                        symbol,
                        coid_fallback,
                    )
                    return resp2, coid_fallback, "fallback_no_reduce_only"
                except Exception as e2:
                    logger.error(
                        "[exec_bracket] %s fallback without reduce_only failed for %s: %s",
                        tag,
                        symbol,
                        e2,
                    )
                    return None, None, msg

            # Любая другая ошибка — просто bubbling вверх по логике caller'а.
            return None, None, msg

    # ----------------------------------------------------------------- Brackets --

    async def place_bracket(
        self,
        symbol: str,
        side: Side,
        qty: float,
        *,
        sl_px: Optional[float] = None,
        tp_px: Optional[float] = None,
    ) -> Dict[str, Any]:
        """
        Выставление брекетов (SL/TP) под уже открытую позицию.

        SL → STOP_MARKET (taker, reduce_only, при ошибке -2022 fallback без reduce_only)
        TP → LIMIT (maker-first, reduce_only, при ошибке -2022 fallback без reduce_only)

        ВАЖНО:
          - sl_order_id / tp_order_id — это ИМЕННО clientOrderId,
            которые реально висят на бирже (учитывая fallback).
        """
        sym = symbol
        s = side

        # --- 0. Эффективный размер под позицию и шаг qty ---
        qty_eff = await self._effective_bracket_qty(sym, s, qty)
        if qty_eff <= 0.0:
            return {
                "ok": False,
                "reason": "qty_rounded_to_zero",
                "sl_order_id": None,
                "tp_order_id": None,
            }

        sl_oid: Optional[str] = None
        tp_oid: Optional[str] = None
        ok = True
        reason_parts: list[str] = []

        # best цены нужны для TP (post-only clamp)
        try:
            bid, ask = self.get_best(sym)
        except Exception:
            bid = ask = 0.0

        price_tick = max(float(self.c.price_tick), 1e-9)
        exit_side: Side = "SELL" if s == "BUY" else "BUY"

        has_get_order = hasattr(self.a, "get_order")

        # --- 1. SL: STOP_MARKET ---
        if sl_px is not None and sl_px > 0:
            sl_raw = float(sl_px)
            # Жёстко квантим стоп до шага tick, чтобы не ловить -1111 по precision
            sl_px_q = _round_to_step(sl_raw, price_tick, "nearest")
            if sl_px_q <= 0.0:
                sl_px_q = price_tick

            sl_coid = _coid("sl", sym)
            sl_req = OrderReq(
                symbol=sym,
                side=exit_side,
                type="STOP_MARKET",
                qty=float(qty_eff),
                stop_price=float(sl_px_q),
                reduce_only=True,
                client_order_id=sl_coid,
                close_position=False,
            )
            try:
                logger.debug(
                    "[exec_bracket] create SL %s side=%s qty=%.6f stop=%.8f coid=%s",
                    sym,
                    exit_side,
                    qty_eff,
                    sl_px_q,
                    sl_coid,
                )

                resp, final_coid, fb_reason = await self._submit_reduce_only_with_fallback(
                    sl_req,
                    tag="SL",
                )

                if resp is None or final_coid is None:
                    ok = False
                    reason_parts.append("sl_create_failed")
                    sl_oid = None
                else:
                    sl_oid = final_coid

                    self._repo_save_order(
                        order_id=final_coid,
                        symbol=sym,
                        side=exit_side,
                        otype="STOP_MARKET",
                        reduce_only=True if fb_reason is None else False,
                        px=None,
                        qty=qty_eff,
                        status=resp.status,
                        created_ms=resp.ts,
                    )

                    if has_get_order:
                        try:
                            state_raw = await self.a.get_order(sym, client_order_id=final_coid)  # type: ignore[arg-type]
                            state = self._to_order_resp(state_raw, coid=final_coid)
                            if state.status in ("CANCELED", "REJECTED", "UNKNOWN"):
                                ok = False
                                reason_parts.append(f"sl_status_{state.status}")
                                logger.warning(
                                    "[exec_bracket] SL %s coid=%s bad status=%s",
                                    sym,
                                    final_coid,
                                    state.status,
                                )
                                sl_oid = None
                        except Exception as e:
                            ok = False
                            reason_parts.append("sl_get_order_failed")
                            logger.warning(
                                "[exec_bracket] SL get_order failed %s coid=%s: %s",
                                sym,
                                final_coid,
                                e,
                            )

                    if fb_reason:
                        reason_parts.append(f"sl_{fb_reason}")

            except Exception as e:
                ok = False
                reason_parts.append("sl_create_failed")
                logger.warning("[exec_bracket] place_bracket SL failed for %s: %s", sym, e)
                sl_oid = None

        # --- 2. TP: LIMIT ---
        if tp_px is not None and tp_px > 0:
            tp_raw = float(tp_px)
            # Квантим по тику, НО БЕЗ post-only (GTX), чтобы Binance не резал reduceOnly
            tp_px_q = _round_price_to_tick(tp_raw, price_tick, exit_side)

            if tp_px_q <= 0.0:
                tp_px_q = price_tick

            tp_coid = _coid("tp", sym)
            tp_req = OrderReq(
                symbol=sym,
                side=exit_side,
                type="LIMIT",
                qty=float(qty_eff),
                price=float(tp_px_q),
                time_in_force="GTC",
                reduce_only=True,
                client_order_id=tp_coid,
            )

            try:
                logger.debug(
                    "[exec_bracket] create TP %s side=%s qty=%.6f px=%.8f coid=%s",
                    sym,
                    exit_side,
                    qty_eff,
                    tp_px_q,
                    tp_coid,
                )

                resp, final_coid, fb_reason = await self._submit_reduce_only_with_fallback(
                    tp_req,
                    tag="TP",
                )

                if resp is None or final_coid is None:
                    ok = False
                    reason_parts.append("tp_create_failed")
                    tp_oid = None
                else:
                    tp_oid = final_coid

                    self._repo_save_order(
                        order_id=final_coid,
                        symbol=sym,
                        side=exit_side,
                        otype="LIMIT",
                        reduce_only=True if fb_reason is None else False,
                        px=tp_px_q,
                        qty=qty_eff,
                        status=resp.status,
                        created_ms=resp.ts,
                    )

                    if has_get_order:
                        try:
                            state_raw = await self.a.get_order(sym, client_order_id=final_coid)  # type: ignore[arg-type]
                            state = self._to_order_resp(state_raw, coid=final_coid)
                            if state.status in ("CANCELED", "REJECTED", "UNKNOWN"):
                                ok = False
                                reason_parts.append(f"tp_status_{state.status}")
                                logger.warning(
                                    "[exec_bracket] TP %s coid=%s bad status=%s",
                                    sym,
                                    final_coid,
                                    state.status,
                                )
                                tp_oid = None
                        except Exception as e:
                            ok = False
                            reason_parts.append("tp_get_order_failed")
                            logger.warning(
                                "[exec_bracket] TP get_order failed %s coid=%s: %s",
                                sym,
                                final_coid,
                                e,
                            )

                    if fb_reason:
                        reason_parts.append(f"tp_{fb_reason}")

            except Exception as e:
                ok = False
                reason_parts.append("tp_create_failed")
                logger.warning("[exec_bracket] place_bracket TP failed for %s: %s", sym, e)
                tp_oid = None

        return {
            "ok": bool(ok),
            "sl_order_id": sl_oid,
            "tp_order_id": tp_oid,
            "reason": ":".join(reason_parts) if reason_parts else None,
        }

    async def cancel_bracket(
        self,
        symbol: str,
        *,
        sl_order_id: Optional[str] = None,
        tp_order_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Отмена уже выставленных брекетов по clientOrderId.

        Здесь sl_order_id / tp_order_id — это clientOrderId,
        которые вернул place_bracket (с учётом fallback).
        """
        sym = symbol
        ok = True
        out: Dict[str, Any] = {"ok": True}

        # SL
        if sl_order_id:
            try:
                logger.debug("[exec_bracket] cancel SL %s coid=%s", sym, sl_order_id)
                await self.a.cancel_order(sym, client_order_id=sl_order_id)  # type: ignore[arg-type]
                out["sl_canceled"] = True
            except Exception as e:
                ok = False
                out["sl_canceled"] = False
                out["sl_error"] = str(e)
                logger.warning(
                    "[exec_bracket] cancel_bracket SL failed for %s: %s",
                    sym,
                    e,
                )

        # TP
        if tp_order_id:
            try:
                logger.debug("[exec_bracket] cancel TP %s coid=%s", sym, tp_order_id)
                await self.a.cancel_order(sym, client_order_id=tp_order_id)  # type: ignore[arg-type]
                out["tp_canceled"] = True
            except Exception as e:
                ok = False
                out["tp_canceled"] = False
                out["tp_error"] = str(e)
                logger.warning(
                    "[exec_bracket] cancel_bracket TP failed for %s: %s",
                    sym,
                    e,
                )

        out["ok"] = bool(ok)
        return out
