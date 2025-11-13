from __future__ import annotations

import asyncio
import contextlib
import logging
import math
import time
import uuid
from dataclasses import dataclass
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

    Поля совместимы с adapters.binance_rest.{OrderReq,OrderResp},
    чтобы PaperAdapter / реальный адаптер работали без правок.
    """
    symbol: str
    side: Side
    type: Literal["LIMIT", "MARKET"]
    qty: float
    price: Optional[float] = None
    time_in_force: Optional[str] = None
    reduce_only: bool = False
    client_order_id: Optional[str] = None


@dataclass
class OrderResp:
    order_id: str
    status: Literal["NEW", "PARTIALLY_FILLED", "FILLED", "CANCELED", "REJECTED"]
    filled_qty: float
    avg_px: Optional[float]
    ts: int


class ExecAdapter(Protocol):
    """
    Минимальный контракт адаптера.

    PaperAdapter его удовлетворяет.
    Реальный биржевой адаптер должен вести себя аналогично.
    """

    async def create_order(self, req: OrderReq) -> OrderResp:
        ...

    async def cancel_order(self, symbol: str, client_order_id: str) -> OrderResp:
        ...

    # Необязательный метод: если есть — используем для опроса лимитки.
    async def get_order(self, symbol: str, client_order_id: str) -> OrderResp:  # type: ignore[override]
        ...


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
    limit_oid: Optional[str]
    market_oid: Optional[str]
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
    if step is None or step <= 0:
        return float(val)
    v = float(val)
    if mode == "down":
        return math.floor(v / step) * step
    if mode == "up":
        return math.ceil(v / step) * step
    return round(v / step) * step


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

    # ------------------------------------------------------------------ Repo I/O

    def _repo_save_order(
        self,
        *,
        order_id: str,
        symbol: str,
        side: Side,
        otype: Literal["LIMIT", "MARKET"],
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
            # Fallback на лучшие цены
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

        # если был post-only кламп — явно помечаем это,
        # чтобы Worker._fee_bps_for_steps видел maker-like поведение
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

        lim = await self.a.create_order(limit_req)
        limit_oid: Optional[str] = lim.order_id
        market_oid: Optional[str] = None

        # Аудит: NEW (даже если FILLED сразу — дальше обновим/перекроем статус).
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

                # мгновенный fill по лимитке — по сути taker
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
                    state: OrderResp = await get_order(symbol, limit_coid)  # type: ignore[misc]
                except Exception:
                    continue

                part = _safe_float(state.filled_qty)
                avg = _safe_float(state.avg_px)

                if part > filled_qty:
                    delta = part - filled_qty
                    filled_qty = part
                    vwap_cash += delta * avg
                    steps.append(f"limit_partial:{delta}@{avg}")

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
                    avg_px = vwap_cash / max(filled_qty, 1e-12)

                    # явный maker-маркер для fee-аналитики
                    steps.append("limit_filled_resting")
                    steps.append(
                        f"limit_filled_total:{filled_qty}@{avg_px:.8f}"
                    )
                    latency_ms = int((time.monotonic() - t_start) * 1000)
                    steps.append(f"latency_ms:{latency_ms}")

        # --- 6. Таймаут: отменяем лимитку (если остался объём) ---
        if filled_qty < qty_rounded - 1e-12:
            steps.append("limit_cancel")
            with contextlib.suppress(Exception):
                cancel_resp = await self.a.cancel_order(symbol, limit_coid)
                part = _safe_float(cancel_resp.filled_qty)
                avg = _safe_float(cancel_resp.avg_px)

                if part > filled_qty:
                    delta = part - filled_qty
                    filled_qty = part
                    vwap_cash += delta * avg
                    steps.append(f"limit_partial_after_cancel:{delta}@{avg}")

                    fee_bps = self.c.fee_bps_maker
                    fee_usd = abs(delta * avg) * (fee_bps / 10_000.0)
                    self._repo_save_fill(
                        order_id=limit_coid,
                        px=avg,
                        qty=delta,
                        fee_usd=fee_usd,
                        ts_ms=cancel_resp.ts,
                    )

            # лимитка помечается CANCELED; fills уже есть — ок.
            self._repo_save_order(
                order_id=limit_coid,
                symbol=symbol,
                side=side,
                otype="LIMIT",
                reduce_only=bool(reduce_only),
                px=limit_px,
                qty=qty_rounded,
                status="CANCELED",
            )

        # --- 7. Остаток → MARKET (если есть смысл) ---
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

            # Аудит MARKET NEW заранее, чтобы REJECT был в истории.
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

            m = await self.a.create_order(mreq)
            market_oid = m.order_id

            if m.status in ("PARTIALLY_FILLED", "FILLED") and m.filled_qty > 0:
                part = _safe_float(m.filled_qty)
                avg = _safe_float(m.avg_px)
                if part > 0:
                    filled_qty += part
                    vwap_cash += part * avg
                    steps.append(f"market_filled:{part}@{avg}")

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
