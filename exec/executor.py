# exec/executor.py
from __future__ import annotations

import asyncio
import contextlib
import logging
import math
import time
import uuid
from dataclasses import dataclass
from typing import Optional, Literal, Protocol, Callable, Tuple, Dict, Any

logger = logging.getLogger(__name__)
Side = Literal["BUY", "SELL"]


# ====== Public DTOs / Protocols (совместимо) =================================

@dataclass
class OrderReq:
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
    async def create_order(self, req: OrderReq) -> OrderResp: ...
    async def cancel_order(self, symbol: str, client_order_id: str) -> OrderResp: ...
    # optional:
    # async def get_order(self, symbol: str, client_order_id: str) -> OrderResp: ...


@dataclass
class ExecCfg:
    price_tick: float = 0.1
    qty_step: float = 0.001
    limit_offset_ticks: int = 1
    limit_timeout_ms: int = 500
    time_in_force: str = "GTC"
    poll_ms: int = 50
    prefer_maker: bool = False  # post-only эмуляция
    fee_bps_maker: float = 2.0  # опционально для save_fill (бумага)
    fee_bps_taker: float = 4.0  # опционально для save_fill (бумага)


@dataclass
class ExecutionReport:
    status: Literal["FILLED", "PARTIAL", "CANCELED"]
    filled_qty: float
    avg_px: Optional[float]
    limit_oid: Optional[str]
    market_oid: Optional[str]
    steps: list[str]
    ts: int


# ====== Local helpers ==========================================================

def _now_ms() -> int:
    return int(time.time() * 1000)


def _round_to_step(val: float, step: float, mode: Literal["down", "up", "nearest"] = "down") -> float:
    if step is None or step <= 0:
        return float(val)
    if mode == "down":
        return math.floor(val / step) * step
    if mode == "up":
        return math.ceil(val / step) * step
    return round(val / step) * step


def _round_price_to_tick(px: float, tick: float, side: Side) -> float:
    # для BUY — округляем вниз, для SELL — вверх, чтобы не переехать через лучшую цену
    mode: Literal["down", "up"] = "down" if side == "BUY" else "up"
    v = _round_to_step(px, tick, mode)
    return max(tick, v)


def _coid(prefix: str, symbol: str) -> str:
    # читаемый clientOrderId
    return f"{prefix}-{symbol}-{_now_ms()}"


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        v = float(x)
        if math.isnan(v) or math.isinf(v):
            return default
        return v
    except Exception:
        return default


# ====== Executor ===============================================================

class Executor:
    """
    Исполнение заявки: LIMIT -> (poll до timeout) -> cancel -> MARKET.
    Аудит в SQLite: save_order/save_fill — безопасно (если storage.repo нет, просто пропускаем).
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

    # -------- Repo hooks (safe) --------

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
        """Безопасно пишет orders; молча пропускает, если хранилище не подключено."""
        try:
            from storage.repo import save_order  # type: ignore
            save_order({
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
            })
        except ModuleNotFoundError:
            return
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
        """Безопасно пишет fills; молча пропускает, если хранилище не подключено."""
        try:
            from storage.repo import save_fill  # type: ignore
            fill_id = str(uuid.uuid4())
            save_fill({
                "id": fill_id,
                "order_id": order_id,
                "ts_ms": int(ts_ms or _now_ms()),
                "px": float(px),
                "qty": float(qty),
                "fee_usd": float(fee_usd),
            })
        except ModuleNotFoundError:
            return
        except Exception as e:
            logger.warning("save_fill failed: %s", e)

    # -------- Price helpers --------

    def _price_from_signed_offset(self, side: Side, bid: float, ask: float, ticks: int, tick: float) -> float:
        # универсальная функция смещения от лучшей цены с учётом стороны
        ticks = int(ticks)
        tick = max(float(tick), 0.0)
        if side == "BUY":
            if ticks >= 0:
                base = ask
                px = base + ticks * tick
            else:
                base = bid
                px = base - abs(ticks) * tick
        else:
            if ticks >= 0:
                base = bid
                px = base - ticks * tick
            else:
                base = ask
                px = base + abs(ticks) * tick
        return px

    def _enforce_post_only(self, side: Side, bid: float, ask: float, px: float, tick: float) -> tuple[float, Optional[str]]:
        # если prefer_maker=True — запрещаем пересечение спреда; прижимаем цену к лучшей пассивной
        note = None
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

    # -------- Main: entry placement --------

    async def place_entry(self, symbol: str, side: Side, qty: float, *, reduce_only: bool = False) -> ExecutionReport:
        """
        Создаёт лимитный вход, ждёт до timeout, затем отменяет и добирает остаток маркетом.
        Возвращает подробный отчёт по шагам. Параллельно пишет аудит в SQLite (если доступен).
        """
        steps: list[str] = []
        t_start = time.monotonic()

        # qty → шаг инструмента
        qty_raw = _safe_float(qty)
        qty_rounded = max(0.0, _round_to_step(qty_raw, self.c.qty_step, "down"))
        if qty_rounded <= 0:
            return ExecutionReport("CANCELED", 0.0, None, None, None, ["qty_rounded_to_zero"], _now_ms())

        # лучшая цена
        bid, ask = self.get_best(symbol)

        # лимитная цена (отступ в тиках)
        raw_px = self._price_from_signed_offset(side, bid, ask, self.c.limit_offset_ticks, self.c.price_tick)
        limit_px = _round_price_to_tick(_safe_float(raw_px), self.c.price_tick, side)
        limit_px, post_note = self._enforce_post_only(side, bid, ask, limit_px, self.c.price_tick)
        if post_note:
            steps.append(post_note)
        if limit_px <= 0:
            limit_px = max(self.c.price_tick, 1e-6)

        # лимитный ордер
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

        steps.append(f"limit_submit:{limit_px} qty:{qty_rounded}" + (" reduce_only" if reduce_only else ""))

        # --- отправка LIMIT
        lim = await self.a.create_order(limit_req)
        limit_oid: Optional[str] = lim.order_id
        market_oid: Optional[str] = None

        # аудит: ордер NEW
        self._repo_save_order(
            order_id=limit_coid,
            symbol=symbol,
            side=side,
            otype="LIMIT",
            reduce_only=False,
            px=limit_px,
            qty=qty_rounded,
            status="NEW",
        )

        filled_qty = 0.0
        vwap_cash = 0.0

        # если адаптер сразу дал частичное/полное исполнение
        if lim.status in ("PARTIALLY_FILLED", "FILLED") and lim.filled_qty > 0:
            part = _safe_float(lim.filled_qty)
            avg = _safe_float(lim.avg_px, default=0.0)
            if part > 0:
                filled_qty += part
                vwap_cash += part * avg
                steps.append(f"limit_filled_immediate:{part}@{avg}")
                # аудит fill
                self._repo_save_fill(order_id=limit_coid, px=avg, qty=part, fee_usd=0.0)

                # апдейт статуса ордера
                self._repo_save_order(
                    order_id=limit_coid,
                    symbol=symbol,
                    side=side,
                    otype="LIMIT",
                    reduce_only=False,
                    px=avg,
                    qty=qty_rounded,  # исходное qty
                    status=("FILLED" if lim.status == "FILLED" else "PARTIALLY_FILLED"),
                )

        # опрос до таймаута
        deadline = time.monotonic() + (self.c.limit_timeout_ms / 1000.0)
        get_order = getattr(self.a, "get_order", None)  # type: ignore[attr-defined]
        while time.monotonic() < deadline:
            await asyncio.sleep(max(0.0, (self.c.poll_ms or 50) / 1000.0))
            if not callable(get_order):
                continue
            try:
                state: OrderResp = await get_order(symbol, limit_coid)  # type: ignore[misc]
            except Exception:
                continue

            part = _safe_float(state.filled_qty)
            avg = _safe_float(state.avg_px, default=0.0)

            if part > filled_qty:
                # новая порция исполнения
                delta = part - filled_qty
                vwap_cash += delta * avg
                filled_qty = part
                steps.append(f"limit_partial:{delta}@{avg}")

                # аудит fill (частичный)
                self._repo_save_fill(order_id=limit_coid, px=avg, qty=delta, fee_usd=0.0)

                # обновим статус ордера до PARTIALLY_FILLED
                self._repo_save_order(
                    order_id=limit_coid,
                    symbol=symbol,
                    side=side,
                    otype="LIMIT",
                    reduce_only=False,
                    px=avg,
                    qty=qty_rounded,
                    status=("FILLED" if state.status == "FILLED" else "PARTIALLY_FILLED"),
                )

            if state.status == "FILLED":
                # лимит полностью исполнен — готовим репорт
                steps.append(f"limit_filled_total:{filled_qty}@{(vwap_cash/max(filled_qty,1e-12)):.8f}")
                avg_px = vwap_cash / max(filled_qty, 1e-12)
                latency_ms = int((time.monotonic() - t_start) * 1000)
                steps.append(f"latency_ms:{latency_ms}")

                # финальная фиксация ордера как FILLED (на всякий)
                self._repo_save_order(
                    order_id=limit_coid,
                    symbol=symbol,
                    side=side,
                    otype="LIMIT",
                    reduce_only=False,
                    px=avg_px,
                    qty=qty_rounded,
                    status="FILLED",
                )

                return ExecutionReport(
                    status="FILLED",
                    filled_qty=float(filled_qty),
                    avg_px=float(avg_px),
                    limit_oid=limit_oid,
                    market_oid=market_oid,
                    steps=steps,
                    ts=_now_ms(),
                )

        # таймаут: отменяем лимит
        steps.append("limit_cancel")
        with contextlib.suppress(Exception):
            cancel_resp = await self.a.cancel_order(symbol, limit_coid)
            part = _safe_float(cancel_resp.filled_qty)
            avg = _safe_float(cancel_resp.avg_px, default=0.0)
            if part > filled_qty:
                delta = part - filled_qty
                vwap_cash += delta * avg
                filled_qty = part
                steps.append(f"limit_partial_after_cancel:{delta}@{avg}")
                # аудит fill на долив после cancel
                self._repo_save_fill(order_id=limit_coid, px=avg, qty=delta, fee_usd=0.0)

        # помечаем лимит как отменённый
        self._repo_save_order(
            order_id=limit_coid,
            symbol=symbol,
            side=side,
            otype="LIMIT",
            reduce_only=False,
            px=limit_px,
            qty=qty_rounded,
            status="CANCELED",
        )

        # оставшийся объём добираем маркетом
        remaining = max(0.0, qty_rounded - filled_qty)
        if remaining > 0:
            steps.append(f"market_submit:{remaining}")
            market_coid = _coid("mkt", symbol)
            mreq = OrderReq(
                symbol=symbol,
                side=side,
                type="MARKET",
                qty=float(remaining),
                reduce_only=bool(reduce_only),
                client_order_id=market_coid,
            )
            steps.append(f"market_submit:{remaining}" + (" reduce_only" if reduce_only else ""))
            
            # аудит: создаём MARKET NEW (до отправки, чтобы был след даже при REJECT)
            self._repo_save_order(
                order_id=market_coid,
                symbol=symbol,
                side=side,
                otype="MARKET",
                reduce_only=False,
                px=None,
                qty=remaining,
                status="NEW",
            )

            m = await self.a.create_order(mreq)
            market_oid = m.order_id

            if m.status in ("PARTIALLY_FILLED", "FILLED") and m.filled_qty > 0:
                part = _safe_float(m.filled_qty)
                avg = _safe_float(m.avg_px, default=0.0)
                filled_qty += part
                vwap_cash += part * avg
                steps.append(f"market_filled:{part}@{avg}")

                # аудит fill маркетом
                # оценим «комиссию» для бумаги (не критично)
                fee_bps = self.c.fee_bps_taker
                fee_usd = (abs(part * avg) * (fee_bps / 10_000.0))
                self._repo_save_fill(order_id=market_coid, px=avg, qty=part, fee_usd=fee_usd)

                # и статус ордера
                self._repo_save_order(
                    order_id=market_coid,
                    symbol=symbol,
                    side=side,
                    otype="MARKET",
                    reduce_only=False,
                    px=avg,
                    qty=remaining,
                    status=("FILLED" if m.status == "FILLED" else "PARTIALLY_FILLED"),
                )

            elif m.status == "REJECTED":
                steps.append("market_rejected")
                # зафиксируем REJECTED
                self._repo_save_order(
                    order_id=market_coid,
                    symbol=symbol,
                    side=side,
                    otype="MARKET",
                    reduce_only=False,
                    px=None,
                    qty=remaining,
                    status="REJECTED",
                )

        # финальный статус
        avg_px: Optional[float] = (vwap_cash / max(filled_qty, 1e-12)) if filled_qty > 1e-12 else None
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
            symbol, side, qty_rounded, filled_qty, (None if avg_px is None else f"{avg_px:.8f}"), status, steps
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
