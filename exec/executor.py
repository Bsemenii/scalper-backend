# exec/executor.py
from __future__ import annotations

import asyncio
import contextlib
import logging
import math
import time
from dataclasses import dataclass
from typing import Optional, Literal, Protocol, Callable, Tuple, Dict, Any

logger = logging.getLogger(__name__)
Side = Literal["BUY", "SELL"]

# ====== Контракты низкого уровня (не Pydantic) ======

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
    """Мини-контракт исполнителя, который покрывают Paper/Live-адаптеры."""
    async def create_order(self, req: OrderReq) -> OrderResp: ...
    async def cancel_order(self, symbol: str, client_order_id: str) -> OrderResp: ...
    # Опционально: если адаптер реализует get_order — будем «пуллить» частичные fill в окне таймаута
    # async def get_order(self, symbol: str, client_order_id: str) -> OrderResp: ...


# ====== Конфиг и отчёт ======

@dataclass
class ExecCfg:
    price_tick: float = 0.1       # шаг цены
    qty_step: float = 0.001       # шаг количества
    limit_offset_ticks: int = 1   # на сколько тиков от best
    limit_timeout_ms: int = 500   # ожидание лимита (окно before cancel)
    time_in_force: str = "GTC"    # GTC/IOC/FOK
    poll_ms: int = 50             # период опроса частичных исполнений (если доступен get_order)


@dataclass
class ExecutionReport:
    status: Literal["FILLED", "PARTIAL", "CANCELED"]
    filled_qty: float
    avg_px: Optional[float]
    limit_oid: Optional[str]
    market_oid: Optional[str]
    steps: list[str]
    ts: int


# ====== Утилиты ======

def _now_ms() -> int:
    return int(time.time() * 1000)


def _round_to_step(val: float, step: float, mode: Literal["down", "up", "nearest"] = "down") -> float:
    if step is None or step <= 0:
        return float(val)
    if mode == "down":
        return math.floor(val / step) * step
    if mode == "up":
        return math.ceil(val / step) * step
    # nearest
    return round(val / step) * step


def _round_price_to_tick(px: float, tick: float, side: Side) -> float:
    """
    Безопасное округление цены по шагу в «выгодную» сторону:
    BUY -> down (не дороже), SELL -> up (не дешевле).
    """
    mode: Literal["down", "up"] = "down" if side == "BUY" else "up"
    v = _round_to_step(px, tick, mode)
    return max(tick, v)


def _coid(prefix: str, symbol: str) -> str:
    return f"{prefix}-{symbol}-{_now_ms()}"


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        v = float(x)
        if math.isnan(v) or math.isinf(v):
            return default
        return v
    except Exception:
        return default


# ====== Исполнитель ======

class Executor:
    """
    Политика входа:
      1) LIMIT @ (best ± offset*tick), с корректным округлением цены/qty
      2) ждём limit_timeout_ms, опционально «пуллим» частичные fill (если есть adapter.get_order)
      3) CANCEL лимита, учитываем возможные дорезки между submit и cancel
      4) MARKET остатка
      5) Консолидация VWAP, подробные шаги в steps
    """

    def __init__(
        self,
        adapter: ExecAdapter,
        cfg: ExecCfg,
        get_best: Callable[[str], Tuple[float, float]],
    ) -> None:
        self.a = adapter
        self.c = cfg
        self.get_best = get_best  # (symbol) -> (bid, ask)

    async def place_entry(self, symbol: str, side: Side, qty: float) -> ExecutionReport:
        steps: list[str] = []
        t_start = time.monotonic()

        # 1) Округляем qty по шагу
        qty_raw = _safe_float(qty)
        qty_rounded = max(0.0, _round_to_step(qty_raw, self.c.qty_step, "down"))
        if qty_rounded <= 0:
            logger.info("[exec] qty rounded to zero; skip. qty_raw=%s step=%s", qty_raw, self.c.qty_step)
            return ExecutionReport(
                status="CANCELED",
                filled_qty=0.0,
                avg_px=None,
                limit_oid=None,
                market_oid=None,
                steps=["qty_rounded_to_zero"],
                ts=_now_ms(),
            )

        # 2) Целевая лимит-цена от best (ask для BUY, bid для SELL) с оффсетом в тиках
        bid, ask = self.get_best(symbol)
        if side == "BUY":
            ref_px = _safe_float(ask, default=_safe_float(bid, 0.0))
            raw_px = ref_px + max(0, int(self.c.limit_offset_ticks)) * max(self.c.price_tick, 0.0)
        else:
            ref_px = _safe_float(bid, default=_safe_float(ask, 0.0))
            raw_px = ref_px - max(0, int(self.c.limit_offset_ticks)) * max(self.c.price_tick, 0.0)

        limit_px = _round_price_to_tick(raw_px, max(self.c.price_tick, 0.0), side)
        if limit_px <= 0:
            limit_px = max(self.c.price_tick, 0.0) or 1e-6  # страховка от нулевой цены

        limit_coid = _coid("lim", symbol)
        limit_req = OrderReq(
            symbol=symbol,
            side=side,
            type="LIMIT",
            qty=float(qty_rounded),
            price=float(limit_px),
            time_in_force=self.c.time_in_force or "GTC",
            client_order_id=limit_coid,
        )

        steps.append(f"limit_submit:{limit_px} qty:{qty_rounded}")
        lim = await self.a.create_order(limit_req)
        limit_oid: Optional[str] = lim.order_id
        market_oid: Optional[str] = None

        # Агрегаторы VWAP по всем частям
        filled_qty = 0.0
        vwap_cash = 0.0

        # Учтём моментальные частичные/полные исполнения из create_order
        if lim.status in ("PARTIALLY_FILLED", "FILLED") and lim.filled_qty > 0:
            part = _safe_float(lim.filled_qty)
            avg = _safe_float(lim.avg_px, default=0.0)
            filled_qty += part
            vwap_cash += part * avg
            steps.append(f"limit_filled_immediate:{part}@{avg}")

        # 3) Окно ожидания исполнения лимита с возможным polling (если адаптер умеет get_order)
        deadline = time.monotonic() + (self.c.limit_timeout_ms / 1000.0)
        get_order = getattr(self.a, "get_order", None)  # type: ignore[attr-defined]
        while time.monotonic() < deadline:
            await asyncio.sleep(max(0.0, (self.c.poll_ms or 50) / 1000.0))
            if not callable(get_order):
                continue
            try:
                state: OrderResp = await get_order(symbol, limit_coid)  # type: ignore[misc]
            except Exception:
                # опрос не обязателен; не считаем это ошибкой
                continue
            # консолидируем новые частичные
            part = _safe_float(state.filled_qty)
            avg = _safe_float(state.avg_px, default=0.0)
            if part > filled_qty:
                delta = part - filled_qty
                vwap_cash += delta * avg
                filled_qty = part
                steps.append(f"limit_partial:{delta}@{avg}")
            if state.status == "FILLED":
                steps.append(f"limit_filled_total:{filled_qty}@{(vwap_cash/max(filled_qty,1e-12)):.8f}")
                avg_px = vwap_cash / max(filled_qty, 1e-12)
                latency_ms = int((time.monotonic() - t_start) * 1000)
                steps.append(f"latency_ms:{latency_ms}")
                return ExecutionReport(
                    status="FILLED",
                    filled_qty=float(filled_qty),
                    avg_px=float(avg_px),
                    limit_oid=limit_oid,
                    market_oid=market_oid,
                    steps=steps,
                    ts=_now_ms(),
                )

        # 4) Отмена лимитного ордера (и учёт дорезок до момента cancel)
        steps.append("limit_cancel")
        with contextlib.suppress(Exception):
            cancel_resp = await self.a.cancel_order(symbol, limit_coid)
            # если между submit и cancel успели дорезать часть — добавим
            part = _safe_float(cancel_resp.filled_qty)
            avg = _safe_float(cancel_resp.avg_px, default=0.0)
            if part > filled_qty:
                delta = part - filled_qty
                vwap_cash += delta * avg
                filled_qty = part
                steps.append(f"limit_partial_after_cancel:{delta}@{avg}")

        # 5) Добиваем остаток маркетом
        remaining = max(0.0, qty_rounded - filled_qty)
        if remaining > 0:
            steps.append(f"market_submit:{remaining}")
            mreq = OrderReq(symbol=symbol, side=side, type="MARKET", qty=float(remaining))
            m = await self.a.create_order(mreq)
            market_oid = m.order_id
            if m.status in ("PARTIALLY_FILLED", "FILLED") and m.filled_qty > 0:
                part = _safe_float(m.filled_qty)
                avg = _safe_float(m.avg_px, default=0.0)
                filled_qty += part
                vwap_cash += part * avg
                steps.append(f"market_filled:{part}@{avg}")
            elif m.status == "REJECTED":
                steps.append("market_rejected")

        # Финальная консолидация и статус
        avg_px = (vwap_cash / max(filled_qty, 1e-12)) if filled_qty > 0 else None
        if filled_qty <= 1e-12:
            status: Literal["FILLED", "PARTIAL", "CANCELED"] = "CANCELED"
        elif filled_qty < qty_rounded - 1e-12:
            status = "PARTIAL"
        else:
            status = "FILLED"

        latency_ms = int((time.monotonic() - t_start) * 1000)
        steps.append(f"latency_ms:{latency_ms}")

        # Логи (DEBUG) — удобно при разборе проблем
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
