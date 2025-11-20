from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from sqlalchemy import text

from .db import get_engine


# ========= utils =========


def now_ms() -> int:
    """Текущее время в миллисекундах UTC."""
    return int(time.time() * 1000)


def day_utc_from_ms(ts_ms: int) -> str:
    """День в формате 'YYYY-MM-DD' по UTC для таймстампа в мс."""
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")


# ========= Trades =========


@dataclass
class TradeOpenCtx:
    symbol: str
    side: str           # "LONG" | "SHORT"
    qty: float
    entry_px: float
    sl_px: Optional[float]
    tp_px: Optional[float]
    reason_open: str = ""
    meta: Optional[Dict[str, Any]] = None


def open_trade(ctx: TradeOpenCtx) -> str:
    """
    Создаёт запись сделки при открытии позиции.
    Возвращает trade_id (uuid4).
    exit_* и pnl_* будут заполнены при закрытии.
    """
    trade_id = str(uuid.uuid4())
    eng = get_engine()

    entry_ts = now_ms()
    meta_json = json.dumps(ctx.meta or {}, ensure_ascii=False, default=str)

    with eng.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO trades (
                  id, symbol, side, qty,
                  entry_ts_ms, exit_ts_ms,
                  entry_px, exit_px,
                  sl_px, tp_px,
                  r, pnl_usd,
                  reason_open, reason_close, meta
                )
                VALUES (
                  :id, :symbol, :side, :qty,
                  :ets, 0,
                  :epx, 0,
                  :sl, :tp,
                  0, 0,
                  :ro, '', :meta
                )
                """
            ),
            dict(
                id=trade_id,
                symbol=ctx.symbol,
                side=ctx.side,
                qty=float(ctx.qty),
                ets=entry_ts,
                epx=float(ctx.entry_px),
                sl=(None if ctx.sl_px is None else float(ctx.sl_px)),
                tp=(None if ctx.tp_px is None else float(ctx.tp_px)),
                ro=ctx.reason_open or "",
                meta=meta_json,
            ),
        )

    return trade_id


def close_trade(
    trade_id: str,
    exit_px: float,
    r: float,
    pnl_usd: float,
    reason_close: str,
) -> None:
    """
    Закрывает сделку: проставляет exit_ts_ms, exit_px, r, pnl_usд, reason_close.
    """
    eng = get_engine()

    with eng.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE trades
                   SET exit_ts_ms   = :xms,
                       exit_px      = :xpx,
                       r            = :r,
                       pnl_usд      = :pnl,
                       reason_close = :rc
                 WHERE id = :id
                """
            ),
            dict(
                id=trade_id,
                xms=now_ms(),
                xpx=float(exit_px),
                r=float(r),
                pnl=float(pnl_usd),
                rc=reason_close or "",
            ),
        )


# ========= Orders / Fills =========


# Допустимые значения для валидации перед SQLite (чтобы ловить ошибки раньше, чем CHECK)
_ALLOWED_ORDER_TYPES = {
    "LIMIT",
    "MARKET",
    "STOP_MARKET",
    "TAKE_PROFIT_MARKET",
}

_ALLOWED_ORDER_SIDES = {"BUY", "SELL"}


def save_order(order: Dict[str, Any]) -> None:
    """
    Идемпотентная запись ордера (INSERT ... ON CONFLICT UPDATE).

    Ожидаемые поля в order:
      id          : str (clientOrderId)
      trade_id    : Optional[str]
      symbol      : str
      side        : "BUY" | "SELL"
      type        : "LIMIT" | "MARKET" | "STOP_MARKET" | "TAKE_PROFIT_MARKET"
      reduce_only : bool | int
      px          : Optional[float]  (None для MARKET)
      qty         : float
      status      : str
      created_ms  : int (опционально, по умолчанию now_ms())
      updated_ms  : int (опционально, по умолчанию now_ms())
    """
    eng = get_engine()

    side = str(order["side"]).upper()
    otype = str(order["type"]).upper()

    if side not in _ALLOWED_ORDER_SIDES:
        raise ValueError(f"save_order: invalid side={side!r} for order_id={order.get('id')}")
    if otype not in _ALLOWED_ORDER_TYPES:
        raise ValueError(f"save_order: invalid type={otype!r} for order_id={order.get('id')}")

    created_ms = int(order.get("created_ms", now_ms()))
    updated_ms = int(order.get("updated_ms", now_ms()))

    with eng.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO orders (
                  id, trade_id, symbol, side, type, reduce_only,
                  px, qty, status, created_ms, updated_ms
                )
                VALUES (
                  :id, :tid, :sym, :side, :type, :ro,
                  :px, :qty, :st, :cms, :ums
                )
                ON CONFLICT(id) DO UPDATE SET
                  trade_id   = excluded.trade_id,
                  px         = excluded.px,
                  qty        = excluded.qty,
                  status     = excluded.status,
                  updated_ms = excluded.updated_ms
                """
            ),
            dict(
                id=order["id"],
                tid=order.get("trade_id"),
                sym=order["symbol"],
                side=side,
                type=otype,
                ro=int(bool(order.get("reduce_only", 0))),
                px=order.get("px"),
                qty=float(order["qty"]),
                st=order["status"],
                cms=created_ms,
                ums=updated_ms,
            ),
        )


def save_fill(fill: Dict[str, Any]) -> None:
    """
    Запись исполнения ордера.

    Ожидаемые поля в fill:
      id       : str
      order_id : str
      ts_ms    : int (опционально, по умолчанию now_ms())
      px       : float
      qty      : float
      fee_usd  : float (опционально, по умолчанию 0.0)
    """
    eng = get_engine()

    ts_ms = int(fill.get("ts_ms", now_ms()))
    fee = float(fill.get("fee_usd", 0.0))

    with eng.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO fills (
                  id, order_id, ts_ms, px, qty, fee_usd
                )
                VALUES (:id, :oid, :t, :px, :q, :fee)
                """
            ),
            dict(
                id=fill["id"],
                oid=fill["order_id"],
                t=ts_ms,
                px=float(fill["px"]),
                q=float(fill["qty"]),
                fee=fee,
            ),
        )


# ========= Daily stats =========


def upsert_daily_stats(
    day_utc: str,
    delta_r: float,
    delta_usd: float,
    win: Optional[bool],
) -> None:
    """
    Обновляет агрегаты по дню (UTC). Если строки нет — создаёт.

    day_utc   : 'YYYY-MM-DD'
    delta_r   : результат сделки в R
    delta_usd : результат сделки в USD
    win       : True (вин), False (лосс), None (ни то ни другое — например, BE)
    """
    eng = get_engine()

    with eng.begin() as conn:
        row = conn.execute(
            text(
                """
                SELECT trades, wins, losses, pnl_usd, pnl_r, max_dd_r
                  FROM daily_stats
                 WHERE day_utc = :d
                """
            ),
            {"d": day_utc},
        ).fetchone()

        if row:
            trades, wins, losses, pnl_usd, pnl_r, max_dd_r = row

            trades = int(trades) + 1
            if win is True:
                wins = int(wins) + 1
            elif win is False:
                losses = int(losses) + 1

            pnl_usd = float(pnl_usd) + float(delta_usd)
            pnl_r = float(pnl_r) + float(delta_r)

            # max_dd_r — минимум по equity в R (чем меньше, тем глубже просадка)
            max_dd_r = min(float(max_dd_r), float(pnl_r))

            conn.execute(
                text(
                    """
                    UPDATE daily_stats
                       SET trades   = :t,
                           wins     = :w,
                           losses   = :l,
                           pnl_usд  = :u,
                           pnl_r    = :r,
                           max_dd_r = :dd
                     WHERE day_utc = :d
                    """
                ),
                dict(
                    t=trades,
                    w=wins,
                    l=losses,
                    u=pnl_usd,
                    r=pnl_r,
                    dd=max_dd_r,
                    d=day_utc,
                ),
            )
        else:
            conn.execute(
                text(
                    """
                    INSERT INTO daily_stats (
                      day_utc, trades, wins, losses, pnl_usд, pnl_r, max_dd_r
                    )
                    VALUES (:d, :t, :w, :l, :u, :r, :dd)
                    """
                ),
                dict(
                    d=day_utc,
                    t=1,
                    w=(1 if win else 0),
                    l=(1 if win is False else 0),
                    u=float(delta_usd),
                    r=float(delta_r),
                    dd=min(0.0, float(delta_r)),
                ),
            )
