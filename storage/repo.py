from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from sqlalchemy import text

from .db import get_engine


def now_ms() -> int:
    return int(time.time() * 1000)


def day_utc_from_ms(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")


# -------- Trades --------

@dataclass
class TradeOpenCtx:
    symbol: str
    side: str  # "LONG" | "SHORT"
    qty: float
    entry_px: float
    sl_px: Optional[float]
    tp_px: Optional[float]
    reason_open: str = ""
    meta: Optional[Dict[str, Any]] = None


def open_trade(ctx: TradeOpenCtx) -> str:
    """
    Создаёт запись сделки при открытии позиции. Возвращает trade_id (uuid).
    exit_* и pnl_* будут заполнены при закрытии.
    """
    trade_id = str(uuid.uuid4())
    eng = get_engine()
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
                ets=now_ms(),
                epx=float(ctx.entry_px),
                sl=(None if ctx.sl_px is None else float(ctx.sl_px)),
                tp=(None if ctx.tp_px is None else float(ctx.tp_px)),
                ro=ctx.reason_open or "",
                meta=json.dumps(ctx.meta or {}),
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
    Закрывает сделку: проставляет exit_ts_ms/exit_px/r/pnl_usd/reason_close.
    """
    eng = get_engine()
    with eng.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE trades
                   SET exit_ts_ms = :xms,
                       exit_px    = :xpx,
                       r          = :r,
                       pnl_usd    = :pnl,
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


# -------- Orders / Fills --------

def save_order(order: Dict[str, Any]) -> None:
    """
    Идемпотентная запись ордера (INSERT ON CONFLICT UPDATE).
    Ожидаемые поля:
      id, trade_id (опционально), symbol, side("BUY"/"SELL"), type("LIMIT"/"MARKET"),
      reduce_only(0/1), px (nullable для MARKET), qty, status, created_ms, updated_ms
    """
    eng = get_engine()
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
                side=order["side"],
                type=order["type"],
                ro=int(bool(order.get("reduce_only", 0))),
                px=order.get("px"),
                qty=float(order["qty"]),
                st=order["status"],
                cms=int(order.get("created_ms", now_ms())),
                ums=int(order.get("updated_ms", now_ms())),
            ),
        )


def save_fill(fill: Dict[str, Any]) -> None:
    """
    Запись исполнения. Поля:
      id, order_id, ts_ms, px, qty, fee_usd (опционально)
    """
    eng = get_engine()
    with eng.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO fills (id, order_id, ts_ms, px, qty, fee_usd)
                VALUES (:id, :oid, :t, :px, :q, :fee)
                """
            ),
            dict(
                id=fill["id"],
                oid=fill["order_id"],
                t=int(fill.get("ts_ms", now_ms())),
                px=float(fill["px"]),
                q=float(fill["qty"]),
                fee=float(fill.get("fee_usd", 0.0)),
            ),
        )


# -------- Daily stats --------

def upsert_daily_stats(day_utc: str, delta_r: float, delta_usd: float, win: Optional[bool]) -> None:
    """
    Обновляет агрегаты по дню (UTC). Если строки нет — создаёт.
    """
    eng = get_engine()
    with eng.begin() as conn:
        row = conn.execute(
            text(
                "SELECT trades, wins, losses, pnl_usd, pnl_r, max_dd_r FROM daily_stats WHERE day_utc = :d"
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
            max_dd_r = min(float(max_dd_r), float(pnl_r))
            conn.execute(
                text(
                    """
                    UPDATE daily_stats
                       SET trades = :t,
                           wins   = :w,
                           losses = :l,
                           pnl_usd = :u,
                           pnl_r   = :r,
                           max_dd_r = :dd
                     WHERE day_utc = :d
                    """
                ),
                dict(t=trades, w=wins, l=losses, u=pnl_usd, r=pnl_r, dd=max_dd_r, d=day_utc),
            )
        else:
            conn.execute(
                text(
                    """
                    INSERT INTO daily_stats (day_utc, trades, wins, losses, pnl_usd, pnl_r, max_dd_r)
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
