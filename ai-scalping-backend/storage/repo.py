from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional, List

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
                  r, pnl_usd, pnl_r, fees_usd,
                  reason_open, reason_close, meta
                )
                VALUES (
                  :id, :symbol, :side, :qty,
                  :ets, 0,
                  :epx, 0,
                  :sl, :tp,
                  0, 0, 0, 0,
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
    Закрывает сделку: проставляет exit_ts_ms, exit_px, r, pnl_usd, pnl_r, fees_usd, reason_close.

    pnl_usd пересчитывается здесь по факту:
      - по entry_px/qty/side из trades (side может быть "LONG"/"SHORT" или "BUY"/"SELL"),
      - по суммарным fee_usd из fills, привязанных к этому trade_id через orders.

    Аргумент pnl_usd, переданный из воркера, используется только как fallback,
    если не удалось прочитать данные из БД.
    """
    import logging

    logger = logging.getLogger(__name__)
    eng = get_engine()

    with eng.begin() as conn:
        # 1. Загружаем сделку одним SELECT (получаем entry_px, qty, side, symbol)
        trade_row = conn.execute(
            text(
                """
                SELECT entry_px, qty, side, symbol
                  FROM trades
                 WHERE id = :id
                """
            ),
            {"id": trade_id},
        ).fetchone()

        entry_px_val: Optional[float] = None
        qty_val: Optional[float] = None
        side_raw: Optional[str] = None
        symbol: Optional[str] = None

        if trade_row is not None:
            try:
                if hasattr(trade_row, "_mapping"):
                    mapping = trade_row._mapping
                    entry_px_val = float(mapping["entry_px"]) if mapping["entry_px"] is not None else None
                    qty_val = float(mapping["qty"]) if mapping["qty"] is not None else None
                    side_raw = (mapping["side"] or "").upper()
                    symbol = mapping.get("symbol")
                else:
                    entry_px_val = float(trade_row[0]) if trade_row[0] is not None else None
                    qty_val = float(trade_row[1]) if trade_row[1] is not None else None
                    side_raw = (trade_row[2] or "").upper()
                    symbol = trade_row[3] if len(trade_row) > 3 else None
            except (IndexError, KeyError, ValueError, TypeError) as e:
                logger.warning(
                    "[close_trade] Failed to parse trade row for trade_id=%s: %s. "
                    "Falling back to pnl_usd argument.",
                    trade_id, e,
                )
                entry_px_val = None
                qty_val = None
                side_raw = None

        # 2. Нормализуем сторону: "LONG"→BUY, "SHORT"→SELL, также принимаем BUY/SELL
        direction: Optional[str] = None
        if side_raw in ("BUY", "LONG"):
            direction = "BUY"
        elif side_raw in ("SELL", "SHORT"):
            direction = "SELL"
        else:
            if trade_row is not None:
                logger.warning(
                    "[close_trade] Invalid or missing side='%s' for trade_id=%s. "
                    "Falling back to pnl_usd argument.",
                    side_raw, trade_id,
                )

        # 3. Считаем суммарные комиссии из fills через JOIN с orders
        fee_sum_usd = 0.0
        try:
            fee_row = conn.execute(
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

            if fee_row is not None:
                if hasattr(fee_row, "_mapping"):
                    fee_sum_usd = float(fee_row._mapping["fee_sum"] or 0.0)
                else:
                    fee_sum_usd = float(fee_row[0] or 0.0)
        except Exception as e:
            logger.warning(
                "[close_trade] Failed to compute fee_sum_usd for trade_id=%s: %s. Using 0.0.",
                trade_id, e,
            )
            fee_sum_usd = 0.0

        # 4. Считаем pnl_gross по entry/exit/qty/direction из Binance данных
        pnl_gross: Optional[float] = None
        if entry_px_val is not None and qty_val is not None and direction in ("BUY", "SELL"):
            if direction == "BUY":
                # LONG: прибыль когда exit_px > entry_px
                pnl_gross = (float(exit_px) - entry_px_val) * qty_val
            else:
                # SHORT: прибыль когда exit_px < entry_px
                pnl_gross = (entry_px_val - float(exit_px)) * qty_val

        # 5. Вычисляем итоговый net PnL
        if pnl_gross is not None:
            pnl_net = pnl_gross - fee_sum_usd
        else:
            logger.warning(
                "[close_trade] Could not compute pnl_gross for trade_id=%s. "
                "Using fallback pnl_usd=%s.",
                trade_id, pnl_usd,
            )
            pnl_net = float(pnl_usd)

        # 6. Обновляем запись сделки в trades
        exit_ts_ms = now_ms()
        conn.execute(
            text(
                """
                UPDATE trades
                   SET exit_ts_ms   = :xms,
                       exit_px      = :xpx,
                       r            = :r,
                       pnl_r        = :r,
                       pnl_usd      = :pnl,
                       fees_usd     = :fees,
                       reason_close = :rc
                 WHERE id = :id
                """
            ),
            dict(
                id=trade_id,
                xms=exit_ts_ms,
                xpx=float(exit_px),
                r=float(r),
                pnl=pnl_net,
                fees=fee_sum_usd,
                rc=reason_close or "",
            ),
        )

        # 7. Обновляем daily_stats: единственное место где мы учитываем закрытые сделки
        try:
            day_utc = day_utc_from_ms(exit_ts_ms)

            win: Optional[bool] = None
            if pnl_net > 0.01:
                win = True
            elif pnl_net < -0.01:
                win = False

            upsert_daily_stats(
                day_utc=day_utc,
                delta_r=float(r),
                delta_usd=pnl_net,
                win=win,
            )
            logger.info(
                "[close_trade] Updated daily_stats for %s: trade_id=%s, pnl_usd=%.4f, r=%.4f, win=%s",
                day_utc, trade_id, pnl_net, r, win,
            )
        except Exception as e_stats:
            logger.error(
                "[close_trade] Failed to update daily_stats for trade_id=%s: %s",
                trade_id, e_stats,
                exc_info=True,
            )


def get_trade(trade_id: str) -> Optional[Dict[str, Any]]:
    """
    Возвращает сделку по id как dict или None, если не найдено.
    """
    import logging
    logger = logging.getLogger(__name__)
    
    if not trade_id or not isinstance(trade_id, str):
        logger.warning("[get_trade] Invalid trade_id: %r", trade_id)
        return None
    
    eng = get_engine()
    try:
        with eng.begin() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT
                      id, symbol, side, qty,
                      entry_ts_ms, exit_ts_ms,
                      entry_px, exit_px,
                      sl_px, tp_px,
                      r, pnl_usd, pnl_r, fees_usd,
                      reason_open, reason_close,
                      meta
                    FROM trades
                    WHERE id = :id
                    """
                ),
                {"id": str(trade_id)},
            ).fetchone()

        if row is None:
            return None

        # Convert SQLAlchemy Row to dict properly
        d: Dict[str, Any] = {}
        try:
            if hasattr(row, "_mapping"):
                d = dict(row._mapping)
            elif hasattr(row, "_asdict"):
                d = row._asdict()
            elif hasattr(row, "_fields"):
                # Named tuple style
                d = {field: getattr(row, field) for field in row._fields}
            else:
                # Fallback: manual mapping by index
                if len(row) >= 17:
                    d = {
                        "id": row[0],
                        "symbol": row[1],
                        "side": row[2],
                        "qty": row[3],
                        "entry_ts_ms": row[4],
                        "exit_ts_ms": row[5],
                        "entry_px": row[6],
                        "exit_px": row[7],
                        "sl_px": row[8],
                        "tp_px": row[9],
                        "r": row[10],
                        "pnl_usd": row[11],
                        "pnl_r": row[12],
                        "fees_usd": row[13],
                        "reason_open": row[14],
                        "reason_close": row[15],
                        "meta": row[16],
                    }
                else:
                    logger.error("[get_trade] Row has unexpected length: %d", len(row))
                    return None
        except Exception as e:
            logger.error("[get_trade] Failed to convert row to dict for trade_id=%s: %s", trade_id, e, exc_info=True)
            return None
        
        # Parse meta JSON if present
        meta_raw = d.get("meta")
        if isinstance(meta_raw, str) and meta_raw:
            try:
                d["meta"] = json.loads(meta_raw)
            except Exception:
                pass

        return d
    except Exception as e:
        logger.error("[get_trade] Database error for trade_id=%s: %s", trade_id, e, exc_info=True)
        return None


def load_recent_trades(limit: int = 100) -> List[Dict[str, Any]]:
    """
    Возвращает последние N сделок (по entry_ts_ms DESC) как список dict.
    Используется в diag() воркера для отображения real PnL.
    """
    eng = get_engine()
    limit = int(limit) if limit and limit > 0 else 100

    with eng.begin() as conn:
        rows = conn.execute(
            text(
                """
                SELECT
                  id, symbol, side, qty,
                  entry_ts_ms, exit_ts_ms,
                  entry_px, exit_px,
                  sl_px, tp_px,
                  r, pnl_usd, pnl_r, fees_usd,
                  reason_open, reason_close,
                  meta
                FROM trades
                ORDER BY entry_ts_ms DESC
                LIMIT :limit
                """
            ),
            {"limit": limit},
        ).fetchall()

    result: List[Dict[str, Any]] = []
    for row in rows:
        # Convert SQLAlchemy Row to dict properly
        if hasattr(row, "_mapping"):
            d = dict(row._mapping)
        elif hasattr(row, "_asdict"):
            d = row._asdict()
        else:
            # Fallback: manual mapping
            d = {
                "id": row[0] if len(row) > 0 else None,
                "symbol": row[1] if len(row) > 1 else None,
                "side": row[2] if len(row) > 2 else None,
                "qty": row[3] if len(row) > 3 else None,
                "entry_ts_ms": row[4] if len(row) > 4 else None,
                "exit_ts_ms": row[5] if len(row) > 5 else None,
                "entry_px": row[6] if len(row) > 6 else None,
                "exit_px": row[7] if len(row) > 7 else None,
                "sl_px": row[8] if len(row) > 8 else None,
                "tp_px": row[9] if len(row) > 9 else None,
                "r": row[10] if len(row) > 10 else None,
                "pnl_usd": row[11] if len(row) > 11 else None,
                "pnl_r": row[12] if len(row) > 12 else None,
                "fees_usd": row[13] if len(row) > 13 else None,
                "reason_open": row[14] if len(row) > 14 else None,
                "reason_close": row[15] if len(row) > 15 else None,
                "meta": row[16] if len(row) > 16 else None,
            }
        
        meta_raw = d.get("meta")
        if isinstance(meta_raw, str) and meta_raw:
            try:
                d["meta"] = json.loads(meta_raw)
            except Exception:
                pass
        result.append(d)

    return result


def attach_orders_to_trade(trade_id: str, order_ids: List[str]) -> None:
    """
    Привязка существующих ордеров к сделке.

    Используется после того, как:
      - trade_id уже создан (open_trade),
      - ордера (entry / SL / TP / exit) уже сохранены в таблицу orders.

    Обновляет orders.trade_id по списку id.
    """
    if not trade_id:
        return
    if not order_ids:
        return

    ids = [str(oid) for oid in order_ids if oid]
    if not ids:
        return

    eng = get_engine()

    with eng.begin() as conn:
        for oid in ids:
            try:
                conn.execute(
                    text(
                        """
                        UPDATE orders
                           SET trade_id = :tid
                         WHERE id = :oid
                        """
                    ),
                    dict(tid=trade_id, oid=oid),
                )
            except Exception as e:
                # Логируем, но не роняем приложение
                print(f"[storage] attach_orders_to_trade failed for trade_id={trade_id} oid={oid}: {e}")


# ========= Orders / Fills =========


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
    delta_usд : результат сделки в USD
    win       : True (вин), False (лосс), None (ни то ни другое — например, BE)
    """
    import logging
    import time
    
    logger = logging.getLogger(__name__)
    eng = get_engine()

    # Retry logic for database locking with exponential backoff
    max_retries = 10
    base_delay = 0.05
    
    for attempt in range(max_retries):
        try:
            # Use a shorter transaction to reduce lock contention
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

                    max_dd_r = min(float(max_dd_r), float(pnl_r))

                    conn.execute(
                        text(
                            """
                            UPDATE daily_stats
                               SET trades   = :t,
                                   wins     = :w,
                                   losses   = :l,
                                   pnl_usd  = :u,
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
                    trades = 1
                    wins = 1 if win is True else 0
                    losses = 1 if win is False else 0
                    pnl_usd = float(delta_usd)
                    pnl_r = float(delta_r)
                    max_dd_r = min(0.0, float(delta_r))

                    conn.execute(
                        text(
                            """
                            INSERT INTO daily_stats (
                              day_utc, trades, wins, losses, pnl_usd, pnl_r, max_dd_r
                            )
                            VALUES (:d, :t, :w, :l, :u, :r, :dd)
                            """
                        ),
                        dict(
                            d=day_utc,
                            t=trades,
                            w=wins,
                            l=losses,
                            u=pnl_usd,
                            r=pnl_r,
                            dd=max_dd_r,
                        ),
                    )
            # Success - break out of retry loop
            return
        except Exception as e:
            if "locked" in str(e).lower() and attempt < max_retries - 1:
                retry_delay = base_delay * (2 ** attempt)
                logger.warning(
                    "[upsert_daily_stats] Database locked (attempt %d/%d), retrying in %.3fs...",
                    attempt + 1, max_retries, retry_delay
                )
                time.sleep(retry_delay)
            else:
                logger.error(
                    "[upsert_daily_stats] Failed to update daily_stats for %s: %s",
                    day_utc, e,
                    exc_info=True,
                )
                # Don't raise - this is non-critical
                return
