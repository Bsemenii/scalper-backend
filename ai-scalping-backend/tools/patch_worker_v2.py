import os, sys, textwrap, re

PATH = "bot/worker.py"

# --- новые версии методов (без ведущих отступов) ---

PLACE_ENTRY_NEW = """
async def place_entry(self, symbol: str, side: Side, qty: float) -> Dict[str, Any]:
    \"""
    Прямой тестовый вход (обходит стратегии) + риск-фильтры + постановка SL/TP (paper-план).
    FSM: FLAT -> ENTERING -> OPEN; таймаут -> EXITING -> FLAT.
    Возвращает словарь с ключом "report" для совместимости с API.
    \"""
    symbol = symbol.upper()
    if symbol not in self.execs:
        raise ValueError(f"Unsupported symbol: {symbol}")

    # === 0) дневные лимиты (до захвата мьютекса) ===
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
        return {"ok": False, "reason": ",".join(["day_"+r for r in day_dec.reasons]), "symbol": symbol, "side": side, "qty": qty}

    lock = self._locks[symbol]
    async with lock:
        pos = self._pos[symbol]
        if pos.state != "FLAT":
            self._inc_block_reason("busy_" + pos.state)
            return {"ok": False, "reason": f"busy:{pos.state}", "symbol": symbol, "side": side, "qty": qty}

        # === 1) safety-фильтры до входа ===
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
            return {"ok": False, "reason": ",".join(pre_dec.reasons), "symbol": symbol, "side": side, "qty": qty}

        # --- FSM: ENTERING (мягко)
        import contextlib as _ctx
        with _ctx.suppress(Exception):
            await self._fsm[symbol].on_entering()

        # ENTERING в рантайме
        pos.state = "ENTERING"
        pos.side = side

        # === 2) исполнение ===
        rep: ExecutionReport = await self.execs[symbol].place_entry(symbol, side, qty)
        self._accumulate_exec_counters(rep.steps)

        if rep.status not in ("FILLED", "PARTIAL") or rep.filled_qty <= 0.0 or rep.avg_px is None:
            # не вошли — откат в FLAT
            self._pos[symbol] = PositionState(
                state="FLAT", side=None, qty=0.0, entry_px=0.0,
                sl_px=None, tp_px=None, opened_ts_ms=0, timeout_ms=pos.timeout_ms
            )
            with _ctx.suppress(Exception):
                await self._fsm[symbol].on_flat()
            return {
                "symbol": symbol,
                "side": side,
                "qty": qty,
                "report": {
                    "status": rep.status,
                    "filled_qty": rep.filled_qty,
                    "avg_px": rep.avg_px,
                    "limit_oid": rep.limit_oid,
                    "market_oid": rep.market_oid,
                    "steps": rep.steps,
                    "ts": rep.ts,
                },
            }

        # === 3) расчёт SL/TP и post safety-проверка ===
        entry_px = float(rep.avg_px)
        qty_open = float(rep.filled_qty)

        min_stop_ticks = self._min_stop_ticks_default()
        sl_distance_px = max(min_stop_ticks * spec.price_tick, max(spread_ticks, 1.0) * spec.price_tick)

        plan: SLTPPlan = compute_sltp(
            side=side, entry_px=entry_px, qty=qty_open,
            price_tick=spec.price_tick, sl_distance_px=sl_distance_px, rr=1.8
        )

        post_dec = check_entry_safety(
            side,
            micro=micro,
            time_ctx=tctx,
            pos_ctx=PositionalCtx(entry_px=entry_px, sl_px=plan.sl_px, leverage=self._leverage),
            safety=self._safety_cfg,
        )
        if not post_dec.allow:
            for r in post_dec.reasons:
                self._inc_block_reason(r)
            self._pos[symbol] = PositionState(
                state="FLAT", side=None, qty=0.0, entry_px=0.0,
                sl_px=None, tp_px=None, opened_ts_ms=0, timeout_ms=pos.timeout_ms
            )
            with _ctx.suppress(Exception):
                await self._fsm[symbol].on_flat()
            return {
                "ok": False,
                "reason": ",".join(post_dec.reasons),
                "symbol": symbol, "side": side, "qty": qty,
                "report": {
                    "status": rep.status,
                    "filled_qty": rep.filled_qty,
                    "avg_px": rep.avg_px,
                    "limit_oid": rep.limit_oid,
                    "market_oid": rep.market_oid,
                    "steps": rep.steps + ["blocked_after_plan:" + "|".join(post_dec.reasons)],
                    "ts": rep.ts,
                },
            }

        # === 4) открываем позицию ===
        opened_ts_ms = self._now_ms()
        self._pos[symbol] = PositionState(
            state="OPEN", side=side, qty=qty_open, entry_px=entry_px,
            sl_px=plan.sl_px, tp_px=plan.tp_px, opened_ts_ms=opened_ts_ms, timeout_ms=pos.timeout_ms
        )
        self._entry_steps[symbol] = list(rep.steps)

        # --- FSM: OPEN (запись в БД)
        with _ctx.suppress(Exception):
            await self._fsm[symbol].on_open(
                entry_px=entry_px,
                qty=qty_open,
                sl=float(plan.sl_px) if plan.sl_px is not None else entry_px,
                tp=float(plan.tp_px) if plan.tp_px is not None else None,
                rr=1.8,
                opened_ts=opened_ts_ms,
                symbol=symbol,
                side=(CoreSide.BUY if side == "BUY" else CoreSide.SELL),
                reason_open="strategy:auto",
                meta={"steps": self._entry_steps.get(symbol, [])},
            )

        return {
            "symbol": symbol,
            "side": side,
            "qty": qty,
            "report": {
                "status": rep.status,
                "filled_qty": qty_open,
                "avg_px": entry_px,
                "limit_oid": rep.limit_oid,
                "market_oid": rep.market_oid,
                "steps": rep.steps + [f"protection_set:SL@{plan.sl_px} TP@{plan.tp_px}"],
                "ts": self._now_ms(),
            },
        }
""".strip("\n")

PAPER_CLOSE_NEW = """
async def _paper_close(self, sym: str, pos: PositionState, *, reason: str) -> None:
    close_side: Side = "SELL" if (pos.side or "BUY") == "BUY" else "BUY"
    import contextlib as _ctx
    try:
        ex = self.execs[sym]
        rep: ExecutionReport = await ex.place_entry(sym, close_side, pos.qty)
        self._accumulate_exec_counters(rep.steps)

        exit_px = rep.avg_px
        if exit_px is not None:
            entry_steps = self._entry_steps.get(sym, [])
            trade = self._build_trade_record(
                sym, pos, float(exit_px),
                reason=reason, entry_steps=entry_steps, exit_steps=list(rep.steps),
            )
            self._register_trade(trade)

            # --- FSM: CLOSE (запись в БД и daily_stats)
            with _ctx.suppress(Exception):
                await self._fsm[sym].on_close(exit_px=float(exit_px), reason_close=reason)

    finally:
        # Сброс позиции в FLAT
        self._pos[sym] = PositionState(
            state="FLAT", side=None, qty=0.0, entry_px=0.0,
            sl_px=None, tp_px=None, opened_ts_ms=0, timeout_ms=pos.timeout_ms
        )
        self._entry_steps.pop(sym, None)
        with _ctx.suppress(Exception):
            await self._fsm[sym].on_flat()
""".strip("\n")

FLATTEN_NEW = """
async def flatten(self, symbol: str) -> Dict[str, Any]:
    symbol = symbol.upper()
    lock = self._locks[symbol]
    import contextlib as _ctx
    async with lock:
        pos = self._pos[symbol]
        if pos.state != "OPEN":
            self._pos[symbol] = PositionState(
                state="FLAT", side=None, qty=0.0, entry_px=0.0,
                sl_px=None, tp_px=None, opened_ts_ms=0, timeout_ms=pos.timeout_ms
            )
            with _ctx.suppress(Exception):
                await self._fsm[symbol].on_flat()
            return {"ok": True, "symbol": symbol, "state": "FLAT", "reason": "no_open_position"}

        await self._paper_close(symbol, pos, reason="flatten_forced")
        return {"ok": True, "symbol": symbol, "state": "FLAT", "reason": "flatten_forced"}
""".strip("\n")

def load() -> str:
    if not os.path.exists(PATH):
        sys.exit(f"[patch] файл не найден: {PATH}")
    with open(PATH, "r", encoding="utf-8") as f:
        return f.read()

def save(s: str):
    with open(PATH, "w", encoding="utf-8") as f:
        f.write(s)

def find_class_block(src: str, class_name: str) -> tuple[int, int, int]:
    m = re.search(rf"^class\\s+{class_name}\\b.*?:\\s*$", src, re.MULTILINE)
    if not m:
        sys.exit(f"[patch] класс не найден: {class_name}")
    start = m.start()
    # найти конец класса: следующая строка без отступа, начинающаяся НЕ с декоратора/def/класса, или EOF
    # проще: конец — перед следующей строкой, начинающейся с 'class ' на колонке 0 или EOF
    m2 = re.search(r"^class\\s+\\w+\\b.*?:\\s*$", src[m.end():], re.MULTILINE)
    end = m.end() + (m2.start() if m2 else len(src[m.end():]))
    # глубина отступа методов в классе (обычно 4)
    indent = 4
    return start, end, indent

def replace_method_in_class(src: str, class_name: str, method_name: str, new_src: str) -> str:
    cs, ce, base_indent = find_class_block(src, class_name)
    class_body = src[cs:ce]
    # Метод на уровне класса: строка "async def method_name(" с отступом base_indent
    pat = re.compile(rf"^\\s{{{base_indent}}}async\\s+def\\s+{method_name}\\s*\\(.*?\\):[\\s\\S]*?(?=^\\s{{{base_indent}}}(?:async\\s+def|def)\\s+|^\\S|\\Z)", re.MULTILINE)
    m = pat.search(class_body)
    if not m:
        # Попробуем искать без привязки к exact indent (на случай 8 пробелов)
        pat2 = re.compile(rf"^\\s+async\\s+def\\s+{method_name}\\s*\\(.*?\\):[\\s\\S]*?(?=^\\s+async\\s+def\\s+|^\\s+def\\s+|^\\S|\\Z)", re.MULTILINE)
        m = pat2.search(class_body)
        if not m:
            sys.exit(f"[patch] метод не найден: {method_name}")

    # Выровнять новый метод с тем же начальным отступом
    # Определим фактический отступ существующего метода
    line_start = class_body.rfind("\\n", 0, m.start()) + 1
    existing_indent = len(class_body[line_start:m.start()]) - len(class_body[line_start:m.start()].lstrip(" "))
    new_indented = textwrap.indent(new_src.strip("\\n") + "\\n", " " * existing_indent)

    new_class_body = class_body[:m.start()] + new_indented + class_body[m.end():]
    return src[:cs] + new_class_body + src[ce:]

def main():
    s = load()
    s = replace_method_in_class(s, "Worker", "place_entry", PLACE_ENTRY_NEW)
    s = replace_method_in_class(s, "Worker", "_paper_close", PAPER_CLOSE_NEW)
    s = replace_method_in_class(s, "Worker", "flatten", FLATTEN_NEW)
    save(s)
    print("[patch v2] OK: методы заменены (place_entry, _paper_close, flatten)")

if __name__ == "__main__":
    main()
