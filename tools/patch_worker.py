import io, os, re, sys, textwrap

WORKER_PATH = "bot/worker.py"

IMPORTS_TO_ADD = textwrap.dedent("""
    from exec.fsm import PositionFSM
    from bot.core.types import Side as CoreSide  # enum для FSM: CoreSide.BUY/CoreSide.SELL
""").strip()

FSM_FIELD_BLOCK = "self._fsm: Dict[str, PositionFSM] = {s: PositionFSM() for s in self.symbols}"

PLACE_ENTRY_NEW = textwrap.dedent(r'''
    async def place_entry(self, symbol: str, side: Side, qty: float) -> Dict[str, Any]:
        """
        Прямой тестовый вход (обходит стратегии) + риск-фильтры + постановка SL/TP (paper-план).
        FSM: FLAT -> ENTERING -> OPEN; таймаут -> EXITING -> FLAT.
        Возвращает словарь с ключом "report" для совместимости с API.
        """
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

            # --- FSM: ENTERING (мягко, без влияния на твою модель позиций)
            with contextlib.suppress(Exception):
                await self._fsm[symbol].on_entering()

            # ENTERING в твоём рантайме
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
                # FSM → FLAT
                with contextlib.suppress(Exception):
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
                # FSM → FLAT
                with contextlib.suppress(Exception):
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

            # --- FSM: OPEN (пишет сделку в SQLite)
            with contextlib.suppress(Exception):
                await self._fsm[symbol].on_open(
                    entry_px=entry_px,
                    qty=qty_open,
                    sl=float(plan.sl_px) if plan.sl_px is not None else entry_px,  # fallback, чтобы R не был NaN
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
''').strip("\n")

PAPER_CLOSE_NEW = textwrap.dedent(r'''
    async def _paper_close(self, sym: str, pos: PositionState, *, reason: str) -> None:
        close_side: Side = "SELL" if (pos.side or "BUY") == "BUY" else "BUY"
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

                # --- FSM: CLOSE (закрыть сделку в SQLite и обновить daily_stats)
                with contextlib.suppress(Exception):
                    await self._fsm[sym].on_close(exit_px=float(exit_px), reason_close=reason)

        finally:
            # Сброс позиции в FLAT
            self._pos[sym] = PositionState(
                state="FLAT", side=None, qty=0.0, entry_px=0.0,
                sl_px=None, tp_px=None, opened_ts_ms=0, timeout_ms=pos.timeout_ms
            )
            self._entry_steps.pop(sym, None)

            # На всякий — синхронизировать FSM
            with contextlib.suppress(Exception):
                await self._fsm[sym].on_flat()
''').strip("\n")

FLATTEN_NEW = textwrap.dedent(r'''
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
                with contextlib.suppress(Exception):
                    await self._fsm[symbol].on_flat()
                return {"ok": True, "symbol": symbol, "state": "FLAT", "reason": "no_open_position"}

            await self._paper_close(symbol, pos, reason="flatten_forced")
            return {"ok": True, "symbol": symbol, "state": "FLAT", "reason": "flatten_forced"}
''').strip("\n")

def read(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()

def write(path: str, data: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        f.write(data)

def ensure_imports(src: str) -> str:
    if "from exec.fsm import PositionFSM" in src and "from bot.core.types import Side as CoreSide" in src:
        return src
    # найдём блок импортов (после шебанга/ future и до первого пустого ряда класса/функции)
    insert_after = 0
    lines = src.splitlines()
    for i, line in enumerate(lines[:200]):
        if line.strip().startswith("import ") or line.strip().startswith("from "):
            insert_after = i
    if insert_after == 0:
        # вставим после верхних future/encoding
        header_block = 0
        for i, line in enumerate(lines[:50]):
            if line.strip() == "" or line.strip().startswith("import ") or line.strip().startswith("from "):
                header_block = i
        insert_after = header_block
    lines.insert(insert_after+1, IMPORTS_TO_ADD)
    return "\n".join(lines)

def ensure_fsm_field(src: str) -> str:
    if FSM_FIELD_BLOCK in src:
        return src
    # вставим после словаря локов/позиций — найдём создание поз словаря
    m = re.search(r"self\\._pos\\s*:\\s*Dict\\[str,\\s*PositionState\\][\\s\\S]*?\\n\\s*\\}", src)
    if not m:
        # fallback: после инициализации локов
        m = re.search(r"self\\._locks\\s*:\\s*Dict\\[str,\\s*asyncio\\.Lock\\]\\s*=\\s*\\{[\\s\\S]*?\\n\\s*\\}", src)
    if not m:
        # не нашли — добавим в конце __init__
        m = re.search(r"def\\s+__init__\\(.*?\\):[\\s\\S]*?\\n\\s*#\\s*пер-символьные\\s+таймштампы", src)
    if m:
        end = m.end()
        return src[:end] + "\n        " + FSM_FIELD_BLOCK + "\n" + src[end:]
    return src

def replace_function(src: str, func_name: str, new_body: str) -> str:
    # ищем def func_name( ... ):
    pattern = re.compile(rf"^\\s*async\\s+def\\s+{func_name}\\s*\\(.*?\\):[\\s\\S]*?(?=^\\s*async\\s+def\\s+|^\\s*def\\s+|^\\S)", re.MULTILINE)
    m = pattern.search(src)
    if not m:
        raise SystemExit(f"[patch] Не найден метод: {func_name}")
    # сохраним исходную ведущую табуляцию
    start_line = src[:m.start()].splitlines()[-1] if m.start() > 0 else ""
    indent = ""
    for ch in start_line:
        if ch in " \t":
            indent += ch
        else:
            indent = ""
    # выровняем новый текст под ту же глубину (обычно 4 пробела слева внутри класса)
    new_indented = textwrap.indent(new_body, " " * 4)
    return src[:m.start()] + new_indented + "\n\n" + src[m.end():]

def main():
    if not os.path.exists(WORKER_PATH):
        sys.exit(f"[patch] Файл не найден: {WORKER_PATH}")

    src = read(WORKER_PATH)

    # 1) импорт
    src = ensure_imports(src)

    # 2) поле FSM
    src = ensure_fsm_field(src)

    # 3) замены функций
    src = replace_function(src, "place_entry", PLACE_ENTRY_NEW)
    src = replace_function(src, "_paper_close", PAPER_CLOSE_NEW)
    src = replace_function(src, "flatten", FLATTEN_NEW)

    write(WORKER_PATH, src)
    print("[patch] OK: bot/worker.py пропатчен")

if __name__ == "__main__":
    main()
