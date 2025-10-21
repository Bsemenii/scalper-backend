# exec/fsm.py
from __future__ import annotations
import asyncio
from dataclasses import dataclass
from typing import Optional
from bot.core.types import PosState, PositionState, Side

@dataclass
class PosRuntime:
    state: PosState = PosState.FLAT
    entry_px: Optional[float] = None
    qty: float = 0.0
    sl: Optional[float] = None
    tp: Optional[float] = None
    rr: float = 0.0
    opened_ts: Optional[int] = None

class PositionFSM:
    def __init__(self):
        self._lock = asyncio.Lock()
        self.pos = PosRuntime()

    async def can_enter(self) -> bool:
        return self.pos.state == PosState.FLAT

    async def on_entering(self):
        self.pos.state = PosState.ENTERING

    async def on_open(self, entry_px: float, qty: float, sl: float, tp: Optional[float], rr: float, opened_ts: int):
        self.pos.state = PosState.OPEN
        self.pos.entry_px = entry_px
        self.pos.qty = qty
        self.pos.sl = sl
        self.pos.tp = tp
        self.pos.rr = rr
        self.pos.opened_ts = opened_ts

    async def on_exiting(self):
        self.pos.state = PosState.EXITING

    async def on_flat(self):
        self.pos = PosRuntime()

    def snapshot(self) -> PositionState:
        return PositionState(
            symbol="",  # заполним снаружи
            state=self.pos.state,
            entry_px=self.pos.entry_px or 0.0,
            qty=self.pos.qty,
            sl=self.pos.sl,
            tp=self.pos.tp,
            rr=self.pos.rr,
            opened_ts=self.pos.opened_ts or 0,
        )
