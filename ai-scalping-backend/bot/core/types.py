from __future__ import annotations

from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, Field


class Side(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class PosState(str, Enum):
    FLAT = "FLAT"
    ENTERING = "ENTERING"
    OPEN = "OPEN"
    EXITING = "EXITING"


class Snapshot(BaseModel):
    symbol: str
    ts: int  # epoch ms
    mid: float
    bid1: float
    ask1: float
    spread_ticks: int
    top5_liq_usd: float
    trades_delta: float
    mark_price: float


class FeatureVec(BaseModel):
    symbol: str
    ts: int
    obi: float = 0.0
    microprice_drift: float = 0.0
    vwap_drift: float = 0.0
    ema9: float = 0.0
    ema21: float = 0.0
    ema_slope: float = 0.0
    bb_z: float = 0.0
    rsi: float = 50.0
    realized_vol: float = 0.0
    tick_vel: float = 0.0
    aggressor_ratio: float = 0.5


class EntryCandidate(BaseModel):
    symbol: str
    ts: int
    side: Side
    stop_distance: float  # in price units
    reason_tags: List[str] = Field(default_factory=list)
    features: FeatureVec


class OrderIntent(BaseModel):
    symbol: str
    side: Side
    qty: float
    limit_offset_ticks: int
    timeout_ms: int
    sl: Optional[float] = None
    tp: Optional[float] = None


class ExecutionStatus(str, Enum):
    FILLED = "FILLED"
    PARTIAL = "PARTIAL"
    CANCELED = "CANCELED"
    FAILED = "FAILED"


class ExecutionReport(BaseModel):
    order_id: str
    status: ExecutionStatus
    filled_qty: float
    avg_px: float
    ts: int
    details: Optional[str] = None


class PositionState(BaseModel):
    symbol: str
    state: PosState = PosState.FLAT
    entry_px: Optional[float] = None
    qty: float = 0.0
    sl: Optional[float] = None
    tp: Optional[float] = None
    rr: Optional[float] = None
    opened_ts: Optional[int] = None


class RiskState(BaseModel):
    daily_pnl_r: float = 0.0
    daily_pnl_usd: float = 0.0
    losses_streak: int = 0
    trading_enabled: bool = True


class Trade(BaseModel):
    id: str
    symbol: str
    side: Side
    opened_ts: int
    closed_ts: int
    entry_px: float
    exit_px: float
    pnl_r: float
    pnl_usd: float
    fees: float


class DailyStats(BaseModel):
    day: str  # YYYY-MM-DD
    symbol: str
    trades: int
    winrate: float
    avg_r: float
    pnl_r: float
    pnl_usd: float
    max_dd_r: float
