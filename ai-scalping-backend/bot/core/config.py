from __future__ import annotations

import json
import os
import threading
from pathlib import Path
from typing import Optional, Dict
from pydantic import BaseModel, Field


class StreamsCfg(BaseModel):
    coalesce_ms: int = 75


class RiskCfg(BaseModel):
    risk_per_trade_pct: float = 0.25
    risk_per_trade_usd: float = 20.0  # Fixed USD risk per trade (for R-scaling with stop_distance_usd)
    daily_stop_r: float = -10.0
    daily_target_r: float = 15.0
    max_daily_loss_usd: float = 100.0  # Hard daily loss limit in USD (blocks new entries if realized_pnl_today <= -max_daily_loss_usd)
    max_consec_losses: int = 3
    cooldown_after_sl_s: int = 120
    min_risk_usd_floor: float = 0.25


class SafetyCfg(BaseModel):
    max_spread_ticks: int = 3
    min_top5_liquidity_usd: float = 300_000.0
    skip_funding_minute: bool = True
    skip_minute_zero: bool = True
    min_liq_buffer_sl_mult: float = 3.0


class FeesCfg(BaseModel):
    taker_rate: float = 0.0005
    maker_rate: float = 0.0002


class RegimeCfg(BaseModel):
    vol_window_s: int = 60


class MomentumCfg(BaseModel):
    obi_t: float = 0.12
    tp_r: float = 1.6
    stop_k_sigma: float = 1.1


class ReversionCfg(BaseModel):
    bb_z: float = 2.0  # Legacy: use bb_z_trigger instead
    bb_z_trigger: float = 1.2
    bb_k: float = 2.0
    stop_k_sigma: float = 0.45
    tp_rr_main: float = 1.6
    tp_r: float = 1.6  # Alias for tp_rr_main for compatibility
    max_mid_slope_bp: float = 45.0
    entry_band_buffer_bp: float = 20.0
    min_rr_after_fees: float = 0.9
    allow_micro_pierce: bool = True
    rsi: int = 70
    tp_to_vwap: bool = True


class BollingerBasicCfg(BaseModel):
    tp_pct: float = 0.004  # 0.4% take profit
    sl_pct: float = 0.0025  # 0.25% stop loss
    entry_threshold_pct: float = 0.0005  # 0.05% threshold for entry
    require_bandwidth_min: Optional[float] = None  # Minimum bandwidth (optional filter)
    entry_usd: float = 25.0  # Entry risk in USD (20-30 USD range)
    leverage: float = 10.0  # Leverage multiplier (10x default)


class StrategyCfg(BaseModel):
    regime: RegimeCfg = RegimeCfg()
    momentum: MomentumCfg = MomentumCfg()
    reversion: ReversionCfg = ReversionCfg()
    bollinger_basic: BollingerBasicCfg = BollingerBasicCfg()
    mode: str = "reversion_only"  # Strategy routing mode: "reversion_only" | "momentum_first" | "bollinger_basic" | etc.


class ExecutionCfg(BaseModel):
    limit_offset_ticks: int = 1
    limit_timeout_ms: int = 500
    max_slippage_bp: float = 6
    time_in_force: str = "GTC"
    fee_bps_maker: float = 1.0
    fee_bps_taker: float = 3.5
    min_stop_ticks: int = 2


class MLCfg(BaseModel):
    enabled: bool = False
    p_threshold_mom: float = 0.55
    p_threshold_rev: float = 0.60
    model_path: str = "./ml_artifacts/model.bin"


class AccountCfg(BaseModel):
    starting_equity_usd: float = 1000.0
    leverage: float = 15.0
    min_notional_usd: float = 5.0
    mode: str = "paper"                     # "paper" или "live"
    exchange: str = "binance_usdtm"         

    # опционально, чтобы не терять per_symbol_leverage из settings.json
    per_symbol_leverage: Dict[str, float] = Field(default_factory=dict)
    fees: FeesCfg = FeesCfg()


class Settings(BaseModel):
    mode: str = "paper"
    symbols: list[str] = Field(default_factory=lambda: ["GALAUSDT"])  # TEMP: single-symbol mode GALAUSDT, 10x leverage
    streams: StreamsCfg = StreamsCfg()
    risk: RiskCfg = RiskCfg()
    safety: SafetyCfg = SafetyCfg()
    strategy: StrategyCfg = StrategyCfg()
    execution: ExecutionCfg = ExecutionCfg()
    ml: MLCfg = MLCfg()
    account: AccountCfg = AccountCfg()
    log_dir: str = "./logs"
    log_level: str = "INFO"

    source_path: Optional[str] = None
    source_mtime: Optional[float] = None


_LOCK = threading.RLock()
_SETTINGS: Optional[Settings] = None


def _candidate_paths() -> list[Path]:
    env_path = os.getenv("SETTINGS_PATH")
    paths: list[Path] = []
    if env_path:
        paths.append(Path(env_path).expanduser().resolve())
    cwd = Path.cwd()
    paths.append((cwd / "settings.json").resolve())
    paths.append((cwd / "config" / "settings.json").resolve())
    return paths


def _load_from_path(p: Path) -> Settings:
    with p.open("r", encoding="utf-8") as f:
        data = json.load(f)
    s = Settings(**data)
    s.source_path = str(p)
    try:
        s.source_mtime = p.stat().st_mtime
    except Exception:
        s.source_mtime = None
    return s


def reload_settings() -> Settings:
    with _LOCK:
        for p in _candidate_paths():
            if p.exists():
                cfg = _load_from_path(p)
                globals()["_SETTINGS"] = cfg
                return cfg
        # ни один файл не найден — вернём дефолты, но явно укажем источник
        cfg = Settings()
        cfg.source_path = None
        cfg.source_mtime = None
        globals()["_SETTINGS"] = cfg
        return cfg


def get_settings() -> Settings:
    with _LOCK:
        global _SETTINGS
        if _SETTINGS is None:
            _SETTINGS = reload_settings()
        return _SETTINGS
