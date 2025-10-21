import json
import os
from functools import lru_cache
from typing import List, Optional

from pydantic import BaseModel, BaseSettings, Field, validator


class StreamsCfg(BaseModel):
    coalesce_ms: int = 75


class RiskCfg(BaseModel):
    risk_per_trade_pct: float = 0.25
    daily_stop_r: float = -2.0
    daily_target_r: float = 4.0
    max_consec_losses: int = 3
    cooldown_after_sl_s: int = 120


class SafetyCfg(BaseModel):
    max_spread_ticks: int = 3
    min_top5_liquidity_usd: int = 300_000
    skip_funding_minute: bool = True
    skip_minute_zero: bool = True
    min_liq_buffer_sl_mult: float = 3.0


class RegimeCfg(BaseModel):
    vol_window_s: int = 60


class MomentumCfg(BaseModel):
    obi_t: float = 0.10
    tp_r: float = 1.8
    stop_k_sigma: float = 1.0


class ReversionCfg(BaseModel):
    bb_z: float = 2.0
    rsi: int = 70
    tp_to_vwap: bool = True


class StrategyCfg(BaseModel):
    regime: RegimeCfg = RegimeCfg()
    momentum: MomentumCfg = MomentumCfg()
    reversion: ReversionCfg = ReversionCfg()


class ExecutionCfg(BaseModel):
    limit_offset_ticks: int = 1
    limit_timeout_ms: int = 500
    max_slippage_bp: int = 6


class MLCfg(BaseModel):
    enabled: bool = False
    p_threshold_mom: float = 0.55
    p_threshold_rev: float = 0.60
    model_path: str = "./ml_artifacts/model.bin"


class SettingsFile(BaseModel):
    mode: str = "paper"                 # paper|live
    symbols: List[str] = ["BTCUSDT"]
    streams: StreamsCfg = StreamsCfg()
    risk: RiskCfg = RiskCfg()
    safety: SafetyCfg = SafetyCfg()
    strategy: StrategyCfg = StrategyCfg()
    execution: ExecutionCfg = ExecutionCfg()
    ml: MLCfg = MLCfg()

    @validator("mode")
    def _mode(cls, v):
        v = (v or "").lower()
        if v not in ("paper", "live"):
            raise ValueError("mode must be 'paper' or 'live'")
        return v

    @validator("symbols")
    def _symbols(cls, v):
        if not v:
            raise ValueError("symbols must not be empty")
        return v


class EnvSettings(BaseSettings):
    # env overrides
    MODE: Optional[str] = Field(None, env="MODE")
    BINANCE_API_KEY: Optional[str] = None
    BINANCE_API_SECRET: Optional[str] = None
    LOG_DIR: str = "./logs"
    LOG_LEVEL: str = "INFO"

    class Config:
        case_sensitive = False


class Settings(BaseModel):
    # merged settings.json + env
    mode: str
    symbols: List[str]
    streams: StreamsCfg
    risk: RiskCfg
    safety: SafetyCfg
    strategy: StrategyCfg
    execution: ExecutionCfg
    ml: MLCfg

    # env-exposed
    binance_api_key: Optional[str] = None
    binance_api_secret: Optional[str] = None
    log_dir: str = "./logs"
    log_level: str = "INFO"


def _load_json_settings(path: str) -> SettingsFile:
    if not os.path.exists(path):
        raise FileNotFoundError(f"settings.json not found at {path}")
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return SettingsFile(**data)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    cfg_path = os.environ.get("SETTINGS_PATH", "./config/settings.json")
    file_cfg = _load_json_settings(cfg_path)
    env = EnvSettings()

    # env MODE overrides file mode if provided
    mode = (env.MODE or file_cfg.mode).lower()
    merged = Settings(
        mode=mode,
        symbols=file_cfg.symbols,
        streams=file_cfg.streams,
        risk=file_cfg.risk,
        safety=file_cfg.safety,
        strategy=file_cfg.strategy,
        execution=file_cfg.execution,
        ml=file_cfg.ml,
        binance_api_key=env.BINANCE_API_KEY,
        binance_api_secret=env.BINANCE_API_SECRET,
        log_dir=env.LOG_DIR,
        log_level=env.LOG_LEVEL,
    )
    return merged
