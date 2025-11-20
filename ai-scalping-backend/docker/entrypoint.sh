#!/usr/bin/env bash
set -euo pipefail

APP_DIR="/app"
CONF_DIR="$APP_DIR/config"
DATA_DIR="$APP_DIR/data"
LOGS_DIR="$APP_DIR/logs"
DEFAULT_CONF="$APP_DIR/docker/default_settings.json"
TARGET_CONF="$CONF_DIR/settings.json"

echo "[bootstrap] ensure folders..."
mkdir -p "$CONF_DIR" "$DATA_DIR" "$LOGS_DIR"

echo "[bootstrap] ensure settings.json..."
if [ ! -f "$TARGET_CONF" ]; then
  if [ -f "$DEFAULT_CONF" ]; then
    cp "$DEFAULT_CONF" "$TARGET_CONF"
    echo "[bootstrap] default settings copied to $TARGET_CONF"
  else
    cat > "$TARGET_CONF" <<'JSON'
{
  "mode": "paper",
  "symbols": ["BTCUSDT","ETHUSDT","SOLUSDT"],
  "streams": { "coalesce_ms": 75 },
  "risk": { "risk_per_trade_pct": 0.10, "daily_stop_r": -4, "daily_target_r": 6, "max_consec_losses": 3, "cooldown_after_sl_s": 90, "min_risk_usd_floor": 1.5 },
  "safety": { "max_spread_ticks": 8, "min_top5_liquidity_usd": 300000, "skip_funding_minute": true, "skip_minute_zero": true, "min_liq_buffer_sl_mult": 3 },
  "execution": { "limit_offset_ticks": 1, "limit_timeout_ms": 2000, "max_slippage_bp": 4, "time_in_force": "GTX", "fee_bps_maker": 0.0, "fee_bps_taker": 0.0, "min_stop_ticks": 24 },
  "ml": { "enabled": false, "p_threshold_mom": 0.55, "p_threshold_rev": 0.60, "model_path": "./ml_artifacts/model.bin" },
  "account": { "starting_equity_usd": 1000, "leverage": 1, "min_notional_usd": 5 },
  "log_dir": "./logs",
  "log_level": "INFO"
}
JSON
    echo "[bootstrap] generated minimal settings.json"
  fi
fi

echo "[bootstrap] starting app..."
exec uvicorn bot.api.app:app --host 0.0.0.0 --port 8010
