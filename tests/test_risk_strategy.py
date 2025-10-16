# tests/test_risk_strategy.py
import pytest
from risk.filters import SafetyCfg, LimitsCfg, LimitsState, evaluate_safety
from risk.sizing import calc_qty
from strategy.momentum import signal_momentum, MomentumCfg
from strategy.reversion import signal_reversion, ReversionCfg

def _micro(mid=100.0, spread=1.0, liq=1_000_000.0, obi=0.2):
    return {"mid": mid, "spread_ticks": spread, "top_liq_usd": liq, "obi": obi}

def _indi(ema9=101, ema21=100, vwap_drift=1.0, bb_z=0.0, rv_bp=20.0):
    return {"ema9": ema9, "ema21": ema21, "vwap_drift": vwap_drift, "bb_z": bb_z, "realized_vol_bp": rv_bp}

def test_safety_ok():
    ok, reasons = evaluate_safety(micro=_micro(spread=1.0, liq=1_000_000), indi={}, safety=SafetyCfg())
    assert ok and reasons == []

def test_safety_spread_liq_fail():
    ok, reasons = evaluate_safety(micro=_micro(spread=10.0, liq=10_000), indi={}, safety=SafetyCfg())
    assert not ok and "spread" in reasons and "liq" in reasons

def test_limits_cooldown():
    st = LimitsState()
    cfg = LimitsCfg(cooldown_after_sl_s=5, max_consec_losses=3)
    now = 1_000_000
    st.on_trade_closed(-0.5, now, cfg)
    ok, rs = st.can_trade(now + 1000, cfg)
    assert not ok and "cooldown" in rs

def test_sizing_min_qty():
    q = calc_qty(equity_usd=1000, risk_per_trade_pct=0.5, stop_usd=10, min_qty=0.01, step_qty=0.001)
    assert q > 0

def test_momentum_long():
    c = signal_momentum(symbol="X", micro=_micro(obi=0.3), indi=_indi(ema9=101, ema21=100, vwap_drift=0.5), cfg=MomentumCfg(obi_t=0.1))
    assert c and c.side == "LONG"

def test_reversion_short():
    c = signal_reversion(symbol="X", micro=_micro(), indi=_indi(bb_z=2.5), cfg=ReversionCfg(bb_z=2.0))
    assert c and c.side == "SHORT"
