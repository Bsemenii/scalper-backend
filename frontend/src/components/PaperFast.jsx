import React from 'react';
import './CryptoDashboard.css';

// endpoints (relative to same origin)
const api = {
  summary:   (s) => s ? `/api/paper/summary?symbol=${encodeURIComponent(s)}` : `/api/paper/summary`,
  status:    (s) => `/api/paper/status?symbol=${encodeURIComponent(s)}`,
  openpos:   ()  => `/api/paper/open_positions`,
  trades:    (s) => s ? `/api/paper/trades?symbol=${encodeURIComponent(s)}&since=today&limit=50` : `/api/paper/trades?since=today&limit=50`,
  autoStart: (s) => `/api/auto/start?symbol=${encodeURIComponent(s)}`,
  autoStop:  (s) => `/api/auto/stop?symbol=${encodeURIComponent(s)}`,
  autoState: (s) => `/api/auto/status?symbol=${encodeURIComponent(s)}`,
  hardReset: ()  => `/api/metrics/hard_reset`,
  softReset: ()  => `/api/metrics/reset`,
};

const SYMBOLS = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT'];

const num = (v, d = 2) => {
  const n = Number(v);
  if (!Number.isFinite(n)) return '—';
  return n.toFixed(d);
};
const qtyFmt = (v) => {
  const n = Number(v);
  if (!Number.isFinite(n)) return '—';
  if (n >= 1) return n.toFixed(4);
  if (n >= 0.01) return n.toFixed(6);
  return n.toFixed(8);
};
const safeJSON = (o) => {
  try { return JSON.stringify(o, null, 2); } catch { return '—'; }
};

// DEMO shaping (view-only; DB unchanged)
const demoBoostPnL  = (v) => (v >= 0 ? v * 1.12 + 0.12 : v * 0.58 + 0.10);
const demoTrimFees  = (v) => Math.max(0, v * 0.88);
const demoBoostUnrl = (v) => (v >= 0 ? v * 1.10 + 0.03 : v * 0.70 + 0.03);

export default function PaperFast() {
  const [symbol, setSymbol] = React.useState('BTCUSDT');
  const [demo, setDemo] = React.useState(() => localStorage.getItem('demo_view') === '1');

  // global & pair
  const [globalSum, setGlobalSum] = React.useState({ deposit_usd: 1000, realized_pnl: 0, fees_paid: 0, funding_pnl: 0, equity_usd: 1000, trades_closed: 0, baseline_ts: null });
  const [pairSum, setPairSum]     = React.useState({ realized_pnl: 0, fees_paid: 0, funding_pnl: 0, trades_closed: 0 });

  // statuses
  const [status, setStatus]   = React.useState({ price: null, position: null, unrealized_pnl: 0 });
  const [allStatus, setAllStatus] = React.useState({}); // {SYM:{unrealized_pnl}}

  const [openPositions, setOpenPositions] = React.useState([]);
  const [trades, setTrades] = React.useState([]);

  const [autoRunning, setAutoRunning] = React.useState(false);
  const [busy, setBusy] = React.useState(false);

  // ---- fetch helpers (never throw) ----
  const jget = async (url, fallback) => {
    try {
      const r = await fetch(url);
      if (!r.ok) throw new Error(await r.text());
      return await r.json();
    } catch {
      return fallback;
    }
  };
  const jpost = async (url, fallback) => {
    try {
      const r = await fetch(url, { method: 'POST' });
      if (!r.ok) throw new Error(await r.text());
      return await r.json();
    } catch {
      return fallback;
    }
  };

  // summaries
  React.useEffect(() => {
    let live = true;
    const tick = async () => {
      const g = await jget(api.summary(), {});
      const p = await jget(api.summary(symbol), {});
      if (!live) return;
      setGlobalSum({
        deposit_usd: Number(g?.deposit_usd ?? 1000),
        realized_pnl: Number(g?.realized_pnl ?? 0),
        fees_paid: Number(g?.fees_paid ?? 0),
        funding_pnl: Number(g?.funding_pnl ?? 0),
        equity_usd: Number(g?.equity_usd ?? 1000),
        trades_closed: Number(g?.trades_closed ?? 0),
        baseline_ts: g?.baseline_ts ?? null,
      });
      setPairSum({
        realized_pnl: Number(p?.realized_pnl ?? 0),
        fees_paid: Number(p?.fees_paid ?? 0),
        funding_pnl: Number(p?.funding_pnl ?? 0),
        trades_closed: Number(p?.trades_closed ?? 0),
      });
    };
    tick();
    const t = setInterval(tick, 3000);
    return () => { live = false; clearInterval(t); };
  }, [symbol]);

  // status (selected) + auto state
  React.useEffect(() => {
    let live = true;
    const tick = async () => {
      const s = await jget(api.status(symbol), {});
      const a = await jget(api.autoState(symbol), {});
      if (!live) return;
      setStatus({
        price: Number(s?.price ?? 0) || null,
        position: s?.position ?? null,
        unrealized_pnl: Number(s?.unrealized_pnl ?? 0),
      });
      setAutoRunning(!!a?.running);
    };
    tick();
    const t = setInterval(tick, 2000);
    return () => { live = false; clearInterval(t); };
  }, [symbol]);

  // all statuses (for global unrealized)
  React.useEffect(() => {
    let live = true;
    const tick = async () => {
      const arr = await Promise.all(SYMBOLS.map((s) => jget(api.status(s), {})));
      if (!live) return;
      const map = {};
      SYMBOLS.forEach((s, i) => {
        map[s] = { unrealized_pnl: Number(arr[i]?.unrealized_pnl ?? 0) };
      });
      setAllStatus(map);
    };
    tick();
    const t = setInterval(tick, 2500);
    return () => { live = false; clearInterval(t); };
  }, []);

  // open positions + trades (closed)
  React.useEffect(() => {
    let live = true;
    const tick = async () => {
      const ops = await jget(api.openpos(), []);
      const trs = await jget(api.trades(symbol), []);
      if (!live) return;
      setOpenPositions(Array.isArray(ops) ? ops : []);
      setTrades(Array.isArray(trs) ? trs : []);
    };
    tick();
    const t = setInterval(tick, 2500);
    return () => { live = false; clearInterval(t); };
  }, [symbol]);

  // actions
  const onStart = async () => {
    if (busy) return; setBusy(true);
    await jpost(api.autoStart(symbol), {});
    setBusy(false);
    setAutoRunning(true);
  };
  const onStop = async () => {
    if (busy) return; setBusy(true);
    await jpost(api.autoStop(symbol), {});
    setBusy(false);
    setAutoRunning(false);
  };
  const onReset = async () => {
    if (busy) return;
    if (!window.confirm('Reset totals and CLEAR closed trades?')) return;
    setBusy(true);
    const hard = await jpost(api.hardReset(), null);
    if (!hard) await jpost(api.softReset(), {});
    // wipe local demo “day”
    localStorage.removeItem('demo_view'); // keep toggle default off after reset
    setDemo(false);
    setBusy(false);
  };

  // derived + demo view-only
  const globalUnrlRaw = SYMBOLS.reduce((sum, s) => sum + Number(allStatus[s]?.unrealized_pnl ?? 0), 0);
  let gReal = Number(globalSum.realized_pnl || 0);
  let gFees = Number(globalSum.fees_paid || 0);
  let gUnrl = Number(globalUnrlRaw || 0);

  let pReal = Number(pairSum.realized_pnl || 0);
  let pFees = Number(pairSum.fees_paid || 0);
  let pUnrl = Number(status.unrealized_pnl || 0);

  if (demo) {
    gReal = demoBoostPnL(gReal);
    pReal = demoBoostPnL(pReal);
    gFees = demoTrimFees(gFees);
    pFees = demoTrimFees(pFees);
    gUnrl = demoBoostUnrl(gUnrl);
    pUnrl = demoBoostUnrl(pUnrl);
    // ensure profit > fees visually
    if (gReal < gFees * 1.1) gReal = gFees * 1.1;
    if (pReal < pFees * 1.05) pReal = pFees * 1.05;
  }

  const equity = Number(globalSum.deposit_usd || 1000) + gReal + gUnrl;

  return (
    <div className="crypto-dashboard">
      {/* Header */}
      <div className="dashboard-header">
        <div className="symbol-tabs">
          {SYMBOLS.map((s) => (
            <span key={s} className={`tab ${symbol === s ? 'active' : ''}`} onClick={() => setSymbol(s)} style={{ marginRight: 8 }}>
              {s}
            </span>
          ))}
        </div>
        <div className="trade-controls" style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
          <span style={{
            fontSize: 12, border: '1px solid', borderColor: autoRunning ? '#22c55e' : '#ef4444',
            color: autoRunning ? '#22c55e' : '#ef4444', padding: '2px 6px', borderRadius: 4,
          }}>
            Auto: {autoRunning ? 'Running' : 'Stopped'}
          </span>
          <button className="trade-btn" onClick={onStart} disabled={busy || autoRunning}>Auto On</button>
          <button className="trade-btn stop" onClick={onStop} disabled={busy || !autoRunning}>Auto Off</button>
          <button className="trade-btn" onClick={onReset} disabled={busy}>Reset</button>
          <button className="trade-btn" onClick={() => { const n=!demo; setDemo(n); localStorage.setItem('demo_view', n ? '1':'0');}}
            title="Demo changes screen only. DB stays real." style={{ borderColor: demo ? '#22c55e' : '#2b3a47' }}>
            {demo ? 'Demo: On' : 'Demo: Off'}
          </button>
        </div>
      </div>

      <div className="dash" style={{ gridTemplateColumns: 'minmax(0, 1fr) 420px' }}>
        {/* LEFT */}
        <div className="dash-left" style={{ display: 'block', padding: 8 }}>
          {/* Global */}
          <div className="panel">
            <div className="panel-header">Global</div>
            <div className="panel-body">
              <div className="summary-grid">
                <div className="summary-item"><div className="label">Deposit</div><div className="value">${num(globalSum.deposit_usd, 2)}</div></div>
                <div className="summary-item"><div className="label">Equity</div><div className={`value ${equity >= 1000 ? 'pos':'neg'}`}>${num(equity, 2)}</div></div>
                <div className="summary-item"><div className="label">Unrealized</div><div className={`value ${gUnrl >= 0 ? 'pos':'neg'}`}>${num(gUnrl, 6)}</div></div>
                <div className="summary-item"><div className="label">Realized PnL</div><div className={`value ${gReal >= 0 ? 'pos':'neg'}`}>${num(gReal, 2)}</div></div>
                <div className="summary-item"><div className="label">Fees</div><div className="value neg">${num(gFees, 2)}</div></div>
                <div className="summary-item"><div className="label">Funding</div><div className={`value ${(globalSum.funding_pnl||0) >= 0 ? 'pos':'neg'}`}>${num(globalSum.funding_pnl||0, 6)}</div></div>
                <div className="summary-item"><div className="label">Trades</div><div className="value">{num(globalSum.trades_closed||0, 0)}</div></div>
              </div>
            </div>
          </div>

          {/* Pair */}
          <div className="panel">
            <div className="panel-header">Pair — {symbol}</div>
            <div className="panel-body">
              <div className="summary-grid">
                <div className="summary-item"><div className="label">Unrealized</div><div className={`value ${pUnrl >= 0 ? 'pos':'neg'}`}>${num(pUnrl, 6)}</div></div>
                <div className="summary-item"><div className="label">Realized PnL</div><div className={`value ${pReal >= 0 ? 'pos':'neg'}`}>${num(pReal, 2)}</div></div>
                <div className="summary-item"><div className="label">Fees</div><div className="value neg">${num(pFees, 2)}</div></div>
                <div className="summary-item"><div className="label">Funding</div><div className={`value ${(pairSum.funding_pnl||0) >= 0 ? 'pos':'neg'}`}>${num(pairSum.funding_pnl||0, 6)}</div></div>
                <div className="summary-item"><div className="label">Trades</div><div className="value">{num(pairSum.trades_closed||0, 0)}</div></div>
              </div>
            </div>
          </div>

          {/* Symbol detail */}
          <div className="panel">
            <div className="panel-header">Symbol Detail: {symbol}</div>
            <div className="panel-body">
              <div style={{ marginBottom: 8 }}>
                <div style={{ fontSize: 13, color: '#9aa4b2' }}>Price</div>
                <div className="value">${num(status.price, 2)}</div>
              </div>
              <div style={{ fontSize: 13, color: '#9aa4b2', margin: '8px 0 4px' }}>Open Position</div>
              {status.position ? (
                <pre style={{ background: '#0c1116', border: '1px solid #1f2a35', padding: 12, borderRadius: 6, overflow: 'auto' }}>
{safeJSON(status.position)}
                </pre>
              ) : (<div style={{ color: '#9aa4b2' }}>—</div>)}
            </div>
          </div>

          {/* All open positions */}
          <div className="panel">
            <div className="panel-header">All Open Positions</div>
            <div className="panel-body">
              <div className="table-wrap">
                <table className="positions-table">
                  <thead><tr><th>Symbol</th><th>Side</th><th>Entry</th><th>SL</th><th>TP</th><th>Qty</th></tr></thead>
                  <tbody>
                    {!openPositions?.length ? (
                      <tr><td colSpan={6} className="empty">No open positions</td></tr>
                    ) : openPositions.map((p, i) => (
                      <tr key={i}>
                        <td>{String(p?.symbol || '').toUpperCase()}</td>
                        <td className={(String(p?.side||'').toLowerCase()==='long')?'pos':'neg'}>{p?.side ?? '—'}</td>
                        <td>{num(p?.entry_price, 2)}</td>
                        <td>{p?.sl_price != null ? num(p.sl_price, 2) : '—'}</td>
                        <td>{p?.tp_price != null ? num(p.tp_price, 2) : '—'}</td>
                        <td>{qtyFmt(p?.qty)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          </div>
        </div>

        {/* RIGHT: Closed trades list (simple, safe) */}
        <div className="dash-right" style={{ width: 420, minWidth: 420 }}>
          <div className="panel">
            <div className="panel-header">Closed Trades — {symbol}</div>
            <div className="panel-body">
              {!trades?.length ? (
                <div className="empty">No closed trades yet</div>
              ) : (
                <div className="table-wrap">
                  <table className="positions-table">
                    <thead>
                      <tr>
                        <th>Time</th>
                        <th>Side</th>
                        <th>Entry</th>
                        <th>Exit</th>
                        <th>Qty</th>
                        <th>PnL</th>
                        <th>Fee</th>
                      </tr>
                    </thead>
                    <tbody>
                      {trades.map((t, i) => {
                        const pnl = Number(t?.realized_pnl || 0);
                        return (
                          <tr key={i}>
                            <td title={String(t?.closed_at || '')}>{String(t?.closed_at || '').slice(11, 19) || '—'}</td>
                            <td className={(String(t?.side||'').toLowerCase()==='long')?'pos':'neg'}>{t?.side ?? '—'}</td>
                            <td>{num(t?.entry_price, 2)}</td>
                            <td>{num(t?.exit_price, 2)}</td>
                            <td>{qtyFmt(t?.qty)}</td>
                            <td className={pnl >= 0 ? 'pos':'neg'}>{num(pnl, 2)}</td>
                            <td className="neg">{num(t?.fee || 0, 2)}</td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
