// src/pages/PaperPage.jsx
import React, { useEffect, useState } from 'react';
import CandleChart from '../components/CandleChart';
import {
  getPnlNow,
  getTrades,
} from '../services/cryptoApi';
import './PaperPage.css';

const SYMBOLS = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT'];

const fmtNumber = (v, digits = 2) => {
  if (v == null || Number.isNaN(v)) return '—';
  return Number(v).toFixed(digits);
};

const fmtTs = (ts) => {
  if (!ts) return '—';
  const date = new Date(ts);
  if (Number.isNaN(date.getTime())) return '—';
  return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
};

const PaperPage = () => {
  const [activeSymbol, setActiveSymbol] = useState('BTCUSDT');
  const [pnlNow, setPnlNow] = useState(null);
  const [trades, setTrades] = useState([]);
  const [loadingTrades, setLoadingTrades] = useState(true);

  // PnL now (день + unrealized)
  useEffect(() => {
    let alive = true;

    const load = async () => {
      try {
        const data = await getPnlNow();
        if (!alive) return;
        setPnlNow(data);
      } catch (e) {
        console.error('Failed to load pnl/now', e);
      }
    };

    load();
    const id = setInterval(load, 4000);

    return () => {
      alive = false;
      clearInterval(id);
    };
  }, []);

  // Trades для активного символу
  useEffect(() => {
    let alive = true;

    const loadTrades = async () => {
      setLoadingTrades(true);
      try {
        const { items } = await getTrades({ symbol: activeSymbol, limit: 300 });
        if (!alive) return;
        setTrades(items || []);
        setLoadingTrades(false);
      } catch (e) {
        console.error('Failed to load trades', e);
        setLoadingTrades(false);
      }
    };

    loadTrades();
    const id = setInterval(loadTrades, 5000);

    return () => {
      alive = false;
      clearInterval(id);
    };
  }, [activeSymbol]);

  const day = pnlNow?.pnl_day?.day ?? '';
  const pnlUsd = pnlNow?.pnl_day?.pnl_usd ?? 0;
  const feesUsd = pnlNow?.pnl_day?.fees_usd ?? 0;
  const grossPnlUsd = pnlUsd + feesUsd;
  const pnlR = pnlNow?.pnl_day?.pnl_r ?? 0;
  const tradesCount = pnlNow?.pnl_day?.trades ?? 0;
  const winrate = pnlNow?.pnl_day?.winrate;
  const avgR = pnlNow?.pnl_day?.avg_r;
  const maxDdR = pnlNow?.pnl_day?.max_dd_r;
  const unrealizedTotal = pnlNow?.unrealized?.total_usd ?? 0;
  const openPositions = pnlNow?.open_positions ?? 0;

  return (
    <div className="pp-root">
      <div className="pp-header">
        <div className="pp-header-left">
          <h1 className="pp-title">Trading Dashboard</h1>
          <span className="pp-subtitle">
            {day && <>Session: <strong>{day}</strong></>}
          </span>
        </div>

        <div className="pp-header-right">
          <div className="pp-metric">
            <span className="pp-metric-label">PnL Net (USD)</span>
            <span className={`pp-metric-value ${pnlUsd >= 0 ? 'pp-pos' : 'pp-neg'}`}>
              {fmtNumber(pnlUsd, 2)}
            </span>
          </div>
          <div className="pp-metric">
            <span className="pp-metric-label">PnL Gross (USD)</span>
            <span className={`pp-metric-value ${grossPnlUsd >= 0 ? 'pp-pos' : 'pp-neg'}`}>
              {fmtNumber(grossPnlUsd, 2)}
            </span>
          </div>
          <div className="pp-metric">
            <span className="pp-metric-label">Fees Paid (USD)</span>
            <span className="pp-metric-value" style={{ color: '#ef4444' }}>
              {fmtNumber(feesUsd, 2)}
            </span>
          </div>
          <div className="pp-metric">
            <span className="pp-metric-label">PnL (R)</span>
            <span className={`pp-metric-value ${pnlR >= 0 ? 'pp-pos' : 'pp-neg'}`}>
              {fmtNumber(pnlR, 2)}
            </span>
          </div>
          <div className="pp-metric">
            <span className="pp-metric-label">Avg R</span>
            <span className="pp-metric-value">{avgR != null ? fmtNumber(avgR, 2) : '—'}</span>
          </div>
          <div className="pp-metric">
            <span className="pp-metric-label">Winrate</span>
            <span className="pp-metric-value">
              {winrate != null ? `${(winrate * 100).toFixed(1)}%` : '—'}
            </span>
          </div>
          <div className="pp-metric">
            <span className="pp-metric-label">Max DD (R)</span>
            <span className="pp-metric-value">
              {maxDdR != null ? fmtNumber(maxDdR, 2) : '—'}
            </span>
          </div>
          <div className="pp-metric">
            <span className="pp-metric-label">Trades</span>
            <span className="pp-metric-value">{tradesCount}</span>
          </div>
          <div className="pp-metric">
            <span className="pp-metric-label">uPnL (USD)</span>
            <span className={`pp-metric-value ${unrealizedTotal >= 0 ? 'pp-pos' : 'pp-neg'}`}>
              {fmtNumber(unrealizedTotal, 2)}
            </span>
          </div>
          <div className="pp-metric">
            <span className="pp-metric-label">Open positions</span>
            <span className="pp-metric-value">{openPositions}</span>
          </div>
        </div>
      </div>

      <div className="pp-toolbar">
        <div className="pp-symbol-toggle">
          {SYMBOLS.map((sym) => (
            <button
              key={sym}
              type="button"
              className={`pp-symbol-btn ${sym === activeSymbol ? 'pp-symbol-btn-active' : ''}`}
              onClick={() => setActiveSymbol(sym)}
            >
              {sym}
            </button>
          ))}
        </div>
      </div>

      <div className="pp-content">
        <div className="pp-left">
          <CandleChart
            symbol={activeSymbol}
            timeframe="1m"
            title={`${activeSymbol}`}
            trades={trades}
          />
        </div>

        <div className="pp-right">
          <div className="pp-trades-header">
            <span className="pp-trades-title">Closed trades — {activeSymbol}</span>
            {loadingTrades && <span className="pp-trades-loading">Refreshing…</span>}
          </div>

          <div className="pp-trades-table-wrapper">
            <table className="pp-trades-table">
              <thead>
                <tr>
                  <th>Opened</th>
                  <th>Side</th>
                  <th>Qty</th>
                  <th>Entry</th>
                  <th>Exit</th>
                  <th>PnL $</th>
                  <th>PnL R</th>
                  <th>Fees $</th>
                  <th>Reason</th>
                </tr>
              </thead>
              <tbody>
                {trades.length === 0 && !loadingTrades && (
                  <tr>
                    <td colSpan={9} className="pp-trades-empty">
                      No trades yet for {activeSymbol}.
                    </td>
                  </tr>
                )}
                {trades.map((t, idx) => (
                  <tr key={`${t.symbol || activeSymbol}-${t.opened_ts || idx}`}>
                    <td>{fmtTs(t.opened_ts || t.opened_ts_ms)}</td>
                    <td className={String(t.side || '').toUpperCase() === 'BUY' || String(t.side || '').toUpperCase() === 'LONG'
                      ? 'pp-side-long'
                      : 'pp-side-short'}
                    >
                      {String(t.side || '').toUpperCase()}
                    </td>
                    <td>{fmtNumber(t.qty, 4)}</td>
                    <td>{fmtNumber(t.entry_px ?? t.entry_price, 2)}</td>
                    <td>{t.exit_px != null ? fmtNumber(t.exit_px, 2) : '—'}</td>
                    <td className={t.pnl_usd >= 0 ? 'pp-pos' : 'pp-neg'}>
                      {fmtNumber(t.pnl_usd, 2)}
                    </td>
                    <td className={t.pnl_r >= 0 ? 'pp-pos' : 'pp-neg'}>
                      {t.pnl_r != null ? fmtNumber(t.pnl_r, 2) : '—'}
                    </td>
                    <td style={{ color: '#6b7280' }}>
                      {t.fees_usd != null ? fmtNumber(t.fees_usd, 2) : '—'}
                    </td>
                    <td>{t.reason || '—'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  );
};

export default PaperPage;
