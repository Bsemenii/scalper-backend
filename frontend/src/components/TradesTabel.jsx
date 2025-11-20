// src/components/TradesTable.jsx
import React from 'react';

function formatTs(ts) {
  if (!ts && ts !== 0) return '—';
  const n = Number(ts);
  if (!Number.isFinite(n)) return '—';
  const d = new Date(n);
  return d.toLocaleTimeString(undefined, { hour: '2-digit', minute: '2-digit', second: '2-digit' });
}

function formatNumber(v, digits = 2) {
  if (v === null || v === undefined) return '—';
  const n = Number(v);
  if (!Number.isFinite(n)) return '—';
  return n.toFixed(digits);
}

const cellBase = {
  padding: '6px 10px',
  fontSize: 12,
  borderBottom: '1px solid #0f172a',
  whiteSpace: 'nowrap',
};

const headerCellBase = {
  ...cellBase,
  fontSize: 11,
  textTransform: 'uppercase',
  letterSpacing: '0.04em',
  color: '#9ca3af',
  background: '#020617',
};

const TradesTable = ({ trades }) => {
  const rows = Array.isArray(trades) ? trades : [];

  return (
    <div
      style={{
        marginTop: 16,
        borderRadius: 12,
        border: '1px solid #1f2937',
        background: '#020617',
        overflow: 'hidden',
      }}
    >
      <div
        style={{
          padding: '8px 12px',
          borderBottom: '1px solid #111827',
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
          background: 'linear-gradient(to right, #020617, #020617)',
        }}
      >
        <span style={{ color: '#e5e7eb', fontSize: 13, fontWeight: 500 }}>
          Recent Trades
        </span>
        <span style={{ color: '#6b7280', fontSize: 11 }}>
          {rows.length} trade{rows.length === 1 ? '' : 's'}
        </span>
      </div>

      <div style={{ maxHeight: 260, overflowY: 'auto' }}>
        <table style={{ width: '100%', borderCollapse: 'collapse' }}>
          <thead>
            <tr>
              <th style={{ ...headerCellBase, textAlign: 'left' }}>Symbol</th>
              <th style={{ ...headerCellBase, textAlign: 'left' }}>Side</th>
              <th style={{ ...headerCellBase, textAlign: 'right' }}>Qty</th>
              <th style={{ ...headerCellBase, textAlign: 'right' }}>Entry</th>
              <th style={{ ...headerCellBase, textAlign: 'right' }}>Exit</th>
              <th style={{ ...headerCellBase, textAlign: 'right' }}>PnL (USD)</th>
              <th style={{ ...headerCellBase, textAlign: 'right' }}>PnL (R)</th>
              <th style={{ ...headerCellBase, textAlign: 'left' }}>Reason</th>
              <th style={{ ...headerCellBase, textAlign: 'left' }}>Opened</th>
              <th style={{ ...headerCellBase, textAlign: 'left' }}>Closed</th>
            </tr>
          </thead>
          <tbody>
            {rows.length === 0 ? (
              <tr>
                <td
                  colSpan={10}
                  style={{
                    ...cellBase,
                    textAlign: 'center',
                    padding: '14px 10px',
                    color: '#6b7280',
                  }}
                >
                  No trades yet.
                </td>
              </tr>
            ) : (
              rows.map((t, idx) => {
                const side = String(t.side || '').toUpperCase();
                const pnlUsd = Number(t.pnl_usd ?? 0);
                const colorPnl =
                  pnlUsd > 0 ? '#22c55e' : pnlUsd < 0 ? '#f97316' : '#e5e7eb';

                return (
                  <tr key={`${t.symbol || 'sym'}-${t.opened_ts || idx}`}>
                    <td style={{ ...cellBase, color: '#e5e7eb' }}>
                      {String(t.symbol || '').toUpperCase() || '—'}
                    </td>
                    <td
                      style={{
                        ...cellBase,
                        color:
                          side === 'BUY'
                            ? '#22c55e'
                            : side === 'SELL'
                            ? '#f97316'
                            : '#e5e7eb',
                        fontWeight: 500,
                      }}
                    >
                      {side || '—'}
                    </td>
                    <td style={{ ...cellBase, textAlign: 'right', color: '#e5e7eb' }}>
                      {formatNumber(t.qty, 4)}
                    </td>
                    <td
                      style={{
                        ...cellBase,
                        textAlign: 'right',
                        color: '#9ca3af',
                      }}
                    >
                      {formatNumber(t.entry_px ?? t.entry_price, 2)}
                    </td>
                    <td
                      style={{
                        ...cellBase,
                        textAlign: 'right',
                        color: '#9ca3af',
                      }}
                    >
                      {formatNumber(t.exit_px, 2)}
                    </td>
                    <td
                      style={{
                        ...cellBase,
                        textAlign: 'right',
                        color: colorPnl,
                        fontWeight: 500,
                      }}
                    >
                      {formatNumber(t.pnl_usd, 2)}
                    </td>
                    <td
                      style={{
                        ...cellBase,
                        textAlign: 'right',
                        color: '#e5e7eb',
                      }}
                    >
                      {formatNumber(t.pnl_r, 2)}
                    </td>
                    <td
                      style={{
                        ...cellBase,
                        maxWidth: 160,
                        overflow: 'hidden',
                        textOverflow: 'ellipsis',
                        color: '#9ca3af',
                      }}
                      title={t.reason || ''}
                    >
                      {t.reason || '—'}
                    </td>
                    <td style={{ ...cellBase, color: '#6b7280' }}>
                      {formatTs(t.opened_ts)}
                    </td>
                    <td style={{ ...cellBase, color: '#6b7280' }}>
                      {formatTs(t.closed_ts)}
                    </td>
                  </tr>
                );
              })
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
};

export default TradesTable;
