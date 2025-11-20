// src/components/OpenPositionsPanel.jsx
import React from 'react';

function formatNumber(v, digits = 2) {
  if (v === null || v === undefined) return '—';
  const n = Number(v);
  if (!Number.isFinite(n)) return '—';
  return n.toFixed(digits);
}

function formatTimeMs(ts) {
  if (!ts && ts !== 0) return '—';
  const n = Number(ts);
  if (!Number.isFinite(n)) return '—';
  const d = new Date(n);
  return d.toLocaleTimeString(undefined, {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  });
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

const OpenPositionsPanel = ({ positions }) => {
  const list = [];

  if (positions && typeof positions === 'object') {
    for (const [sym, p] of Object.entries(positions)) {
      if (!p) continue;
      if (String(p.state || '').toUpperCase() !== 'OPEN') continue;
      list.push({ symbol: sym, ...p });
    }
  }

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
        }}
      >
        <span style={{ color: '#e5e7eb', fontSize: 13, fontWeight: 500 }}>
          Open Positions
        </span>
        <span style={{ color: '#6b7280', fontSize: 11 }}>
          {list.length} open
        </span>
      </div>

      <div style={{ maxHeight: 220, overflowY: 'auto' }}>
        <table style={{ width: '100%', borderCollapse: 'collapse' }}>
          <thead>
            <tr>
              <th style={{ ...headerCellBase, textAlign: 'left' }}>Symbol</th>
              <th style={{ ...headerCellBase, textAlign: 'left' }}>Side</th>
              <th style={{ ...headerCellBase, textAlign: 'right' }}>Qty</th>
              <th style={{ ...headerCellBase, textAlign: 'right' }}>Entry</th>
              <th style={{ ...headerCellBase, textAlign: 'right' }}>SL</th>
              <th style={{ ...headerCellBase, textAlign: 'right' }}>TP</th>
              <th style={{ ...headerCellBase, textAlign: 'right' }}>Timeout</th>
              <th style={{ ...headerCellBase, textAlign: 'left' }}>Opened</th>
            </tr>
          </thead>
          <tbody>
            {list.length === 0 ? (
              <tr>
                <td
                  colSpan={8}
                  style={{
                    ...cellBase,
                    textAlign: 'center',
                    padding: '14px 10px',
                    color: '#6b7280',
                  }}
                >
                  No open positions.
                </td>
              </tr>
            ) : (
              list.map((p, idx) => {
                const side = String(p.side || '').toUpperCase();
                const sideColor =
                  side === 'BUY'
                    ? '#22c55e'
                    : side === 'SELL'
                    ? '#f97316'
                    : '#e5e7eb';

                return (
                  <tr key={`${p.symbol || 'sym'}-${idx}`}>
                    <td style={{ ...cellBase, color: '#e5e7eb' }}>
                      {String(p.symbol || '').toUpperCase()}
                    </td>
                    <td
                      style={{
                        ...cellBase,
                        color: sideColor,
                        fontWeight: 500,
                      }}
                    >
                      {side}
                    </td>
                    <td
                      style={{
                        ...cellBase,
                        textAlign: 'right',
                        color: '#e5e7eb',
                      }}
                    >
                      {formatNumber(p.qty, 4)}
                    </td>
                    <td
                      style={{
                        ...cellBase,
                        textAlign: 'right',
                        color: '#9ca3af',
                      }}
                    >
                      {formatNumber(p.entry_px, 2)}
                    </td>
                    <td
                      style={{
                        ...cellBase,
                        textAlign: 'right',
                        color: '#f97316',
                      }}
                    >
                      {p.sl_px != null ? formatNumber(p.sl_px, 2) : '—'}
                    </td>
                    <td
                      style={{
                        ...cellBase,
                        textAlign: 'right',
                        color: '#22c55e',
                      }}
                    >
                      {p.tp_px != null ? formatNumber(p.tp_px, 2) : '—'}
                    </td>
                    <td
                      style={{
                        ...cellBase,
                        textAlign: 'right',
                        color: '#9ca3af',
                      }}
                    >
                      {p.timeout_ms != null ? `${Math.round(p.timeout_ms / 1000)}s` : '—'}
                    </td>
                    <td
                      style={{
                        ...cellBase,
                        color: '#6b7280',
                      }}
                    >
                      {p.opened_ts_ms ? formatTimeMs(p.opened_ts_ms) : '—'}
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

export default OpenPositionsPanel;
