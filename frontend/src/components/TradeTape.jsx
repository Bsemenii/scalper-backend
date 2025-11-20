import React, { useEffect, useState, useRef } from 'react';
import './TradeTape.css';
import tradesManager from '../services/tradesManager';

const TradeTape = ({ symbol = 'BTCUSDT' }) => {
  const [trades, setTrades] = useState([]);
  const [volumeBreakdown, setVolumeBreakdown] = useState({});
  const [selectedInterval, setSelectedInterval] = useState('1m');
  const [isConnected, setIsConnected] = useState(false);
  const volumeIntervalsRef = useRef({});

  const intervals = [
    { key: '5s', label: '5s', ms: 5000 },
    { key: '15s', label: '15s', ms: 15000 },
    { key: '30s', label: '30s', ms: 30000 },
    { key: '45s', label: '45s', ms: 45000 },
    { key: '1m', label: '1m', ms: 60000 },
    { key: '2m', label: '2m', ms: 120000 },
    { key: '5m', label: '5m', ms: 300000 }
  ];

  useEffect(() => {
    const unsubscribe = tradesManager.subscribe(
      symbol,
      (payload) => {
        setTrades(payload.trades);
        // copy to refs for breakdown computation
        volumeIntervalsRef.current = payload.volumes;
        setVolumeBreakdown({ ...payload.volumes });
      },
      (status) => {
        setIsConnected(!!status.connected);
      }
    );

    // setup periodic refresh to keep breakdown in sync with rolling resets
    const tick = setInterval(() => {
      const payload = tradesManager.getSnapshot ? tradesManager.getSnapshot(symbol) : null;
      if (payload) {
        volumeIntervalsRef.current = payload.volumes;
        setVolumeBreakdown({ ...payload.volumes });
      }
    }, 1000);

    return () => {
      if (typeof unsubscribe === 'function') unsubscribe();
      clearInterval(tick);
    };
  }, [symbol]);

  // When timeframe is changed, immediately refresh from manager snapshot to avoid waiting for new trades
  useEffect(() => {
    const payload = tradesManager.getSnapshot ? tradesManager.getSnapshot(symbol) : null;
    if (payload) {
      volumeIntervalsRef.current = payload.volumes;
      setVolumeBreakdown({ ...payload.volumes });
    }
  }, [selectedInterval, symbol]);

  const updateVolumeBreakdown = () => {
    setVolumeBreakdown({ ...volumeIntervalsRef.current });
  };

  const formatPrice = (price) => {
    return price.toFixed(2);
  };

  const formatVolume = (volume) => {
    if (volume >= 1000000) {
      return (volume / 1000000).toFixed(1) + 'M';
    } else if (volume >= 1000) {
      return (volume / 1000).toFixed(1) + 'K';
    }
    return volume.toFixed(2);
  };

  const formatTime = (timestamp) => {
    const date = new Date(timestamp);
    return date.toLocaleTimeString('en-US', { 
      hour12: false, 
      hour: '2-digit', 
      minute: '2-digit', 
      second: '2-digit',
      timeZone: 'UTC'
    });
  };

  const getVolumeRatio = (buyVol, sellVol) => {
    const total = buyVol + sellVol;
    return total > 0 ? {
      buyPercent: (buyVol / total) * 100,
      sellPercent: (sellVol / total) * 100
    } : { buyPercent: 50, sellPercent: 50 };
  };

  const selectedVolumeData = volumeBreakdown[selectedInterval] || { buyVolume: 0, sellVolume: 0 };
  const volumeRatio = getVolumeRatio(selectedVolumeData.buyVolume, selectedVolumeData.sellVolume);

  return (
    <div className="trade-tape">
      <div className="trade-tape-header">
        <div className="header-title">
          <span className="symbol-name">{symbol}</span>
          <span className="tape-type">Time & Sales</span>
        </div>
        <div className="connection-status">
          <span className={`status-dot ${isConnected ? 'connected' : 'disconnected'}`}></span>
          <span className="status-text">{isConnected ? 'Live' : 'Offline'}</span>
        </div>
      </div>

      {/* Volume Breakdown by Intervals */}
      <div className="volume-breakdown">
        <div className="interval-selector">
          {intervals.map(({ key, label }) => (
            <button
              key={key}
              className={`interval-btn ${selectedInterval === key ? 'active' : ''}`}
              onClick={() => setSelectedInterval(key)}
            >
              {label}
            </button>
          ))}
        </div>

        <div className="volume-display">
          <div className="volume-bars">
            <div className="volume-bar-container">
              <div 
                className="volume-bar buy-bar" 
                style={{ width: `${volumeRatio.buyPercent}%` }}
              ></div>
              <div 
                className="volume-bar sell-bar" 
                style={{ width: `${volumeRatio.sellPercent}%` }}
              ></div>
            </div>
          </div>
          
          <div className="volume-stats">
            <div className="volume-stat buy">
              <span className="volume-label">Buy</span>
              <span className="volume-value">
                {formatVolume(selectedVolumeData.buyVolume)}
              </span>
              <span className="volume-percent">
                {volumeRatio.buyPercent.toFixed(1)}%
              </span>
            </div>
            <div className="volume-stat sell">
              <span className="volume-label">Sell</span>
              <span className="volume-value">
                {formatVolume(selectedVolumeData.sellVolume)}
              </span>
              <span className="volume-percent">
                {volumeRatio.sellPercent.toFixed(1)}%
              </span>
            </div>
          </div>
        </div>
      </div>

      {/* Trade List */}
      <div className="trades-section">
        <div className="trades-header">
          <span className="col-price">Price</span>
          <span className="col-amount">Amount</span>
          <span className="col-time">Time</span>
        </div>
        
        <div className="trades-list">
          {trades.map((trade) => (
            <div 
              key={trade.id} 
              className={`trade-row ${trade.isBuy ? 'buy' : 'sell'}`}
            >
              <span className="trade-price">{formatPrice(trade.price)}</span>
              <span className="trade-amount">{formatVolume(trade.quantity)}</span>
              <span className="trade-time">{formatTime(trade.time)}</span>
            </div>
          ))}
        </div>
      </div>

      <div className="trade-tape-footer">
        <div className="trade-count">
          {trades.length} trades
        </div>
        <div className="last-update">
          Last: {trades[0] ? formatTime(trades[0].time) : 'N/A'}
        </div>
      </div>
    </div>
  );
};

export default TradeTape;