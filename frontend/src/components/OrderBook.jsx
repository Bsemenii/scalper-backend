import React, { useEffect, useState, useRef } from 'react';
import './OrderBook.css';
import orderBookManager from '../services/orderBookManager';

const OrderBook = ({ symbol = 'BTCUSDT' }) => {
  const [orderBook, setOrderBook] = useState({ bids: [], asks: [], bidsTotals: [], asksTotals: [], maxVolume: 0 });
  const [lastUpdate, setLastUpdate] = useState(null);
  const [priceDeltas, setPriceDeltas] = useState({});
  const [isConnected, setIsConnected] = useState(false);
  const [connectionStatus, setConnectionStatus] = useState('Connecting...');
  const lastPricesRef = useRef({});

  useEffect(() => {
    setPriceDeltas({});
    const unsubscribe = orderBookManager.subscribe(
      symbol,
      (display) => {
        const newDeltas = {};
        for (const [price, qty] of [...display.bids, ...display.asks]) {
          const prevQty = lastPricesRef.current[price];
          if (prevQty !== undefined && prevQty !== qty) {
            newDeltas[price] = qty > prevQty ? 'increase' : 'decrease';
          }
        }
        lastPricesRef.current = Object.fromEntries([...display.bids, ...display.asks]);
        setPriceDeltas(newDeltas);
        setTimeout(() => setPriceDeltas({}), 250);
        setOrderBook(display);
        setLastUpdate(Date.now());
      },
      (status) => {
        setIsConnected(!!status.connected);
        setConnectionStatus(status.statusText || (status.connected ? 'Live Updates' : 'Reconnecting...'));
      }
    );

    return () => {
      if (typeof unsubscribe === 'function') unsubscribe();
    };
  }, [symbol]);

  // Manager drives updates; no direct local updater needed

  const formatPrice = (price) => {
    const n = Number(price);
    if (!isFinite(n)) return '-';
    return n.toFixed(2);
  };

  const formatVolume = (volume) => {
    const n = Number(volume);
    if (!isFinite(n)) return '-';
    if (n >= 1000000) {
      return (n / 1000000).toFixed(1) + 'M';
    } else if (n >= 1000) {
      return (n / 1000).toFixed(1) + 'K';
    }
    return n.toFixed(2);
  };

  const getVolumeBarWidth = (volume, maxVolume) => {
    const v = Number(volume);
    const mv = Number(maxVolume);
    if (!isFinite(v) || !isFinite(mv) || mv <= 0) return 0;
    return Math.min((v / mv) * 100, 100);
  };

  const getRowClass = (price, index) => {
    let className = '';
    if (index < 3) className += ' top-level';
    if (priceDeltas[price]) className += ` ${priceDeltas[price]}`;
    return className;
  };

  // Max volume prepared in state for fast rendering
  const maxVolume = orderBook.maxVolume || 0;

  // Get spread info
  const bestBid = orderBook.bids[0]?.[0] || 0;
  const bestAsk = orderBook.asks[0]?.[0] || 0;
  const spread = bestAsk - bestBid;
  const spreadPercent = bestBid > 0 ? ((spread / bestBid) * 100) : 0;

  return (
    <div className="advanced-orderbook">
      <div className="orderbook-header">
        <div className="header-title">
          <span className="symbol-name">{symbol}</span>
          <span className="orderbook-type">Order Book</span>
        </div>
        <div className="orderbook-controls">
          <button className="depth-btn active">0.01</button>
          <button className="depth-btn">0.1</button>
          <button className="depth-btn">1</button>
        </div>
      </div>

      <div className="orderbook-content">
        <div className="orderbook-table-header">
          <span className="header-amount">Amount</span>
          <span className="header-price">Price</span>
          <span className="header-total">Total</span>
        </div>

        {/* Asks (Sell Orders) */}
        <div className="orderbook-asks">
          {orderBook.asks.slice(0, 15).reverse().map(([price, qty], index) => {
            const reversedIndex = 14 - index;
            const total = orderBook.asksTotals?.[reversedIndex] ?? orderBook.asks.slice(0, reversedIndex + 1).reduce((sum, [, q]) => sum + q, 0);
            return (
              <div 
                key={`ask-${price}`} 
                className={`orderbook-row ask${getRowClass(price, reversedIndex)}`}
              >
                <div className="volume-bar ask-bar" style={{ width: `${getVolumeBarWidth(qty, maxVolume)}%` }}></div>
                <span className="amount">{formatVolume(qty)}</span>
                <span className="price">{formatPrice(price)}</span>
                <span className="total">{formatVolume(total)}</span>
              </div>
            );
          })}
        </div>

        {/* Spread */}
        <div className="price-spread">
          <div className="spread-info">
            <span className="spread-value">{formatPrice(spread)}</span>
            <span className="spread-percent">({spreadPercent.toFixed(3)}%)</span>
          </div>
          <div className="market-price">
            <span className="current-price">{formatPrice((bestBid + bestAsk) / 2)}</span>
          </div>
        </div>

        {/* Bids (Buy Orders) */}
        <div className="orderbook-bids">
          {orderBook.bids.slice(0, 15).map(([price, qty], index) => {
            const total = orderBook.bidsTotals?.[index] ?? orderBook.bids.slice(0, index + 1).reduce((sum, [, q]) => sum + q, 0);
            return (
              <div 
                key={`bid-${price}`} 
                className={`orderbook-row bid${getRowClass(price, index)}`}
              >
                <div className="volume-bar bid-bar" style={{ width: `${getVolumeBarWidth(qty, maxVolume)}%` }}></div>
                <span className="amount">{formatVolume(qty)}</span>
                <span className="price">{formatPrice(price)}</span>
                <span className="total">{formatVolume(total)}</span>
              </div>
            );
          })}
        </div>
      </div>

      <div className="orderbook-footer">
        <div className="update-indicator">
          <span className={`update-dot ${isConnected ? 'connected' : 'disconnected'}`}></span>
          <span className="update-text">{connectionStatus}</span>
        </div>
        {lastUpdate && (
          <div className="last-update">
            Updated: {new Date(lastUpdate).toLocaleTimeString()}
          </div>
        )}
      </div>
    </div>
  );
};

export default OrderBook;