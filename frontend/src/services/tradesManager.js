// TradesManager: manages Binance Futures aggTrade streams with per-symbol caches
// - Single WebSocket connection using SUBSCRIBE/UNSUBSCRIBE
// - Per-symbol ring buffers of recent trades + rolling volume stats for multiple intervals
// - Throttled fan-out to subscribers

const FSTREAM_WS_URL = 'wss://fstream.binance.com/stream';

class TradesManager {
  constructor() {
    this.ws = null;
    this.isConnecting = false;
    this.reconnectTimer = null;
    this.connectionId = 0;

    // symbolUpper -> { trades: Trade[], volumes: Map<intervalKey, {buyVolume, sellVolume, lastReset}>, throttleTimer }
    this.symbolState = new Map();
    this.subscribers = new Map(); // symbolUpper -> Set<callback>

    this.intervals = [
      { key: '5s', ms: 5000 },
      { key: '15s', ms: 15000 },
      { key: '30s', ms: 30000 },
      { key: '45s', ms: 45000 },
      { key: '1m', ms: 60000 },
      { key: '2m', ms: 120000 },
      { key: '5m', ms: 300000 }
    ];

    this.defaultSymbols = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT'];
    this.preloaded = false;

    // periodic maintenance to reset rolling intervals even without new trades
    this.maintenanceTimer = setInterval(() => {
      this.tickMaintenance();
    }, 1000);
  }

  ensureConnection() {
    if (this.ws || this.isConnecting) return;
    this.isConnecting = true;
    const myConnId = ++this.connectionId;
    try {
      const ws = new WebSocket(FSTREAM_WS_URL);
      this.ws = ws;

      ws.onopen = () => {
        if (myConnId !== this.connectionId) return;
        this.isConnecting = false;
        // Resubscribe
        const streams = [];
        for (const symbol of this.symbolState.keys()) {
          streams.push(`${symbol.toLowerCase()}@aggTrade`);
        }
        if (streams.length) ws.send(JSON.stringify({ method: 'SUBSCRIBE', params: streams, id: Date.now() }));
      };

      ws.onmessage = (event) => {
        try {
          const msg = JSON.parse(event.data);
          const data = msg.data || msg;
          if (!data || data.e !== 'aggTrade') return;
          const symbol = (data.s || '').toUpperCase();
          if (!symbol) return;
          this.handleTrade(symbol, data);
        } catch (err) {
          // eslint-disable-next-line no-console
          console.error('TradesManager parse error:', err);
        }
      };

      ws.onerror = (err) => {
        // eslint-disable-next-line no-console
        console.error('TradesManager socket error:', err);
      };

      ws.onclose = () => {
        if (myConnId !== this.connectionId) return;
        this.ws = null;
        this.isConnecting = false;
        if (!this.reconnectTimer) {
          this.reconnectTimer = setTimeout(() => {
            this.reconnectTimer = null;
            this.ensureConnection();
          }, 1500);
        }
      };
    } catch (err) {
      this.isConnecting = false;
      // eslint-disable-next-line no-console
      console.error('TradesManager failed to open socket:', err);
      if (!this.reconnectTimer) {
        this.reconnectTimer = setTimeout(() => {
          this.reconnectTimer = null;
          this.ensureConnection();
        }, 1500);
      }
    }
  }

  getOrCreateState(symbolUpper) {
    let state = this.symbolState.get(symbolUpper);
    if (!state) {
      state = {
        trades: [], // newest first
        volumes: new Map(),
        throttleTimer: null
      };
      for (const { key } of this.intervals) {
        state.volumes.set(key, { buyVolume: 0, sellVolume: 0, lastReset: Date.now() });
      }
      this.symbolState.set(symbolUpper, state);
      this.ensureConnection();
      this.subscribeStream(symbolUpper);

      if (!this.preloaded) {
        this.preloaded = true;
        for (const sym of this.defaultSymbols) {
          const s = sym.toUpperCase();
          if (s !== symbolUpper) this.getOrCreateState(s);
        }
      }
    }
    return state;
  }

  subscribeStream(symbolUpper) {
    const ws = this.ws;
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({ method: 'SUBSCRIBE', params: [`${symbolUpper.toLowerCase()}@aggTrade`], id: Date.now() }));
    }
  }

  handleTrade(symbolUpper, trade) {
    const state = this.getOrCreateState(symbolUpper);
    const tradeData = {
      id: trade.a,
      price: parseFloat(trade.p),
      quantity: parseFloat(trade.q),
      time: trade.T,
      isBuy: !trade.m
    };

    // Keep newest first, cap at 300 for smooth scrolling
    state.trades.unshift(tradeData);
    if (state.trades.length > 300) state.trades.length = 300;

    // Update rolling volumes
    const now = Date.now();
    for (const { key, ms } of this.intervals) {
      const v = state.volumes.get(key);
      if (now - v.lastReset >= ms) {
        v.buyVolume = 0;
        v.sellVolume = 0;
        v.lastReset = now;
      }
      if (tradeData.isBuy) v.buyVolume += tradeData.quantity;
      else v.sellVolume += tradeData.quantity;
    }

    this.scheduleFanOut(symbolUpper);
  }

  scheduleFanOut(symbolUpper) {
    const state = this.getOrCreateState(symbolUpper);
    if (state.throttleTimer) return;
    state.throttleTimer = setTimeout(() => {
      state.throttleTimer = null;
      const payload = this.buildPayload(symbolUpper);
      const set = this.subscribers.get(symbolUpper);
      if (set && set.size) {
        for (const cb of set) cb(payload);
      }
    }, 80);
  }

  buildPayload(symbolUpper) {
    const state = this.getOrCreateState(symbolUpper);
    // clone shallow structures for React safety
    const trades = state.trades.slice(0, 100);
    const volumes = {};
    for (const { key } of this.intervals) {
      const v = state.volumes.get(key);
      volumes[key] = { ...v };
    }
    return { trades, volumes };
  }

  getSnapshot(symbol) {
    const symbolUpper = symbol.toUpperCase();
    if (!this.symbolState.has(symbolUpper)) return null;
    return this.buildPayload(symbolUpper);
  }

  tickMaintenance() {
    const now = Date.now();
    for (const [symbolUpper, state] of this.symbolState.entries()) {
      let changed = false;
      for (const { key, ms } of this.intervals) {
        const v = state.volumes.get(key);
        if (now - v.lastReset >= ms) {
          v.buyVolume = 0;
          v.sellVolume = 0;
          v.lastReset = now;
          changed = true;
        }
      }
      if (changed) {
        this.scheduleFanOut(symbolUpper);
      }
    }
  }

  subscribe(symbol, onUpdate, onStatus) {
    const symbolUpper = symbol.toUpperCase();
    this.getOrCreateState(symbolUpper);
    if (!this.subscribers.has(symbolUpper)) this.subscribers.set(symbolUpper, new Set());
    const set = this.subscribers.get(symbolUpper);
    set.add(onUpdate);

    // Emit cached immediately
    try { onUpdate(this.buildPayload(symbolUpper)); } catch {}

    const statusInterval = setInterval(() => {
      if (typeof onStatus === 'function') {
        onStatus({ connected: !!this.ws && this.ws.readyState === WebSocket.OPEN, statusText: this.ws ? 'Live Updates' : 'Reconnecting...' });
      }
    }, 1000);

    return () => {
      set.delete(onUpdate);
      clearInterval(statusInterval);
    };
  }
}

const tradesManager = new TradesManager();
export default tradesManager;


