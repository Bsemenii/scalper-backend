// OrderBookManager: manages Binance Futures depth streams with per-symbol caches
// - Single WebSocket connection using SUBSCRIBE/UNSUBSCRIBE
// - Per-symbol snapshot, continuity, and incremental updates
// - Throttled fan-out to subscribers with precomputed sorted arrays + totals

const FSTREAM_WS_URL = 'wss://fstream.binance.com/stream';
const FAPI_SNAPSHOT_URL = 'https://fapi.binance.com/fapi/v1/depth';

// Prefer explicit env override when provided
const ENV_WS_URL = (typeof import.meta !== 'undefined' && import.meta.env && (
  import.meta.env.VITE_API_WS_URL || import.meta.env.VITE_BACKEND_WS_URL || import.meta.env.VITE_WS_URL
)) || null;

const APP_WS_PATH = '/api/ws/market';

// Temporary flag: disable backend WS path and use Binance directly for visuals
const DISABLE_BACKEND_WS = true;

function resolveBackendWsUrl() {
  if (ENV_WS_URL) {
    // If provided without protocol, assume ws(s) based on current protocol
    try {
      const hasProto = /^(ws|wss):\/\//i.test(ENV_WS_URL);
      if (hasProto) return ENV_WS_URL;
    } catch {}
  }

  try {
    const isBrowser = typeof window !== 'undefined' && typeof window.location !== 'undefined';
    if (!isBrowser) return `ws://localhost:8000${APP_WS_PATH}`;

    const { protocol, hostname, port } = window.location;
    const isHttps = protocol === 'https:';
    const wsProto = isHttps ? 'wss' : 'ws';

    // In local dev (vite on 5173, etc.), always point to backend port 8000
    const isLocalhost = ['localhost', '127.0.0.1', '::1'].includes(hostname) || hostname.endsWith('.local');
    const isDev = !!(typeof import.meta !== 'undefined' && import.meta.env && import.meta.env.DEV);
    if (isLocalhost && (isDev || port !== '8000')) {
      return `${wsProto}://${hostname}:8000${APP_WS_PATH}`;
    }

    // Otherwise, reuse current host/port and just swap protocol to ws(s)
    return `${wsProto}://${hostname}${port ? `:${port}` : ''}${APP_WS_PATH}`;
  } catch (e) {
    return `ws://localhost:8000${APP_WS_PATH}`;
  }
}

const BACKEND_WS_URL = ENV_WS_URL || resolveBackendWsUrl();

class OrderBookManager {
  constructor() {
    this.ws = null;
    this.isConnecting = false;
    this.reconnectTimer = null;
    this.connectionId = 0;

    // symbolUpper -> state
    this.symbolState = new Map();

    // symbolUpper -> Set<callback>
    this.subscribers = new Map();

    // Frame batching via rAF
    this._rafId = null;
    this._latestDisplays = new Map(); // symbolUpper -> display

    // preload commonly used symbols when first used
    this.preloaded = false;
    this.defaultSymbols = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT'];
  }

  ensureConnection() {
    if (this.ws || this.isConnecting) return;
    this.isConnecting = true;
    const myConnId = ++this.connectionId;
    try {
      if (DISABLE_BACKEND_WS) {
        // Connect directly to Binance stream
        const ws2 = new WebSocket(FSTREAM_WS_URL);
        this.ws = ws2;
        const id2 = this.connectionId;
        ws2.onopen = () => {
          if (id2 !== this.connectionId) return;
          this.isConnecting = false;
          const streams = [];
          for (const symbol of this.symbolState.keys()) {
            streams.push(`${symbol.toLowerCase()}@depth@100ms`);
          }
          if (streams.length) {
            ws2.send(JSON.stringify({ method: 'SUBSCRIBE', params: streams, id: Date.now() }));
          }
          for (const [symbol, state] of this.symbolState.entries()) {
            state.initialSnapshotFetched = false;
            state.buffer = [];
            state.lastStreamUpdateId = null;
            this.fetchSnapshot(symbol).catch(() => {});
          }
        };
        ws2.onmessage = (ev) => {
          try {
            const msg = JSON.parse(ev.data);
            const data = msg.data || msg;
            if (!data || data.e !== 'depthUpdate') return;
            const sym = (data.s || '').toUpperCase();
            if (!sym) return;
            this.handleDepthUpdate(sym, data);
          } catch {}
        };
        ws2.onerror = () => {};
        ws2.onclose = () => {
          this.ws = null;
          this.isConnecting = false;
          if (!this.reconnectTimer) {
            this.reconnectTimer = setTimeout(() => {
              this.reconnectTimer = null;
              this.ensureConnection();
            }, 1500);
          }
        };
        return;
      }

      // Existing backend-first path (disabled by flag above)
      const ws = new WebSocket(BACKEND_WS_URL);
      this.ws = ws;

      ws.onopen = () => {
        if (myConnId !== this.connectionId) return;
        this.isConnecting = false;
        // No subscribe needed for backend stream; frames are broadcasted
      };

      ws.onmessage = (event) => {
        try {
          const message = JSON.parse(event.data);
          if (message && message.type === 'market_frame' && message.frame) {
            const f = message.frame;
            const symbol = (f.symbol || '').toUpperCase();
            if (!symbol) return;
            const display = this.computeDisplayFromArrays(f.bids || [], f.asks || []);
            this.enqueueDisplay(symbol, display);
            const state = this.getOrCreateState(symbol);
            state.bidsMap.clear();
            state.asksMap.clear();
            for (const [p, q] of f.bids || []) {
              const price = parseFloat(p);
              const qty = parseFloat(q);
              if (qty > 0) state.bidsMap.set(price, qty);
            }
            for (const [p, q] of f.asks || []) {
              const price = parseFloat(p);
              const qty = parseFloat(q);
              if (qty > 0) state.asksMap.set(price, qty);
            }
            return;
          }
          const data = message.data || message;
          if (data && data.e === 'depthUpdate') {
            const sym = (data.s || '').toUpperCase();
            if (!sym) return;
            this.handleDepthUpdate(sym, data);
          }
        } catch (err) {
          // eslint-disable-next-line no-console
          console.error('OrderBookManager parse error:', err);
        }
      };

      ws.onerror = (err) => {
        // eslint-disable-next-line no-console
        console.error('OrderBookManager socket error:', err);
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
      console.error('OrderBookManager failed to open socket:', err);
      if (!this.reconnectTimer) {
        this.reconnectTimer = setTimeout(() => {
          this.reconnectTimer = null;
          this.ensureConnection();
        }, 1500);
      }
    }
  }

  async fetchSnapshot(symbolUpper) {
    const state = this.getOrCreateState(symbolUpper);
    try {
      const res = await fetch(`${FAPI_SNAPSHOT_URL}?symbol=${symbolUpper}&limit=100`);
      if (!res.ok) throw new Error(`Snapshot HTTP ${res.status}`);
      const data = await res.json();

      state.lastUpdateId = data.lastUpdateId;
      state.bidsMap.clear();
      state.asksMap.clear();
      for (const [p, q] of data.bids) {
        const price = parseFloat(p);
        const qty = parseFloat(q);
        if (qty > 0) state.bidsMap.set(price, qty);
      }
      for (const [p, q] of data.asks) {
        const price = parseFloat(p);
        const qty = parseFloat(q);
        if (qty > 0) state.asksMap.set(price, qty);
      }

      // Apply buffered events bridging the snapshot per Binance docs
      if (Array.isArray(state.buffer) && state.buffer.length) {
        // Sort by u ascending to be safe
        state.buffer.sort((a, b) => a.u - b.u);
        for (const evt of state.buffer) {
          const U = evt.U;
          const u = evt.u;
          if (u <= state.lastUpdateId) {
            continue; // discard old
          }
          if (U <= state.lastUpdateId + 1 && u >= state.lastUpdateId + 1) {
            // this bridges snapshot; apply and then all subsequent
            this.applySideUpdates(state.bidsMap, evt.b);
            this.applySideUpdates(state.asksMap, evt.a);
            state.lastStreamUpdateId = u;
          } else if (state.lastStreamUpdateId !== null && U === state.lastStreamUpdateId + 1) {
            this.applySideUpdates(state.bidsMap, evt.b);
            this.applySideUpdates(state.asksMap, evt.a);
            state.lastStreamUpdateId = u;
          }
        }
      }

      state.initialSnapshotFetched = true;
      state.buffer = [];
      this.scheduleFanOut(symbolUpper);
    } catch (err) {
      // eslint-disable-next-line no-console
      console.error(`OrderBookManager snapshot error for ${symbolUpper}:`, err);
      // retry shortly
      setTimeout(() => this.fetchSnapshot(symbolUpper).catch(() => {}), 1000);
    }
  }

  applySideUpdates(bookMap, updates) {
    for (const [ps, qs] of updates || []) {
      const price = parseFloat(ps);
      const qty = parseFloat(qs);
      if (!isFinite(price)) continue;
      if (qty === 0) bookMap.delete(price);
      else bookMap.set(price, qty);
    }
  }

  handleDepthUpdate(symbolUpper, evt) {
    const state = this.getOrCreateState(symbolUpper);

    if (!state.initialSnapshotFetched) {
      state.buffer.push(evt);
      return;
    }

    const U = evt.U;
    const u = evt.u;
    const pu = evt.pu;

    if (state.lastStreamUpdateId === null) {
      if (!(U <= state.lastUpdateId + 1 && u >= state.lastUpdateId + 1)) {
        return; // wait until the bridging event arrives
      }
    } else {
      if (pu !== undefined) {
        if (pu !== state.lastStreamUpdateId) {
          // sequence broken - refetch snapshot for safety
          state.initialSnapshotFetched = false;
          state.buffer = [];
          state.lastStreamUpdateId = null;
          this.fetchSnapshot(symbolUpper).catch(() => {});
          return;
        }
      } else if (U !== state.lastStreamUpdateId + 1) {
        state.initialSnapshotFetched = false;
        state.buffer = [];
        state.lastStreamUpdateId = null;
        this.fetchSnapshot(symbolUpper).catch(() => {});
        return;
      }
    }

    this.applySideUpdates(state.bidsMap, evt.b);
    this.applySideUpdates(state.asksMap, evt.a);
    state.lastStreamUpdateId = u;
    this.scheduleFanOut(symbolUpper);
  }

  scheduleFanOut(symbolUpper) {
    // Compute and batch via rAF to minimize React renders
    const display = this.computeDisplayState(symbolUpper);
    this.enqueueDisplay(symbolUpper, display);
  }

  computeDisplayState(symbolUpper) {
    const state = this.getOrCreateState(symbolUpper);
    const bidsArr = Array.from(state.bidsMap.entries()).filter(([,q])=>q>0).sort((a, b) => b[0] - a[0]).slice(0, 15);
    const asksArr = Array.from(state.asksMap.entries()).filter(([,q])=>q>0).sort((a, b) => a[0] - b[0]).slice(0, 15);

    const bidsTotals = new Array(bidsArr.length);
    const asksTotals = new Array(asksArr.length);
    let acc = 0;
    for (let i = 0; i < bidsArr.length; i++) {
      acc += bidsArr[i][1];
      bidsTotals[i] = acc;
    }
    acc = 0;
    for (let i = 0; i < asksArr.length; i++) {
      acc += asksArr[i][1];
      asksTotals[i] = acc;
    }

    const maxBidVolume = Math.max(...bidsArr.map(([, q]) => q), 0);
    const maxAskVolume = Math.max(...asksArr.map(([, q]) => q), 0);
    const maxVolume = Math.max(maxBidVolume, maxAskVolume);

    return { bids: bidsArr, asks: asksArr, bidsTotals, asksTotals, maxVolume };
  }

  computeDisplayFromArrays(bids, asks) {
    // Inputs already top-N and sorted from backend
    const bidsArr = bids.map(([p, q]) => [parseFloat(p), parseFloat(q)]).filter(([, q]) => q > 0).slice(0, 15);
    const asksArr = asks.map(([p, q]) => [parseFloat(p), parseFloat(q)]).filter(([, q]) => q > 0).slice(0, 15);

    const bidsTotals = new Array(bidsArr.length);
    const asksTotals = new Array(asksArr.length);
    let acc = 0;
    for (let i = 0; i < bidsArr.length; i++) {
      acc += bidsArr[i][1];
      bidsTotals[i] = acc;
    }
    acc = 0;
    for (let i = 0; i < asksArr.length; i++) {
      acc += asksArr[i][1];
      asksTotals[i] = acc;
    }

    const maxBidVolume = Math.max(...bidsArr.map(([, q]) => q), 0);
    const maxAskVolume = Math.max(...asksArr.map(([, q]) => q), 0);
    const maxVolume = Math.max(maxBidVolume, maxAskVolume);

    return { bids: bidsArr, asks: asksArr, bidsTotals, asksTotals, maxVolume };
  }

  enqueueDisplay(symbolUpper, display) {
    this._latestDisplays.set(symbolUpper, display);
    if (this._rafId !== null) return;
    const raf = (typeof window !== 'undefined' && window.requestAnimationFrame) || ((cb) => setTimeout(cb, 16));
    this._rafId = raf(() => {
      this._rafId = null;
      // Flush batched displays
      for (const [sym, disp] of this._latestDisplays.entries()) {
        const set = this.subscribers.get(sym);
        if (set && set.size) {
          for (const cb of set) cb(disp);
        }
      }
      this._latestDisplays.clear();
    });
  }

  getCachedDisplay(symbol) {
    const symbolUpper = symbol.toUpperCase();
    if (!this.symbolState.has(symbolUpper)) return null;
    return this.computeDisplayState(symbolUpper);
  }

  getOrCreateState(symbolUpper) {
    let state = this.symbolState.get(symbolUpper);
    if (!state) {
      state = {
        bidsMap: new Map(),
        asksMap: new Map(),
        lastUpdateId: null,
        lastStreamUpdateId: null,
        initialSnapshotFetched: false,
        buffer: []
      };
      this.symbolState.set(symbolUpper, state);
      // ensure connection and subscribe
      this.ensureConnection();
      this.subscribeStream(symbolUpper);
      // fetch snapshot in parallel immediately
      this.fetchSnapshot(symbolUpper).catch(() => {});

      // warm defaults once
      if (!this.preloaded) {
        this.preloaded = true;
        for (const sym of this.defaultSymbols) {
          const s = sym.toUpperCase();
          if (s !== symbolUpper) {
            this.getOrCreateState(s);
          }
        }
      }
    }
    return state;
  }

  subscribeStream(symbolUpper) {
    const ws = this.ws;
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({ method: 'SUBSCRIBE', params: [`${symbolUpper.toLowerCase()}@depth@100ms`], id: Date.now() }));
    }
  }

  subscribe(symbol, onUpdate, onStatus) {
    const symbolUpper = symbol.toUpperCase();
    this.getOrCreateState(symbolUpper);

    if (!this.subscribers.has(symbolUpper)) this.subscribers.set(symbolUpper, new Set());
    const set = this.subscribers.get(symbolUpper);
    set.add(onUpdate);

    // Initial emit: cached display if available
    try {
      const cached = this.getCachedDisplay(symbolUpper);
      if (cached) onUpdate(cached);
    } catch {}

    // Connection status pings
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

const orderBookManager = new OrderBookManager();
export default orderBookManager;


