// ChartsManager: manages Binance Futures kline streams with per-symbol/timeframe caches
// - Single WebSocket connection using SUBSCRIBE/UNSUBSCRIBE
// - Per (symbol, timeframe) candle arrays with real-time updates
// - Also listens to markPrice@1s to gently align last candle close to current mark price
// - Throttled fan-out to subscribers

const FSTREAM_WS_URL = 'wss://fstream.binance.com/stream';
const FAPI_KLINES_URL = 'https://fapi.binance.com/fapi/v1/klines';

class ChartsManager {
  constructor() {
    this.ws = null;
    this.isConnecting = false;
    this.connectionId = 0;
    this.reconnectTimer = null;

    // key: SYMBOL:INTERVAL -> { candles: [], throttleTimer: any }
    this.seriesState = new Map();
    // symbolUpper -> true to know which symbols to subscribe markPrice for
    this.symbols = new Set();
    // key -> Set<callback>
    this.subscribers = new Map();

    this.supportedIntervals = ['1m', '5m', '15m'];
    this.defaultSymbols = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT'];
    this.preloaded = false;
  }

  keyFor(symbolUpper, interval) {
    return `${symbolUpper}:${interval}`;
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
        const params = [];
        for (const key of this.seriesState.keys()) {
          const [symbolUpper, interval] = key.split(':');
          params.push(`${symbolUpper.toLowerCase()}@kline_${interval}`);
        }
        for (const symbolUpper of this.symbols) {
          params.push(`${symbolUpper.toLowerCase()}@markPrice@1s`);
        }
        if (params.length) {
          ws.send(JSON.stringify({ method: 'SUBSCRIBE', params, id: Date.now() }));
        }
      };

      ws.onmessage = (event) => {
        try {
          const msg = JSON.parse(event.data);
          const data = msg.data || msg;
          if (!data) return;
          if (data.e === 'kline' && data.k) {
            const symbolUpper = (data.s || '').toUpperCase();
            const k = data.k;
            const interval = k.i;
            if (!this.supportedIntervals.includes(interval)) return;
            this.handleKline(symbolUpper, interval, k);
          } else if (data.e === 'markPriceUpdate') {
            const symbolUpper = (data.s || '').toUpperCase();
            const mark = parseFloat(data.p);
            if (!isFinite(mark)) return;
            this.alignLastCandleClose(symbolUpper, mark);
          }
        } catch (err) {
          // eslint-disable-next-line no-console
          console.error('ChartsManager parse error:', err);
        }
      };

      ws.onerror = (err) => {
        // eslint-disable-next-line no-console
        console.error('ChartsManager socket error:', err);
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
      console.error('ChartsManager failed to open socket:', err);
      if (!this.reconnectTimer) {
        this.reconnectTimer = setTimeout(() => {
          this.reconnectTimer = null;
          this.ensureConnection();
        }, 1500);
      }
    }
  }

  getOrCreateSeries(symbolUpper, interval) {
    const key = this.keyFor(symbolUpper, interval);
    let state = this.seriesState.get(key);
    if (!state) {
      state = { candles: [], throttleTimer: null };
      this.seriesState.set(key, state);
      this.symbols.add(symbolUpper);
      this.ensureConnection();
      this.subscribeStreams(symbolUpper, interval);
      // fetch snapshot immediately
      this.fetchSnapshot(symbolUpper, interval).catch(() => {});

      // no cross-preloading to speed up first paint
    }
    return state;
  }

  subscribeStreams(symbolUpper, interval) {
    const ws = this.ws;
    if (ws && ws.readyState === WebSocket.OPEN) {
      const params = [`${symbolUpper.toLowerCase()}@kline_${interval}`, `${symbolUpper.toLowerCase()}@markPrice@1s`];
      ws.send(JSON.stringify({ method: 'SUBSCRIBE', params, id: Date.now() }));
    }
  }

  async fetchSnapshot(symbolUpper, interval) {
    try {
      const res = await fetch(`${FAPI_KLINES_URL}?symbol=${symbolUpper}&interval=${interval}&limit=500`);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const raw = await res.json();
      const candles = raw.map((c) => ({
        time: Math.floor(c[0] / 1000),
        open: parseFloat(c[1]),
        high: parseFloat(c[2]),
        low: parseFloat(c[3]),
        close: parseFloat(c[4]),
        volume: parseFloat(c[5])
      }));
      const state = this.getOrCreateSeries(symbolUpper, interval);
      state.candles = candles;
      this.scheduleFanOut(symbolUpper, interval);
    } catch (err) {
      // eslint-disable-next-line no-console
      console.error('ChartsManager snapshot error:', err);
      setTimeout(() => this.fetchSnapshot(symbolUpper, interval).catch(() => {}), 1000);
    }
  }

  handleKline(symbolUpper, interval, k) {
    const state = this.getOrCreateSeries(symbolUpper, interval);
    const openTimeSec = Math.floor(k.t / 1000);
    const existsIdx = state.candles.findIndex((c) => c.time === openTimeSec);
    const candle = {
      time: openTimeSec,
      open: parseFloat(k.o),
      high: parseFloat(k.h),
      low: parseFloat(k.l),
      close: parseFloat(k.c),
      volume: parseFloat(k.v)
    };
    if (existsIdx >= 0) {
      state.candles[existsIdx] = candle;
    } else {
      // append while keeping order
      state.candles.push(candle);
      if (state.candles.length > 600) state.candles.shift();
    }
    this.scheduleFanOut(symbolUpper, interval);
  }

  alignLastCandleClose(symbolUpper, markPrice) {
    for (const intv of this.supportedIntervals) {
      const key = this.keyFor(symbolUpper, intv);
      const state = this.seriesState.get(key);
      if (!state || state.candles.length === 0) continue;
      const last = state.candles[state.candles.length - 1];
      if (!last) continue;
      // update close only; keep OH and L intact to respect kline extremes
      if (Math.abs(last.close - markPrice) / Math.max(1, last.close) > 0.0001) {
        last.close = markPrice;
        this.scheduleFanOut(symbolUpper, intv);
      }
    }
  }

  scheduleFanOut(symbolUpper, interval) {
    const key = this.keyFor(symbolUpper, interval);
    const state = this.getOrCreateSeries(symbolUpper, interval);
    if (state.throttleTimer) return;
    state.throttleTimer = setTimeout(() => {
      state.throttleTimer = null;
      const payload = this.buildPayload(symbolUpper, interval);
      const set = this.subscribers.get(key);
      if (set && set.size) {
        for (const cb of set) cb(payload);
      }
    }, 120);
  }

  buildPayload(symbolUpper, interval) {
    const state = this.getOrCreateSeries(symbolUpper, interval);
    return { candles: state.candles.slice() };
  }

  getSnapshot(symbol, interval) {
    const symbolUpper = symbol.toUpperCase();
    const key = this.keyFor(symbolUpper, interval);
    if (!this.seriesState.has(key)) return null;
    return this.buildPayload(symbolUpper, interval);
  }

  subscribe(symbol, interval, onUpdate, onStatus) {
    const symbolUpper = symbol.toUpperCase();
    this.getOrCreateSeries(symbolUpper, interval);
    const key = this.keyFor(symbolUpper, interval);
    if (!this.subscribers.has(key)) this.subscribers.set(key, new Set());
    const set = this.subscribers.get(key);
    set.add(onUpdate);

    // initial snapshot
    try {
      const snap = this.getSnapshot(symbolUpper, interval);
      if (snap) onUpdate(snap);
    } catch {}

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

const chartsManager = new ChartsManager();
export default chartsManager;


