// Simple Binance WebSocket for multiple timeframes
const BASE_URL = 'wss://stream.binance.com:9443/stream?streams=' +
  ['btcusdt@kline_1m', 'btcusdt@kline_5m', 'btcusdt@kline_15m'].join('/');

export function connectBinanceCandles(onMessage) {
  const ws = new WebSocket(BASE_URL);
  ws.onmessage = (event) => {
    const data = JSON.parse(event.data);
    if (onMessage) onMessage(data);
  };
  return ws;
}