// Connects to Binance Futures WebSocket for BTCUSDT streams
const BINANCE_WS_URL = 'wss://fstream.binance.com/ws/btcusdt@kline_1m';
const BINANCE_TICKER_URL = 'wss://fstream.binance.com/ws/btcusdt@ticker';

export function connectBTC1mCandles(onMessage) {
  const ws = new WebSocket(BINANCE_WS_URL);
  ws.onopen = () => {
    console.log('WebSocket connected to Binance Futures BTCUSDT 1m');
  };
  ws.onmessage = (event) => {
    const msg = JSON.parse(event.data);
    // For /ws endpoint, kline is under msg.k
    if (msg.k) {
      if (onMessage) {
        onMessage(msg);
      } else {
        console.log('BTCUSDT 1m kline:', msg.k);
      }
    } else {
      // For debugging: print the message if not kline
      console.log('Other message:', msg);
    }
  };
  ws.onerror = (err) => {
    console.error('WebSocket error:', err);
  };
  ws.onclose = () => {
    console.log('WebSocket closed');
  };
  return ws;
}

export function connectBTCTicker(onTickerUpdate) {
  let ws;
  let reconnectTimer;
  let isConnected = false;
  let shouldReconnect = true;

  function connect() {
    try {
      ws = new WebSocket(BINANCE_TICKER_URL);
      
      ws.onopen = () => {
        console.log('WebSocket connected to Binance Futures BTCUSDT ticker');
        isConnected = true;
        // Clear any pending reconnection
        if (reconnectTimer) {
          clearTimeout(reconnectTimer);
          reconnectTimer = null;
        }
      };
      
      ws.onmessage = (event) => {
        try {
          const ticker = JSON.parse(event.data);
          console.log('Received ticker data:', { symbol: ticker.s, price: ticker.c, time: new Date().toLocaleTimeString() });
          if (onTickerUpdate && ticker.c) {
            // Only update if we have a valid current price
            onTickerUpdate(ticker);
          }
        } catch (err) {
          console.error('Error parsing ticker data:', err);
        }
      };
      
      ws.onerror = (err) => {
        console.error('Ticker WebSocket error:', err);
        isConnected = false;
      };
      
      ws.onclose = () => {
        console.log('Ticker WebSocket closed');
        isConnected = false;
        
        // Attempt to reconnect if connection was not intentionally closed
        if (shouldReconnect && !reconnectTimer) {
          console.log('Attempting to reconnect ticker WebSocket in 3 seconds...');
          reconnectTimer = setTimeout(() => {
            reconnectTimer = null;
            connect();
          }, 3000);
        }
      };
    } catch (err) {
      console.error('Failed to create WebSocket connection:', err);
      // Retry connection
      if (shouldReconnect && !reconnectTimer) {
        reconnectTimer = setTimeout(() => {
          reconnectTimer = null;
          connect();
        }, 3000);
      }
    }
  }

  // Initial connection
  connect();

  // Return object with close method
  return {
    close() {
      shouldReconnect = false;
      if (reconnectTimer) {
        clearTimeout(reconnectTimer);
        reconnectTimer = null;
      }
      if (ws && ws.readyState === WebSocket.OPEN) {
        ws.close();
      }
    },
    isConnected() {
      return isConnected;
    }
  };
}