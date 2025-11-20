import React, { useEffect, useState } from 'react';
import { connectBinanceCandles } from '../services/binanceWs';

const ChartPanel = () => {
  const [candles, setCandles] = useState({ '1m': [], '5m': [], '15m': [] });

  useEffect(() => {
    const ws = connectBinanceCandles((data) => {
      if (data.stream && data.data && data.data.k) {
        const tf = data.stream.split('_')[1]; // '1m', '5m', '15m'
        setCandles(prev => ({
          ...prev,
          [tf]: [...prev[tf], data.data.k]
        }));
      }
    });
    return () => ws.close();
  }, []);

  return <div>Charts coming soon! <pre>{JSON.stringify(candles, null, 2)}</pre></div>;
};

export default ChartPanel;