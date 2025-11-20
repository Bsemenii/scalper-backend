import React, { useEffect, useRef, useState } from 'react';
import { createChart } from 'lightweight-charts';

const MAX_CANDLES = 100;
const API_URL = 'http://localhost:8000/api/klines_1m'; // BTC futures 1m klines

function mapBackendCandle(c) {
  return {
    time: Math.floor(c.open_time / 1000),
    open: parseFloat(c.open),
    high: parseFloat(c.high),
    low: parseFloat(c.low),
    close: parseFloat(c.close),
    volume: parseFloat(c.volume),
  };
}

// Indicator calculation functions
function calculateEMA(data, period) {
  const ema = [];
  const multiplier = 2 / (period + 1);
  
  if (data.length === 0) return ema;
  
  ema[0] = data[0];
  
  for (let i = 1; i < data.length; i++) {
    ema[i] = (data[i] * multiplier) + (ema[i - 1] * (1 - multiplier));
  }
  
  return ema;
}

function calculateBollingerBands(data, period = 20, stdDev = 2) {
  const bands = { upper: [], lower: [], middle: [] };
  
  for (let i = period - 1; i < data.length; i++) {
    const slice = data.slice(i - period + 1, i + 1);
    const mean = slice.reduce((sum, val) => sum + val, 0) / period;
    const variance = slice.reduce((sum, val) => sum + Math.pow(val - mean, 2), 0) / period;
    const standardDeviation = Math.sqrt(variance);
    
    bands.middle[i] = mean;
    bands.upper[i] = mean + (standardDeviation * stdDev);
    bands.lower[i] = mean - (standardDeviation * stdDev);
  }
  
  return bands;
}

function calculateRSI(data, period = 14) {
  const rsi = [];
  const gains = [];
  const losses = [];
  
  for (let i = 1; i < data.length; i++) {
    const change = data[i] - data[i - 1];
    gains[i] = change > 0 ? change : 0;
    losses[i] = change < 0 ? Math.abs(change) : 0;
  }
  
  for (let i = period; i < data.length; i++) {
    const avgGain = gains.slice(i - period + 1, i + 1).reduce((sum, val) => sum + val, 0) / period;
    const avgLoss = losses.slice(i - period + 1, i + 1).reduce((sum, val) => sum + val, 0) / period;
    
    if (avgLoss === 0) {
      rsi[i] = 100;
    } else {
      const rs = avgGain / avgLoss;
      rsi[i] = 100 - (100 / (1 + rs));
    }
  }
  
  return rsi;
}

function calculateMACD(data, fastPeriod = 12, slowPeriod = 26, signalPeriod = 9) {
  const fastEMA = calculateEMA(data, fastPeriod);
  const slowEMA = calculateEMA(data, slowPeriod);
  const macdLine = [];
  
  for (let i = 0; i < data.length; i++) {
    if (fastEMA[i] !== undefined && slowEMA[i] !== undefined) {
      macdLine[i] = fastEMA[i] - slowEMA[i];
    }
  }
  
  const signalLine = calculateEMA(macdLine.filter(val => val !== undefined), signalPeriod);
  const histogram = [];
  
  let signalIndex = 0;
  for (let i = 0; i < macdLine.length; i++) {
    if (macdLine[i] !== undefined) {
      if (signalLine[signalIndex] !== undefined) {
        histogram[i] = macdLine[i] - signalLine[signalIndex];
        signalIndex++;
      }
    }
  }
  
  return { macd: macdLine, signal: signalLine, histogram };
}

const Chart1m = () => {
  const chartContainerRef = useRef();
  const chartRef = useRef();
  const candleSeriesRef = useRef();
  const volumeSeriesRef = useRef();
  const ema9SeriesRef = useRef();
  const ema21SeriesRef = useRef();
  const ema50SeriesRef = useRef();
  const bollingerUpperRef = useRef();
  const bollingerLowerRef = useRef();
  const rsiSeriesRef = useRef();
  const macdSeriesRef = useRef();
  const macdSignalRef = useRef();
  const macdHistogramRef = useRef();
  const [candles, setCandles] = useState([]);

  // Fetch candles from backend
  const fetchCandles = async () => {
    try {
      const res = await fetch(API_URL);
      let data = await res.json();
      // Sort by open_time ascending and filter duplicates
      data = data
        .sort((a, b) => a.open_time - b.open_time)
        .filter((c, i, arr) => i === 0 || c.open_time !== arr[i - 1].open_time);
      setCandles(data.map(mapBackendCandle));
    } catch (err) {
      console.error('Failed to fetch candles:', err);
    }
  };

  useEffect(() => {
    if (!chartContainerRef.current) return;

    const chart = createChart(chartContainerRef.current, {
      width: chartContainerRef.current.clientWidth,
      height: chartContainerRef.current.clientHeight,
      layout: {
        background: { type: 'solid', color: '#131722' },
        textColor: '#d1d4dc',
        fontSize: 12,
        fontFamily: 'system-ui, -apple-system, sans-serif',
      },
      grid: {
        vertLines: { 
          color: '#2a2e39',
          style: 0,
          visible: true,
        },
        horzLines: { 
          color: '#2a2e39',
          style: 0,
          visible: true,
        },
      },
      crosshair: {
        mode: 1,
        vertLine: {
          color: '#758696',
          width: 1,
          style: 3,
          visible: true,
          labelVisible: true,
        },
        horzLine: {
          color: '#758696',
          width: 1,
          style: 3,
          visible: true,
          labelVisible: true,
        },
      },
      rightPriceScale: {
        borderColor: '#2a2e39',
        borderVisible: true,
        scaleMargins: {
          top: 0.05,
          bottom: 0.25,
        },
      },
      timeScale: {
        borderColor: '#2a2e39',
        borderVisible: true,
        timeVisible: true,
        secondsVisible: false,
        rightOffset: 5,
        barSpacing: 8,
        minBarSpacing: 2,
      },
      watermark: {
        visible: false,
      },
      handleScroll: {
        mouseWheel: true,
        pressedMouseMove: true,
        horzTouchDrag: true,
        vertTouchDrag: true,
      },
      handleScale: {
        axisPressedMouseMove: true,
        mouseWheel: true,
        pinch: true,
      },
    });
    
    chartRef.current = chart;
    
    // Add candlestick series
    const candleSeries = chart.addCandlestickSeries({
      upColor: '#26a69a',
      downColor: '#ef5350',
      borderDownColor: '#ef5350',
      borderUpColor: '#26a69a',
      wickDownColor: '#ef5350',
      wickUpColor: '#26a69a',
      priceScaleId: 'right',
    });
    
    candleSeriesRef.current = candleSeries;
    
    // Add EMA series
    const ema9Series = chart.addLineSeries({
      color: '#2196f3',
      lineWidth: 1,
      title: 'EMA 9',
      priceScaleId: 'right',
    });
    ema9SeriesRef.current = ema9Series;
    
    const ema21Series = chart.addLineSeries({
      color: '#ff9800',
      lineWidth: 1,
      title: 'EMA 21',
      priceScaleId: 'right',
    });
    ema21SeriesRef.current = ema21Series;
    
    const ema50Series = chart.addLineSeries({
      color: '#9c27b0',
      lineWidth: 2,
      title: 'EMA 50',
      priceScaleId: 'right',
    });
    ema50SeriesRef.current = ema50Series;
    
    // Add Bollinger Bands
    const bollingerUpper = chart.addLineSeries({
      color: 'rgba(120, 123, 134, 0.7)',
      lineWidth: 1,
      lineStyle: 2, // dotted
      title: 'BB Upper',
      priceScaleId: 'right',
    });
    bollingerUpperRef.current = bollingerUpper;
    
    const bollingerLower = chart.addLineSeries({
      color: 'rgba(120, 123, 134, 0.7)',
      lineWidth: 1,
      lineStyle: 2, // dotted
      title: 'BB Lower',
      priceScaleId: 'right',
    });
    bollingerLowerRef.current = bollingerLower;
    
    // Add RSI series (separate pane)
    const rsiSeries = chart.addLineSeries({
      color: '#e91e63',
      lineWidth: 2,
      title: 'RSI',
      priceScaleId: 'rsi',
      scaleMargins: {
        top: 0.85,
        bottom: 0,
      },
    });
    rsiSeriesRef.current = rsiSeries;
    
    // Add MACD series (separate pane)
    const macdSeries = chart.addLineSeries({
      color: '#00bcd4',
      lineWidth: 2,
      title: 'MACD',
      priceScaleId: 'macd',
      scaleMargins: {
        top: 0.9,
        bottom: 0.05,
      },
    });
    macdSeriesRef.current = macdSeries;
    
    const macdSignal = chart.addLineSeries({
      color: '#ff5722',
      lineWidth: 1,
      title: 'MACD Signal',
      priceScaleId: 'macd',
    });
    macdSignalRef.current = macdSignal;
    
    const macdHistogram = chart.addHistogramSeries({
      color: '#607d8b',
      title: 'MACD Histogram',
      priceScaleId: 'macd',
    });
    macdHistogramRef.current = macdHistogram;
    
    // Add volume series
    const volumeSeries = chart.addHistogramSeries({
      color: '#26a69a',
      priceFormat: {
        type: 'volume',
      },
      priceScaleId: 'volume',
      scaleMargins: {
        top: 0.75,
        bottom: 0,
      },
    });
    
    volumeSeriesRef.current = volumeSeries;
    
    // Configure volume price scale
    chart.priceScale('volume').applyOptions({
      scaleMargins: {
        top: 0.75,
        bottom: 0,
      },
      mode: 2,
      borderVisible: false,
    });
    
    // Configure RSI price scale
    chart.priceScale('rsi').applyOptions({
      scaleMargins: {
        top: 0.85,
        bottom: 0,
      },
      mode: 0,
      autoScale: false,
      invertScale: false,
      alignLabels: true,
      borderVisible: false,
      borderColor: '#2a2e39',
      entireTextOnly: false,
      minimumWidth: 0,
      ticksVisible: true,
    });
    
    // Configure MACD price scale
    chart.priceScale('macd').applyOptions({
      scaleMargins: {
        top: 0.9,
        bottom: 0.05,
      },
      mode: 2,
      borderVisible: false,
      borderColor: '#2a2e39',
    });
    
    // Handle window resize
    const handleResize = () => {
      if (chart && chartContainerRef.current) {
        chart.applyOptions({
          width: chartContainerRef.current.clientWidth,
          height: chartContainerRef.current.clientHeight,
        });
      }
    };
    
    window.addEventListener('resize', handleResize);
    
    return () => {
      window.removeEventListener('resize', handleResize);
      if (chart) {
        chart.remove();
      }
    };
  }, []);

  useEffect(() => {
    if (candles.length > 0 && candleSeriesRef.current && volumeSeriesRef.current) {
      // Prepare candlestick data
      const candleData = candles.map(candle => ({
        time: candle.time,
        open: candle.open,
        high: candle.high,
        low: candle.low,
        close: candle.close,
      }));
      
      // Prepare volume data with colors based on price movement
      const volumeData = candles.map(candle => ({
        time: candle.time,
        value: candle.volume,
        color: candle.close >= candle.open ? 'rgba(38, 166, 154, 0.5)' : 'rgba(239, 83, 80, 0.5)',
      }));
      
      // Calculate indicators
      const closePrices = candles.map(c => c.close);
      
      // EMA calculations
      const ema9Data = calculateEMA(closePrices, 9);
      const ema21Data = calculateEMA(closePrices, 21);
      const ema50Data = calculateEMA(closePrices, 50);
      
      // Bollinger Bands
      const bollingerBands = calculateBollingerBands(closePrices, 20, 2);
      
      // RSI
      const rsiData = calculateRSI(closePrices, 14);
      
      // MACD
      const macdData = calculateMACD(closePrices, 12, 26, 9);
      
      // Set candlestick and volume data
      candleSeriesRef.current.setData(candleData);
      volumeSeriesRef.current.setData(volumeData);
      
      // Set EMA data
      if (ema9SeriesRef.current) {
        const ema9Series = ema9Data.map((value, index) => ({
          time: candles[index].time,
          value: value,
        })).filter(item => item.value !== undefined);
        ema9SeriesRef.current.setData(ema9Series);
      }
      
      if (ema21SeriesRef.current) {
        const ema21Series = ema21Data.map((value, index) => ({
          time: candles[index].time,
          value: value,
        })).filter(item => item.value !== undefined);
        ema21SeriesRef.current.setData(ema21Series);
      }
      
      if (ema50SeriesRef.current) {
        const ema50Series = ema50Data.map((value, index) => ({
          time: candles[index].time,
          value: value,
        })).filter(item => item.value !== undefined);
        ema50SeriesRef.current.setData(ema50Series);
      }
      
      // Set Bollinger Bands data
      if (bollingerUpperRef.current && bollingerLowerRef.current) {
        const upperData = bollingerBands.upper.map((value, index) => ({
          time: candles[index].time,
          value: value,
        })).filter(item => item.value !== undefined);
        
        const lowerData = bollingerBands.lower.map((value, index) => ({
          time: candles[index].time,
          value: value,
        })).filter(item => item.value !== undefined);
        
        bollingerUpperRef.current.setData(upperData);
        bollingerLowerRef.current.setData(lowerData);
      }
      
      // Set RSI data
      if (rsiSeriesRef.current) {
        const rsiSeries = rsiData.map((value, index) => ({
          time: candles[index].time,
          value: value,
        })).filter(item => item.value !== undefined && !isNaN(item.value));
        rsiSeriesRef.current.setData(rsiSeries);
      }
      
      // Set MACD data
      if (macdSeriesRef.current && macdSignalRef.current && macdHistogramRef.current) {
        const macdSeries = macdData.macd.map((value, index) => ({
          time: candles[index].time,
          value: value,
        })).filter(item => item.value !== undefined && !isNaN(item.value));
        
        let signalIndex = 0;
        const signalSeries = [];
        for (let i = 0; i < candles.length; i++) {
          if (macdData.signal[signalIndex] !== undefined) {
            signalSeries.push({
              time: candles[i].time,
              value: macdData.signal[signalIndex],
            });
            signalIndex++;
          }
        }
        
        const histogramSeries = macdData.histogram.map((value, index) => ({
          time: candles[index].time,
          value: value,
          color: value >= 0 ? 'rgba(38, 166, 154, 0.7)' : 'rgba(239, 83, 80, 0.7)',
        })).filter(item => item.value !== undefined && !isNaN(item.value));
        
        macdSeriesRef.current.setData(macdSeries);
        macdSignalRef.current.setData(signalSeries);
        macdHistogramRef.current.setData(histogramSeries);
      }
      
      // Auto-scale to fit data
      if (chartRef.current) {
        chartRef.current.timeScale().fitContent();
      }
    }
  }, [candles]);

  useEffect(() => {
    fetchCandles();
    const interval = setInterval(fetchCandles, 2000); // Poll every 2 seconds
    return () => clearInterval(interval);
  }, []);

  return (
    <div style={{ position: 'relative', width: '100%', height: '100%' }}>
      <div 
        ref={chartContainerRef} 
        style={{ 
          width: '100%', 
          height: '100%',
          position: 'relative',
          background: '#131722'
        }} 
      />
      

    </div>
  );
};

export default Chart1m;