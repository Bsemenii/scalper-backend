import React, { useEffect, useRef, useState } from 'react';
import { createChart } from 'lightweight-charts';
import chartsManager from '../services/chartsManager';

// --- Indicator helpers ---------------------------------------------------

function calculateEMA(values, period) {
  if (!values || values.length === 0) return [];
  const k = 2 / (period + 1);
  const ema = new Array(values.length);
  ema[0] = values[0];
  for (let i = 1; i < values.length; i++) {
    ema[i] = values[i] * k + ema[i - 1] * (1 - k);
  }
  return ema;
}

function calculateBollinger(values, period = 20, stdDev = 2) {
  const upper = new Array(values.length);
  const lower = new Array(values.length);
  const middle = new Array(values.length);

  for (let i = period - 1; i < values.length; i++) {
    const slice = values.slice(i - period + 1, i + 1);
    const mean = slice.reduce((s, v) => s + v, 0) / period;
    const variance = slice.reduce((s, v) => s + Math.pow(v - mean, 2), 0) / period;
    const sd = Math.sqrt(variance);
    middle[i] = mean;
    upper[i] = mean + sd * stdDev;
    lower[i] = mean - sd * stdDev;
  }
  return { upper, lower, middle };
}

function calculateRSI(closes, period = 14) {
  const rsi = new Array(closes.length);
  const gains = new Array(closes.length).fill(0);
  const losses = new Array(closes.length).fill(0);

  for (let i = 1; i < closes.length; i++) {
    const change = closes[i] - closes[i - 1];
    gains[i] = Math.max(0, change);
    losses[i] = Math.max(0, -change);
  }

  for (let i = period; i < closes.length; i++) {
    const avgGain = gains.slice(i - period + 1, i + 1).reduce((s, v) => s + v, 0) / period;
    const avgLoss = losses.slice(i - period + 1, i + 1).reduce((s, v) => s + v, 0) / period;
    if (avgLoss === 0) {
      rsi[i] = 100;
    } else {
      const rs = avgGain / avgLoss;
      rsi[i] = 100 - 100 / (1 + rs);
    }
  }
  return rsi;
}

function calculateMACD(closes, fast = 12, slow = 26, signal = 9) {
  const emaFast = calculateEMA(closes, fast);
  const emaSlow = calculateEMA(closes, slow);

  const macd = emaFast.map((v, i) =>
    v !== undefined && emaSlow[i] !== undefined ? v - emaSlow[i] : undefined,
  );

  const macdFiltered = macd.filter((v) => v !== undefined);
  const signalLine = calculateEMA(macdFiltered, signal);

  const histogram = new Array(macd.length);
  let j = 0;
  for (let i = 0; i < macd.length; i++) {
    if (macd[i] !== undefined && signalLine[j] !== undefined) {
      histogram[i] = macd[i] - signalLine[j];
      j++;
    }
  }
  return { macd, signal: signalLine, histogram };
}

function calculateVWAP(candles) {
  let cumulativePV = 0;
  let cumulativeV = 0;
  const vwap = new Array(candles.length);

  for (let i = 0; i < candles.length; i++) {
    const tp = (candles[i].high + candles[i].low + candles[i].close) / 3;
    cumulativePV += tp * candles[i].volume;
    cumulativeV += candles[i].volume;
    vwap[i] = cumulativeV > 0 ? cumulativePV / cumulativeV : undefined;
  }
  return vwap;
}

// --- Trade markers helpers ----------------------------------------------

function normalizeSide(side) {
  const s = String(side || '').toUpperCase();
  if (s === 'LONG') return 'BUY';
  if (s === 'SHORT') return 'SELL';
  if (s === 'BUY' || s === 'SELL') return s;
  return 'BUY';
}

function tsToSeconds(ts) {
  if (!ts && ts !== 0) return undefined;
  const n = Number(ts);
  if (!Number.isFinite(n)) return undefined;
  return Math.floor(n / 1000); // backend дає ms → тут seconds
}

/**
 * Очікуємо trades у форматі /trades з бекенда:
 *  {
 *    symbol,
 *    side,
 *    entry_px,
 *    sl_px?,
 *    tp_px?,
 *    opened_ts / opened_ts_ms,
 *    closed_ts?,
 *    ...
 *  }
 */
function buildMarkersFromTrades(trades) {
  if (!Array.isArray(trades) || trades.length === 0) return [];

  const markers = [];

  for (const t of trades) {
    const side = normalizeSide(t.side);
    const entryPx = Number(t.entry_px ?? t.entry_price ?? 0);
    const slPx = t.sl_px != null ? Number(t.sl_px) : undefined;
    const tpPx = t.tp_px != null ? Number(t.tp_px) : undefined;

    const time =
      tsToSeconds(t.opened_ts_ms ?? t.opened_ts) ??
      tsToSeconds(t.closed_ts);

    if (!time || !entryPx || !Number.isFinite(entryPx)) continue;

    // Entry marker
    markers.push({
      time,
      position: side === 'BUY' ? 'belowBar' : 'aboveBar',
      shape: side === 'BUY' ? 'arrowUp' : 'arrowDown',
      color: side === 'BUY' ? '#26a69a' : '#ef5350',
      text: `${side} ${entryPx.toFixed(2)}`,
    });

    if (slPx && Number.isFinite(slPx)) {
      markers.push({
        time,
        position: 'belowBar',
        shape: 'circle',
        color: '#ef4444',
        text: `SL ${slPx.toFixed(2)}`,
      });
    }

    if (tpPx && Number.isFinite(tpPx)) {
      markers.push({
        time,
        position: 'aboveBar',
        shape: 'circle',
        color: '#22c55e',
        text: `TP ${tpPx.toFixed(2)}`,
      });
    }
  }

  return markers;
}

// --- Component -----------------------------------------------------------

const CandleChart = ({
  timeframe = '1m',
  title,
  symbol = 'BTCUSDT',
  openPositions = [],
  trades = [],
}) => {
  const chartContainerRef = useRef(null);
  const chartRef = useRef(null);
  const candleSeriesRef = useRef(null);
  const volumeSeriesRef = useRef(null);

  const ema9Ref = useRef(null);
  const ema21Ref = useRef(null);
  const ema50Ref = useRef(null);
  const bbUpperRef = useRef(null);
  const bbLowerRef = useRef(null);
  const vwapRef = useRef(null);
  const rsiRef = useRef(null);
  const macdRef = useRef(null);
  const macdSignalRef = useRef(null);
  const macdHistRef = useRef(null);
  const priceLinesRef = useRef([]);
  const chart2Ref = useRef(null);
  const series2Ref = useRef(null);
  const chartContainer2Ref = useRef(null);


  

  const [candles, setCandles] = useState([]);
  const lastSizeRef = useRef({ width: 0, height: 0 });
  const isAutoFitRef = useRef(true);
  const isProgrammaticZoomRef = useRef(false);

  // --- init chart --------------------------------------------------------
  useEffect(() => {
    if (!chartContainerRef.current) return;

    const initialWidth = chartContainerRef.current.clientWidth || 500;
    const initialHeight = chartContainerRef.current.clientHeight || 360;
    lastSizeRef.current = { width: initialWidth, height: initialHeight };

    const chart = createChart(chartContainerRef.current, {
      width: initialWidth,
      height: initialHeight,
      layout: {
        background: { type: 'solid', color: '#131722' },
        textColor: '#e5e7eb',
        fontSize: 12,
        fontFamily: 'system-ui, -apple-system, sans-serif',
      },
      grid: {
        vertLines: { color: '#2a2e39', style: 0, visible: true },
        horzLines: { color: '#2a2e39', style: 0, visible: true },
      },
      crosshair: { mode: 1 },
      rightPriceScale: {
        borderColor: '#2a2e39',
        borderVisible: true,
        scaleMargins: { top: 0.02, bottom: 0.18 },
      },
      timeScale: {
        borderColor: '#2a2e39',
        borderVisible: true,
        timeVisible: true,
        secondsVisible: false,
        rightOffset: 8,
        barSpacing: 13,
        minBarSpacing: 3,
      },
      handleScroll: { mouseWheel: true, pressedMouseMove: true },
      handleScale: { axisPressedMouseMove: true, mouseWheel: true, pinch: true },
    });

    chartRef.current = chart;

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

    const volumeSeries = chart.addHistogramSeries({
      color: '#26a69a',
      priceFormat: { type: 'volume' },
      priceScaleId: 'volume',
      scaleMargins: { top: 0.82, bottom: 0 },
    });
    volumeSeriesRef.current = volumeSeries;

    chart.priceScale('volume').applyOptions({
      scaleMargins: { top: 0.82, bottom: 0 },
      mode: 2,
      borderVisible: false,
    });

    // Indicator series on main pane
    ema9Ref.current = chart.addLineSeries({
      color: '#60a5fa',
      lineWidth: 1,
      priceScaleId: 'right',
    });
    ema21Ref.current = chart.addLineSeries({
      color: '#fb923c',
      lineWidth: 1,
      priceScaleId: 'right',
    });
    ema50Ref.current = chart.addLineSeries({
      color: '#a855f7',
      lineWidth: 2,
      priceScaleId: 'right',
    });
    vwapRef.current = chart.addLineSeries({
      color: '#22c55e',
      lineWidth: 1,
      priceScaleId: 'right',
    });
    bbUpperRef.current = chart.addLineSeries({
      color: 'rgba(148,163,184,0.7)',
      lineWidth: 1,
      priceScaleId: 'right',
    });
    bbLowerRef.current = chart.addLineSeries({
      color: 'rgba(148,163,184,0.7)',
      lineWidth: 1,
      priceScaleId: 'right',
    });

    // Secondary overlays for RSI and MACD
    rsiRef.current = chart.addLineSeries({
      color: '#fb7185',
      lineWidth: 2,
      priceScaleId: 'rsi',
    });
    macdRef.current = chart.addLineSeries({
      color: '#22d3ee',
      lineWidth: 2,
      priceScaleId: 'macd',
    });
    macdSignalRef.current = chart.addLineSeries({
      color: '#f97316',
      lineWidth: 1,
      priceScaleId: 'macd',
    });
    macdHistRef.current = chart.addHistogramSeries({
      color: '#64748b',
      priceScaleId: 'macd',
    });

    chart.priceScale('rsi').applyOptions({
      scaleMargins: { top: 0.86, bottom: 0 },
      mode: 0,
      borderVisible: false,
    });
    chart.priceScale('macd').applyOptions({
      scaleMargins: { top: 0.91, bottom: 0.05 },
      mode: 2,
      borderVisible: false,
    });

    const applySizeIfChanged = () => {
      if (!chartContainerRef.current || !chart) return;
      const w = chartContainerRef.current.clientWidth;
      const h = chartContainerRef.current.clientHeight;
      if (w <= 0 || h <= 0) return;
      if (w === lastSizeRef.current.width && h === lastSizeRef.current.height) return;
      lastSizeRef.current = { width: w, height: h };
      chart.applyOptions({ width: w, height: h });
    };

    const handleResize = () => {
      applySizeIfChanged();
    };
    window.addEventListener('resize', handleResize);

    let resizeObserver;
    if (window.ResizeObserver) {
      resizeObserver = new ResizeObserver(() => {
        applySizeIfChanged();
      });
      resizeObserver.observe(chartContainerRef.current);
    }

    const timeScale = chart.timeScale();
    const onRangeChanged = () => {
      if (!isProgrammaticZoomRef.current) {
        isAutoFitRef.current = false;
      }
    };
    timeScale.subscribeVisibleTimeRangeChange(onRangeChanged);

    return () => {
      window.removeEventListener('resize', handleResize);
      if (resizeObserver) resizeObserver.disconnect();
      if (timeScale && onRangeChanged) {
        timeScale.unsubscribeVisibleTimeRangeChange(onRangeChanged);
      }
      if (chart) chart.remove();
    };
  }, []);

  // --- subscribe to candles via chartsManager ----------------------------
  useEffect(() => {
    let unsubscribe;
    try {
      unsubscribe = chartsManager.subscribe(
        symbol,
        timeframe,
        (payload) => {
          if (payload && Array.isArray(payload.candles)) {
            setCandles(payload.candles);
          }
        },
        () => {},
      );
    } catch (e) {
      console.error('chartsManager.subscribe error', e);
    }

    return () => {
      if (typeof unsubscribe === 'function') {
        try {
          unsubscribe();
        } catch (e) {
          console.warn('chartsManager.unsubscribe error', e);
        }
      }
    };
  }, [symbol, timeframe]);

  // --- update candles + indicators --------------------------------------
  useEffect(() => {
    if (!candles.length || !candleSeriesRef.current || !volumeSeriesRef.current) return;

    const candleData = candles.map((c) => ({
      time: c.time,
      open: c.open,
      high: c.high,
      low: c.low,
      close: c.close,
    }));

    const volumeData = candles.map((c) => ({
      time: c.time,
      value: c.volume,
      color: c.close >= c.open ? 'rgba(34,197,94,0.45)' : 'rgba(248,113,113,0.55)',
    }));

    candleSeriesRef.current.setData(candleData);
    volumeSeriesRef.current.setData(volumeData);

    const closes = candles.map((c) => c.close);

    const ema9Arr = calculateEMA(closes, 9);
    const ema21Arr = calculateEMA(closes, 21);
    const ema50Arr = calculateEMA(closes, 50);
    const vwapArr = calculateVWAP(candles);
    const bb = calculateBollinger(closes, 20, 2);
    const rsiArr = calculateRSI(closes, 14);
    const macdData = calculateMACD(closes, 12, 26, 9);

    const ema9 = ema9Arr
      .map((v, i) => (v !== undefined ? { time: candles[i].time, value: v } : null))
      .filter(Boolean);
    const ema21 = ema21Arr
      .map((v, i) => (v !== undefined ? { time: candles[i].time, value: v } : null))
      .filter(Boolean);
    const ema50 = ema50Arr
      .map((v, i) => (v !== undefined ? { time: candles[i].time, value: v } : null))
      .filter(Boolean);
    const vwap = vwapArr
      .map((v, i) => (v !== undefined ? { time: candles[i].time, value: v } : null))
      .filter(Boolean);

    const bbUpper = bb.upper
      .map((v, i) => (v !== undefined ? { time: candles[i].time, value: v } : null))
      .filter(Boolean);
    const bbLower = bb.lower
      .map((v, i) => (v !== undefined ? { time: candles[i].time, value: v } : null))
      .filter(Boolean);

    const rsi = rsiArr
      .map((v, i) => (v !== undefined ? { time: candles[i].time, value: v } : null))
      .filter(Boolean);

    const macdSeries = macdData.macd
      .map((v, i) => (v !== undefined ? { time: candles[i].time, value: v } : null))
      .filter(Boolean);

    const signal = [];
    let idx = 0;
    for (let i = 0; i < candles.length; i++) {
      if (
        macdData.macd[i] !== undefined &&
        macdData.signal[idx] !== undefined
      ) {
        signal.push({
          time: candles[i].time,
          value: macdData.signal[idx],
        });
        idx++;
      }
    }

    const hist = macdData.histogram
      .map((v, i) =>
        v !== undefined
          ? {
              time: candles[i].time,
              value: v,
              color:
                v >= 0
                  ? 'rgba(34,197,94,0.7)'
                  : 'rgba(248,113,113,0.75)',
            }
          : null,
      )
      .filter(Boolean);

    if (ema9Ref.current) ema9Ref.current.setData(ema9);
    if (ema21Ref.current) ema21Ref.current.setData(ema21);
    if (ema50Ref.current) ema50Ref.current.setData(ema50);
    if (vwapRef.current) vwapRef.current.setData(vwap);
    if (bbUpperRef.current) bbUpperRef.current.setData(bbUpper);
    if (bbLowerRef.current) bbLowerRef.current.setData(bbLower);
    if (rsiRef.current) rsiRef.current.setData(rsi);
    if (macdRef.current) macdRef.current.setData(macdSeries);
    if (macdSignalRef.current) macdSignalRef.current.setData(signal);
    if (macdHistRef.current) macdHistRef.current.setData(hist);

    if (isAutoFitRef.current && chartRef.current) {
      chartRef.current.timeScale().fitContent();
    }
  }, [candles]);

  // --- apply trade markers ----------------------------------------------
  useEffect(() => {
    if (!candleSeriesRef.current) return;
    // Поки не малюємо маркери з історичних трейдів – будемо працювати через лінії по відкритим позам
    candleSeriesRef.current.setMarkers([]);
  }, [trades]);

  useEffect(() => {
    if (!candleSeriesRef.current) return;
  
    const series = candleSeriesRef.current;
  
    // 1) Прибрати старі лінії
    if (priceLinesRef.current && priceLinesRef.current.length) {
      priceLinesRef.current.forEach((line) => {
        try {
          series.removePriceLine(line);
        } catch (e) {
          // ignore
        }
      });
    }
    priceLinesRef.current = [];
  
    // 2) Якщо немає відкритих позицій — просто нічого не малюємо
    if (!openPositions || openPositions.length === 0) return;
  
    openPositions.forEach((pos) => {
      const side = normalizeSide(pos.side);
      const entry = Number(pos.entry_px ?? pos.entry_price);
      const sl = pos.sl_px != null ? Number(pos.sl_px) : undefined;
      const tp = pos.tp_px != null ? Number(pos.tp_px) : undefined;
  
      if (Number.isFinite(entry)) {
        const entryLine = series.createPriceLine({
          price: entry,
          color: side === 'BUY' ? '#22c55e' : '#f97316',
          lineWidth: 2,
          lineStyle: 0,
          axisLabelVisible: true,
          title: 'ENTRY',
        });
        priceLinesRef.current.push(entryLine);
      }
  
      if (Number.isFinite(sl)) {
        const slLine = series.createPriceLine({
          price: sl,
          color: '#ef4444',
          lineWidth: 2,
          lineStyle: 2,
          axisLabelVisible: true,
          title: 'SL',
        });
        priceLinesRef.current.push(slLine);
      }
  
      if (Number.isFinite(tp)) {
        const tpLine = series.createPriceLine({
          price: tp,
          color: '#22c55e',
          lineWidth: 2,
          lineStyle: 2,
          axisLabelVisible: true,
          title: 'TP',
        });
        priceLinesRef.current.push(tpLine);
      }
    });
  }, [openPositions]);

  // --- controls ----------------------------------------------------------
  const zoomIn = () => {
    if (!chartRef.current) return;
    const ts = chartRef.current.timeScale();
    ts.setBarSpacing(Math.min(ts.barSpacing() + 2, 40));
    isAutoFitRef.current = false;
  };

  const zoomOut = () => {
    if (!chartRef.current) return;
    const ts = chartRef.current.timeScale();
    ts.setBarSpacing(Math.max(ts.barSpacing() - 2, 3));
    isAutoFitRef.current = false;
  };

  const resetView = () => {
    if (!chartRef.current) return;
    isAutoFitRef.current = true;
    isProgrammaticZoomRef.current = true;
    chartRef.current.timeScale().fitContent();
    setTimeout(() => {
      isProgrammaticZoomRef.current = false;
    }, 0);
  };



  return (
    <div
      style={{
        position: 'relative',
        width: '100%',
        height: '380px',         // робимо графік вищим
        minWidth: 0,
        minHeight: '320px',
      }}
    >
      <div
        style={{
          position: 'absolute',
          top: 8,
          left: 10,
          zIndex: 2,
          display: 'flex',
          gap: 8,
        }}
      >
        <span
          style={{
            color: '#e5e7eb',
            fontSize: 12,
            padding: '5px 10px',
            background: '#020617',
            border: '1px solid #1f2937',
            borderRadius: 6,
          }}
        >
          {title || `${symbol} ${timeframe}`}
        </span>
      </div>

      <div
        style={{
          position: 'absolute',
          top: 8,
          right: 10,
          zIndex: 2,
          display: 'flex',
          gap: 6,
        }}
      >
        <button
          onClick={zoomIn}
          style={{
            background: '#020617',
            color: '#e5e7eb',
            border: '1px solid #1f2937',
            padding: '4px 10px',
            fontSize: 11,
            borderRadius: 6,
            cursor: 'pointer',
          }}
        >
          Zoom +
        </button>
        <button
          onClick={zoomOut}
          style={{
            background: '#020617',
            color: '#e5e7eb',
            border: '1px solid #1f2937',
            padding: '4px 10px',
            fontSize: 11,
            borderRadius: 6,
            cursor: 'pointer',
          }}
        >
          Zoom -
        </button>
        <button
          onClick={resetView}
          style={{
            background: '#020617',
            color: '#e5e7eb',
            border: '1px solid #1f2937',
            padding: '4px 10px',
            fontSize: 11,
            borderRadius: 6,
            cursor: 'pointer',
          }}
        >
          Reset
        </button>
      </div>

      <div
        ref={chartContainerRef}
        style={{
          width: '100%',
          height: '100%',
          position: 'relative',
          background: '#020617',
          minWidth: 0,
          minHeight: 0,
        }}
      />
    </div>
  );
};


export default CandleChart;