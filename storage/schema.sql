-- trades: завершённые сделки (round-trip)
CREATE TABLE IF NOT EXISTS trades (
  id            TEXT PRIMARY KEY,            -- uuid
  symbol        TEXT NOT NULL,
  side          TEXT CHECK(side IN ('LONG','SHORT')) NOT NULL,
  qty           REAL NOT NULL,
  entry_ts_ms   INTEGER NOT NULL,
  exit_ts_ms    INTEGER NOT NULL,
  entry_px      REAL NOT NULL,
  exit_px       REAL NOT NULL,
  sl_px         REAL,
  tp_px         REAL,
  r             REAL NOT NULL DEFAULT 0,     -- итог в R
  pnl_usd       REAL NOT NULL DEFAULT 0,
  reason_open   TEXT,
  reason_close  TEXT,
  meta          TEXT                         -- JSON (фичи/фильтры)
);

-- orders: заявки (для аудита исполнения)
CREATE TABLE IF NOT EXISTS orders (
  id            TEXT PRIMARY KEY,            -- clOrdId/uuid
  trade_id      TEXT,                        -- может быть NULL до связывания
  symbol        TEXT NOT NULL,
  side          TEXT CHECK(side IN ('BUY','SELL')) NOT NULL,
  type          TEXT CHECK(type IN ('LIMIT','MARKET')) NOT NULL,
  reduce_only   INTEGER NOT NULL,            -- 0/1
  px            REAL,
  qty           REAL NOT NULL,
  status        TEXT NOT NULL,               -- NEW/FILLED/CANCELED/EXPIRED/PARTIAL
  created_ms    INTEGER NOT NULL,
  updated_ms    INTEGER NOT NULL
);

-- fills: исполнения (частичные/полные)
CREATE TABLE IF NOT EXISTS fills (
  id            TEXT PRIMARY KEY,            -- uuid
  order_id      TEXT NOT NULL,
  ts_ms         INTEGER NOT NULL,
  px            REAL NOT NULL,
  qty           REAL NOT NULL,
  fee_usd       REAL DEFAULT 0
);

-- daily_stats: агрегаты по дню (UTC)
CREATE TABLE IF NOT EXISTS daily_stats (
  day_utc       TEXT PRIMARY KEY,            -- 'YYYY-MM-DD'
  trades        INTEGER NOT NULL,
  wins          INTEGER NOT NULL,
  losses        INTEGER NOT NULL,
  pnl_usd       REAL NOT NULL,
  pnl_r         REAL NOT NULL,
  max_dd_r      REAL NOT NULL
);

-- Индексы
CREATE INDEX IF NOT EXISTS idx_trades_symbol ON trades(symbol);
CREATE INDEX IF NOT EXISTS idx_orders_symbol ON orders(symbol);
