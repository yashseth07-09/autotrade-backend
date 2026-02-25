CREATE TABLE IF NOT EXISTS trades (
  id TEXT PRIMARY KEY,
  symbol TEXT NOT NULL,
  pair TEXT NOT NULL,
  margin_currency TEXT NOT NULL,
  side TEXT NOT NULL,
  setup TEXT NOT NULL,
  status TEXT NOT NULL,
  leverage INTEGER NOT NULL,
  qty REAL NOT NULL,
  entry_price REAL NOT NULL,
  stop_price REAL NOT NULL,
  target_price REAL,
  mark_price REAL,
  ltp REAL,
  liquidation_price REAL,
  risk_r REAL NOT NULL DEFAULT 1.0,
  opened_at TEXT NOT NULL,
  closed_at TEXT,
  exit_price REAL,
  pnl_usdt REAL,
  pnl_r REAL,
  notes TEXT
);

CREATE INDEX IF NOT EXISTS idx_trades_symbol_opened_at ON trades(symbol, opened_at DESC);
CREATE INDEX IF NOT EXISTS idx_trades_pair_opened_at ON trades(pair, opened_at DESC);
CREATE INDEX IF NOT EXISTS idx_trades_status ON trades(status);

CREATE TABLE IF NOT EXISTS trade_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  trade_id TEXT NOT NULL,
  event_type TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_trade_events_trade_id ON trade_events(trade_id, created_at DESC);
