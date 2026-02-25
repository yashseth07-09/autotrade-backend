from __future__ import annotations

import asyncio
import json
import sqlite3
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path

from autotrade.analytics import daily_trade_metrics
from autotrade.persistence.state_resume_store import StateResumeStore


class StateResumeAndMetricsTests(unittest.TestCase):
    def test_state_resume_write_and_read(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "state_resume.json"
            store = StateResumeStore(path)
            payload = {
                "session_id": "abc",
                "risk_state": {"daily_realized_pnl": 12.5, "daily_R": 1.25},
                "open_positions": [{"id": "p1", "pair": "B-TRX_USDT"}],
            }
            asyncio.run(store.write(payload))
            loaded = asyncio.run(store.load())
            self.assertEqual(loaded, payload)
            self.assertTrue(path.exists())

    def test_daily_metrics_reconstruction_from_sqlite(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db = Path(td) / "trades.sqlite"
            con = sqlite3.connect(db)
            try:
                con.executescript(
                    """
                    CREATE TABLE trades (
                      id TEXT PRIMARY KEY, symbol TEXT, pair TEXT, margin_currency TEXT, side TEXT, setup TEXT,
                      status TEXT, leverage INTEGER, qty REAL, entry_price REAL, stop_price REAL, target_price REAL,
                      mark_price REAL, ltp REAL, liquidation_price REAL, risk_r REAL, opened_at TEXT, closed_at TEXT,
                      exit_price REAL, pnl_usdt REAL, pnl_r REAL, notes TEXT
                    );
                    """
                )
                now = datetime.now(tz=UTC).replace(hour=10, minute=0, second=0, microsecond=0)
                rows = [
                    ("t1", "TRX", "B-TRX_USDT", "USDT", "LONG", "PULLBACK_CONTINUATION", "CLOSED", 3, 10, 1, 0.9, None, None, None, None, 1, now.isoformat(), now.isoformat(), 1.1, 5.0, 0.5, "{}"),
                    ("t2", "ETH", "B-ETH_USDT", "USDT", "SHORT", "BREAKOUT_CLOSE", "CLOSED", 3, 1, 100, 102, None, None, None, None, 1, now.isoformat(), now.isoformat(), 101, -3.0, -0.3, "{}"),
                    ("old", "BTC", "B-BTC_USDT", "USDT", "LONG", "BREAKOUT_CLOSE", "CLOSED", 3, 1, 100, 95, None, None, None, None, 1, "2020-01-01T00:00:00+00:00", "2020-01-01T01:00:00+00:00", 110, 10.0, 1.0, "{}"),
                ]
                con.executemany(
                    "INSERT INTO trades VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    rows,
                )
                con.commit()
            finally:
                con.close()

            m = daily_trade_metrics(db)
            self.assertEqual(m["trades_today"], 2)
            self.assertAlmostEqual(m["realized_today"], 2.0, places=6)
            self.assertAlmostEqual(m["realized_today_r"], 0.2, places=6)
            self.assertEqual(m["wins_today"], 1)
            self.assertEqual(m["losses_today"], 1)
            self.assertAlmostEqual(m["win_rate_today"], 50.0, places=6)


if __name__ == "__main__":
    unittest.main()

