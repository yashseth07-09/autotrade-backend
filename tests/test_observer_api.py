from __future__ import annotations

import importlib.util
import json
import sqlite3
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path

from observer_api import create_app


def _write_sample_files(root: Path, data_dir: Path) -> None:
    data_dir.mkdir(parents=True, exist_ok=True)
    snapshot = {
        "ts": datetime.now(tz=UTC).isoformat(),
        "health": {"bot_running": True, "last_cycle_ok": True, "cycle_errors": 0},
        "runtime": {"cycle_ms": 321.0, "dry_run": True, "last_cycle_ok": True, "last_cycle_error": None},
        "btc_macro": {"bias_4h": "NEUTRAL", "enabled": False},
        "top_candidates": [],
        "open_positions": [],
        "risk": {"equity_usdt": 1000, "can_trade": True, "consecutive_losses": 0},
        "pnl": {"realized_today_usdt": 0.0, "realized_today_r": 0.0, "unrealized_usdt": 0.0},
        "metrics": {"trades_today": 0, "win_rate_today": 0.0, "avg_R_today": 0.0, "max_drawdown_today": 0.0},
        "diagnostics": {"last_http_error": None, "last_http_error_ts": None, "last_exchange_latency_ms": 12.5},
        "top_rejects": [{"symbol": "B-TRX_USDT", "stage": "signal_confirmed_15m_close", "reason": "score_below_threshold"}],
        "config_view": {"runtime": {"dry_run": True}, "exchange": {"provider": "COINDCX_FUTURES", "margin_currency": "USDT"}},
        "recent_events_tail": [],
    }
    (data_dir / "latest_snapshot.json").write_text(json.dumps(snapshot), encoding="utf-8")
    events = [
        {"ts": datetime.now(tz=UTC).isoformat(), "type": "STAGE", "payload": {"symbol": "B-TRX_USDT", "stage": "signal_confirmed_15m_close", "passed": False, "message": "score_below_threshold"}},
        {"ts": datetime.now(tz=UTC).isoformat(), "type": "DIAG_HTTP_ERROR", "payload": {"url": "https://api.coindcx.com", "status": 500, "exception": "boom"}},
        {"ts": datetime.now(tz=UTC).isoformat(), "type": "CYCLE_ERROR", "payload": {"error": "x"}},
    ]
    with (data_dir / "events.jsonl").open("w", encoding="utf-8") as fh:
        for e in events:
            fh.write(json.dumps(e) + "\n")

    schema = (root / "sql" / "schema.sql").read_text(encoding="utf-8")
    con = sqlite3.connect(data_dir / "trades.sqlite")
    try:
        con.executescript(schema)
        now = datetime.now(tz=UTC).isoformat()
        rows = [
            ("t1", "TRX", "B-TRX_USDT", "USDT", "LONG", "PULLBACK_CONTINUATION", "CLOSED", 3, 10.0, 1.0, 0.9, None, None, None, None, 1.0, now, now, 1.1, 4.0, 0.4, "{}"),
            ("t2", "ETH", "B-ETH_USDT", "USDT", "SHORT", "BREAKOUT_CLOSE", "CLOSED", 3, 1.0, 100.0, 102.0, None, None, None, None, 1.0, now, now, 101.0, -2.0, -0.2, "{}"),
        ]
        con.executemany("INSERT INTO trades VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)
        con.commit()
    finally:
        con.close()


class ObserverApiTests(unittest.TestCase):
    @unittest.skipUnless(importlib.util.find_spec("httpx") is not None, "httpx is required for FastAPI TestClient")
    def test_filters_pagination_and_metrics(self) -> None:
        from fastapi.testclient import TestClient

        with tempfile.TemporaryDirectory() as td:
            temp_root = Path(td)
            data_dir = temp_root / "data"
            project_root = Path(__file__).resolve().parents[1]
            _write_sample_files(project_root, data_dir)
            cfg = temp_root / "config.yaml"
            cfg.write_text(
                "\n".join(
                    [
                        "runtime:",
                        f"  data_dir: {data_dir.as_posix()}",
                        "observer:",
                        "  host: 127.0.0.1",
                        "  port: 8000",
                        "  max_trade_rows: 500",
                        "  max_events_limit: 500",
                    ]
                ),
                encoding="utf-8",
            )

            app = create_app(str(cfg))
            client = TestClient(app)

            r = client.get("/events", params={"type": "DIAG", "limit": 10, "offset": 0})
            self.assertEqual(r.status_code, 200)
            data = r.json()
            self.assertEqual(data["count"], 1)
            self.assertEqual(data["items"][0]["type"], "DIAG_HTTP_ERROR")

            r = client.get("/events", params={"limit": 1, "offset": 1})
            self.assertEqual(r.status_code, 200)
            self.assertEqual(r.json()["limit"], 1)
            self.assertEqual(r.json()["offset"], 1)

            r = client.get("/trades", params={"symbol": "B-TRX_USDT", "limit": 10})
            self.assertEqual(r.status_code, 200)
            self.assertEqual(r.json()["count"], 1)
            self.assertEqual(r.json()["items"][0]["pair"], "B-TRX_USDT")

            r = client.get("/metrics")
            self.assertEqual(r.status_code, 200)
            m = r.json()
            self.assertEqual(m["trades_today"], 2)
            self.assertAlmostEqual(m["realized_today"], 2.0, places=6)
            self.assertAlmostEqual(m["win_rate_today"], 50.0, places=6)


if __name__ == "__main__":
    unittest.main()
