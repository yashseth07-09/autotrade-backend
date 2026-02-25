from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any


def _parse_iso(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=UTC)
    except Exception:
        return None


def day_bounds_utc(now: datetime | None = None) -> tuple[datetime, datetime]:
    now = now or datetime.now(tz=UTC)
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1)
    return start, end


def _db_rows(db_path: Path, sql: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
    if not db_path.exists():
        return []
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    try:
        rows = con.execute(sql, params).fetchall()
        return [dict(r) for r in rows]
    finally:
        con.close()


def list_trades(
    db_path: Path,
    *,
    symbol: str | None = None,
    from_ts: str | None = None,
    to_ts: str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> list[dict[str, Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    if symbol:
        clauses.append("(symbol = ? OR pair = ?)")
        params.extend([symbol, symbol])
    if from_ts:
        clauses.append("opened_at >= ?")
        params.append(from_ts)
    if to_ts:
        clauses.append("opened_at <= ?")
        params.append(to_ts)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    sql = f"SELECT * FROM trades {where} ORDER BY opened_at DESC LIMIT ? OFFSET ?"
    params.extend([int(limit), int(offset)])
    return _db_rows(db_path, sql, tuple(params))


def list_open_trades(db_path: Path) -> list[dict[str, Any]]:
    return _db_rows(
        db_path,
        "SELECT * FROM trades WHERE status IN ('OPEN','PARTIAL') ORDER BY opened_at DESC",
        (),
    )


def daily_trade_metrics(db_path: Path, *, day_start: datetime | None = None, day_end: datetime | None = None) -> dict[str, Any]:
    if day_start is None and day_end is None:
        day_start, day_end = day_bounds_utc()
    else:
        base = day_start or datetime.now(tz=UTC)
        if base.tzinfo is None:
            base = base.replace(tzinfo=UTC)
        day_start = (day_start or base).replace(tzinfo=(day_start or base).tzinfo or UTC)
        day_end = day_end or (day_start + timedelta(days=1))
    rows = _db_rows(
        db_path,
        """
        SELECT * FROM trades
        WHERE closed_at IS NOT NULL
          AND closed_at >= ?
          AND closed_at < ?
        ORDER BY closed_at ASC
        """,
        (day_start.isoformat(), day_end.isoformat()),
    )
    return compute_metrics_from_closed_trades(rows)


def compute_metrics_from_closed_trades(rows: list[dict[str, Any]]) -> dict[str, Any]:
    trades_today = len(rows)
    realized_today = 0.0
    realized_r_today = 0.0
    wins = 0
    losses = 0
    current_loss_streak = 0
    max_consecutive_losses_today = 0
    cumulative_usdt = 0.0
    peak_usdt = 0.0
    max_dd_today = 0.0
    last_trade_ts = None
    last_exit_symbol = None
    last_entry_symbol = None

    for row in rows:
        pnl_usdt = float(row.get("pnl_usdt") or 0.0)
        pnl_r = float(row.get("pnl_r") or 0.0)
        realized_today += pnl_usdt
        realized_r_today += pnl_r
        if pnl_usdt > 0:
            wins += 1
        elif pnl_usdt < 0:
            losses += 1
        if pnl_r < 0:
            current_loss_streak += 1
            if current_loss_streak > max_consecutive_losses_today:
                max_consecutive_losses_today = current_loss_streak
        elif pnl_r > 0:
            current_loss_streak = 0
        cumulative_usdt += pnl_usdt
        peak_usdt = max(peak_usdt, cumulative_usdt)
        dd = peak_usdt - cumulative_usdt
        max_dd_today = max(max_dd_today, dd)
        last_trade_ts = row.get("closed_at") or row.get("opened_at") or last_trade_ts
        last_exit_symbol = row.get("pair") or row.get("symbol") or last_exit_symbol
        last_entry_symbol = row.get("pair") or row.get("symbol") or last_entry_symbol

    win_rate_today = (wins / trades_today * 100.0) if trades_today else 0.0
    avg_R_today = (realized_r_today / trades_today) if trades_today else 0.0
    return {
        "trades_today": trades_today,
        "wins_today": wins,
        "losses_today": losses,
        "win_rate_today": round(win_rate_today, 2),
        "realized_today": round(realized_today, 8),
        "realized_today_r": round(realized_r_today, 8),
        "avg_R_today": round(avg_R_today, 6),
        "max_drawdown_today": round(max_dd_today, 8),
        "max_consecutive_losses_today": max_consecutive_losses_today,
        "consecutive_losses": current_loss_streak,
        "last_trade_ts": last_trade_ts,
        "last_entry_symbol": last_entry_symbol,
        "last_exit_symbol": last_exit_symbol,
    }


def aggregate_metrics_from_snapshot_and_db(snapshot: dict[str, Any] | None, db_path: Path) -> dict[str, Any]:
    daily = daily_trade_metrics(db_path)
    latest_trade = list_trades(db_path, limit=1, offset=0)
    latest_closed = _db_rows(
        db_path,
        "SELECT * FROM trades WHERE closed_at IS NOT NULL ORDER BY closed_at DESC LIMIT 1",
        (),
    )
    last_cycle = (snapshot or {}).get("runtime", {}) if snapshot else {}
    return {
        "trades_today": daily["trades_today"],
        "win_rate_today": daily["win_rate_today"],
        "realized_today": daily["realized_today"],
        "avg_R_today": daily["avg_R_today"],
        "max_dd_today": daily["max_drawdown_today"],
        "last_trade_ts": (latest_closed[0].get("closed_at") if latest_closed else None) or (latest_trade[0].get("opened_at") if latest_trade else None),
        "last_entry_symbol": (latest_trade[0].get("pair") or latest_trade[0].get("symbol")) if latest_trade else None,
        "last_exit_symbol": (latest_closed[0].get("pair") or latest_closed[0].get("symbol")) if latest_closed else None,
        "cycle_ms": last_cycle.get("cycle_ms"),
    }
