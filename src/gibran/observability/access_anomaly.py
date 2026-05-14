"""Access-pattern anomaly detection over gibran_query_log.

Reuses the same trailing-N-sigma rule as the quality-rule anomaly type,
but the observation is per-(user, day) query volume from the audit
log rather than a column statistic. Catches "user X just queried 100x
their typical daily volume" kind of patterns.

Surface: a single function `detect_access_anomalies(con, ...)` that
returns a list of `AccessAnomaly` records. The CLI's
`gibran detect-access-anomalies` command (added separately) prints them.

V1 scope:
  * Volume by (user_id, calendar-day-UTC), default trailing 14 days
  * Single statistic: total queries per day
  * Bootstrapping: users with fewer than 3 history days are skipped
  * No "block this user" action -- the function just reports
"""
from __future__ import annotations

from dataclasses import dataclass

import duckdb


@dataclass(frozen=True)
class AccessAnomaly:
    user_id: str
    role_id: str | None
    today_count: int
    mean: float
    stddev: float
    z_score: float
    trailing_days: int


def detect_access_anomalies(
    con: duckdb.DuckDBPyConnection,
    *,
    trailing_days: int = 14,
    n_sigma: float = 3.0,
    min_history_days: int = 3,
) -> list[AccessAnomaly]:
    """Walk gibran_query_log; flag users whose query volume today is
    > `n_sigma` standard deviations above their trailing-day mean.

    Returns one AccessAnomaly per flagged (user_id, role_id) pair.
    Users with insufficient history are silently skipped.
    """
    if trailing_days < 2:
        raise ValueError("trailing_days must be >= 2")
    if n_sigma <= 0:
        raise ValueError("n_sigma must be > 0")

    # Bucket per (user, role, day). DuckDB's DATE_TRUNC keeps the
    # comparison aligned to UTC days regardless of local time. The
    # INTERVAL literal is interpolated rather than parameterized -- DuckDB
    # doesn't accept `INTERVAL ?` placeholders, and trailing_days is an
    # int we control (no injection surface).
    days = int(trailing_days) + 1
    rows = con.execute(
        f"SELECT user_id, role_id, DATE_TRUNC('day', created_at) AS d, "
        f"COUNT(*) AS n "
        f"FROM gibran_query_log "
        f"WHERE created_at >= now() - INTERVAL '{days}' DAY "
        f"GROUP BY user_id, role_id, d"
    ).fetchall()

    # DuckDB's now() returns tz-AWARE datetime; DATE_TRUNC over a naive
    # TIMESTAMP column returns NAIVE. Comparing the two in Python returns
    # False even when the wall-clock day matches. Compare by .date() to
    # sidestep tzinfo differences entirely.
    today_dt = con.execute(
        "SELECT DATE_TRUNC('day', now())"
    ).fetchone()[0]
    today_date = today_dt.date() if hasattr(today_dt, "date") else today_dt

    per_user: dict[tuple[str, str | None], dict] = {}
    for user_id, role_id, day, n in rows:
        key = (user_id, role_id)
        bucket = per_user.setdefault(key, {"history": [], "today": 0})
        day_date = day.date() if hasattr(day, "date") else day
        if day_date == today_date:
            bucket["today"] = int(n)
        else:
            bucket["history"].append(int(n))

    anomalies: list[AccessAnomaly] = []
    for (user_id, role_id), bucket in per_user.items():
        history = bucket["history"]
        today_count = bucket["today"]
        if len(history) < min_history_days:
            continue
        mean = sum(history) / len(history)
        variance = sum((h - mean) ** 2 for h in history) / (len(history) - 1) \
            if len(history) > 1 else 0.0
        stddev = variance ** 0.5
        if stddev == 0:
            # Constant history -- flag only if today differs.
            if today_count != mean:
                anomalies.append(AccessAnomaly(
                    user_id=user_id, role_id=role_id,
                    today_count=today_count, mean=mean, stddev=0.0,
                    z_score=float("inf"), trailing_days=len(history),
                ))
            continue
        z = (today_count - mean) / stddev
        if z > n_sigma:
            anomalies.append(AccessAnomaly(
                user_id=user_id, role_id=role_id,
                today_count=today_count, mean=mean, stddev=stddev,
                z_score=z, trailing_days=len(history),
            ))
    return anomalies
