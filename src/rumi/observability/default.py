"""DefaultObservability: V1.5 + V2 hybrid.

V2 path: latest_blocking_failures reads from rumi_source_health (O(1) PK
lookup) when the row exists. The cache is populated by refresh_health,
which is called by `rumi check` after running rules.

V1.5 fallback: when no rumi_source_health row exists for a source (e.g.
the first time a query touches it, before any `rumi check` has run), we
re-aggregate from rumi_quality_runs + the rule tables directly. This means
the system is *correct* even without the cache, just slower per call.

Time arithmetic is done in SQL via DATE_DIFF -- avoids the
DuckDB-local vs Python-UTC drift that bit us during V1.5 bring-up.

`record_run` is the runtime/runner path -- not called from query
execution. The query path only calls `latest_blocking_failures`."""
from __future__ import annotations

import json
import uuid
from dataclasses import asdict
from datetime import datetime
from typing import Any, Literal

import duckdb

from rumi.observability.types import (
    BlockingFailure,
    resolve_staleness_seconds,
)


_LATEST_BLOCKING_SQL = """
WITH ranked AS (
    SELECT
        rule_id, rule_kind, passed, observed_value, ran_at,
        ROW_NUMBER() OVER (
            PARTITION BY rule_id, rule_kind
            ORDER BY ran_at DESC
        ) AS rn
    FROM rumi_quality_runs
),
latest AS (
    SELECT rule_id, rule_kind, passed, observed_value, ran_at
    FROM ranked
    WHERE rn = 1
)
SELECT
    qr.rule_id, 'quality' AS rule_kind, qr.severity, qr.cost_class,
    qr.staleness_seconds, l.passed, l.ran_at, l.observed_value,
    CASE WHEN l.ran_at IS NULL
         THEN NULL
         ELSE DATE_DIFF('second', l.ran_at, now())
    END AS seconds_since
FROM rumi_quality_rules qr
LEFT JOIN latest l ON l.rule_id = qr.rule_id AND l.rule_kind = 'quality'
WHERE qr.source_id = ? AND qr.severity = 'block' AND qr.enabled = TRUE

UNION ALL

SELECT
    fr.rule_id, 'freshness' AS rule_kind, fr.severity, NULL AS cost_class,
    fr.staleness_seconds, l.passed, l.ran_at, l.observed_value,
    CASE WHEN l.ran_at IS NULL
         THEN NULL
         ELSE DATE_DIFF('second', l.ran_at, now())
    END AS seconds_since
FROM rumi_freshness_rules fr
LEFT JOIN latest l ON l.rule_id = fr.rule_id AND l.rule_kind = 'freshness'
WHERE fr.source_id = ? AND fr.severity = 'block'
"""


class DefaultObservability:
    def __init__(self, con: duckdb.DuckDBPyConnection) -> None:
        self.con = con

    # ------------------------------------------------------------------
    # latest_blocking_failures: cache-first, V1.5 aggregation as fallback
    # ------------------------------------------------------------------

    def latest_blocking_failures(
        self, source_id: str
    ) -> tuple[BlockingFailure, ...]:
        cache_row = self.con.execute(
            "SELECT blocking_failures FROM rumi_source_health WHERE source_id = ?",
            [source_id],
        ).fetchone()
        if cache_row is not None:
            return tuple(
                _deserialize_failure(d) for d in json.loads(cache_row[0])
            )
        return self._aggregate_blocking_failures(source_id)

    # ------------------------------------------------------------------
    # record_run: append-only log of rule evaluations
    # ------------------------------------------------------------------

    def record_run(
        self,
        rule_id: str,
        rule_kind: Literal["quality", "freshness"],
        passed: bool,
        observed_value: dict | None = None,
    ) -> str:
        run_id = str(uuid.uuid4())
        self.con.execute(
            "INSERT INTO rumi_quality_runs "
            "(run_id, rule_id, rule_kind, passed, observed_value) "
            "VALUES (?, ?, ?, ?, ?)",
            [
                run_id, rule_id, rule_kind, passed,
                json.dumps(observed_value) if observed_value is not None else None,
            ],
        )
        return run_id

    # ------------------------------------------------------------------
    # refresh_health: denormalize latest state into rumi_source_health
    # ------------------------------------------------------------------

    def refresh_health(self, source_id: str) -> None:
        """Recompute the cached blocking_failures for a source.

        Idempotent. Called by `rumi check` after running rules. Safe to
        call without runs having been recorded (will simply cache an
        all-`never_run` result, or empty if the source has no rules)."""
        failures = self._aggregate_blocking_failures(source_id)
        blocking_json = json.dumps([_serialize_failure(f) for f in failures])
        status = "block" if failures else "healthy"
        # rumi_source_health is a leaf table (no inbound FKs); ON CONFLICT
        # DO UPDATE works here (unlike rumi_metrics where the FK quirk bites).
        self.con.execute(
            "INSERT INTO rumi_source_health "
            "(source_id, status, blocking_failures, warnings, refreshed_at) "
            "VALUES (?, ?, ?, '[]', now()) "
            "ON CONFLICT (source_id) DO UPDATE SET "
            "status = EXCLUDED.status, "
            "blocking_failures = EXCLUDED.blocking_failures, "
            "refreshed_at = now()",
            [source_id, status, blocking_json],
        )

    # ------------------------------------------------------------------
    # Internal: V1.5 aggregation (the source-of-truth computation)
    # ------------------------------------------------------------------

    def _aggregate_blocking_failures(
        self, source_id: str
    ) -> tuple[BlockingFailure, ...]:
        rows = self.con.execute(
            _LATEST_BLOCKING_SQL, [source_id, source_id]
        ).fetchall()
        failures: list[BlockingFailure] = []
        for (
            rule_id, rule_kind, severity, cost_class,
            staleness_seconds, passed, ran_at, observed_value, seconds_since,
        ) in rows:
            failure = self._classify(
                rule_id=rule_id, rule_kind=rule_kind, severity=severity,
                cost_class=cost_class, staleness_seconds=staleness_seconds,
                passed=passed, ran_at=ran_at, observed_value=observed_value,
                seconds_since=seconds_since,
            )
            if failure is not None:
                failures.append(failure)
        return tuple(failures)

    @staticmethod
    def _classify(
        *,
        rule_id: str,
        rule_kind: Literal["quality", "freshness"],
        severity: Literal["warn", "block"],
        cost_class: Literal["cheap", "expensive"] | None,
        staleness_seconds: int | None,
        passed: bool | None,
        ran_at: datetime | None,
        observed_value: str | None,
        seconds_since: int | None,
    ) -> BlockingFailure | None:
        effective = resolve_staleness_seconds(
            rule_kind, cost_class, staleness_seconds
        )
        if ran_at is None:
            return BlockingFailure(
                rule_id=rule_id, rule_kind=rule_kind, severity=severity,
                cost_class=cost_class, last_run_at=None,
                reason="never_run",
                detail=f"{rule_id}: no run recorded",
                seconds_overdue=None,
            )
        if not passed:
            detail = (
                f"{rule_id}: failed"
                if observed_value is None
                else f"{rule_id}: failed; observed={observed_value}"
            )
            return BlockingFailure(
                rule_id=rule_id, rule_kind=rule_kind, severity=severity,
                cost_class=cost_class, last_run_at=ran_at,
                reason="rule_failed", detail=detail,
                seconds_overdue=None,
            )
        if seconds_since is not None and seconds_since > effective:
            return BlockingFailure(
                rule_id=rule_id, rule_kind=rule_kind, severity=severity,
                cost_class=cost_class, last_run_at=ran_at,
                reason="stale_check",
                detail=(
                    f"{rule_id}: last run {seconds_since}s ago; "
                    f"window {effective}s"
                ),
                seconds_overdue=seconds_since - effective,
            )
        return None


# ---------------------------------------------------------------------------
# JSON serialization for cache
# ---------------------------------------------------------------------------

def _serialize_failure(f: BlockingFailure) -> dict[str, Any]:
    d = asdict(f)
    if d["last_run_at"] is not None:
        d["last_run_at"] = d["last_run_at"].isoformat()
    return d


def _deserialize_failure(d: dict[str, Any]) -> BlockingFailure:
    last_run_at = d.get("last_run_at")
    if isinstance(last_run_at, str):
        last_run_at = datetime.fromisoformat(last_run_at)
    return BlockingFailure(
        rule_id=d["rule_id"],
        rule_kind=d["rule_kind"],
        severity=d["severity"],
        cost_class=d.get("cost_class"),
        last_run_at=last_run_at,
        reason=d["reason"],
        detail=d.get("detail"),
        seconds_overdue=d.get("seconds_overdue"),
    )
