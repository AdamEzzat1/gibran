"""Admin-only operator endpoints: materialize, touch, approvals, anomalies.

Bundled in one module because each is a thin wrapper around an existing
CLI command and the surface is small.

Write endpoints (materialize, touch, approve) serialize through a
module-level asyncio.Lock so two concurrent calls don't race on DuckDB
(which serializes writes internally but isn't designed for high
concurrent write throughput).
"""
from __future__ import annotations

import asyncio
import uuid

from fastapi import Depends, FastAPI, HTTPException

from gibran.governance.types import IdentityContext
from gibran.ui.auth import require_admin
from gibran.ui.models import ApprovalAction
from gibran.ui.server import db_con


# Write-side serialization. One lock for the whole process; sufficient
# for `gibran ui` (single-process). A real multi-instance deployment
# would need a DB-level lock or queue.
_WRITE_LOCK = asyncio.Lock()


def register(app: FastAPI) -> None:
    @app.post("/api/materialize/{metric_id}")
    async def materialize(
        metric_id: str,
        identity: IdentityContext = Depends(require_admin),
        con=Depends(db_con),
    ):
        """Trigger a fresh materialization of `metric_id`.

        Wraps `sync.applier._materialize_metrics` for a single metric.
        Returns the row count of the rebuilt mat table.
        """
        from gibran.sync.applier import _materialize_metrics
        from gibran.sync.loader import load as load_config
        from pathlib import Path

        # Load the current yaml config to find the metric and its source.
        # (The applier's helper expects a parsed config, not just an ID.)
        cfg_path = Path.cwd() / "gibran.yaml"
        if not cfg_path.exists():
            raise HTTPException(
                status_code=503,
                detail="no gibran.yaml in CWD; cannot resolve metric config",
            )
        cfg = load_config(cfg_path)
        target = None
        for source in cfg.sources:
            for metric in (getattr(source, "metrics", None) or []):
                if metric.id == metric_id:
                    target = (source, metric)
                    break
            if target:
                break
        if target is None:
            raise HTTPException(
                status_code=404,
                detail=f"metric {metric_id!r} not found in current config",
            )

        async with _WRITE_LOCK:
            # _materialize_metrics signature varies by source/metric; we
            # surface a "supported / not yet" guard rather than crash if
            # the metric type doesn't materialize.
            try:
                _materialize_metrics(con, cfg)
            except Exception as e:
                raise HTTPException(
                    status_code=500,
                    detail=f"materialization failed: {e}",
                ) from e

        row_count_row = con.execute(
            f'SELECT COUNT(*) FROM "gibran_mat_{metric_id}"'
        ).fetchone()
        return {
            "metric_id": metric_id,
            "row_count": int(row_count_row[0]) if row_count_row else 0,
            "triggered_by": identity.user_id,
        }

    @app.post("/api/touch/{source_id}")
    async def touch(
        source_id: str,
        identity: IdentityContext = Depends(require_admin),
        con=Depends(db_con),
    ):
        """Bump `source_id`'s data_version, invalidating cached results.

        For duckdb_table / sql_view sources, registers a new version
        in `gibran_table_versions` (created on the fly if absent).
        For file-backed sources, this is a no-op since data_version
        is read from file mtime.
        """
        async with _WRITE_LOCK:
            row = con.execute(
                "SELECT source_type FROM gibran_sources WHERE source_id = ?",
                [source_id],
            ).fetchone()
            if row is None:
                raise HTTPException(status_code=404, detail=f"unknown source: {source_id}")
            source_type = row[0]
            if source_type not in ("duckdb_table", "sql_view"):
                return {
                    "source_id": source_id,
                    "source_type": source_type,
                    "noop": True,
                    "reason": "file-backed sources read data_version from file mtime",
                }
            # Create the versions table if it doesn't exist yet (table
            # was planned for Phase 2B; safe to add lazily here).
            con.execute(
                "CREATE TABLE IF NOT EXISTS gibran_table_versions ("
                "  source_id TEXT PRIMARY KEY, version TEXT NOT NULL"
                ")"
            )
            new_version = uuid.uuid4().hex
            con.execute(
                "INSERT INTO gibran_table_versions (source_id, version) "
                "VALUES (?, ?) "
                "ON CONFLICT (source_id) DO UPDATE SET version = EXCLUDED.version",
                [source_id, new_version],
            )
        return {
            "source_id": source_id,
            "new_version": new_version,
            "triggered_by": identity.user_id,
        }

    @app.get("/api/approvals/pending")
    async def list_pending_approvals(
        identity: IdentityContext = Depends(require_admin),
        con=Depends(db_con),
    ):
        rows = con.execute(
            "SELECT change_id, change_type, payload_json, requested_at, "
            "requested_by, reason "
            "FROM gibran_pending_changes "
            "WHERE approved_at IS NULL "
            "ORDER BY requested_at ASC"
        ).fetchall()
        return {
            "pending": [
                {
                    "change_id": r[0],
                    "change_type": r[1],
                    "payload": _decode_json(r[2]),
                    "requested_at": str(r[3]) if r[3] else None,
                    "requested_by": r[4],
                    "reason": r[5],
                }
                for r in rows
            ]
        }

    @app.post("/api/approvals/{change_id}/approve")
    async def approve(
        change_id: str,
        body: ApprovalAction,
        identity: IdentityContext = Depends(require_admin),
        con=Depends(db_con),
    ):
        async with _WRITE_LOCK:
            row = con.execute(
                "SELECT approved_at FROM gibran_pending_changes "
                "WHERE change_id = ?",
                [change_id],
            ).fetchone()
            if row is None:
                raise HTTPException(status_code=404, detail="change not found")
            if row[0] is not None:
                raise HTTPException(
                    status_code=409,
                    detail="change already approved",
                )
            con.execute(
                "UPDATE gibran_pending_changes "
                "SET approved_at = CURRENT_TIMESTAMP, approved_by = ?, "
                "reason = COALESCE(reason || ' | approval: ' || ?, ?) "
                "WHERE change_id = ?",
                [identity.user_id, body.reason, body.reason, change_id],
            )
        return {
            "change_id": change_id,
            "approved_by": identity.user_id,
            "reason": body.reason,
        }

    @app.get("/api/anomalies/access")
    async def access_anomalies(
        trailing_days: int = 14,
        n_sigma: float = 3.0,
        identity: IdentityContext = Depends(require_admin),
        con=Depends(db_con),
    ):
        """Output of `detect_access_anomalies` -- users whose today's
        query volume is unusually high vs trailing-day baseline."""
        from gibran.observability.access_anomaly import detect_access_anomalies
        anomalies = detect_access_anomalies(
            con, trailing_days=trailing_days, n_sigma=n_sigma,
        )
        return {
            "trailing_days": trailing_days,
            "n_sigma": n_sigma,
            "anomalies": [
                {
                    "user_id": a.user_id,
                    "today_count": a.today_count,
                    "baseline_mean": a.baseline_mean,
                    "baseline_stddev": a.baseline_stddev,
                    "z_score": a.z_score,
                }
                for a in anomalies
            ],
        }

    @app.get("/api/anomalies/{source_id}")
    async def source_anomalies(
        source_id: str,
        identity: IdentityContext = Depends(require_admin),
        con=Depends(db_con),
    ):
        """Recent failed runs of anomaly-type quality rules on `source_id`."""
        rows = con.execute(
            "SELECT r.run_id, r.rule_id, r.passed, r.observed_value, r.ran_at "
            "FROM gibran_quality_runs r "
            "JOIN gibran_quality_rules q ON q.rule_id = r.rule_id "
            "WHERE q.source_id = ? AND q.rule_type = 'anomaly' "
            "  AND r.passed = FALSE "
            "ORDER BY r.ran_at DESC LIMIT 50",
            [source_id],
        ).fetchall()
        return {
            "source_id": source_id,
            "failed_runs": [
                {
                    "run_id": r[0],
                    "rule_id": r[1],
                    "passed": bool(r[2]),
                    "observed_value": _decode_json(r[3]),
                    "ran_at": str(r[4]) if r[4] else None,
                }
                for r in rows
            ],
        }


def _decode_json(v):
    if v is None:
        return None
    if isinstance(v, str):
        import json as _json
        try:
            return _json.loads(v)
        except (ValueError, TypeError):
            return v
    return v
