"""GET /api/health/{source_id} -- latest source-health snapshot.

Returns the cached `gibran_source_health` row plus the most recent N
quality/freshness rule outcomes for the source. Powered by the same
queries the CLI's `gibran check` uses for its output.
"""
from __future__ import annotations

from fastapi import Depends, FastAPI, HTTPException

from gibran.governance.default import DefaultGovernance
from gibran.governance.types import IdentityContext
from gibran.ui.auth import current_identity
from gibran.ui.server import db_con


def register(app: FastAPI) -> None:
    @app.get("/api/health/{source_id}")
    async def get_health(
        source_id: str,
        identity: IdentityContext = Depends(current_identity),
        con=Depends(db_con),
    ):
        # Visibility check: caller must have schema access to the source
        # for health info to leak. The cache row exists even for sources
        # the role can't query, so we gate explicitly.
        gov = DefaultGovernance(con)
        try:
            gov.preview_schema(identity, source_id)
        except ValueError as e:
            raise HTTPException(status_code=404, detail=str(e)) from e

        row = con.execute(
            "SELECT status, blocking_failures, warnings, refreshed_at "
            "FROM gibran_source_health WHERE source_id = ?",
            [source_id],
        ).fetchone()
        if row is None:
            return {
                "source_id": source_id,
                "status": "unknown",
                "blocking_failures": [],
                "warnings": [],
                "refreshed_at": None,
                "note": "no cached health (run `gibran check` to populate)",
            }
        status, blocking, warnings, refreshed_at = row
        recent = con.execute(
            "SELECT run_id, rule_id, rule_kind, passed, observed_value, ran_at "
            "FROM gibran_quality_runs "
            "WHERE rule_id IN ("
            "  SELECT rule_id FROM gibran_quality_rules WHERE source_id = ? "
            "  UNION ALL "
            "  SELECT rule_id FROM gibran_freshness_rules WHERE source_id = ?"
            ") "
            "ORDER BY ran_at DESC LIMIT 20",
            [source_id, source_id],
        ).fetchall()
        return {
            "source_id": source_id,
            "status": status,
            "blocking_failures": _decode_json(blocking),
            "warnings": _decode_json(warnings),
            "refreshed_at": str(refreshed_at) if refreshed_at else None,
            "recent_runs": [
                {
                    "run_id": r[0],
                    "rule_id": r[1],
                    "rule_kind": r[2],
                    "passed": bool(r[3]),
                    "observed_value": _decode_json(r[4]),
                    "ran_at": str(r[5]) if r[5] else None,
                }
                for r in recent
            ],
        }


def _decode_json(v):
    """DuckDB JSON columns surface as strings; parse for client consumption."""
    if v is None:
        return None
    if isinstance(v, str):
        import json as _json
        try:
            return _json.loads(v)
        except (ValueError, TypeError):
            return v
    return v
