"""Approval workflow for high-sensitivity policy changes.

Operations (called from the CLI; pure-function over a DuckDB
connection):
  * `submit_change(con, change_type, payload, requested_by, reason)` ->
    writes a pending row to gibran_pending_changes
  * `list_pending(con)` -> rows still awaiting approval
  * `approve(con, change_id, approved_by)` -> stamps the approval and
    returns the pending row's payload, ready for the caller to apply

V1 scope: this module manages the queue. *Applying* the approved
change (writing the policy / role / sensitivity update) is the
caller's responsibility -- they have to deserialize the payload
according to its change_type and route it through the appropriate
applier. Keeping the queue passive avoids tangling the approval table
with the apply machinery.

Why this exists: per HANDOFF.md, high-sensitivity changes (touching
PII / restricted columns, modifying break-glass roles) shouldn't be
single-author commits to the catalog. The approval queue gives an
out-of-band reviewer a chance to inspect before the change becomes
effective.
"""
from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from typing import Any

import duckdb


@dataclass(frozen=True)
class PendingChange:
    change_id: str
    change_type: str
    payload: dict[str, Any]
    requested_at: object        # datetime; left unparsed
    requested_by: str | None
    reason: str | None


def submit_change(
    con: duckdb.DuckDBPyConnection,
    *,
    change_type: str,
    payload: dict[str, Any],
    requested_by: str | None = None,
    reason: str | None = None,
) -> str:
    """Insert a pending row; return the new change_id. The caller is
    expected to advertise this change_id to whoever approves."""
    change_id = str(uuid.uuid4())
    con.execute(
        "INSERT INTO gibran_pending_changes "
        "(change_id, change_type, payload_json, requested_by, reason) "
        "VALUES (?, ?, ?, ?, ?)",
        [change_id, change_type, json.dumps(payload), requested_by, reason],
    )
    return change_id


def list_pending(
    con: duckdb.DuckDBPyConnection,
) -> list[PendingChange]:
    rows = con.execute(
        "SELECT change_id, change_type, payload_json, requested_at, "
        "requested_by, reason "
        "FROM gibran_pending_changes "
        "WHERE approved_at IS NULL "
        "ORDER BY requested_at"
    ).fetchall()
    return [
        PendingChange(
            change_id=r[0], change_type=r[1],
            payload=json.loads(r[2]),
            requested_at=r[3], requested_by=r[4], reason=r[5],
        )
        for r in rows
    ]


def approve(
    con: duckdb.DuckDBPyConnection, change_id: str, *,
    approved_by: str,
) -> PendingChange:
    """Stamp the approval and return the payload for the caller to apply.

    Raises ValueError if the change_id doesn't exist or is already
    approved (re-approval is intentionally rejected to keep the audit
    trail honest)."""
    row = con.execute(
        "SELECT change_id, change_type, payload_json, requested_at, "
        "requested_by, reason, approved_at "
        "FROM gibran_pending_changes WHERE change_id = ?",
        [change_id],
    ).fetchone()
    if row is None:
        raise ValueError(f"unknown change_id: {change_id!r}")
    if row[6] is not None:
        raise ValueError(f"change {change_id!r} is already approved")
    con.execute(
        "UPDATE gibran_pending_changes "
        "SET approved_at = CURRENT_TIMESTAMP, approved_by = ? "
        "WHERE change_id = ?",
        [approved_by, change_id],
    )
    return PendingChange(
        change_id=row[0], change_type=row[1],
        payload=json.loads(row[2]),
        requested_at=row[3], requested_by=row[4], reason=row[5],
    )
