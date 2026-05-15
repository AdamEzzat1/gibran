"""POST /api/ask -- NL prompt to query result.

Wraps `gibran.nl.runner.run_nl_query`. If no pattern matches, returns
200 with `matched: false` and a `hint` listing example questions from
the gibran NL surface (rather than an error -- the UI shows this in an
empty-state panel).

If a pattern matches but the resolved query is denied or errors, the
underlying DSLRunResult is surfaced in the response so the UI can show
clear denial/error messaging.
"""
from __future__ import annotations

from fastapi import Depends, FastAPI, HTTPException

from gibran.governance.default import DefaultGovernance
from gibran.governance.types import IdentityContext
from gibran.nl.runner import run_nl_query
from gibran.ui.auth import current_identity
from gibran.ui.models import AskBody
from gibran.ui.server import db_con


def register(app: FastAPI) -> None:
    @app.post("/api/ask")
    async def ask(
        body: AskBody,
        identity: IdentityContext = Depends(current_identity),
        con=Depends(db_con),
    ):
        gov = DefaultGovernance(con)

        # If no source hint, pick the first source the identity can see.
        source_id = body.source
        if source_id is None:
            source_id = _first_visible_source(con, gov, identity)
            if source_id is None:
                raise HTTPException(
                    status_code=404,
                    detail="no source visible to this identity; cannot route NL",
                )

        result = run_nl_query(con, gov, identity, body.prompt, source_id)
        if result.match is None:
            return {
                "matched": False,
                "hint": (
                    "No pattern matched your question. Try a simpler shape "
                    "like 'top 5 region by revenue' or 'revenue by month'."
                ),
                "prompt": body.prompt,
                "source_id": source_id,
            }

        # Pattern matched. The DSL runner may have succeeded, denied, or
        # errored -- surface all three to the caller.
        return _serialize_nl_result(result, source_id)


def _first_visible_source(con, gov, identity: IdentityContext) -> str | None:
    """First source_id the identity can see at all. None if none."""
    for (sid,) in con.execute("SELECT source_id FROM gibran_sources").fetchall():
        try:
            gov.preview_schema(identity, sid)
            return sid
        except ValueError:
            continue
    return None


def _serialize_nl_result(result, source_id: str) -> dict:
    payload = {
        "matched": True,
        "pattern_name": result.match.pattern_name,
        "matched_text": result.match.matched_text,
        "intent": result.match.intent,
        "source_id": source_id,
    }
    run = result.run_result
    if run is None:
        return payload
    payload["stage"] = run.stage
    payload["duration_ms"] = run.duration_ms
    if run.pre_compile_error:
        payload["error"] = run.pre_compile_error
        return payload
    if run.query_result is not None:
        qr = run.query_result
        payload["status"] = qr.status
        payload["compiled_sql"] = qr.rewritten_sql or qr.original_sql
        if qr.status == "ok":
            payload["columns"] = list(qr.columns or [])
            payload["rows"] = [list(r) for r in (qr.rows or [])]
            payload["row_count"] = len(qr.rows) if qr.rows else 0
        elif qr.status == "denied":
            payload["deny_reason"] = (
                qr.deny_reason.value if qr.deny_reason else None
            )
            payload["deny_detail"] = qr.deny_detail
        else:
            payload["error"] = qr.error_message
    return payload
