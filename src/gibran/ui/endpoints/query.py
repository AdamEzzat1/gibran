"""POST /api/query and POST /api/explain.

`/api/query` runs an intent through the DSL pipeline (parse + validate
+ compile + execute) and returns rows.

`/api/explain` runs the same path but stops short of execution -- it
returns the compiled SQL plus the AllowedSchema and the applied
governance constraints. Useful for the Workbench's "show me what this
will do before I run it" pane.
"""
from __future__ import annotations

from fastapi import Depends, FastAPI

from gibran.dsl.compile import Catalog, compile_intent
from gibran.dsl.run import run_dsl_query
from gibran.dsl.types import QueryIntent
from gibran.dsl.validate import validate_intent
from gibran.governance.default import DefaultGovernance
from gibran.governance.types import IdentityContext
from gibran.ui.auth import current_identity
from gibran.ui.models import IntentBody
from gibran.ui.server import db_con


def register(app: FastAPI) -> None:
    @app.post("/api/query")
    async def query(
        body: IntentBody,
        identity: IdentityContext = Depends(current_identity),
        con=Depends(db_con),
    ):
        gov = DefaultGovernance(con)
        result = run_dsl_query(con, gov, identity, body.intent)
        return _serialize_run_result(result)

    @app.post("/api/explain")
    async def explain(
        body: IntentBody,
        identity: IdentityContext = Depends(current_identity),
        con=Depends(db_con),
    ):
        """Compile the intent and return the SQL + governance preview
        without executing. Errors at parse / validate / compile time
        are returned in the response (200), not raised."""
        gov = DefaultGovernance(con)
        try:
            intent = QueryIntent.model_validate(body.intent)
        except Exception as e:
            return {"stage": "parsed", "error": str(e)}
        try:
            schema = gov.preview_schema(identity, intent.source)
        except ValueError as e:
            return {"stage": "parsed", "error": f"unknown_source: {e}"}
        try:
            validate_intent(intent, schema, con=con)
        except Exception as e:
            return {"stage": "validated", "error": str(e)}
        try:
            sql = compile_intent(intent, Catalog(con)).render()
        except Exception as e:
            return {"stage": "compiled", "error": str(e)}
        return {
            "stage": "compiled",
            "compiled_sql": sql,
            "source_id": intent.source,
            "schema_preview": {
                "columns_visible": [c.name for c in schema.columns],
                "metrics_visible": [m.metric_id for m in schema.metrics],
                "dimensions_visible": [d.dimension_id for d in schema.dimensions],
            },
        }


def _serialize_run_result(result) -> dict:
    """Same shape as ask.py's serializer for the run result. Kept
    parallel so the frontend can display both via the same component."""
    payload = {
        "stage": result.stage,
        "duration_ms": result.duration_ms,
    }
    if result.pre_compile_error:
        payload["error"] = result.pre_compile_error
        return payload
    if result.query_result is not None:
        qr = result.query_result
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
