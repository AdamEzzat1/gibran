"""GET /api/catalog and GET /api/describe/{source_id}.

Both endpoints return AllowedSchema-shaped JSON: what columns / metrics
/ dimensions the requesting identity is permitted to see, after the
governance layer applies row+column policies.

The catalog endpoint enumerates every source visible to the identity.
The describe endpoint returns one source's full detail (used by the
Catalog Browser view's drill-down).
"""
from __future__ import annotations

from fastapi import Depends, FastAPI, HTTPException

from gibran.governance.default import DefaultGovernance
from gibran.governance.types import IdentityContext
from gibran.ui.auth import current_identity
from gibran.ui.server import db_con


def register(app: FastAPI) -> None:
    @app.get("/api/catalog")
    async def get_catalog(
        identity: IdentityContext = Depends(current_identity),
        con=Depends(db_con),
    ):
        """List every source the identity can see, with full AllowedSchema.

        Sources where the role has zero column access are omitted (it's
        not informative to surface a source you can't query anything on).
        """
        gov = DefaultGovernance(con)
        source_ids = [
            r[0]
            for r in con.execute("SELECT source_id FROM gibran_sources ORDER BY source_id").fetchall()
        ]
        sources_payload = []
        for sid in source_ids:
            try:
                schema = gov.preview_schema(identity, sid)
            except ValueError:
                continue  # source not visible to identity
            if not schema.columns and not schema.metrics and not schema.dimensions:
                continue  # role has nothing -- skip
            sources_payload.append(_schema_to_dict(schema))
        return {"sources": sources_payload, "user": identity.user_id, "role": identity.role_id}

    @app.get("/api/describe/{source_id}")
    async def describe_source(
        source_id: str,
        identity: IdentityContext = Depends(current_identity),
        con=Depends(db_con),
    ):
        """One source's full AllowedSchema. 404 if not visible to the role."""
        gov = DefaultGovernance(con)
        try:
            schema = gov.preview_schema(identity, source_id)
        except ValueError as e:
            raise HTTPException(status_code=404, detail=str(e)) from e
        return _schema_to_dict(schema)


def _schema_to_dict(schema) -> dict:
    """Serialize an AllowedSchema dataclass to a JSON-friendly dict.

    AllowedSchema is a frozen dataclass with nested ColumnView /
    DimensionView / MetricView tuples; we walk them explicitly so the
    serialization shape stays stable (and so internal fields like
    cache_version don't leak into the API response)."""
    return {
        "source_id": schema.source_id,
        "display_name": schema.source_display_name,
        "columns": [
            {
                "name": c.name,
                "display_name": c.display_name,
                "data_type": c.data_type,
                "sensitivity": c.sensitivity,
                "description": c.description,
                "example_values": list(c.example_values) if c.example_values else None,
            }
            for c in schema.columns
        ],
        "dimensions": [
            {
                "id": d.dimension_id,
                "column": d.column_name,
                "display_name": d.display_name,
                "type": d.dim_type,
                "description": d.description,
            }
            for d in schema.dimensions
        ],
        "metrics": [
            {
                "id": m.metric_id,
                "display_name": m.display_name,
                "type": m.metric_type,
                "unit": m.unit,
                "description": m.description,
                "depends_on": list(m.depends_on),
            }
            for m in schema.metrics
        ],
    }
