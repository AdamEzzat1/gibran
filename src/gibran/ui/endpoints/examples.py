"""GET /api/examples -- auto-generated NL example questions.

Walks the AllowedSchema for a source (or the first visible source if
no hint) and synthesizes a short list of questions that the NL pattern
matcher should be able to answer. Powers the Workbench's "what can I
ask?" panel so a first-time viewer knows what shapes work.

The examples are TEMPLATES filled with real metric / dimension names
from the user's catalog, not a hardcoded list -- this means the
examples adapt as the catalog grows.
"""
from __future__ import annotations

from fastapi import Depends, FastAPI, Query

from gibran.governance.default import DefaultGovernance
from gibran.governance.types import IdentityContext
from gibran.ui.auth import current_identity
from gibran.ui.server import db_con


def register(app: FastAPI) -> None:
    @app.get("/api/examples")
    async def examples(
        source_id: str | None = Query(default=None),
        identity: IdentityContext = Depends(current_identity),
        con=Depends(db_con),
    ):
        gov = DefaultGovernance(con)
        # If no source hint, take the first visible one.
        if source_id is None:
            for (sid,) in con.execute(
                "SELECT source_id FROM gibran_sources ORDER BY source_id"
            ).fetchall():
                try:
                    gov.preview_schema(identity, sid)
                    source_id = sid
                    break
                except ValueError:
                    continue
            if source_id is None:
                return {"examples": [], "source_id": None}
        try:
            schema = gov.preview_schema(identity, source_id)
        except ValueError:
            return {"examples": [], "source_id": source_id}

        examples = _generate_examples(schema)
        return {"examples": examples, "source_id": source_id}


def _generate_examples(schema) -> list[dict]:
    """Template-fill from the user's actual metric/dimension names.

    Each template is paired with the pattern it's designed to hit, so
    if a viewer clicks an example the Workbench knows which pattern
    they're exercising. Order matters: most-impressive templates first
    (top_n_by is the demo-flagship pattern -- it shows the row filter,
    the aggregation, the limit, and the column truncation all at once).
    """
    examples: list[dict] = []
    # We use `display_name` rather than `metric_id` for the surface text
    # because the NL `_resolve_metric` does exact match against both
    # metric_id ("avg_order_value") and display_name ("Average Order
    # Value"). Munging metric_id ("avg order value") matches neither.
    # Display name is the safe choice and reads more naturally to humans.
    metrics = list(schema.metrics)
    # Prefer categorical dims for "top N by" / "by" -- temporal dims for
    # those shapes ("top 5 order_date by revenue") are syntactically valid
    # but semantically weird. Fall back to any dim if no categorical exists.
    categorical_dims = [d for d in schema.dimensions if d.dim_type == "categorical"]
    other_dims = [d for d in schema.dimensions if d.dim_type != "categorical"]
    ordered_dims = categorical_dims + other_dims

    # top N by metric -- needs one metric + one categorical dim
    if metrics and ordered_dims:
        m_name = metrics[0].display_name.lower()
        d_name = ordered_dims[0].display_name.lower()
        examples.append({
            "question": f"top 5 {d_name} by {m_name}",
            "pattern": "top_n_by_metric",
        })

    # metric by dimension (no top) -- the simplest aggregation shape
    if metrics and ordered_dims:
        m_name = (metrics[-1] if len(metrics) > 1 else metrics[0]).display_name.lower()
        d_name = (ordered_dims[1] if len(ordered_dims) > 1 else ordered_dims[0]).display_name.lower()
        examples.append({
            "question": f"{m_name} by {d_name}",
            "pattern": "metric_by_dimension",
        })

    # bare metric -- "what's our X" / "how many Y"
    if metrics:
        examples.append({
            "question": metrics[0].display_name.lower(),
            "pattern": "metric_only",
        })

    # metric by month/week/etc (metric_by_grain pattern -- needs a temporal dim)
    temporal_dim = next(
        (d for d in schema.dimensions if d.dim_type == "temporal"),
        None,
    )
    if temporal_dim and metrics:
        examples.append({
            "question": f"{metrics[0].display_name.lower()} by month",
            "pattern": "metric_by_grain",
        })

    # filtered metric -- "<metric> for <value>" where value is one of
    # a column's example_values directly (pattern: metric_filtered_by_value).
    # Prefer columns tied to a categorical dimension (their values are
    # human-readable like "west" / "east") over raw numeric columns
    # (like "100.00"). Falls back to any column with examples if no
    # categorical-linked one exists.
    cat_column_names = {
        d.column_name for d in schema.dimensions if d.dim_type == "categorical"
    }
    preferred_col = next(
        (
            c for c in schema.columns
            if c.example_values and c.name in cat_column_names
        ),
        None,
    )
    if preferred_col is None:
        preferred_col = next(
            (c for c in schema.columns if c.example_values),
            None,
        )
    if preferred_col and metrics:
        value = preferred_col.example_values[0]
        examples.append({
            "question": f"{metrics[0].display_name.lower()} for {value}",
            "pattern": "metric_filtered_by_value",
        })

    return examples
