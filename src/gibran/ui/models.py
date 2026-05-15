"""Pydantic request/response models for the UI's HTTP endpoints.

Kept narrow on purpose: the UI sits on top of gibran's existing core
APIs (governance.preview_schema, dsl.run.run_dsl_query, etc.), so most
"models" here are thin wrappers that match the input shape those APIs
already expect (intent JSON, NL prompt string, etc.).
"""
from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class AskBody(BaseModel):
    """POST /api/ask body."""
    prompt: str = Field(..., min_length=1, max_length=2000)
    source: str | None = Field(
        default=None,
        description="Optional source_id hint. If omitted, NL layer "
                    "picks the first matching pattern's resolved source.",
    )


class IntentBody(BaseModel):
    """POST /api/query and POST /api/explain body.

    The `intent` is the full QueryIntent JSON shape gibran's DSL
    accepts; we don't re-validate here -- the DSL runner does that
    inside its parse step.
    """
    intent: dict[str, Any]


class LogFilter(BaseModel):
    """GET /api/log query params (as a Pydantic model for typing).

    `cursor` is the audit log's `created_at` of the last row from the
    previous page (encoded as ISO timestamp). Stable under inserts
    because gibran_query_log rows are append-only.
    """
    user_id: str | None = None
    role_id: str | None = None
    status: str | None = None
    source_id: str | None = None
    cursor: str | None = None
    limit: int = Field(default=50, ge=1, le=500)


class ApprovalAction(BaseModel):
    """POST /api/approvals/{change_id}/approve body."""
    reason: str = Field(..., min_length=1, max_length=1000)


class HealthResponse(BaseModel):
    source_id: str
    status: str
    blocking_failures: list[Any]
    warnings: list[Any]
    refreshed_at: str | None
