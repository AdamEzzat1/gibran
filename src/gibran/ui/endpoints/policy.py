"""GET /api/policy/{role_id} -- admin-only "as if I were this role" preview.

Returns what AllowedSchema the named role would see for every source
in the catalog. Powers the Policy Visualizer UI view: admins can
inspect any role's effective access without impersonating.

Logged to gibran_query_log with a synthetic policy_view marker so
break-glass-style auditing applies to access reviews too.
"""
from __future__ import annotations

import uuid

from fastapi import Depends, FastAPI, HTTPException

from gibran.governance.default import DefaultGovernance
from gibran.governance.types import IdentityContext
from gibran.ui.auth import require_admin
from gibran.ui.endpoints.catalog import _schema_to_dict
from gibran.ui.server import db_con


def register(app: FastAPI) -> None:
    @app.get("/api/roles")
    async def list_roles(
        identity: IdentityContext = Depends(require_admin),
        con=Depends(db_con),
    ):
        """Admin-only: enumerate every role in gibran_roles for the
        Policy Visualizer's picker."""
        rows = con.execute(
            "SELECT role_id, display_name, is_break_glass "
            "FROM gibran_roles ORDER BY role_id"
        ).fetchall()
        return {
            "roles": [
                {
                    "id": r[0],
                    "display_name": r[1],
                    "is_break_glass": bool(r[2]) if r[2] is not None else False,
                }
                for r in rows
            ]
        }

    @app.get("/api/policy/{role_id}")
    async def get_policy(
        role_id: str,
        identity: IdentityContext = Depends(require_admin),
        con=Depends(db_con),
    ):
        # Verify the target role exists.
        row = con.execute(
            "SELECT display_name FROM gibran_roles WHERE role_id = ?",
            [role_id],
        ).fetchone()
        if row is None:
            raise HTTPException(
                status_code=404,
                detail=f"unknown role: {role_id!r}",
            )
        display_name = row[0]

        # Synthetic identity: same user_id as the admin (for audit
        # traceability) but the target role_id and that role's
        # default attributes (if any).
        attr_rows = con.execute(
            "SELECT attribute_key, attribute_value "
            "FROM gibran_role_attributes WHERE role_id = ?",
            [role_id],
        ).fetchall()
        attrs = {k: v for k, v in attr_rows}
        synthetic = IdentityContext(
            user_id=identity.user_id,
            role_id=role_id,
            attributes=attrs,
            source="policy-preview",
        )

        gov = DefaultGovernance(con)
        source_ids = [
            r[0]
            for r in con.execute(
                "SELECT source_id FROM gibran_sources ORDER BY source_id"
            ).fetchall()
        ]
        previews = []
        for sid in source_ids:
            try:
                schema = gov.preview_schema(synthetic, sid)
                previews.append(_schema_to_dict(schema))
            except ValueError:
                # Role can't see this source at all -- record it explicitly
                # rather than silently dropping (operators want the full map).
                previews.append({
                    "source_id": sid,
                    "denied": True,
                    "reason": "role has no policy for this source",
                })

        # Audit: write a synthetic query_log entry so admin policy
        # inspections are traceable. Same pattern as break-glass queries.
        con.execute(
            "INSERT INTO gibran_query_log "
            "(query_id, user_id, role_id, nl_prompt, generated_sql, "
            "status, deny_reason, row_count, duration_ms, is_break_glass) "
            "VALUES (?, ?, ?, ?, ?, 'ok', NULL, NULL, 0, TRUE)",
            [
                str(uuid.uuid4()),
                identity.user_id,
                identity.role_id,
                f"policy_view target_role={role_id}",
                "",  # no SQL was emitted
            ],
        )

        return {
            "target_role": role_id,
            "target_role_display_name": display_name,
            "target_role_attributes": attrs,
            "previews": previews,
            "viewed_by": identity.user_id,
        }
