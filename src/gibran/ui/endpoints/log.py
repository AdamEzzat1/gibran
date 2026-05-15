"""GET /api/log -- audit log viewer (admin-only for cross-user view).

Non-admin identities see only their own queries (the analyst's "what
have I asked recently" view). Admins see everything.

Pagination is cursor-based: the `cursor` query param is the
`created_at` ISO timestamp of the last row from the previous page.
Stable under inserts because `gibran_query_log` is append-only.
"""
from __future__ import annotations

from fastapi import Depends, FastAPI, Query

from gibran.governance.types import IdentityContext
from gibran.ui.auth import current_identity
from gibran.ui.server import db_con


def register(app: FastAPI) -> None:
    @app.get("/api/log")
    async def get_log(
        user_id: str | None = Query(default=None),
        role_id: str | None = Query(default=None),
        status: str | None = Query(default=None),
        source_id: str | None = Query(default=None),
        cursor: str | None = Query(default=None),
        limit: int = Query(default=50, ge=1, le=500),
        identity: IdentityContext = Depends(current_identity),
        con=Depends(db_con),
    ):
        # Admin check: is this role break-glass? If not, override
        # filters to scope to the caller's own user_id (analysts can
        # see their own history, not other users').
        is_admin_row = con.execute(
            "SELECT is_break_glass FROM gibran_roles WHERE role_id = ?",
            [identity.role_id],
        ).fetchone()
        is_admin = bool(is_admin_row[0]) if is_admin_row else False

        where_clauses = []
        params: list = []
        if not is_admin:
            # Non-admin: forced to own user_id, ignore the query param.
            where_clauses.append("user_id = ?")
            params.append(identity.user_id)
        else:
            if user_id:
                where_clauses.append("user_id = ?")
                params.append(user_id)
        if role_id:
            where_clauses.append("role_id = ?")
            params.append(role_id)
        if status:
            where_clauses.append("status = ?")
            params.append(status)
        # source_id filter is more complex (audit log doesn't store
        # source directly; would need to derive from generated_sql).
        # Skipped in this initial cut -- the UI can filter client-side.
        if cursor:
            where_clauses.append("created_at < ?")
            params.append(cursor)

        where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""
        sql = (
            f"SELECT query_id, user_id, role_id, nl_prompt, generated_sql, "
            f"status, deny_reason, row_count, duration_ms, is_break_glass, "
            f"created_at "
            f"FROM gibran_query_log {where_sql} "
            f"ORDER BY created_at DESC LIMIT ?"
        )
        params.append(limit + 1)  # +1 so we can detect next-page existence

        rows = con.execute(sql, params).fetchall()
        has_more = len(rows) > limit
        page = rows[:limit]
        next_cursor = str(page[-1][10]) if (has_more and page) else None

        return {
            "rows": [
                {
                    "query_id": r[0],
                    "user_id": r[1],
                    "role_id": r[2],
                    "nl_prompt": r[3],
                    "generated_sql": r[4],
                    "status": r[5],
                    "deny_reason": r[6],
                    "row_count": r[7],
                    "duration_ms": r[8],
                    "is_break_glass": bool(r[9]) if r[9] is not None else False,
                    "created_at": str(r[10]) if r[10] else None,
                }
                for r in page
            ],
            "next_cursor": next_cursor,
            "scoped_to_self": not is_admin,
        }
