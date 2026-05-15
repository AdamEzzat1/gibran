"""DefaultGovernance: in-process implementation of GovernanceAPI.

Reads policy + catalog state directly from a DuckDB connection. This is
the V1 implementation -- single-source enforcement, no quality/freshness
consultation yet (deferred until obs layer integration), no audit log
writes (deferred until nl_to_sql provides query_id), no compiled-policy
caching (deferred until profiling demands it).

The Protocol contract is in gibran.governance.types.GovernanceAPI; this
class satisfies it structurally."""
from __future__ import annotations

import json

import duckdb

from gibran.governance.ast import (
    ASTValidationError,
    compile_policy_to_sql,
    validate_policy_ast,
)
from gibran.governance.types import (
    AllowedSchema,
    ColumnView,
    Constraint,
    DenyReason,
    DimensionView,
    GovernanceDecision,
    IdentityContext,
    MetricView,
)
from gibran.observability.types import ObservabilityAPI


class DefaultGovernance:
    def __init__(
        self,
        con: duckdb.DuckDBPyConnection,
        observability: ObservabilityAPI | None = None,
        rate_limiter=None,  # RateLimiter | None; typed-loose to avoid circular import
    ) -> None:
        self.con = con
        self.observability = observability
        self.rate_limiter = rate_limiter

    # ------------------------------------------------------------------
    # preview_schema
    # ------------------------------------------------------------------
    def preview_schema(
        self, identity: IdentityContext, source_id: str
    ) -> AllowedSchema:
        source_row = self.con.execute(
            "SELECT display_name, schema_version FROM gibran_sources WHERE source_id = ?",
            [source_id],
        ).fetchone()
        if source_row is None:
            raise ValueError(f"unknown source: {source_id!r}")
        source_display_name, source_schema_version = source_row

        policy_row = self._fetch_policy(identity.role_id, source_id)
        if policy_row is None:
            return AllowedSchema(
                source_id=source_id,
                source_display_name=source_display_name,
                columns=(), dimensions=(), metrics=(),
                fixed_constraints=(),
                cache_version=(source_schema_version, 0),
            )
        (
            policy_id, _row_filter_json, default_mode, policy_schema_version,
            _valid_until, expired,
        ) = policy_row
        # Expired policy -> same view as "no policy": nothing visible. The
        # AllowedSchema must not leak schema names a non-permitted role
        # could probe.
        if expired:
            return AllowedSchema(
                source_id=source_id,
                source_display_name=source_display_name,
                columns=(), dimensions=(), metrics=(),
                fixed_constraints=(),
                cache_version=(source_schema_version, 0),
            )

        allowed = self._compute_allowed_columns(source_id, policy_id, default_mode)
        columns = self._build_column_views(source_id, allowed)
        dimensions = self._build_dimension_views(source_id, allowed)
        metrics = self._build_metric_views(source_id)

        return AllowedSchema(
            source_id=source_id,
            source_display_name=source_display_name,
            columns=columns,
            dimensions=dimensions,
            metrics=metrics,
            fixed_constraints=(),  # V1: empty; V2 walks the AST into Constraint[]
            cache_version=(source_schema_version, policy_schema_version),
        )

    # ------------------------------------------------------------------
    # evaluate
    # ------------------------------------------------------------------
    def evaluate(
        self,
        identity: IdentityContext,
        source_ids: frozenset[str],
        requested_columns: frozenset[str],
        requested_metrics: tuple[str, ...],
    ) -> GovernanceDecision:
        if len(source_ids) != 1:
            raise NotImplementedError(
                "cross-source evaluation deferred to V2 (single-source only in V1)"
            )
        [source_id] = source_ids

        # Rate-limit check runs BEFORE policy lookup so an attacker
        # hammering with bogus roles still consumes a token. This is the
        # only place where a deny can happen without the source/policy
        # being resolved -- the deny_detail records the (user, role) key.
        if self.rate_limiter is not None and not self.rate_limiter.acquire(
            identity.user_id, identity.role_id,
        ):
            return _deny(
                DenyReason.RATE_LIMITED,
                f"user={identity.user_id} role={identity.role_id}",
                column_allowlist=frozenset(),
            )

        policy_row = self._fetch_policy(identity.role_id, source_id)
        if policy_row is None:
            return _deny(
                DenyReason.NO_POLICY,
                f"role={identity.role_id} source={source_id}",
                column_allowlist=frozenset(),
            )
        (
            policy_id, row_filter_json, default_mode, _policy_schema_version,
            valid_until, expired,
        ) = policy_row
        # Expired policies fail BEFORE quality/freshness consultation -- an
        # expired grant means the user has no access regardless of source
        # health, so don't pay the obs round-trip to learn that.
        if expired:
            return _deny(
                DenyReason.POLICY_EXPIRED,
                f"valid_until={valid_until.isoformat()}",
                column_allowlist=frozenset(),
            )

        allowed = self._compute_allowed_columns(source_id, policy_id, default_mode)

        # Quality / freshness consultation (V1.5: SQL-aggregated; V2: cache-table).
        # Block before any column / metric work -- a stale or failed source means
        # no answer is trustworthy regardless of which columns the user asked for.
        if self.observability is not None:
            failures = self.observability.latest_blocking_failures(source_id)
            if failures:
                first = failures[0]
                deny_reason = (
                    DenyReason.QUALITY_BLOCK
                    if first.rule_kind == "quality"
                    else DenyReason.FRESHNESS_BLOCK
                )
                return _deny(
                    deny_reason,
                    first.detail or first.reason,
                    column_allowlist=allowed,
                    quality_holds=tuple(f.rule_id for f in failures),
                )

        for col in requested_columns:
            if col not in allowed:
                return _deny(DenyReason.COLUMN_DENIED, col, column_allowlist=allowed)

        metric_versions: list[tuple[str, int]] = []
        for metric_id in requested_metrics:
            row = self.con.execute(
                "SELECT current_version, source_id FROM gibran_metrics WHERE metric_id = ?",
                [metric_id],
            ).fetchone()
            if row is None or row[1] != source_id:
                return _deny(DenyReason.METRIC_DENIED, metric_id, column_allowlist=allowed)
            metric_versions.append((metric_id, row[0]))

        injected_filter_sql: str | None = None
        if row_filter_json:
            ast = json.loads(row_filter_json)
            try:
                # Defense-in-depth: validate at evaluate time too. The loader
                # already validated at sync time, but a runtime check costs
                # microseconds and catches DB tampering.
                validate_policy_ast(ast, frozenset(self._all_columns_for(source_id)))
                injected_filter_sql = compile_policy_to_sql(ast, identity)
            except ASTValidationError as e:
                return _deny(DenyReason.AST_INVALID, str(e), column_allowlist=allowed)
            except KeyError as e:
                return _deny(
                    DenyReason.ATTRIBUTE_MISSING,
                    str(e).strip("'\""),
                    column_allowlist=allowed,
                )

        # Quality/freshness consultation: deferred. When obs layer lands,
        # call obs.latest_blocking_failures(source_id) here; if any, deny
        # with QUALITY_BLOCK / FRESHNESS_BLOCK.

        return GovernanceDecision(
            allowed=True,
            deny_reason=None,
            deny_detail=None,
            column_allowlist=allowed,
            injected_filter_sql=injected_filter_sql,
            applied_constraints=(),  # V1: empty; V2 walks the AST
            metric_versions=tuple(metric_versions),
            quality_holds=(),
        )

    # ------------------------------------------------------------------
    # validate_alternatives
    # ------------------------------------------------------------------
    def validate_alternatives(
        self,
        identity: IdentityContext,
        source_ids: frozenset[str],
        candidates: tuple[tuple[frozenset[str], tuple[str, ...]], ...],
    ) -> tuple[GovernanceDecision, ...]:
        # V1: naive per-candidate evaluate. The Protocol contract calls for
        # O(1) per candidate after the first via shared compiled-policy +
        # AllowedSchema cache; that optimization lands when profiling
        # demonstrates the need (NL pipeline doesn't exist yet).
        return tuple(
            self.evaluate(identity, source_ids, requested_columns, requested_metrics)
            for requested_columns, requested_metrics in candidates
        )

    # ------------------------------------------------------------------
    # internal helpers
    # ------------------------------------------------------------------
    def _fetch_policy(self, role_id: str, source_id: str):
        # `expired` is computed inside SQL so the comparison uses DuckDB's
        # clock -- matches the precedent set by latest_blocking_failures
        # for staleness (mixing Python wall-clock with DB CURRENT_TIMESTAMP
        # produces UTC-offset drift on local-time installs).
        return self.con.execute(
            "SELECT policy_id, row_filter_ast, default_column_mode, schema_version, "
            "valid_until, "
            "(valid_until IS NOT NULL AND valid_until < CURRENT_TIMESTAMP) AS expired "
            "FROM gibran_policies WHERE role_id = ? AND source_id = ?",
            [role_id, source_id],
        ).fetchone()

    def _all_columns_for(self, source_id: str) -> set[str]:
        return {
            r[0]
            for r in self.con.execute(
                "SELECT column_name FROM gibran_columns WHERE source_id = ?",
                [source_id],
            ).fetchall()
        }

    def _compute_allowed_columns(
        self, source_id: str, policy_id: str, default_mode: str
    ) -> frozenset[str]:
        all_cols = self._all_columns_for(source_id)
        overrides = dict(
            self.con.execute(
                "SELECT column_name, granted FROM gibran_policy_columns WHERE policy_id = ?",
                [policy_id],
            ).fetchall()
        )
        allowed: set[str] = set()
        for col in all_cols:
            override = overrides.get(col)
            if override is True:
                allowed.add(col)
            elif override is False:
                continue
            elif default_mode == "allow":
                allowed.add(col)
        return frozenset(allowed)

    def _build_column_views(
        self, source_id: str, allowed: frozenset[str]
    ) -> tuple[ColumnView, ...]:
        rows = self.con.execute(
            "SELECT column_name, data_type, sensitivity, description, "
            "example_values "
            "FROM gibran_columns WHERE source_id = ? ORDER BY column_name",
            [source_id],
        ).fetchall()
        views: list[ColumnView] = []
        for name, data_type, sensitivity, description, ex_json in rows:
            if name not in allowed:
                continue
            example_values: tuple[str, ...] | None = None
            if ex_json is not None:
                try:
                    parsed = json.loads(ex_json)
                    if isinstance(parsed, list):
                        # Tuple of strings for the contract; preserve None as
                        # the literal string "null" so the rendering layer
                        # can spot it. Numeric/bool values get str()'d.
                        example_values = tuple(
                            "null" if v is None else str(v) for v in parsed
                        )
                except (ValueError, TypeError):
                    example_values = None
            views.append(ColumnView(
                name=name,
                display_name=name,
                data_type=data_type,
                sensitivity=sensitivity,
                description=description,
                example_values=example_values,
            ))
        return tuple(views)

    def _build_dimension_views(
        self, source_id: str, allowed: frozenset[str]
    ) -> tuple[DimensionView, ...]:
        rows = self.con.execute(
            "SELECT dimension_id, column_name, display_name, dim_type, description "
            "FROM gibran_dimensions WHERE source_id = ? ORDER BY dimension_id",
            [source_id],
        ).fetchall()
        return tuple(
            DimensionView(
                dimension_id=did,
                column_name=col,
                display_name=display,
                dim_type=dtype,
                description=desc,
            )
            for did, col, display, dtype, desc in rows
            if col in allowed
        )

    def _build_metric_views(self, source_id: str) -> tuple[MetricView, ...]:
        metric_rows = self.con.execute(
            "SELECT metric_id, display_name, metric_type, unit, description "
            "FROM gibran_metrics WHERE source_id = ? ORDER BY metric_id",
            [source_id],
        ).fetchall()
        if not metric_rows:
            return ()
        metric_ids = [r[0] for r in metric_rows]
        placeholders = ",".join(["?"] * len(metric_ids))
        dep_rows = self.con.execute(
            f"SELECT metric_id, depends_on_id FROM gibran_metric_dependencies "
            f"WHERE metric_id IN ({placeholders})",
            metric_ids,
        ).fetchall()
        deps_by_metric: dict[str, list[str]] = {}
        for mid, dep in dep_rows:
            deps_by_metric.setdefault(mid, []).append(dep)

        # For ratio metrics, parse the stored expression `{num}/{denom}`
        # so MetricView can expose numerator/denominator explicitly.
        # Avoids forcing NL pattern code to re-parse via the compile
        # module's regex (and to re-query the catalog from there).
        ratio_ids = [
            r[0] for r in metric_rows if r[2] == "ratio"
        ]
        num_denom_by_metric: dict[str, tuple[str | None, str | None]] = {}
        if ratio_ids:
            import re as _re
            _ratio_re = _re.compile(
                r"^\{([a-zA-Z_][a-zA-Z0-9_]*)\}/\{([a-zA-Z_][a-zA-Z0-9_]*)\}$"
            )
            ratio_placeholders = ",".join(["?"] * len(ratio_ids))
            expr_rows = self.con.execute(
                "SELECT metric_id, expression FROM gibran_metric_versions "
                "WHERE metric_id IN (" + ratio_placeholders + ") "
                "AND effective_to IS NULL",
                ratio_ids,
            ).fetchall()
            for mid, expr in expr_rows:
                match = _ratio_re.match(expr or "")
                if match:
                    num_denom_by_metric[mid] = (match.group(1), match.group(2))

        return tuple(
            MetricView(
                metric_id=mid,
                display_name=display,
                metric_type=mtype,
                unit=unit,
                description=desc,
                depends_on=tuple(sorted(deps_by_metric.get(mid, []))),
                numerator=num_denom_by_metric.get(mid, (None, None))[0],
                denominator=num_denom_by_metric.get(mid, (None, None))[1],
            )
            for mid, display, mtype, unit, desc in metric_rows
        )


def _deny(
    reason: DenyReason,
    detail: str,
    *,
    column_allowlist: frozenset[str],
    quality_holds: tuple[str, ...] = (),
) -> GovernanceDecision:
    return GovernanceDecision(
        allowed=False,
        deny_reason=reason,
        deny_detail=detail,
        column_allowlist=column_allowlist,
        injected_filter_sql=None,
        applied_constraints=(),
        metric_versions=(),
        quality_holds=quality_holds,
    )
