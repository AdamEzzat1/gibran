from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Protocol


@dataclass(frozen=True)
class IdentityContext:
    """Resolved identity for one request. Produced by an IdentityResolver,
    consumed by every downstream layer. Attributes merge role attrs first,
    then user attrs (user-specific values win on key collision)."""
    user_id: str
    role_id: str
    attributes: dict[str, str]
    source: str  # 'jwt' | 'env' | 'cli'


class IdentityResolver(Protocol):
    """Pluggable identity resolution. Production -> JWTResolver.
    EnvResolver and CLIResolver exist for local dev and CI scripts."""
    def resolve(self, request_context: object) -> IdentityContext: ...


ALLOWED_AST_OPS: frozenset[str] = frozenset({
    "and", "or", "not",
    "eq", "neq", "lt", "lte", "gt", "gte",
    "in", "not_in",
    "is_null", "is_not_null",
    "between",
})


class DenyReason(str, Enum):
    NO_POLICY         = "policy:no_policy_for_role"
    COLUMN_DENIED     = "policy:no_column_access"
    METRIC_DENIED     = "policy:no_metric_access"
    AST_INVALID       = "policy:ast_invalid"
    ATTRIBUTE_MISSING = "policy:attribute_unresolved"
    POLICY_EXPIRED    = "policy:expired"
    QUALITY_BLOCK     = "quality:rule_failed"
    FRESHNESS_BLOCK   = "freshness:rule_failed"


@dataclass(frozen=True)
class Constraint:
    """Structured form of an applied policy predicate. Travels alongside
    injected_filter_sql so NL/UI consumers can introspect constraints
    without reparsing the rewritten SQL."""
    column: str
    op: str
    value: str | tuple[str, ...] | None
    source: str  # 'policy_filter' | 'role_pin' | 'attribute'
    rationale: str


@dataclass(frozen=True)
class GovernanceDecision:
    allowed: bool
    deny_reason: DenyReason | None
    deny_detail: str | None
    column_allowlist: frozenset[str]
    injected_filter_sql: str | None
    applied_constraints: tuple[Constraint, ...]
    metric_versions: tuple[tuple[str, int], ...]
    quality_holds: tuple[str, ...]


@dataclass(frozen=True)
class ColumnView:
    """Prompt-buildable projection of a column for the NL pipeline.

    example_values is populated only when:
      sensitivity == 'public' and expose_examples is not False, OR
      expose_examples is explicitly True.
    Never populated for 'pii' or 'restricted' regardless of expose_examples.
    Auto-inferred 'unclassified' columns never expose examples."""
    name: str
    display_name: str
    data_type: str
    sensitivity: str
    description: str | None
    example_values: tuple[str, ...] | None


@dataclass(frozen=True)
class DimensionView:
    dimension_id: str
    column_name: str
    display_name: str
    dim_type: str
    description: str | None


@dataclass(frozen=True)
class MetricView:
    metric_id: str
    display_name: str
    metric_type: str
    unit: str | None
    description: str | None
    depends_on: tuple[str, ...]


@dataclass(frozen=True)
class AllowedSchema:
    """What an identity may see on a single source. Returned by
    governance.preview_schema(). NL prompt builders consume this and
    nothing else from the catalog."""
    source_id: str
    source_display_name: str
    columns: tuple[ColumnView, ...]
    dimensions: tuple[DimensionView, ...]
    metrics: tuple[MetricView, ...]
    fixed_constraints: tuple[Constraint, ...]
    cache_version: tuple[int, int]  # (source.schema_version, policy.schema_version)


class GovernanceAPI(Protocol):
    """Single contract for every consumer of governance.

    V1 enforces single-source semantics in evaluate() and
    validate_alternatives() but the API shape commits to the
    cross-source V2 today (frozenset[str] of source_ids)."""

    def preview_schema(
        self, identity: IdentityContext, source_id: str
    ) -> AllowedSchema: ...

    def evaluate(
        self,
        identity: IdentityContext,
        source_ids: frozenset[str],
        requested_columns: frozenset[str],
        requested_metrics: tuple[str, ...],
    ) -> GovernanceDecision: ...

    def validate_alternatives(
        self,
        identity: IdentityContext,
        source_ids: frozenset[str],
        candidates: tuple[
            tuple[frozenset[str], tuple[str, ...]], ...
        ],
    ) -> tuple[GovernanceDecision, ...]:
        """Validate NL-generated candidate (columns, metrics) tuples.
        Cost contract: O(1) per candidate after the first; implementations
        must amortize compiled-policy + AllowedSchema across candidates."""
        ...
