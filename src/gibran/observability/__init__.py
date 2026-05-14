"""Observability layer.

Owns gibran_quality_rules, gibran_freshness_rules, gibran_quality_runs,
gibran_query_log, gibran_query_metrics, gibran_source_health. Provides a
consultation API used by governance.evaluate() to enforce blocking
quality/freshness rules, and a runner that executes the actual checks
(`gibran check`).

Public API:
- BlockingFailure (dataclass)
- ObservabilityAPI (Protocol)
- DefaultObservability (V2 implementation: cache + V1.5 fallback)
- run_checks(con, source_id, observability) -> RunChecksResult
- RuleResult, RunChecksResult (dataclasses)
"""
from gibran.observability.default import DefaultObservability
from gibran.observability.runner import RuleResult, RunChecksResult, run_checks
from gibran.observability.types import (
    BlockingFailure,
    ObservabilityAPI,
    resolve_staleness_seconds,
)

__all__ = [
    "BlockingFailure",
    "DefaultObservability",
    "ObservabilityAPI",
    "RuleResult",
    "RunChecksResult",
    "resolve_staleness_seconds",
    "run_checks",
]
