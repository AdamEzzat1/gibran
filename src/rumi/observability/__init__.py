"""Observability layer.

Owns rumi_quality_rules, rumi_freshness_rules, rumi_quality_runs,
rumi_query_log, rumi_query_metrics, rumi_source_health. Provides a
consultation API used by governance.evaluate() to enforce blocking
quality/freshness rules, and a runner that executes the actual checks
(`rumi check`).

Public API:
- BlockingFailure (dataclass)
- ObservabilityAPI (Protocol)
- DefaultObservability (V2 implementation: cache + V1.5 fallback)
- run_checks(con, source_id, observability) -> RunChecksResult
- RuleResult, RunChecksResult (dataclasses)
"""
from rumi.observability.default import DefaultObservability
from rumi.observability.runner import RuleResult, RunChecksResult, run_checks
from rumi.observability.types import (
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
