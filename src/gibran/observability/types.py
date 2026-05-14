"""Observability types and Protocol.

Exposed so that governance.evaluate can consult the obs layer without
knowing the specific implementation. V1.5 ships DefaultObservability;
V2 will swap in a cache-table-backed implementation behind the same
Protocol."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal, Protocol


_DEFAULT_STALENESS_QUALITY_CHEAP = 600       # 10 minutes
_DEFAULT_STALENESS_QUALITY_EXPENSIVE = 3600  # 1 hour
_DEFAULT_STALENESS_FRESHNESS = 300           # 5 minutes


def resolve_staleness_seconds(
    rule_kind: Literal["quality", "freshness"],
    cost_class: Literal["cheap", "expensive"] | None,
    staleness_seconds: int | None,
) -> int:
    """Resolve effective staleness window.

    Explicit YAML value wins. Otherwise fall back to defaults by kind/cost_class.
    """
    if staleness_seconds is not None:
        return staleness_seconds
    if rule_kind == "freshness":
        return _DEFAULT_STALENESS_FRESHNESS
    if cost_class == "cheap":
        return _DEFAULT_STALENESS_QUALITY_CHEAP
    return _DEFAULT_STALENESS_QUALITY_EXPENSIVE


@dataclass(frozen=True)
class BlockingFailure:
    """A reason a query against this source must be denied.

    `reason` distinguishes the *category* of failure (the rule failed,
    or the latest run is too old to trust, or the rule has never been
    run); `detail` carries the specific rule_id + observed-value summary
    for the audit log."""
    rule_id: str
    rule_kind: Literal["quality", "freshness"]
    severity: Literal["warn", "block"]
    cost_class: Literal["cheap", "expensive"] | None  # None for freshness
    last_run_at: datetime | None
    reason: Literal["rule_failed", "stale_check", "never_run"]
    detail: str | None
    seconds_overdue: int | None


class ObservabilityAPI(Protocol):
    """Contract consumed by governance.evaluate.

    `latest_blocking_failures` is on the query hot path -- implementations
    must keep it cheap (V1.5: single SQL query; V2: O(1) cache lookup).

    `record_run` is called by the runner / `gibran check` (not the query path)."""

    def latest_blocking_failures(
        self, source_id: str
    ) -> tuple[BlockingFailure, ...]: ...

    def record_run(
        self,
        rule_id: str,
        rule_kind: Literal["quality", "freshness"],
        passed: bool,
        observed_value: dict | None = None,
    ) -> str: ...
