"""Quality + freshness rule runners.

`run_checks(con, source_id)` reads enabled rules for the source from the
catalog, executes each one against the actual data, calls
`DefaultObservability.record_run` to log the result, and then triggers
`refresh_health` to denormalize the latest state into rumi_source_health.

The FROM-clause for each rule is built via `_source_dispatch`, so parquet
and csv sources work natively (no manual `CREATE VIEW` required). The
rule's SQL is the same shape regardless of source_type -- only the
relation fragment differs."""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Literal

import duckdb

from rumi._source_dispatch import from_clause_for_source
from rumi._sql import qident, render_literal


@dataclass(frozen=True)
class RuleResult:
    rule_id: str
    rule_kind: Literal["quality", "freshness"]
    rule_type: str         # e.g. 'not_null', 'unique', 'range', 'custom_sql', 'freshness'
    passed: bool
    severity: Literal["warn", "block"]
    observed_value: dict[str, Any]
    error: str | None      # if the check itself errored


@dataclass(frozen=True)
class RunChecksResult:
    source_id: str
    total: int
    passed: int
    failed: int
    errored: int
    results: tuple[RuleResult, ...]


def run_checks(
    con: duckdb.DuckDBPyConnection,
    source_id: str,
    observability,                   # DefaultObservability; avoids circular import
) -> RunChecksResult:
    """Run all enabled quality + freshness rules for a source.

    Records each run via `observability.record_run`, then triggers
    `observability.refresh_health(source_id)` so the cache reflects the
    new latest-run-per-rule state."""
    results: list[RuleResult] = []

    # Quality rules
    quality_rows = con.execute(
        "SELECT rule_id, rule_type, rule_config, severity "
        "FROM rumi_quality_rules WHERE source_id = ? AND enabled = TRUE",
        [source_id],
    ).fetchall()
    for rule_id, rule_type, rule_config_json, severity in quality_rows:
        rule_config = json.loads(rule_config_json)
        try:
            passed, observed = _evaluate_quality_rule(
                con, source_id, rule_type, rule_config
            )
            err = None
        except Exception as e:
            passed = False
            observed = {"error": str(e)}
            err = str(e)
        observability.record_run(rule_id, "quality", passed, observed)
        results.append(RuleResult(
            rule_id=rule_id, rule_kind="quality", rule_type=rule_type,
            passed=passed, severity=severity, observed_value=observed,
            error=err,
        ))

    # Freshness rules
    freshness_rows = con.execute(
        "SELECT rule_id, watermark_column, max_age_seconds, severity "
        "FROM rumi_freshness_rules WHERE source_id = ?",
        [source_id],
    ).fetchall()
    for rule_id, watermark_column, max_age_seconds, severity in freshness_rows:
        try:
            passed, observed = _evaluate_freshness_rule(
                con, source_id, watermark_column, max_age_seconds
            )
            err = None
        except Exception as e:
            passed = False
            observed = {"error": str(e)}
            err = str(e)
        observability.record_run(rule_id, "freshness", passed, observed)
        results.append(RuleResult(
            rule_id=rule_id, rule_kind="freshness", rule_type="freshness",
            passed=passed, severity=severity, observed_value=observed,
            error=err,
        ))

    # Refresh the source health cache to reflect the runs we just recorded.
    observability.refresh_health(source_id)

    total = len(results)
    failed = sum(1 for r in results if not r.passed and r.error is None)
    errored = sum(1 for r in results if r.error is not None)
    passed_count = total - failed - errored
    return RunChecksResult(
        source_id=source_id, total=total, passed=passed_count,
        failed=failed, errored=errored, results=tuple(results),
    )


# ---------------------------------------------------------------------------
# Per-rule-type evaluators
# ---------------------------------------------------------------------------

def _evaluate_quality_rule(
    con: duckdb.DuckDBPyConnection,
    source_id: str,
    rule_type: str,
    rule_config: dict[str, Any],
) -> tuple[bool, dict[str, Any]]:
    relation = from_clause_for_source(con, source_id)

    if rule_type == "not_null":
        col = qident(rule_config["column"])
        cnt = con.execute(
            f"SELECT COUNT(*) FROM {relation} WHERE {col} IS NULL"
        ).fetchone()[0]
        return cnt == 0, {"null_count": int(cnt)}

    if rule_type == "unique":
        col = qident(rule_config["column"])
        total, distinct = con.execute(
            f"SELECT COUNT(*), COUNT(DISTINCT {col}) FROM {relation}"
        ).fetchone()
        return total == distinct, {
            "total": int(total), "distinct": int(distinct),
            "duplicates": int(total - distinct),
        }

    if rule_type == "range":
        col = qident(rule_config["column"])
        clauses: list[str] = []
        observed: dict[str, Any] = {}
        if "min" in rule_config:
            clauses.append(f"{col} < {render_literal(rule_config['min'])}")
            observed["min"] = rule_config["min"]
        if "max" in rule_config:
            clauses.append(f"{col} > {render_literal(rule_config['max'])}")
            observed["max"] = rule_config["max"]
        if not clauses:
            raise ValueError("range rule requires config.min and/or config.max")
        where = " OR ".join(clauses)
        cnt = con.execute(
            f"SELECT COUNT(*) FROM {relation} WHERE {where}"
        ).fetchone()[0]
        observed["out_of_range_count"] = int(cnt)
        return cnt == 0, observed

    if rule_type == "custom_sql":
        sql = rule_config["sql"]
        # Convention: rule SQL must return a single row, single column.
        # Truthy result = passed. The implementor is responsible for any
        # joins to the source; we pass the SQL through unchanged.
        result = con.execute(sql).fetchall()
        if len(result) != 1 or len(result[0]) != 1:
            return False, {
                "error": "custom_sql must return exactly one row, one column",
                "rows_returned": len(result),
            }
        return bool(result[0][0]), {"result": str(result[0][0])}

    raise ValueError(f"unknown quality rule_type: {rule_type!r}")


def _evaluate_freshness_rule(
    con: duckdb.DuckDBPyConnection,
    source_id: str,
    watermark_column: str,
    max_age_seconds: int,
) -> tuple[bool, dict[str, Any]]:
    relation = from_clause_for_source(con, source_id)
    col = qident(watermark_column)
    row = con.execute(
        f"SELECT MAX({col}), "
        f"DATE_DIFF('second', MAX({col}), now()) "
        f"FROM {relation}"
    ).fetchone()
    max_watermark, age_seconds = row
    if max_watermark is None:
        return False, {"error": "no rows in source"}
    return age_seconds <= max_age_seconds, {
        "watermark": str(max_watermark),
        "age_seconds": int(age_seconds),
        "max_age_seconds": int(max_age_seconds),
    }
