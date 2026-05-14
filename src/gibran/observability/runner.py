"""Quality + freshness rule runners.

`run_checks(con, source_id)` reads enabled rules for the source from the
catalog, executes each one against the actual data, calls
`DefaultObservability.record_run` to log the result, and then triggers
`refresh_health` to denormalize the latest state into gibran_source_health.

The FROM-clause for each rule is built via `_source_dispatch`, so parquet
and csv sources work natively (no manual `CREATE VIEW` required). The
rule's SQL is the same shape regardless of source_type -- only the
relation fragment differs."""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Literal

import duckdb

from gibran._source_dispatch import from_clause_for_source
from gibran._sql import qident, render_literal


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
    new latest-run-per-rule state. After all runs land, fires the
    configured `alert_webhook` for any rule that failed at
    severity='block' -- one POST per failing rule, best-effort.
    """
    results: list[RuleResult] = []

    # Quality rules. We need alert_webhook per rule for the outbound
    # alerting step below; select it alongside the evaluation inputs.
    quality_rows = con.execute(
        "SELECT rule_id, rule_type, rule_config, severity, alert_webhook "
        "FROM gibran_quality_rules WHERE source_id = ? AND enabled = TRUE",
        [source_id],
    ).fetchall()
    webhooks: list[tuple[str, str, str, dict]] = []
    for rule_id, rule_type, rule_config_json, severity, webhook in quality_rows:
        rule_config = json.loads(rule_config_json)
        try:
            passed, observed = _evaluate_quality_rule(
                con, source_id, rule_type, rule_config, rule_id=rule_id,
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
        if not passed and severity == "block" and webhook:
            webhooks.append((rule_id, "quality", webhook, observed))

    # Freshness rules
    freshness_rows = con.execute(
        "SELECT rule_id, watermark_column, max_age_seconds, severity "
        "FROM gibran_freshness_rules WHERE source_id = ?",
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

    # Bump the source-health generation token so any in-process result
    # cache entries based on the prior health state become stale on
    # next lookup. (The result cache reads both catalog_generation and
    # source_health_generation; bumping either invalidates.)
    from gibran.execution.result_cache import bump_source_health_generation
    bump_source_health_generation(con)

    # Fire any configured webhooks for block-severity failures. Done AFTER
    # record_run so an alerting outage doesn't lose the run record. Each
    # POST is best-effort -- failures are caught and silently dropped so
    # one bad URL doesn't sink the whole check pass.
    for rule_id, rule_kind, url, observed in webhooks:
        _fire_webhook(url, rule_id, rule_kind, source_id, observed)

    total = len(results)
    failed = sum(1 for r in results if not r.passed and r.error is None)
    errored = sum(1 for r in results if r.error is not None)
    passed_count = total - failed - errored
    return RunChecksResult(
        source_id=source_id, total=total, passed=passed_count,
        failed=failed, errored=errored, results=tuple(results),
    )


def _fire_webhook(
    url: str, rule_id: str, rule_kind: str, source_id: str,
    observed: dict[str, Any],
) -> None:
    """POST a JSON payload to `url`. Best-effort -- network failures
    are swallowed. Synchronous; intentionally not threaded so a runner
    completes deterministically in tests and CI."""
    import json as _json
    import urllib.request

    payload = {
        "rule_id": rule_id,
        "rule_kind": rule_kind,
        "source_id": source_id,
        "observed": observed,
        "severity": "block",
    }
    body = _json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url, data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        urllib.request.urlopen(req, timeout=5).read(0)
    except Exception:
        pass  # alerting failure must not affect run-record correctness


# ---------------------------------------------------------------------------
# Per-rule-type evaluators
# ---------------------------------------------------------------------------

def _evaluate_quality_rule(
    con: duckdb.DuckDBPyConnection,
    source_id: str,
    rule_type: str,
    rule_config: dict[str, Any],
    *,
    rule_id: str | None = None,
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

    if rule_type == "anomaly":
        # Compute the current numeric observation, then compare to the
        # trailing N observations of the same rule. Bootstrap behavior:
        # with fewer than `trailing_periods` history rows, the rule
        # never fails -- we don't have enough data to be confident.
        if rule_id is None:
            raise ValueError(
                "anomaly rules require rule_id to look up history "
                "(callers via run_checks pass it automatically)"
            )
        sql = rule_config["sql"]
        n_sigma = float(rule_config["n_sigma"])
        trailing = int(rule_config["trailing_periods"])
        row = con.execute(sql).fetchone()
        if row is None or len(row) != 1:
            return False, {
                "error": "anomaly sql must return one row, one column",
            }
        current = row[0]
        if current is None:
            return False, {"error": "anomaly sql returned NULL"}
        current_f = float(current)
        history = con.execute(
            "SELECT CAST(observed_value->>'value' AS DOUBLE) "
            "FROM gibran_quality_runs "
            "WHERE rule_id = ? AND rule_kind = 'quality' "
            "AND observed_value IS NOT NULL "
            "AND CAST(observed_value->>'value' AS DOUBLE) IS NOT NULL "
            "ORDER BY ran_at DESC LIMIT ?",
            [rule_id, trailing],
        ).fetchall()
        values = [float(r[0]) for r in history if r[0] is not None]
        observed: dict[str, Any] = {
            "value": current_f,
            "n_sigma": n_sigma,
            "trailing_periods": trailing,
            "history_count": len(values),
        }
        if len(values) < 2:
            observed["bootstrapping"] = True
            return True, observed
        mean = sum(values) / len(values)
        variance = sum((v - mean) ** 2 for v in values) / (len(values) - 1)
        stddev = variance ** 0.5
        observed["mean"] = mean
        observed["stddev"] = stddev
        if stddev == 0:
            observed["constant_history"] = True
            return current_f == mean, observed
        z = abs(current_f - mean) / stddev
        observed["z_score"] = z
        return z <= n_sigma, observed

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
