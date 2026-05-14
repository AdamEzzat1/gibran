"""Sample distinct values for low-cardinality public columns.

Called from the CLI's `gibran sync` after `apply_config` has written
the catalog. For each declared source whose schema we can probe, scan
each public column (gated by `expose_examples is not False`); if the
column has at most `low_cardinality_threshold` distinct values, store
the first `sample_limit` of them as a JSON array in
`gibran_columns.example_values`.

Two safety properties:

  * Sensitivity is a HARD gate -- only `sensitivity == 'public'`
    columns are ever sampled. PII / restricted / internal / unclassified
    columns are never touched, regardless of `expose_examples`. Matches
    the lock in `memory/gibran_constraints.md` and the contract docstring
    on `ColumnView.example_values`.
  * Sources we can't probe (file missing, table doesn't exist yet) are
    silently skipped -- example-value sampling is opportunistic, not a
    correctness gate. The catalog's column rows still get written by
    `apply_config`; example_values just stays NULL for those.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Iterable

import duckdb

from gibran._source_dispatch import SourceDispatchError, build_from_clause
from gibran._sql import qident
from gibran.sync.yaml_schema import GibranConfig, SourceConfig


@dataclass(frozen=True)
class _SamplingResult:
    """Per-column outcome of the sampling pass. Useful for tests and for
    diagnostic output from the CLI."""
    source_id: str
    column_name: str
    status: str  # 'sampled', 'skipped_non_public', 'skipped_opt_out', 'skipped_high_cardinality', 'skipped_unreachable'
    value_count: int | None  # number of sampled values; None for non-sampled cases


def populate_example_values(
    con: duckdb.DuckDBPyConnection,
    config: GibranConfig,
    *,
    low_cardinality_threshold: int = 20,
    sample_limit: int = 10,
) -> list[_SamplingResult]:
    """Walk every source's public columns and update example_values.

    Returns a list of _SamplingResult per column considered, for callers
    that want to log diagnostics. Side effect: UPDATEs gibran_columns
    rows where a value list was sampled.
    """
    results: list[_SamplingResult] = []
    for source in config.sources:
        try:
            from_clause = build_from_clause(source.type, source.uri)
        except SourceDispatchError:
            for col in source.columns:
                results.append(_SamplingResult(
                    source.id, col.name, "skipped_unreachable", None,
                ))
            continue
        for col in source.columns:
            if col.sensitivity != "public":
                results.append(_SamplingResult(
                    source.id, col.name, "skipped_non_public", None,
                ))
                continue
            if col.expose_examples is False:
                results.append(_SamplingResult(
                    source.id, col.name, "skipped_opt_out", None,
                ))
                continue
            outcome = _sample_one(
                con, source, col.name, from_clause,
                low_cardinality_threshold, sample_limit,
            )
            results.append(outcome)
    return results


def _sample_one(
    con: duckdb.DuckDBPyConnection,
    source: SourceConfig,
    col_name: str,
    from_clause: str,
    low_cardinality_threshold: int,
    sample_limit: int,
) -> _SamplingResult:
    qcol = qident(col_name)
    # Probe one row over the cardinality threshold so we can distinguish
    # "exactly at threshold" from "above threshold." If we get back more
    # than `low_cardinality_threshold` rows, we skip the column.
    try:
        rows = con.execute(
            f"SELECT DISTINCT {qcol} FROM {from_clause} "
            f"LIMIT {low_cardinality_threshold + 1}"
        ).fetchall()
    except (duckdb.Error, Exception):
        return _SamplingResult(source.id, col_name, "skipped_unreachable", None)
    if len(rows) > low_cardinality_threshold:
        return _SamplingResult(
            source.id, col_name, "skipped_high_cardinality", None,
        )
    values = _json_safe([r[0] for r in rows[:sample_limit]])
    con.execute(
        "UPDATE gibran_columns SET example_values = ? "
        "WHERE source_id = ? AND column_name = ?",
        [json.dumps(values), source.id, col_name],
    )
    return _SamplingResult(source.id, col_name, "sampled", len(values))


def _json_safe(values: Iterable) -> list:
    """Coerce values to JSON-storable forms. Strings/numbers/bools/None
    pass through; everything else gets str()'d (matches what the value
    looks like in user-facing tools like `gibran describe`)."""
    out = []
    for v in values:
        if v is None or isinstance(v, (str, int, float, bool)):
            out.append(v)
        else:
            out.append(str(v))
    return out
