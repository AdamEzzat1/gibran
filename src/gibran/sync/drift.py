"""Schema-drift detection for `gibran sync`.

Compares each YAML-declared source against the actual schema reachable
through DuckDB. Surfaces three categories of drift:

  - missing_in_db    -- YAML declares a column the source doesn't have
  - missing_in_yaml  -- the source has a column the YAML doesn't declare
  - type_mismatch    -- YAML declares one type, the source has another

Called from the CLI BEFORE `apply_config` writes anything to the catalog,
so the warnings reflect the discrepancy between intent (YAML) and reality
(the live source). Drift is informational -- it produces warnings, not
errors. A source that can't be probed (file not found, table doesn't
exist yet) is silently skipped: drift detection is opportunistic, not a
correctness gate.

The probe uses `DESCRIBE SELECT * FROM <from-clause> LIMIT 0` which works
uniformly across `duckdb_table` / `sql_view` / `parquet` / `csv` source
types because the source dispatcher returns the right FROM snippet for
each. (`LIMIT 0` keeps DuckDB from actually scanning rows; we just want
the column-type metadata.)
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import duckdb

from gibran._source_dispatch import SourceDispatchError, build_from_clause
from gibran.sync.yaml_schema import GibranConfig, SourceConfig


DriftKind = Literal["missing_in_db", "missing_in_yaml", "type_mismatch"]


@dataclass(frozen=True)
class DriftEvent:
    source_id: str
    column: str
    kind: DriftKind
    yaml_type: str | None
    actual_type: str | None

    def as_warning(self) -> str:
        if self.kind == "missing_in_db":
            return (
                f"warning: {self.source_id}.{self.column} declared in YAML as "
                f"{self.yaml_type!r} but missing from the source"
            )
        if self.kind == "missing_in_yaml":
            return (
                f"warning: {self.source_id}.{self.column} present in the source as "
                f"{self.actual_type!r} but not declared in YAML"
            )
        return (
            f"warning: {self.source_id}.{self.column} YAML says {self.yaml_type!r}, "
            f"source has {self.actual_type!r}"
        )


@dataclass(frozen=True)
class UnreachableSource:
    """Recorded when a source can't be probed (file missing, table not yet
    created, etc.). Distinct from a DriftEvent: this means we don't know
    whether there's drift, not that there is."""
    source_id: str
    reason: str


def detect_drift(
    con: duckdb.DuckDBPyConnection, config: GibranConfig
) -> tuple[list[DriftEvent], list[UnreachableSource]]:
    """Probe each YAML-declared source and compare to its YAML column set.

    Returns (drift_events, unreachable_sources). Drift events list concrete
    discrepancies; unreachable sources list probes that errored out (so
    callers can warn separately without confusing the two states).
    """
    drift: list[DriftEvent] = []
    unreachable: list[UnreachableSource] = []
    for s in config.sources:
        try:
            actual = _probe_source_schema(con, s)
        except (duckdb.Error, SourceDispatchError) as e:
            unreachable.append(UnreachableSource(s.id, _short_reason(e)))
            continue
        actual_cols = dict(actual)
        yaml_cols = {c.name: c.type for c in s.columns}

        for name, yaml_type in yaml_cols.items():
            if name not in actual_cols:
                drift.append(
                    DriftEvent(s.id, name, "missing_in_db", yaml_type, None)
                )
                continue
            if _normalize_type(yaml_type) != _normalize_type(actual_cols[name]):
                drift.append(
                    DriftEvent(
                        s.id, name, "type_mismatch",
                        yaml_type, actual_cols[name],
                    )
                )

        for name, actual_type in actual_cols.items():
            if name not in yaml_cols:
                drift.append(
                    DriftEvent(s.id, name, "missing_in_yaml", None, actual_type)
                )

    return drift, unreachable


def _probe_source_schema(
    con: duckdb.DuckDBPyConnection, source: SourceConfig
) -> list[tuple[str, str]]:
    """Return [(column_name, column_type), ...] from the source's live schema.

    Uses DESCRIBE on the dispatcher's FROM snippet so the probe shape is
    identical across parquet / csv / duckdb_table / sql_view sources."""
    from_clause = build_from_clause(source.type, source.uri)
    # DESCRIBE returns (column_name, column_type, null, key, default, extra).
    # LIMIT 0 keeps DuckDB from materializing rows -- we just want metadata.
    rows = con.execute(f"DESCRIBE SELECT * FROM {from_clause} LIMIT 0").fetchall()
    return [(r[0], r[1]) for r in rows]


def _normalize_type(t: str) -> str:
    """Loose normalization for type comparison: strip whitespace, lowercase.

    Does NOT unify aliases like VARCHAR/TEXT/STRING -- if the YAML says
    `VARCHAR` and the source says `TEXT`, that's surfaced as a type
    mismatch. The user can choose to align them; we don't paper over
    differences that might matter (e.g. CHAR(N) vs VARCHAR).
    """
    return t.strip().lower().replace(" ", "")


def _short_reason(e: Exception) -> str:
    """Compress an exception message to one line for the unreachable list."""
    msg = str(e).strip().splitlines()[0]
    return msg if len(msg) <= 120 else msg[:117] + "..."
