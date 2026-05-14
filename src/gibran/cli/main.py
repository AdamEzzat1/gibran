from __future__ import annotations

from pathlib import Path

try:
    import typer
except ImportError as e:
    raise SystemExit(
        "gibran requires `typer`; install with `pip install -e .[dev]`"
    ) from e

import duckdb

from gibran.sync.applier import apply as apply_config
from gibran.sync.loader import load as load_config
from gibran.sync.migrations import apply_all as apply_migrations


app = typer.Typer(
    no_args_is_help=True,
    help="Governed analytics + NL-to-SQL over DuckDB.",
)


def _project_root() -> Path:
    return Path.cwd()


def _db_path(root: Path) -> Path:
    return root / "gibran.duckdb"


def _migrations_dir(root: Path) -> Path:
    return root / "migrations"


def _config_path(root: Path) -> Path:
    return root / "gibran.yaml"


@app.command()
def init(
    sample: bool = typer.Option(
        False, "--sample",
        help="Also drop a starter gibran.yaml and seed a sample orders table.",
    ),
) -> None:
    """Create gibran.duckdb in CWD and apply all migrations.

    With --sample, also writes a starter `gibran.yaml` to CWD and seeds a
    small `orders` table so `gibran sync` + `gibran check` + `gibran query`
    work end-to-end immediately."""
    root = _project_root()
    db = _db_path(root)
    migrations = _migrations_dir(root)
    if not migrations.is_dir():
        typer.echo(f"error: no migrations dir at {migrations}", err=True)
        raise typer.Exit(code=1)
    con = duckdb.connect(str(db))
    try:
        applied = apply_migrations(con, migrations)
        if sample:
            _seed_sample(con, root)
    finally:
        con.close()
    if applied:
        typer.echo(f"applied migrations: {applied}")
    else:
        typer.echo("no migrations to apply (already up to date)")
    if sample:
        cfg = _config_path(root)
        typer.echo(f"wrote sample config: {cfg}")
        typer.echo("seeded sample 'orders' table (4 rows)")
        typer.echo("")
        typer.echo("Next steps:")
        typer.echo("  gibran sync")
        typer.echo("  gibran check")
        typer.echo("  gibran describe orders --role analyst_west --attr region=west")
        typer.echo(
            "  gibran query --role analyst_west --attr region=west "
            "--dsl '{\"source\":\"orders\",\"metrics\":[\"order_count\"]}'"
        )


_SAMPLE_YAML = """\
# Starter gibran.yaml -- a single source ('orders') with a handful of
# columns, two dimensions, a few metrics, two roles, and one
# region-scoped policy. Tweak freely; re-run `gibran sync` after editing.

sources:
  - id: orders
    display_name: Orders
    type: duckdb_table
    uri: orders
    primary_grain: order_id
    columns:
      - name: order_id
        type: VARCHAR
        sensitivity: public
      - name: amount
        type: DECIMAL(18,2)
        sensitivity: public
      - name: order_date
        type: TIMESTAMP
        sensitivity: public
      - name: status
        type: VARCHAR
        sensitivity: public
      - name: region
        type: VARCHAR
        sensitivity: public
    dimensions:
      - id: orders.region
        column: region
        display_name: Region
        type: categorical
      - id: orders.order_date
        column: order_date
        display_name: Order Date
        type: temporal

metrics:
  - id: order_count
    source: orders
    display_name: Order Count
    type: count
  - id: gross_revenue
    source: orders
    display_name: Gross Revenue
    type: sum
    expression: amount
    filter: status = 'paid'
    unit: USD

roles:
  - id: analyst_west
    display_name: West Analyst
    attributes:
      region: west
  - id: admin
    display_name: Admin

policies:
  - id: analyst_west_orders
    role: analyst_west
    source: orders
    default_column_mode: allow
    row_filter:
      op: eq
      column: region
      value:
        $attr: region
  - id: admin_orders
    role: admin
    source: orders
    default_column_mode: allow

quality_rules:
  - id: orders_amount_not_null
    source: orders
    type: not_null
    config:
      column: amount
    severity: warn
"""


def _seed_sample(con: duckdb.DuckDBPyConnection, root: Path) -> None:
    """Write a starter gibran.yaml and create+populate the sample 'orders' table.

    Skips writing the YAML if one already exists (don't clobber user work)."""
    cfg = _config_path(root)
    if not cfg.exists():
        cfg.write_text(_SAMPLE_YAML, encoding="utf-8")
    # The catalog migration tables exist by this point; create the actual
    # data table that the source references. Skip if it already exists so
    # `gibran init --sample` is idempotent.
    exists = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_name = 'orders'"
    ).fetchone()[0]
    if exists:
        return
    con.execute(
        "CREATE TABLE orders ("
        "order_id VARCHAR, amount DECIMAL(18,2), order_date TIMESTAMP, "
        "status VARCHAR, region VARCHAR)"
    )
    con.execute(
        "INSERT INTO orders VALUES "
        "('o1', 100.00, now() - INTERVAL '1 hour',  'paid',    'west'), "
        "('o2', 200.00, now() - INTERVAL '2 hours', 'paid',    'east'), "
        "('o3',  50.00, now() - INTERVAL '3 hours', 'pending', 'west'), "
        "('o4', 300.00, now() - INTERVAL '4 hours', 'paid',    'north')"
    )


@app.command()
def register() -> None:
    """Register a source -- next iteration; use `gibran sync` for now."""
    raise NotImplementedError


@app.command()
def check(
    source: str = typer.Option(
        None, "--source", "-s",
        help="Run rules for a specific source (default: all sources)",
    ),
    watch: bool = typer.Option(
        False, "--watch",
        help="Run repeatedly with --interval seconds between passes",
    ),
    interval: int = typer.Option(
        300, "--interval",
        help="Seconds between passes in --watch mode (default: 300)",
    ),
) -> None:
    """Run all enabled quality + freshness rules and refresh the source-health cache.

    For each rule: executes the underlying check SQL, records pass/fail in
    gibran_quality_runs, and refreshes gibran_source_health. Designed to be run
    on a schedule (cron / CI) -- queries consult the cache on the hot path.

    --watch turns the command into an in-process scheduler that loops on
    --interval seconds. V1 limitation: single-process; for production use
    a real scheduler (cron, systemd timer, k8s CronJob) -- the --watch
    mode is meant for local dev and small deployments where running a
    long-lived `gibran check --watch` is reasonable.

    Exit codes (non-watch mode only): 0 if all rules passed; 1 if any
    rule failed; 2 if any rule errored during evaluation."""
    import time
    from gibran.observability.default import DefaultObservability
    from gibran.observability.runner import run_checks

    root = _project_root()
    db = _db_path(root)
    if not db.exists():
        typer.echo(f"error: no DB at {db}; run `gibran init` first", err=True)
        raise typer.Exit(code=1)

    def _one_pass() -> tuple[int, int]:
        con = duckdb.connect(str(db))
        try:
            if source is None:
                sources = [
                    r[0]
                    for r in con.execute("SELECT source_id FROM gibran_sources").fetchall()
                ]
            else:
                sources = [source]

            obs = DefaultObservability(con)
            total_failed = 0
            total_errored = 0

            for src in sources:
                typer.echo(f"\n=== {src} ===")
                result = run_checks(con, src, obs)
                for r in result.results:
                    if r.error is not None:
                        status_text = "ERROR"
                    elif r.passed:
                        status_text = "PASS"
                    else:
                        status_text = "FAIL"
                    typer.echo(
                        f"  {r.rule_id:35s} {status_text:6s} "
                        f"[{r.rule_kind}/{r.severity}] {r.observed_value}"
                    )
                typer.echo(
                    f"  -- total={result.total} passed={result.passed} "
                    f"failed={result.failed} errored={result.errored}"
                )
                total_failed += result.failed
                total_errored += result.errored
            return total_failed, total_errored
        finally:
            con.close()

    if watch:
        # In-process scheduler: re-run every `interval` seconds.
        # Exits on Ctrl-C only. No retry/backoff -- this is for dev /
        # small deployments, not production reliability.
        while True:
            try:
                _one_pass()
            except Exception as e:
                typer.echo(f"check pass errored: {e}", err=True)
            typer.echo(f"\n[sleeping {interval}s; Ctrl-C to stop]\n")
            try:
                time.sleep(interval)
            except KeyboardInterrupt:
                return
        return

    total_failed, total_errored = _one_pass()
    if total_errored:
        raise typer.Exit(code=2)
    if total_failed:
        raise typer.Exit(code=1)


@app.command("detect-access-anomalies")
def detect_access_anomalies_cmd(
    trailing_days: int = typer.Option(
        14, "--trailing-days",
        help="Days of history to compare today against (default: 14)",
    ),
    n_sigma: float = typer.Option(
        3.0, "--n-sigma",
        help="Z-score threshold for flagging (default: 3.0)",
    ),
) -> None:
    """Scan gibran_query_log for users whose today's query volume is
    > n_sigma above their trailing-day mean. Prints one line per anomaly.

    Exit code 0 with no anomalies; 1 if anomalies found (useful for
    CI / alerting integrations)."""
    from gibran.observability.access_anomaly import detect_access_anomalies

    root = _project_root()
    db = _db_path(root)
    if not db.exists():
        typer.echo(f"error: no DB at {db}", err=True)
        raise typer.Exit(code=1)
    con = duckdb.connect(str(db))
    try:
        anomalies = detect_access_anomalies(
            con, trailing_days=trailing_days, n_sigma=n_sigma,
        )
    finally:
        con.close()
    if not anomalies:
        typer.echo("no access-pattern anomalies detected")
        return
    for a in anomalies:
        typer.echo(
            f"user={a.user_id} role={a.role_id} today={a.today_count} "
            f"mean={a.mean:.1f} stddev={a.stddev:.1f} z={a.z_score:.2f} "
            f"trailing_days={a.trailing_days}"
        )
    raise typer.Exit(code=1)


@app.command()
def approve(
    change_id: str = typer.Argument(...),
    approved_by: str = typer.Option(
        ..., "--by",
        help="Identifier of the approver (recorded in the audit row)",
    ),
) -> None:
    """Approve a pending change submitted via the approval workflow.

    With no change_id, lists outstanding pending changes."""
    from gibran.sync.approval import approve as approve_change

    root = _project_root()
    db = _db_path(root)
    if not db.exists():
        typer.echo(f"error: no DB at {db}", err=True)
        raise typer.Exit(code=1)
    con = duckdb.connect(str(db))
    try:
        change = approve_change(con, change_id, approved_by=approved_by)
    except ValueError as e:
        typer.echo(f"error: {e}", err=True)
        raise typer.Exit(code=1)
    finally:
        con.close()
    typer.echo(
        f"approved {change.change_id} ({change.change_type}) by {approved_by}"
    )
    typer.echo(f"payload: {change.payload}")


@app.command()
def sync(
    config: Path = typer.Option(
        None, "--config", "-c", help="Path to gibran.yaml (default: ./gibran.yaml)"
    ),
    plan: bool = typer.Option(
        False, "--plan", help="Show what would change without applying."
    ),
) -> None:
    """Apply gibran.yaml (sources, columns, dimensions, metrics) to the DB."""
    root = _project_root()
    db = _db_path(root)
    cfg_path = config if config else _config_path(root)
    if not cfg_path.is_file():
        typer.echo(f"error: no config at {cfg_path}", err=True)
        raise typer.Exit(code=1)
    if not db.exists() and not plan:
        typer.echo(f"error: no DB at {db}; run `gibran init` first", err=True)
        raise typer.Exit(code=1)
    validated = load_config(cfg_path)
    if plan:
        c = validated.config
        typer.echo(
            f"would apply: {len(c.sources)} source(s), {len(c.metrics)} metric(s), "
            f"{len(c.roles)} role(s), {len(c.policies)} polic{'y' if len(c.policies) == 1 else 'ies'}, "
            f"{len(c.quality_rules)} quality rule(s), {len(c.freshness_rules)} freshness rule(s)"
        )
        for s in c.sources:
            typer.echo(
                f"  source {s.id}: {len(s.columns)} columns, "
                f"{len(s.dimensions)} dimensions"
            )
        for m in c.metrics:
            typer.echo(f"  metric {m.id} [{m.type}] -> {m.source}")
        for p in c.policies:
            typer.echo(
                f"  policy {p.id}: role={p.role} source={p.source} "
                f"default={p.default_column_mode} overrides={len(p.column_overrides)}"
            )
        for q in c.quality_rules:
            from gibran.sync.loader import resolve_cost_class
            typer.echo(
                f"  quality_rule {q.id}: {q.type} on {q.source} "
                f"[{resolve_cost_class(q)}, {q.severity}]"
            )
        for f in c.freshness_rules:
            typer.echo(
                f"  freshness_rule {f.id}: {f.source}.{f.watermark_column} "
                f"<= {f.max_age_seconds}s [{f.severity}]"
            )
        return
    con = duckdb.connect(str(db))
    try:
        # Drift detection runs BEFORE apply so the warnings reflect the
        # discrepancy between intent (YAML) and reality (the live source).
        # Drift is informational; the apply still proceeds. Sources we
        # can't probe are reported separately as "schema not probable" --
        # not knowing whether there's drift is distinct from there being
        # drift.
        from gibran.sync.drift import detect_drift
        drift_events, unreachable = detect_drift(con, validated.config)
        for e in drift_events:
            typer.echo(e.as_warning(), err=True)
        for u in unreachable:
            typer.echo(
                f"warning: {u.source_id}: schema not probable ({u.reason})",
                err=True,
            )
        counts = apply_config(con, validated)
        # Populate example_values for low-cardinality public columns.
        # Runs AFTER apply so the gibran_columns rows exist to UPDATE.
        # Skipped sources don't fail the sync -- the example-value pass
        # is opportunistic, not a correctness gate.
        from gibran.sync.example_values import populate_example_values
        samples = populate_example_values(con, validated.config)
        sampled_count = sum(1 for r in samples if r.status == "sampled")
        if sampled_count:
            typer.echo(
                f"sampled example_values for {sampled_count} public column(s)"
            )
    finally:
        con.close()
    typer.echo(f"applied: {counts}")


@app.command()
def query(
    sql: str = typer.Argument(
        None, help="SQL query to execute (omit if using --dsl or --dsl-file)"
    ),
    user: str = typer.Option("dev", "--user", help="user_id (recorded to audit log)"),
    role: str = typer.Option(..., "--role", help="role_id (used by governance)"),
    attr: list[str] = typer.Option(
        [], "--attr", help="key=value attribute pair, repeatable"
    ),
    dsl: str = typer.Option(
        None, "--dsl",
        help="DSL query intent as a JSON string (Gibran DSL surface)",
    ),
    dsl_file: str = typer.Option(
        None, "--dsl-file",
        help="Path to a JSON file containing the DSL query intent",
    ),
    output: str = typer.Option(
        "tsv", "--output",
        help="Output format: tsv (default) / csv / json / parquet. "
             "parquet requires --output-file.",
    ),
    output_file: str = typer.Option(
        None, "--output-file",
        help="Write results to this path instead of stdout. Required for "
             "--output parquet; optional for csv / json.",
    ),
) -> None:
    """Execute a governed query.

    Two input shapes:
      Raw SQL:  gibran query --role <r> --attr k=v "<sql>"
      DSL:      gibran query --role <r> --attr k=v --dsl '{...}'
                gibran query --role <r> --attr k=v --dsl-file intent.json

    Output formats:
      tsv      (default) tab-separated, headers on first line, stdout
      csv      comma-separated with headers, stdout or --output-file
      json     array of objects (one per row), stdout or --output-file
      parquet  binary parquet, requires --output-file

    V1 dev-mode: identity comes from --user/--role/--attr (CLIResolver).
    Production paths use JWTResolver.
    """
    import json as _json

    from gibran.dsl.run import run_dsl_query
    from gibran.execution.sql import run_sql_query
    from gibran.governance.default import DefaultGovernance
    from gibran.governance.identity import CLIResolver
    from gibran.observability.default import DefaultObservability

    inputs_provided = sum(x is not None for x in (sql, dsl, dsl_file))
    if inputs_provided == 0:
        typer.echo(
            "error: provide one of: positional SQL, --dsl, or --dsl-file", err=True
        )
        raise typer.Exit(code=1)
    if inputs_provided > 1:
        typer.echo(
            "error: provide exactly one of: positional SQL, --dsl, --dsl-file",
            err=True,
        )
        raise typer.Exit(code=1)

    if output not in ("tsv", "csv", "json", "parquet"):
        typer.echo(
            f"error: --output must be one of tsv/csv/json/parquet, got {output!r}",
            err=True,
        )
        raise typer.Exit(code=1)
    if output == "parquet" and not output_file:
        typer.echo(
            "error: --output parquet requires --output-file (cannot write "
            "binary to stdout)",
            err=True,
        )
        raise typer.Exit(code=1)

    root = _project_root()
    db = _db_path(root)
    if not db.exists():
        typer.echo(f"error: no DB at {db}; run `gibran init` first", err=True)
        raise typer.Exit(code=1)

    attributes: dict[str, str] = {}
    for kv in attr:
        if "=" not in kv:
            typer.echo(f"error: --attr must be key=value, got {kv!r}", err=True)
            raise typer.Exit(code=1)
        k, v = kv.split("=", 1)
        attributes[k] = v

    identity = CLIResolver(
        user_id=user, role_id=role, attributes=attributes
    ).resolve(None)

    raw_intent: dict | None = None
    if dsl is not None:
        try:
            raw_intent = _json.loads(dsl)
        except _json.JSONDecodeError as e:
            typer.echo(f"error: --dsl is not valid JSON: {e}", err=True)
            raise typer.Exit(code=1)
    elif dsl_file is not None:
        try:
            raw_intent = _json.loads(Path(dsl_file).read_text(encoding="utf-8"))
        except (OSError, _json.JSONDecodeError) as e:
            typer.echo(f"error: --dsl-file: {e}", err=True)
            raise typer.Exit(code=1)

    con = duckdb.connect(str(db))
    try:
        obs = DefaultObservability(con)
        gov = DefaultGovernance(con, observability=obs)
        if raw_intent is not None:
            dsl_result = run_dsl_query(con, gov, identity, raw_intent)
            if dsl_result.query_result is None:
                # Pre-compile failure
                typer.echo(
                    f"ERROR: {dsl_result.pre_compile_error} "
                    f"(stage={dsl_result.stage}, query_id={dsl_result.query_id})",
                    err=True,
                )
                raise typer.Exit(code=3)
            result = dsl_result.query_result
        else:
            result = run_sql_query(con, gov, identity, sql)
    finally:
        con.close()

    if result.status == "ok":
        _emit_query_result(result, output, output_file)
        typer.echo(
            f"-- {len(result.rows or ())} row(s) in {result.duration_ms}ms "
            f"(query_id={result.query_id})",
            err=True,
        )
    elif result.status == "denied":
        reason = result.deny_reason.value if result.deny_reason else "denied"
        if result.deny_detail:
            reason += f":{result.deny_detail}"
        typer.echo(
            f"DENIED: {reason} (query_id={result.query_id})", err=True
        )
        raise typer.Exit(code=2)
    else:  # error
        typer.echo(
            f"ERROR: {result.error_message} (query_id={result.query_id})", err=True
        )
        raise typer.Exit(code=3)


def _emit_query_result(result, output: str, output_file: str | None) -> None:
    """Render a successful QueryResult in the requested format.

    For tsv/csv/json: write to stdout if output_file is None, else write
    to the path. For parquet: always write to the path.
    """
    import csv as _csv
    import io as _io
    import json as _json

    columns = list(result.columns or ())
    rows = list(result.rows or ())

    if output == "tsv":
        buf = _io.StringIO()
        if columns:
            buf.write("\t".join(columns) + "\n")
        for row in rows:
            buf.write(
                "\t".join("" if v is None else str(v) for v in row) + "\n"
            )
        _write_or_stdout(buf.getvalue(), output_file)
        return

    if output == "csv":
        buf = _io.StringIO()
        writer = _csv.writer(buf)
        if columns:
            writer.writerow(columns)
        for row in rows:
            writer.writerow(["" if v is None else v for v in row])
        _write_or_stdout(buf.getvalue(), output_file)
        return

    if output == "json":
        records = [
            {col: _json_safe(v) for col, v in zip(columns, row)}
            for row in rows
        ]
        payload = _json.dumps(records, indent=2, default=str)
        _write_or_stdout(payload + "\n", output_file)
        return

    if output == "parquet":
        # Use DuckDB to write the parquet, since we already have it nearby
        # and the result set is already in tuples. Smallest dep surface.
        import duckdb as _duckdb

        con = _duckdb.connect(":memory:")
        try:
            # Build a CREATE TABLE that infers types from the first non-null
            # value in each column. For empty result sets we fall back to
            # VARCHAR -- a degenerate but harmless choice.
            if not rows:
                con.execute(
                    "CREATE TABLE t (" +
                    ", ".join(f'"{c}" VARCHAR' for c in columns) + ")"
                )
            else:
                # Let DuckDB infer types via a VALUES expression.
                con.execute("CREATE TABLE t AS SELECT * FROM (VALUES " + _values_clause(rows) + ") AS t(" + ", ".join(f'"{c}"' for c in columns) + ")")
            con.execute(
                f"COPY t TO '{output_file}' (FORMAT PARQUET)"
            )
        finally:
            con.close()
        return


def _values_clause(rows: list[tuple]) -> str:
    """Render rows as a SQL VALUES tail: `(?,?,?),(?,?,?)`.

    Inlines literals via render_literal -- safe because all values came
    out of DuckDB itself (no user-controlled string interpolation here).
    """
    from gibran._sql import render_literal as _rl

    parts = []
    for row in rows:
        rendered = []
        for v in row:
            if isinstance(v, (str, int, float, bool)) or v is None:
                rendered.append(_rl(v))
            else:
                rendered.append(_rl(str(v)))
        parts.append("(" + ", ".join(rendered) + ")")
    return ", ".join(parts)


def _json_safe(v):
    """Coerce DuckDB row values to JSON-serializable shapes."""
    import datetime as _dt
    import decimal as _dec

    if isinstance(v, (_dt.date, _dt.datetime, _dt.time, _dec.Decimal)):
        return str(v)
    return v


def _write_or_stdout(text: str, output_file: str | None) -> None:
    if output_file is None:
        typer.echo(text, nl=False)
    else:
        # Use binary write so embedded newlines (especially CSV's \r\n)
        # round-trip exactly, without Windows's text-mode translation
        # turning \r\n into \r\r\n. Tests + downstream tools can rely on
        # the exact bytes written.
        Path(output_file).write_bytes(text.encode("utf-8"))


# ---------------------------------------------------------------------------
# Introspection: explain / describe / catalog
# ---------------------------------------------------------------------------

@app.command()
def explain(
    user: str = typer.Option("dev", "--user", help="user_id"),
    role: str = typer.Option(..., "--role", help="role_id"),
    attr: list[str] = typer.Option(
        [], "--attr", help="key=value attribute pair, repeatable"
    ),
    dsl: str = typer.Option(
        None, "--dsl",
        help="DSL query intent as JSON; parsed + validated + compiled WITHOUT executing",
    ),
    dsl_file: str = typer.Option(
        None, "--dsl-file", help="DSL query intent JSON file",
    ),
) -> None:
    """Parse + validate + compile a DSL intent without executing it.

    Prints:
      - the compiled SQL
      - a governance decision summary (allowed columns, injected filter,
        deny reason if any)

    Useful for preview / sandbox / impact analysis. Does NOT write to the
    audit log (no execution attempt was made)."""
    import json as _json

    from gibran.dsl.compile import Catalog, CompileError, compile_intent
    from gibran.dsl.types import QueryIntent
    from gibran.dsl.validate import IntentValidationError, validate_intent
    from gibran.governance.default import DefaultGovernance
    from gibran.governance.identity import CLIResolver
    from gibran.observability.default import DefaultObservability
    from pydantic import ValidationError

    if (dsl is None) == (dsl_file is None):
        typer.echo("error: provide exactly one of --dsl or --dsl-file", err=True)
        raise typer.Exit(code=1)

    root = _project_root()
    db = _db_path(root)
    if not db.exists():
        typer.echo(f"error: no DB at {db}; run `gibran init` first", err=True)
        raise typer.Exit(code=1)

    if dsl is not None:
        try:
            raw_intent = _json.loads(dsl)
        except _json.JSONDecodeError as e:
            typer.echo(f"error: --dsl is not valid JSON: {e}", err=True)
            raise typer.Exit(code=1)
    else:
        try:
            raw_intent = _json.loads(Path(dsl_file).read_text(encoding="utf-8"))
        except (OSError, _json.JSONDecodeError) as e:
            typer.echo(f"error: --dsl-file: {e}", err=True)
            raise typer.Exit(code=1)

    attributes = _parse_attrs(attr)
    identity = CLIResolver(
        user_id=user, role_id=role, attributes=attributes
    ).resolve(None)

    con = duckdb.connect(str(db))
    try:
        obs = DefaultObservability(con)
        gov = DefaultGovernance(con, observability=obs)

        try:
            intent = QueryIntent.model_validate(raw_intent)
        except ValidationError as e:
            typer.echo(f"ERROR: intent_parse: {e}", err=True)
            raise typer.Exit(code=3)

        try:
            schema = gov.preview_schema(identity, intent.source)
        except ValueError as e:
            typer.echo(f"ERROR: unknown_source: {e}", err=True)
            raise typer.Exit(code=3)

        try:
            validate_intent(intent, schema, con=con)
        except IntentValidationError as e:
            typer.echo(f"ERROR: intent_invalid: {e}", err=True)
            raise typer.Exit(code=3)

        try:
            sql = compile_intent(intent, Catalog(con)).render()
        except CompileError as e:
            typer.echo(f"ERROR: compile_failed: {e}", err=True)
            raise typer.Exit(code=3)

        # Now consult governance with the columns + metrics the compiled
        # query would read. This mirrors execution.run_sql_query's evaluate
        # call but with parsed columns from the compiled SQL.
        from gibran.execution.sql import _parse_for_governance

        source_id, requested_columns = _parse_for_governance(sql)
        decision = gov.evaluate(
            identity,
            frozenset({source_id}),
            frozenset(requested_columns),
            tuple(intent.metrics),
        )

        typer.echo("-- Compiled SQL --")
        typer.echo(sql)
        typer.echo("")
        typer.echo("-- Governance Decision --")
        typer.echo(f"allowed:           {decision.allowed}")
        if decision.deny_reason is not None:
            typer.echo(
                f"deny_reason:       {decision.deny_reason.value}"
                + (f":{decision.deny_detail}" if decision.deny_detail else "")
            )
        typer.echo(
            f"column_allowlist:  {sorted(decision.column_allowlist)}"
        )
        typer.echo(
            f"requested_columns: {sorted(requested_columns)}"
        )
        if decision.injected_filter_sql:
            typer.echo(
                f"injected_filter:   {decision.injected_filter_sql}"
            )
        if decision.metric_versions:
            typer.echo(
                "metric_versions:   "
                + ", ".join(f"{mid}@v{ver}" for mid, ver in decision.metric_versions)
            )
        if decision.quality_holds:
            typer.echo(
                f"quality_holds:     {list(decision.quality_holds)}"
            )
    finally:
        con.close()


@app.command()
def describe(
    source: str = typer.Argument(..., help="source_id to describe"),
    user: str = typer.Option("dev", "--user", help="user_id"),
    role: str = typer.Option(..., "--role", help="role_id"),
    attr: list[str] = typer.Option(
        [], "--attr", help="key=value attribute pair, repeatable"
    ),
) -> None:
    """Show the AllowedSchema for a source under this identity.

    Prints columns (with sensitivity), dimensions, metrics, and the row
    filter that would be applied. Useful for first-five-minutes UX: 'what
    can I see and how is it being governed?'"""
    from gibran.governance.default import DefaultGovernance
    from gibran.governance.identity import CLIResolver
    from gibran.observability.default import DefaultObservability

    root = _project_root()
    db = _db_path(root)
    if not db.exists():
        typer.echo(f"error: no DB at {db}; run `gibran init` first", err=True)
        raise typer.Exit(code=1)

    attributes = _parse_attrs(attr)
    identity = CLIResolver(
        user_id=user, role_id=role, attributes=attributes,
    ).resolve(None)

    con = duckdb.connect(str(db))
    try:
        obs = DefaultObservability(con)
        gov = DefaultGovernance(con, observability=obs)
        try:
            schema = gov.preview_schema(identity, source)
        except ValueError as e:
            typer.echo(f"error: {e}", err=True)
            raise typer.Exit(code=1)

        # Row filter is on the policy, not the AllowedSchema. Look it up
        # directly so we can show what filter would be applied.
        policy_row = con.execute(
            "SELECT policy_id, row_filter_ast, default_column_mode "
            "FROM gibran_policies WHERE role_id = ? AND source_id = ?",
            [identity.role_id, source],
        ).fetchone()

        typer.echo(f"source: {schema.source_id} ({schema.source_display_name})")
        typer.echo(f"role:   {identity.role_id}")
        if policy_row is None:
            typer.echo("policy: NONE (no access for this role)")
            return
        policy_id, row_filter_ast, default_mode = policy_row
        typer.echo(
            f"policy: {policy_id} "
            f"(default_column_mode={default_mode})"
        )

        if not schema.columns:
            typer.echo("\ncolumns: (none accessible)")
        else:
            typer.echo("\ncolumns:")
            for c in schema.columns:
                desc = f" -- {c.description}" if c.description else ""
                typer.echo(
                    f"  {c.name:30s} {c.data_type:20s} "
                    f"[{c.sensitivity}]{desc}"
                )

        if not schema.dimensions:
            typer.echo("\ndimensions: (none)")
        else:
            typer.echo("\ndimensions:")
            for d in schema.dimensions:
                desc = f" -- {d.description}" if d.description else ""
                typer.echo(
                    f"  {d.dimension_id:30s} -> {d.column_name:20s} "
                    f"[{d.dim_type}]{desc}"
                )

        if not schema.metrics:
            typer.echo("\nmetrics: (none)")
        else:
            typer.echo("\nmetrics:")
            for m in schema.metrics:
                unit = f" {m.unit}" if m.unit else ""
                deps = f" depends_on={list(m.depends_on)}" if m.depends_on else ""
                desc = f" -- {m.description}" if m.description else ""
                typer.echo(
                    f"  {m.metric_id:30s} [{m.metric_type}]{unit}{deps}{desc}"
                )

        if row_filter_ast:
            typer.echo(f"\nrow_filter (raw AST): {row_filter_ast}")
        else:
            typer.echo("\nrow_filter: (none)")
    finally:
        con.close()


@app.command()
def catalog(
    user: str = typer.Option("dev", "--user", help="user_id"),
    role: str = typer.Option(..., "--role", help="role_id"),
    attr: list[str] = typer.Option(
        [], "--attr", help="key=value attribute pair, repeatable"
    ),
) -> None:
    """List sources accessible to this identity.

    Shows column / dimension / metric counts per source. A source is
    'accessible' if the role has any policy covering it (the policy may
    still deny individual columns)."""
    from gibran.governance.default import DefaultGovernance
    from gibran.governance.identity import CLIResolver
    from gibran.observability.default import DefaultObservability

    root = _project_root()
    db = _db_path(root)
    if not db.exists():
        typer.echo(f"error: no DB at {db}; run `gibran init` first", err=True)
        raise typer.Exit(code=1)

    attributes = _parse_attrs(attr)
    identity = CLIResolver(
        user_id=user, role_id=role, attributes=attributes,
    ).resolve(None)

    con = duckdb.connect(str(db))
    try:
        obs = DefaultObservability(con)
        gov = DefaultGovernance(con, observability=obs)
        source_rows = con.execute(
            "SELECT s.source_id "
            "FROM gibran_sources s "
            "JOIN gibran_policies p ON p.source_id = s.source_id "
            "WHERE p.role_id = ? "
            "ORDER BY s.source_id",
            [identity.role_id],
        ).fetchall()
        if not source_rows:
            typer.echo(f"no sources accessible to role {identity.role_id!r}")
            return

        typer.echo(f"role: {identity.role_id}  ({len(source_rows)} source(s))")
        for (sid,) in source_rows:
            schema = gov.preview_schema(identity, sid)
            typer.echo(
                f"  {sid:30s} "
                f"columns={len(schema.columns):3d} "
                f"dimensions={len(schema.dimensions):3d} "
                f"metrics={len(schema.metrics):3d}"
            )
    finally:
        con.close()


def _parse_attrs(attr: list[str]) -> dict[str, str]:
    attributes: dict[str, str] = {}
    for kv in attr:
        if "=" not in kv:
            typer.echo(f"error: --attr must be key=value, got {kv!r}", err=True)
            raise typer.Exit(code=1)
        k, v = kv.split("=", 1)
        attributes[k] = v
    return attributes


@app.command()
def ask(
    text: str = typer.Argument(
        ..., help='Natural-language question, e.g. "show me gross revenue by region"'
    ),
    source: str = typer.Option(
        ..., "--source", "-s",
        help="Source to query (the NL layer scopes to one source per request)",
    ),
    role: str = typer.Option(..., "--role", "-r"),
    attr: list[str] = typer.Option(
        [], "--attr", help="role attribute, e.g. region=west",
    ),
) -> None:
    """Pattern-template NL layer: match the question against a fixed set
    of templates, resolve slots against the role's AllowedSchema, and
    execute. No invention -- if no pattern matches AND resolves, the
    command prints "I don't know how to answer that" and exits with
    code 4. This is the Tier 5 non-LLM design (per ROADMAP.md): an
    NL layer that can FAIL to parse but cannot fabricate references."""
    from gibran.governance.default import DefaultGovernance
    from gibran.governance.types import IdentityContext
    from gibran.nl.runner import run_nl_query

    root = _project_root()
    db = _db_path(root)
    if not db.exists():
        typer.echo(f"error: no DB at {db}", err=True)
        raise typer.Exit(code=1)

    identity = IdentityContext(
        user_id=f"cli:{role}", role_id=role,
        attributes=_parse_attrs(attr), source="cli",
    )
    con = duckdb.connect(str(db))
    try:
        gov = DefaultGovernance(con)
        result = run_nl_query(con, gov, identity, text, source)
    finally:
        con.close()

    if result.match is None:
        typer.echo("I don't know how to answer that.", err=True)
        typer.echo(
            "(The NL layer matches a fixed set of patterns; rephrase or "
            "use `gibran query --dsl` directly.)",
            err=True,
        )
        raise typer.Exit(code=4)

    typer.echo(f"-- Pattern: {result.match.pattern_name} --")
    import json as _json
    typer.echo(f"-- DSL Intent --")
    typer.echo(_json.dumps(result.match.intent, indent=2))
    qr = result.run_result.query_result if result.run_result else None
    if qr is None or qr.status != "ok":
        typer.echo(
            f"-- Execution failed: "
            f"{getattr(qr, 'deny_reason', None) or getattr(qr, 'error_message', None)}",
            err=True,
        )
        raise typer.Exit(code=2)
    typer.echo("-- Result --")
    if qr.columns:
        typer.echo("\t".join(qr.columns))
    for row in (qr.rows or ()):
        typer.echo("\t".join(str(v) for v in row))


if __name__ == "__main__":
    app()
