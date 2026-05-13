from __future__ import annotations

from pathlib import Path

try:
    import typer
except ImportError as e:
    raise SystemExit(
        "rumi requires `typer`; install with `pip install -e .[dev]`"
    ) from e

import duckdb

from rumi.sync.applier import apply as apply_config
from rumi.sync.loader import load as load_config
from rumi.sync.migrations import apply_all as apply_migrations


app = typer.Typer(
    no_args_is_help=True,
    help="Governed analytics + NL-to-SQL over DuckDB.",
)


def _project_root() -> Path:
    return Path.cwd()


def _db_path(root: Path) -> Path:
    return root / "rumi.duckdb"


def _migrations_dir(root: Path) -> Path:
    return root / "migrations"


def _config_path(root: Path) -> Path:
    return root / "rumi.yaml"


@app.command()
def init() -> None:
    """Create rumi.duckdb in CWD and apply all migrations."""
    root = _project_root()
    db = _db_path(root)
    migrations = _migrations_dir(root)
    if not migrations.is_dir():
        typer.echo(f"error: no migrations dir at {migrations}", err=True)
        raise typer.Exit(code=1)
    con = duckdb.connect(str(db))
    try:
        applied = apply_migrations(con, migrations)
    finally:
        con.close()
    if applied:
        typer.echo(f"applied migrations: {applied}")
    else:
        typer.echo("no migrations to apply (already up to date)")


@app.command()
def register() -> None:
    """Register a source -- next iteration; use `rumi sync` for now."""
    raise NotImplementedError


@app.command()
def check(
    source: str = typer.Option(
        None, "--source", "-s",
        help="Run rules for a specific source (default: all sources)",
    ),
) -> None:
    """Run all enabled quality + freshness rules and refresh the source-health cache.

    For each rule: executes the underlying check SQL, records pass/fail in
    rumi_quality_runs, and refreshes rumi_source_health. Designed to be run
    on a schedule (cron / CI) -- queries consult the cache on the hot path.

    V1 assumption: source_id is also the DuckDB relation name (table/view).
    For parquet/csv sources, register them as views before running.

    Exit codes: 0 if all rules passed; 1 if any rule failed; 2 if any
    rule errored during evaluation."""
    from rumi.observability.default import DefaultObservability
    from rumi.observability.runner import run_checks

    root = _project_root()
    db = _db_path(root)
    if not db.exists():
        typer.echo(f"error: no DB at {db}; run `rumi init` first", err=True)
        raise typer.Exit(code=1)

    con = duckdb.connect(str(db))
    try:
        if source is None:
            sources = [
                r[0]
                for r in con.execute("SELECT source_id FROM rumi_sources").fetchall()
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
    finally:
        con.close()

    if total_errored:
        raise typer.Exit(code=2)
    if total_failed:
        raise typer.Exit(code=1)


@app.command()
def sync(
    config: Path = typer.Option(
        None, "--config", "-c", help="Path to rumi.yaml (default: ./rumi.yaml)"
    ),
    plan: bool = typer.Option(
        False, "--plan", help="Show what would change without applying."
    ),
) -> None:
    """Apply rumi.yaml (sources, columns, dimensions, metrics) to the DB."""
    root = _project_root()
    db = _db_path(root)
    cfg_path = config if config else _config_path(root)
    if not cfg_path.is_file():
        typer.echo(f"error: no config at {cfg_path}", err=True)
        raise typer.Exit(code=1)
    if not db.exists() and not plan:
        typer.echo(f"error: no DB at {db}; run `rumi init` first", err=True)
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
            from rumi.sync.loader import resolve_cost_class
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
        counts = apply_config(con, validated)
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
        help="DSL query intent as a JSON string (Rumi DSL surface)",
    ),
    dsl_file: str = typer.Option(
        None, "--dsl-file",
        help="Path to a JSON file containing the DSL query intent",
    ),
) -> None:
    """Execute a governed query.

    Two input shapes:
      Raw SQL:  rumi query --role <r> --attr k=v "<sql>"
      DSL:      rumi query --role <r> --attr k=v --dsl '{...}'
                rumi query --role <r> --attr k=v --dsl-file intent.json

    V1 dev-mode: identity comes from --user/--role/--attr (CLIResolver).
    Production paths use JWTResolver.
    """
    import json as _json

    from rumi.dsl.run import run_dsl_query
    from rumi.execution.sql import run_sql_query
    from rumi.governance.default import DefaultGovernance
    from rumi.governance.identity import CLIResolver
    from rumi.observability.default import DefaultObservability

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

    root = _project_root()
    db = _db_path(root)
    if not db.exists():
        typer.echo(f"error: no DB at {db}; run `rumi init` first", err=True)
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
        if result.columns:
            typer.echo("\t".join(result.columns))
        for row in result.rows or ():
            typer.echo("\t".join(str(v) if v is not None else "" for v in row))
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


if __name__ == "__main__":
    app()
