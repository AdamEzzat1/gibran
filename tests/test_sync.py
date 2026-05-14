import json
from pathlib import Path

import duckdb
import pytest

from gibran.sync.applier import apply as apply_config
from gibran.sync.loader import (
    ConfigValidationError,
    load as load_config,
    resolve_cost_class,
)
from gibran.sync.migrations import apply_all as apply_migrations
from gibran.sync.yaml_schema import QualityRuleConfig


FIXTURES = Path(__file__).parent / "fixtures"
MIGRATIONS = Path(__file__).parent.parent / "migrations"


def _migrated_db() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(":memory:")
    apply_migrations(con, MIGRATIONS)
    return con


def test_loads_valid_yaml() -> None:
    validated = load_config(FIXTURES / "gibran.yaml")
    assert {s.id for s in validated.config.sources} == {"orders"}
    assert {m.id for m in validated.config.metrics} == {
        "order_count",
        "gross_revenue",
        "avg_order_value",
        "revenue_per_paid_order",
        "p95_amount",
        "revenue_7d_rolling",
        "revenue_mom",
        "customer_retention",
        "paid_funnel",
        # Phase 3 NL fixture additions:
        "unique_customers",
        "avg_amount",
        "max_amount",
        "min_amount",
        "median_amount",
        "first_amount",
        "last_amount",
        # Phase 3 comparison-routing addition:
        "revenue_yoy",
        # Phase 3 cohort_filter primitive addition:
        "jan_to_feb_returners",
        # Phase 3 anomaly_query primitive addition:
        "revenue_anomalies",
    }
    assert validated.metric_dependencies["avg_order_value"] == frozenset(
        {"order_count", "gross_revenue"}
    )
    assert validated.metric_dependencies["order_count"] == frozenset()
    assert validated.metric_dependencies["gross_revenue"] == frozenset()
    # period_over_period declares its base_metric as a dependency.
    assert validated.metric_dependencies["revenue_mom"] == frozenset(
        {"gross_revenue"}
    )
    assert {r.id for r in validated.config.roles} == {"analyst_west", "external_partner"}
    assert {p.id for p in validated.config.policies} == {
        "analyst_west_orders",
        "external_partner_orders",
    }
    assert {q.id for q in validated.config.quality_rules} == {
        "orders_amount_not_null",
        "orders_amount_range",
        "orders_revenue_anomaly",
    }
    assert {f.id for f in validated.config.freshness_rules} == {"orders_freshness_24h"}


def test_apply_populates_catalog_and_governance() -> None:
    con = _migrated_db()
    validated = load_config(FIXTURES / "gibran.yaml")
    counts = apply_config(con, validated)

    assert counts == {
        "sources": 1, "columns": 6, "dimensions": 2, "metrics": 19,
        "roles": 2, "policies": 2, "quality_rules": 3, "freshness_rules": 1,
    }

    assert [r[0] for r in con.execute("SELECT source_id FROM gibran_sources").fetchall()] == ["orders"]
    assert sorted(
        r[0] for r in con.execute("SELECT metric_id FROM gibran_metrics").fetchall()
    ) == [
        "avg_amount", "avg_order_value", "customer_retention", "first_amount",
        "gross_revenue", "jan_to_feb_returners",
        "last_amount", "max_amount", "median_amount", "min_amount",
        "order_count", "p95_amount", "paid_funnel", "revenue_7d_rolling",
        "revenue_anomalies",
        "revenue_mom", "revenue_per_paid_order", "revenue_yoy", "unique_customers",
    ]

    deps = {
        (r[0], r[1])
        for r in con.execute(
            "SELECT metric_id, depends_on_id FROM gibran_metric_dependencies"
        ).fetchall()
    }
    assert deps == {
        ("avg_order_value", "order_count"),
        ("avg_order_value", "gross_revenue"),
        ("revenue_mom", "gross_revenue"),
        ("revenue_yoy", "gross_revenue"),
    }

    versions = {
        r[0]: r[1]
        for r in con.execute(
            "SELECT metric_id, expression FROM gibran_metric_versions "
            "WHERE effective_to IS NULL"
        ).fetchall()
    }
    assert versions["order_count"] == "COUNT(*)"
    assert versions["gross_revenue"] == "SUM(amount)"
    assert versions["avg_order_value"] == "{gross_revenue}/{order_count}"

    pii = con.execute(
        "SELECT sensitivity FROM gibran_columns "
        "WHERE source_id = 'orders' AND column_name = 'customer_email'"
    ).fetchone()[0]
    assert pii == "pii"


def test_apply_idempotent() -> None:
    con = _migrated_db()
    validated = load_config(FIXTURES / "gibran.yaml")
    apply_config(con, validated)
    apply_config(con, validated)
    assert con.execute("SELECT COUNT(*) FROM gibran_metric_versions").fetchone()[0] == 19
    assert con.execute("SELECT COUNT(*) FROM gibran_metrics").fetchone()[0] == 19
    # avg_order_value (ratio) declares 2 deps; revenue_mom (period_over_period)
    # declares 1 dep on its base_metric (gross_revenue); revenue_per_paid_order
    # (expression) deps are not tracked in V1 (loader only extracts deps for
    # ratio and period_over_period metrics). revenue_yoy adds 1 more pop dep.
    assert con.execute("SELECT COUNT(*) FROM gibran_metric_dependencies").fetchone()[0] == 4
    assert con.execute("SELECT COUNT(*) FROM gibran_roles").fetchone()[0] == 2
    assert con.execute("SELECT COUNT(*) FROM gibran_role_attributes").fetchone()[0] == 1
    assert con.execute("SELECT COUNT(*) FROM gibran_policies").fetchone()[0] == 2
    assert con.execute("SELECT COUNT(*) FROM gibran_policy_columns").fetchone()[0] == 3
    assert con.execute("SELECT COUNT(*) FROM gibran_quality_rules").fetchone()[0] == 3
    assert con.execute("SELECT COUNT(*) FROM gibran_freshness_rules").fetchone()[0] == 1


def test_apply_bumps_metric_version_on_expression_change(tmp_path: Path) -> None:
    con = _migrated_db()
    yaml_path = tmp_path / "gibran.yaml"
    yaml_path.write_text(
        (FIXTURES / "gibran.yaml").read_text(encoding="utf-8"), encoding="utf-8"
    )
    apply_config(con, load_config(yaml_path))

    text = yaml_path.read_text(encoding="utf-8")
    text = text.replace("expression: amount\n    filter:", "expression: amount * 1.0\n    filter:")
    yaml_path.write_text(text, encoding="utf-8")
    apply_config(con, load_config(yaml_path))

    versions = con.execute(
        "SELECT version, expression, effective_to FROM gibran_metric_versions "
        "WHERE metric_id = 'gross_revenue' ORDER BY version"
    ).fetchall()
    assert len(versions) == 2
    assert versions[0][2] is not None
    assert versions[1][2] is None
    assert versions[1][1] == "SUM(amount * 1.0)"
    current_v = con.execute(
        "SELECT current_version FROM gibran_metrics WHERE metric_id = 'gross_revenue'"
    ).fetchone()[0]
    assert current_v == 2


class TestGovernanceEntities:
    def test_roles_and_attributes_persist(self) -> None:
        con = _migrated_db()
        apply_config(con, load_config(FIXTURES / "gibran.yaml"))

        roles = {
            r[0]: r[1]
            for r in con.execute(
                "SELECT role_id, display_name FROM gibran_roles"
            ).fetchall()
        }
        assert roles == {
            "analyst_west": "West Region Analyst",
            "external_partner": "External Partner",
        }

        attrs = {
            (r[0], r[1]): r[2]
            for r in con.execute(
                "SELECT role_id, attribute_key, attribute_value FROM gibran_role_attributes"
            ).fetchall()
        }
        assert attrs == {("analyst_west", "region"): "west"}

    def test_policies_persist_with_row_filter_as_json(self) -> None:
        con = _migrated_db()
        apply_config(con, load_config(FIXTURES / "gibran.yaml"))

        analyst = con.execute(
            "SELECT role_id, source_id, default_column_mode, "
            "CAST(row_filter_ast AS VARCHAR) FROM gibran_policies "
            "WHERE policy_id = 'analyst_west_orders'"
        ).fetchone()
        assert analyst[0] == "analyst_west"
        assert analyst[1] == "orders"
        assert analyst[2] == "allow"
        ast = json.loads(analyst[3])
        assert ast == {"op": "eq", "column": "region", "value": {"$attr": "region"}}

    def test_policy_column_overrides_persist(self) -> None:
        con = _migrated_db()
        apply_config(con, load_config(FIXTURES / "gibran.yaml"))
        rows = con.execute(
            "SELECT column_name, granted FROM gibran_policy_columns "
            "WHERE policy_id = 'external_partner_orders' "
            "ORDER BY column_name"
        ).fetchall()
        assert rows == [
            ("amount", True),
            ("order_date", True),
            ("order_id", True),
        ]

    def test_policy_valid_until_defaults_to_null(self) -> None:
        # The fixture YAML doesn't set valid_until on any policy.
        con = _migrated_db()
        apply_config(con, load_config(FIXTURES / "gibran.yaml"))
        rows = con.execute(
            "SELECT policy_id, valid_until FROM gibran_policies ORDER BY policy_id"
        ).fetchall()
        for _policy_id, valid_until in rows:
            assert valid_until is None

    def test_policy_valid_until_round_trips(self, tmp_path: Path) -> None:
        # Author a YAML with one valid_until-bearing policy and confirm it
        # round-trips into gibran_policies as a naive datetime.
        from datetime import datetime

        con = _migrated_db()
        yaml_path = tmp_path / "gibran.yaml"
        yaml_path.write_text(
            (FIXTURES / "gibran.yaml").read_text(encoding="utf-8"), encoding="utf-8"
        )
        text = yaml_path.read_text(encoding="utf-8")
        # Inject valid_until on the analyst_west_orders policy.
        text = text.replace(
            "    default_column_mode: allow\n    row_filter:",
            "    default_column_mode: allow\n"
            "    valid_until: '2027-01-01T00:00:00'\n"
            "    row_filter:",
        )
        yaml_path.write_text(text, encoding="utf-8")
        apply_config(con, load_config(yaml_path))

        stored = con.execute(
            "SELECT valid_until FROM gibran_policies WHERE policy_id = 'analyst_west_orders'"
        ).fetchone()[0]
        assert stored == datetime(2027, 1, 1, 0, 0, 0)

    def test_policy_valid_until_resync_no_spurious_update(self, tmp_path: Path) -> None:
        # Re-applying the same config (including a tz-aware valid_until) must
        # not flap the row. We normalize tz-aware to naive UTC at write time
        # so the change-detection tuple round-trips equal.
        con = _migrated_db()
        yaml_path = tmp_path / "gibran.yaml"
        yaml_path.write_text(
            (FIXTURES / "gibran.yaml").read_text(encoding="utf-8"), encoding="utf-8"
        )
        text = yaml_path.read_text(encoding="utf-8").replace(
            "    default_column_mode: allow\n    row_filter:",
            "    default_column_mode: allow\n"
            "    valid_until: '2027-01-01T00:00:00Z'\n"
            "    row_filter:",
        )
        yaml_path.write_text(text, encoding="utf-8")
        apply_config(con, load_config(yaml_path))
        first = con.execute(
            "SELECT valid_until FROM gibran_policies WHERE policy_id = 'analyst_west_orders'"
        ).fetchone()[0]
        # Re-apply -- value should be identical, no UPDATE needed.
        apply_config(con, load_config(yaml_path))
        second = con.execute(
            "SELECT valid_until FROM gibran_policies WHERE policy_id = 'analyst_west_orders'"
        ).fetchone()[0]
        assert first == second
        assert first.tzinfo is None  # stored as naive UTC

    def test_quality_rules_resolve_cost_class_by_type(self) -> None:
        con = _migrated_db()
        apply_config(con, load_config(FIXTURES / "gibran.yaml"))
        rules = {
            r[0]: r[1]
            for r in con.execute(
                "SELECT rule_id, cost_class FROM gibran_quality_rules"
            ).fetchall()
        }
        assert rules == {
            "orders_amount_not_null": "cheap",
            "orders_amount_range": "expensive",
            "orders_revenue_anomaly": "expensive",
        }

    def test_freshness_rule_persists(self) -> None:
        con = _migrated_db()
        apply_config(con, load_config(FIXTURES / "gibran.yaml"))
        row = con.execute(
            "SELECT source_id, watermark_column, max_age_seconds, severity "
            "FROM gibran_freshness_rules WHERE rule_id = 'orders_freshness_24h'"
        ).fetchone()
        assert row == ("orders", "order_date", 86400, "block")

    def test_role_attributes_replaced_on_resync(self, tmp_path: Path) -> None:
        con = _migrated_db()
        yaml_path = tmp_path / "gibran.yaml"
        yaml_path.write_text(
            (FIXTURES / "gibran.yaml").read_text(encoding="utf-8"), encoding="utf-8"
        )
        apply_config(con, load_config(yaml_path))

        text = yaml_path.read_text(encoding="utf-8").replace(
            "      region: west", "      region: east"
        )
        yaml_path.write_text(text, encoding="utf-8")
        apply_config(con, load_config(yaml_path))

        attrs = con.execute(
            "SELECT attribute_value FROM gibran_role_attributes "
            "WHERE role_id = 'analyst_west' AND attribute_key = 'region'"
        ).fetchone()
        assert attrs == ("east",)
        assert con.execute(
            "SELECT COUNT(*) FROM gibran_role_attributes WHERE role_id = 'analyst_west'"
        ).fetchone()[0] == 1

    def test_quality_rule_removed_from_yaml_disappears(self, tmp_path: Path) -> None:
        con = _migrated_db()
        yaml_path = tmp_path / "gibran.yaml"
        yaml_path.write_text(
            (FIXTURES / "gibran.yaml").read_text(encoding="utf-8"), encoding="utf-8"
        )
        apply_config(con, load_config(yaml_path))
        assert con.execute("SELECT COUNT(*) FROM gibran_quality_rules").fetchone()[0] == 3

        text = yaml_path.read_text(encoding="utf-8")
        # Drop everything from orders_amount_range onwards (which also drops
        # orders_revenue_anomaly added in Phase 3). Leaves only the first
        # rule, orders_amount_not_null.
        cut = text.split("  - id: orders_amount_range")[0]
        # Need to keep everything before, and the freshness_rules block
        rest = text.split("freshness_rules:")[1]
        new = cut + "freshness_rules:" + rest
        yaml_path.write_text(new, encoding="utf-8")
        apply_config(con, load_config(yaml_path))

        rules = {
            r[0]
            for r in con.execute(
                "SELECT rule_id FROM gibran_quality_rules"
            ).fetchall()
        }
        assert rules == {"orders_amount_not_null"}


class TestCostClassResolution:
    def test_not_null_defaults_cheap(self) -> None:
        rule = QualityRuleConfig(
            id="r", source="s", type="not_null",
            config={"column": "c"}, severity="warn",
        )
        assert resolve_cost_class(rule) == "cheap"

    def test_unique_defaults_cheap(self) -> None:
        rule = QualityRuleConfig(
            id="r", source="s", type="unique",
            config={"column": "c"}, severity="warn",
        )
        assert resolve_cost_class(rule) == "cheap"

    def test_range_defaults_expensive(self) -> None:
        rule = QualityRuleConfig(
            id="r", source="s", type="range",
            config={"column": "c", "min": 0}, severity="warn",
        )
        assert resolve_cost_class(rule) == "expensive"

    def test_custom_sql_defaults_expensive(self) -> None:
        rule = QualityRuleConfig(
            id="r", source="s", type="custom_sql",
            config={"sql": "SELECT 1"}, severity="warn",
        )
        assert resolve_cost_class(rule) == "expensive"

    def test_explicit_value_overrides_default(self) -> None:
        rule = QualityRuleConfig(
            id="r", source="s", type="not_null",
            config={"column": "c"}, severity="warn",
            cost_class="expensive",
        )
        assert resolve_cost_class(rule) == "expensive"


class TestLoaderRejections:
    def test_rejects_unknown_source_in_metric(self, tmp_path: Path) -> None:
        f = tmp_path / "bad.yaml"
        f.write_text(
            "metrics:\n"
            "  - id: bad\n"
            "    source: nope\n"
            "    display_name: Bad\n"
            "    type: count\n",
            encoding="utf-8",
        )
        with pytest.raises(ConfigValidationError, match="unknown source"):
            load_config(f)

    def test_rejects_dependency_cycle(self, tmp_path: Path) -> None:
        f = tmp_path / "cycle.yaml"
        f.write_text(
            "sources:\n"
            "  - id: s\n"
            "    display_name: S\n"
            "    type: parquet\n"
            "    uri: x.parquet\n"
            "metrics:\n"
            "  - id: c\n"
            "    source: s\n"
            "    display_name: C\n"
            "    type: count\n"
            "  - id: a\n"
            "    source: s\n"
            "    display_name: A\n"
            "    type: ratio\n"
            "    numerator: b\n"
            "    denominator: c\n"
            "  - id: b\n"
            "    source: s\n"
            "    display_name: B\n"
            "    type: ratio\n"
            "    numerator: a\n"
            "    denominator: c\n",
            encoding="utf-8",
        )
        with pytest.raises(ConfigValidationError, match="cycle"):
            load_config(f)

    def test_rejects_cross_source_ratio(self, tmp_path: Path) -> None:
        f = tmp_path / "cross.yaml"
        f.write_text(
            "sources:\n"
            "  - id: s1\n"
            "    display_name: S1\n"
            "    type: parquet\n"
            "    uri: x.parquet\n"
            "  - id: s2\n"
            "    display_name: S2\n"
            "    type: parquet\n"
            "    uri: y.parquet\n"
            "metrics:\n"
            "  - id: m1\n"
            "    source: s1\n"
            "    display_name: M1\n"
            "    type: count\n"
            "  - id: m2\n"
            "    source: s2\n"
            "    display_name: M2\n"
            "    type: count\n"
            "  - id: r\n"
            "    source: s1\n"
            "    display_name: R\n"
            "    type: ratio\n"
            "    numerator: m1\n"
            "    denominator: m2\n",
            encoding="utf-8",
        )
        with pytest.raises(ConfigValidationError, match="cross-source"):
            load_config(f)

    def test_rejects_dimension_to_unknown_column(self, tmp_path: Path) -> None:
        f = tmp_path / "bad_dim.yaml"
        f.write_text(
            "sources:\n"
            "  - id: s\n"
            "    display_name: S\n"
            "    type: parquet\n"
            "    uri: x.parquet\n"
            "    columns:\n"
            "      - name: a\n"
            "        type: VARCHAR\n"
            "    dimensions:\n"
            "      - id: s.b\n"
            "        column: b\n"
            "        display_name: B\n"
            "        type: categorical\n",
            encoding="utf-8",
        )
        with pytest.raises(ConfigValidationError, match="unknown column"):
            load_config(f)

    def test_rejects_ratio_missing_components(self, tmp_path: Path) -> None:
        f = tmp_path / "bad_ratio.yaml"
        f.write_text(
            "sources:\n"
            "  - id: s\n"
            "    display_name: S\n"
            "    type: parquet\n"
            "    uri: x.parquet\n"
            "metrics:\n"
            "  - id: m\n"
            "    source: s\n"
            "    display_name: M\n"
            "    type: ratio\n",
            encoding="utf-8",
        )
        with pytest.raises(ConfigValidationError, match="ratio requires"):
            load_config(f)

    def test_rejects_extra_field(self, tmp_path: Path) -> None:
        f = tmp_path / "extra.yaml"
        f.write_text(
            "sources:\n"
            "  - id: s\n"
            "    display_name: S\n"
            "    type: parquet\n"
            "    uri: x.parquet\n"
            "    bogus_field: hello\n",
            encoding="utf-8",
        )
        with pytest.raises(ConfigValidationError):
            load_config(f)


class TestGovernanceLoaderRejections:
    SOURCES_BLOCK = (
        "sources:\n"
        "  - id: orders\n"
        "    display_name: Orders\n"
        "    type: parquet\n"
        "    uri: x.parquet\n"
        "    columns:\n"
        "      - name: region\n"
        "        type: VARCHAR\n"
        "      - name: amount\n"
        "        type: DECIMAL(18,2)\n"
    )

    def test_rejects_policy_unknown_role(self, tmp_path: Path) -> None:
        f = tmp_path / "bad_policy.yaml"
        f.write_text(
            self.SOURCES_BLOCK
            + "policies:\n"
            "  - id: p1\n"
            "    role: ghost\n"
            "    source: orders\n"
            "    default_column_mode: deny\n",
            encoding="utf-8",
        )
        with pytest.raises(ConfigValidationError, match="unknown role"):
            load_config(f)

    def test_rejects_policy_unknown_source(self, tmp_path: Path) -> None:
        f = tmp_path / "bad_policy.yaml"
        f.write_text(
            self.SOURCES_BLOCK
            + "roles:\n"
            "  - id: analyst\n"
            "    display_name: Analyst\n"
            "policies:\n"
            "  - id: p1\n"
            "    role: analyst\n"
            "    source: ghost\n",
            encoding="utf-8",
        )
        with pytest.raises(ConfigValidationError, match="unknown source"):
            load_config(f)

    def test_rejects_policy_unknown_column_in_overrides(self, tmp_path: Path) -> None:
        f = tmp_path / "bad_policy.yaml"
        f.write_text(
            self.SOURCES_BLOCK
            + "roles:\n"
            "  - id: analyst\n"
            "    display_name: Analyst\n"
            "policies:\n"
            "  - id: p1\n"
            "    role: analyst\n"
            "    source: orders\n"
            "    column_overrides:\n"
            "      ghost_col: allow\n",
            encoding="utf-8",
        )
        with pytest.raises(
            ConfigValidationError, match="column_override references unknown column"
        ):
            load_config(f)

    def test_rejects_policy_invalid_row_filter_op(self, tmp_path: Path) -> None:
        f = tmp_path / "bad_policy.yaml"
        f.write_text(
            self.SOURCES_BLOCK
            + "roles:\n"
            "  - id: analyst\n"
            "    display_name: Analyst\n"
            "policies:\n"
            "  - id: p1\n"
            "    role: analyst\n"
            "    source: orders\n"
            "    row_filter:\n"
            "      op: regex\n"
            "      column: region\n"
            "      value: ^w\n",
            encoding="utf-8",
        )
        with pytest.raises(ConfigValidationError, match="row_filter AST invalid"):
            load_config(f)

    def test_rejects_policy_unknown_column_in_row_filter(self, tmp_path: Path) -> None:
        f = tmp_path / "bad_policy.yaml"
        f.write_text(
            self.SOURCES_BLOCK
            + "roles:\n"
            "  - id: analyst\n"
            "    display_name: Analyst\n"
            "policies:\n"
            "  - id: p1\n"
            "    role: analyst\n"
            "    source: orders\n"
            "    row_filter:\n"
            "      op: eq\n"
            "      column: ghost\n"
            "      value: x\n",
            encoding="utf-8",
        )
        with pytest.raises(ConfigValidationError, match="row_filter AST invalid"):
            load_config(f)

    def test_rejects_quality_rule_unknown_source(self, tmp_path: Path) -> None:
        f = tmp_path / "bad_q.yaml"
        f.write_text(
            self.SOURCES_BLOCK
            + "quality_rules:\n"
            "  - id: q\n"
            "    source: ghost\n"
            "    type: not_null\n"
            "    config:\n"
            "      column: amount\n"
            "    severity: warn\n",
            encoding="utf-8",
        )
        with pytest.raises(ConfigValidationError, match="unknown source"):
            load_config(f)

    def test_rejects_quality_rule_unknown_column(self, tmp_path: Path) -> None:
        f = tmp_path / "bad_q.yaml"
        f.write_text(
            self.SOURCES_BLOCK
            + "quality_rules:\n"
            "  - id: q\n"
            "    source: orders\n"
            "    type: not_null\n"
            "    config:\n"
            "      column: ghost\n"
            "    severity: warn\n",
            encoding="utf-8",
        )
        with pytest.raises(ConfigValidationError, match="config.column .* not defined"):
            load_config(f)

    def test_rejects_freshness_rule_unknown_watermark(self, tmp_path: Path) -> None:
        f = tmp_path / "bad_f.yaml"
        f.write_text(
            self.SOURCES_BLOCK
            + "freshness_rules:\n"
            "  - id: f\n"
            "    source: orders\n"
            "    watermark_column: ghost\n"
            "    max_age_seconds: 3600\n"
            "    severity: block\n",
            encoding="utf-8",
        )
        with pytest.raises(ConfigValidationError, match="watermark_column .* not defined"):
            load_config(f)

    def test_rejects_quality_rule_missing_required_config(self, tmp_path: Path) -> None:
        f = tmp_path / "bad_q.yaml"
        f.write_text(
            self.SOURCES_BLOCK
            + "quality_rules:\n"
            "  - id: q\n"
            "    source: orders\n"
            "    type: range\n"
            "    config:\n"
            "      column: amount\n"
            "    severity: warn\n",
            encoding="utf-8",
        )
        with pytest.raises(ConfigValidationError, match="range requires config.min"):
            load_config(f)
