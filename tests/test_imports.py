def test_all_modules_import() -> None:
    import rumi  # noqa: F401
    import rumi.catalog  # noqa: F401
    import rumi.governance  # noqa: F401
    import rumi.governance.types  # noqa: F401
    import rumi.governance.identity  # noqa: F401
    import rumi.governance.ast  # noqa: F401
    import rumi.governance.default  # noqa: F401
    import rumi.execution  # noqa: F401
    import rumi.execution.sql  # noqa: F401
    import rumi.observability.types  # noqa: F401
    import rumi.observability.default  # noqa: F401
    import rumi.observability.runner  # noqa: F401
    import rumi._sql  # noqa: F401
    import rumi.dsl  # noqa: F401
    import rumi.dsl.types  # noqa: F401
    import rumi.dsl.validate  # noqa: F401
    import rumi.dsl.compile  # noqa: F401
    import rumi.dsl.run  # noqa: F401
    import rumi.semantic  # noqa: F401
    import rumi.observability  # noqa: F401
    import rumi.nl  # noqa: F401
    import rumi.perf  # noqa: F401
    import rumi.sync  # noqa: F401
    import rumi.sync.migrations  # noqa: F401
    import rumi.cli  # noqa: F401
    import rumi.cli.main  # noqa: F401


def test_governance_public_api() -> None:
    from rumi.governance import (
        ALLOWED_AST_OPS,
        AllowedSchema,
        ColumnView,
        Constraint,
        DenyReason,
        DimensionView,
        GovernanceAPI,
        GovernanceDecision,
        IdentityContext,
        IdentityResolver,
        MetricView,
    )
    assert DenyReason.COLUMN_DENIED.value == "policy:no_column_access"
    assert "eq" in ALLOWED_AST_OPS
    assert isinstance(ALLOWED_AST_OPS, frozenset)

    # smoke: dataclasses construct
    c = Constraint(
        column="region", op="eq", value="west",
        source="policy_filter", rationale="role analyst_west",
    )
    assert c.column == "region"

    cv = ColumnView(
        name="amount", display_name="Amount", data_type="DECIMAL",
        sensitivity="public", description=None, example_values=None,
    )
    assert cv.example_values is None

    mv = MetricView(
        metric_id="gross_revenue", display_name="Gross Revenue",
        metric_type="sum", unit="USD", description=None, depends_on=(),
    )
    assert mv.metric_type == "sum"

    dv = DimensionView(
        dimension_id="orders.region", column_name="region",
        display_name="Region", dim_type="categorical", description=None,
    )
    assert dv.dim_type == "categorical"

    ic = IdentityContext(
        user_id="alice", role_id="analyst_west",
        attributes={"region": "west"}, source="env",
    )
    assert ic.attributes["region"] == "west"


def test_identity_resolvers_signatures() -> None:
    from rumi.governance.identity import CLIResolver, EnvResolver, JWTResolver

    cli = CLIResolver(user_id="alice", role_id="analyst_west", attributes={"region": "west"})
    ident = cli.resolve(None)
    assert ident.user_id == "alice"
    assert ident.source == "cli"

    jwt = JWTResolver(jwks_url="https://idp/jwks", audience="rumi", issuer="https://idp")
    assert jwt.audience == "rumi"

    env = EnvResolver()
    # resolve raises until RUMI_ENV=dev; instantiation alone is fine
    assert env is not None
