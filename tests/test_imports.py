def test_all_modules_import() -> None:
    import gibran  # noqa: F401
    import gibran.catalog  # noqa: F401
    import gibran.governance  # noqa: F401
    import gibran.governance.types  # noqa: F401
    import gibran.governance.identity  # noqa: F401
    import gibran.governance.ast  # noqa: F401
    import gibran.governance.default  # noqa: F401
    import gibran.execution  # noqa: F401
    import gibran.execution.sql  # noqa: F401
    import gibran.observability.types  # noqa: F401
    import gibran.observability.default  # noqa: F401
    import gibran.observability.runner  # noqa: F401
    import gibran._sql  # noqa: F401
    import gibran.dsl  # noqa: F401
    import gibran.dsl.types  # noqa: F401
    import gibran.dsl.validate  # noqa: F401
    import gibran.dsl.compile  # noqa: F401
    import gibran.dsl.run  # noqa: F401
    import gibran.semantic  # noqa: F401
    import gibran.observability  # noqa: F401
    import gibran.nl  # noqa: F401
    import gibran.perf  # noqa: F401
    import gibran.sync  # noqa: F401
    import gibran.sync.migrations  # noqa: F401
    import gibran.cli  # noqa: F401
    import gibran.cli.main  # noqa: F401


def test_governance_public_api() -> None:
    from gibran.governance import (
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
    from gibran.governance.identity import CLIResolver, EnvResolver, JWTResolver

    cli = CLIResolver(user_id="alice", role_id="analyst_west", attributes={"region": "west"})
    ident = cli.resolve(None)
    assert ident.user_id == "alice"
    assert ident.source == "cli"

    jwt = JWTResolver(jwks_url="https://idp/jwks", audience="gibran", issuer="https://idp")
    assert jwt.audience == "gibran"

    env = EnvResolver()
    # resolve raises until GIBRAN_ENV=dev; instantiation alone is fine
    assert env is not None
