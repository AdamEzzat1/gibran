🚀 Just shipped Gibran v0.0.2 — a governed metric layer with NL-to-SQL that cannot hallucinate.

The mechanism: pattern matching with slot resolution against the user's permitted schema. No LLM in the path.

Every metric / dimension / column name a user types gets validated against the role's `AllowedSchema` before becoming part of a query. If a slot can't resolve to a real reference, the matcher returns "I don't know how to answer that" — never invents.

That's a structural guarantee. LLM-based NL layers can't make it: even with constrained decoding, they can pick a plausible-sounding-but-wrong real reference. The pattern matcher fails closed in the same case.

Example:

$ gibran ask "show me customer retention"
→ resolves `customer_retention` (a cohort_retention primitive)
→ emits a 3-CTE cohort retention query

$ gibran ask "top 5 region by gross revenue"
→ emits ORDER BY gross_revenue DESC LIMIT 5

$ gibran ask "why did revenue drop last week"
→ "I don't know how to answer that." (exit code 4)

The third case is the contract you can't get from LLMs. Same role's `AllowedSchema` is consulted for all three — identity-aware row filtering is automatic.

What else ships in v0.0.2:

✓ 19 metric primitives (cohort_retention, funnel, multi_stage_filter, period_over_period, percentile, rolling_window, weighted_avg, count_distinct_approx, …)
✓ Identity-aware governance: row + column policies, time-bound grants, break-glass roles
✓ Audit log with PII literal redaction (both compiled SQL AND DSL intent fields)
✓ Anomaly rules + alert webhooks + access-pattern anomaly detection
✓ Plan + result caching, materialized metrics, schema-drift detection
✓ 456 passing tests, single pip install

Embedded over DuckDB. No server. MIT licensed.

pip install gibran
gibran init --sample
gibran ask "top 5 region by gross revenue" --source orders --role admin

GitHub: github.com/AdamEzzat1/gibran

#data #analytics #sql #python #opensource
