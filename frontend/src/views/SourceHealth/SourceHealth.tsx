import { useState, useEffect } from "react";
import {
  getCatalog,
  getHealth,
  type HealthResponse,
  type SourceSchema,
  ApiError,
} from "../../api/client";

// Source Health: lists every visible source with its latest health
// snapshot from gibran_source_health. Each card shows pass/fail status,
// recent rule runs, and a "run gibran check" hint when the cache is
// empty.

export function SourceHealth() {
  const [sources, setSources] = useState<SourceSchema[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    setLoading(true);
    getCatalog()
      .then((r) => setSources(r.sources))
      .catch((e) => setError(e instanceof ApiError ? e.detail : String(e)))
      .finally(() => setLoading(false));
  }, []);

  if (loading) return <div className="empty">Loading sources…</div>;
  if (error) return <div className="error-banner">{error}</div>;
  if (sources.length === 0) {
    return <div className="empty">No sources visible to this identity.</div>;
  }

  return (
    <div>
      <h2>Source Health</h2>
      <p style={{ color: "var(--fg-muted)", fontSize: 12 }}>
        Live snapshot of <code>gibran_source_health</code>. Run{" "}
        <code>gibran check</code> to refresh. Each card shows the latest
        quality-rule outcomes for the source.
      </p>
      {sources.map((s) => <HealthCard key={s.source_id} source={s} />)}
    </div>
  );
}

function HealthCard({ source }: { source: SourceSchema }) {
  const [health, setHealth] = useState<HealthResponse | null>(null);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    getHealth(source.source_id)
      .then(setHealth)
      .catch((e) => setErr(e instanceof ApiError ? e.detail : String(e)));
  }, [source.source_id]);

  return (
    <div className="card">
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
        <div>
          <strong>{source.display_name}</strong>{" "}
          <code style={{ fontSize: 11, color: "var(--fg-muted)" }}>
            {source.source_id}
          </code>
        </div>
        {health && <StatusBadge status={health.status} />}
      </div>
      {err && <div className="error-banner">{err}</div>}
      {health && (
        <>
          {health.note && (
            <div
              style={{
                marginTop: 8,
                padding: 8,
                background: "var(--bg-sunken)",
                fontSize: 12,
                color: "var(--fg-muted)",
                borderRadius: 4,
              }}
            >
              {health.note}
            </div>
          )}
          {health.refreshed_at && (
            <div style={{ fontSize: 11, color: "var(--fg-muted)", marginTop: 4 }}>
              refreshed: {health.refreshed_at}
            </div>
          )}
          {Array.isArray(health.blocking_failures) && health.blocking_failures.length > 0 && (
            <div style={{ marginTop: 8 }}>
              <strong style={{ color: "var(--danger)" }}>Blocking failures:</strong>
              <ul>
                {health.blocking_failures.map((f, i) => (
                  <li key={i}><code>{JSON.stringify(f)}</code></li>
                ))}
              </ul>
            </div>
          )}
          {health.recent_runs && health.recent_runs.length > 0 && (
            <div style={{ marginTop: 12 }}>
              <h3>Recent runs</h3>
              <table>
                <thead>
                  <tr>
                    <th>Rule</th><th>Kind</th><th>Result</th><th>Observed</th><th>When</th>
                  </tr>
                </thead>
                <tbody>
                  {health.recent_runs.slice(0, 10).map((r) => (
                    <tr key={r.run_id}>
                      <td><code style={{ fontSize: 11 }}>{r.rule_id}</code></td>
                      <td>{r.rule_kind}</td>
                      <td className={r.passed ? "status-ok" : "status-error"}>
                        {r.passed ? "pass" : "fail"}
                      </td>
                      <td style={{ fontSize: 11 }}>
                        {r.observed_value !== null
                          ? <code>{JSON.stringify(r.observed_value)}</code>
                          : "—"}
                      </td>
                      <td style={{ fontSize: 11, color: "var(--fg-muted)" }}>
                        {r.ran_at?.split(".")[0]}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </>
      )}
    </div>
  );
}

function StatusBadge({ status }: { status: HealthResponse["status"] }) {
  const colors: Record<string, string> = {
    healthy: "var(--ok)",
    warn: "var(--warn)",
    block: "var(--danger)",
    unknown: "var(--fg-muted)",
  };
  return (
    <span
      style={{
        background: colors[status] ?? "var(--fg-muted)",
        color: "white",
        padding: "4px 12px",
        borderRadius: 4,
        fontSize: 11,
        fontWeight: 600,
        textTransform: "uppercase",
        letterSpacing: "0.05em",
      }}
    >
      {status}
    </span>
  );
}
