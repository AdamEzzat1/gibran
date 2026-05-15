import { useState, useEffect } from "react";
import {
  getCatalog,
  type CatalogResponse,
  type SourceSchema,
  ApiError,
} from "../../api/client";

// Catalog Browser: tree of sources -> their columns / metrics /
// dimensions. The shape mirrors AllowedSchema -- only what the role
// can actually see, with sensitivity badges that double as a visual
// audit of the role's column-level access.

export function CatalogBrowser() {
  const [catalog, setCatalog] = useState<CatalogResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [expandedSource, setExpandedSource] = useState<string | null>(null);

  useEffect(() => {
    setLoading(true);
    getCatalog()
      .then((r) => {
        setCatalog(r);
        if (r.sources.length > 0) {
          setExpandedSource(r.sources[0].source_id);
        }
      })
      .catch((e) => setError(e instanceof ApiError ? e.detail : String(e)))
      .finally(() => setLoading(false));
  }, []);

  if (loading) return <div className="empty">Loading catalog…</div>;
  if (error) return <div className="error-banner">{error}</div>;
  if (!catalog || catalog.sources.length === 0) {
    return (
      <div className="empty">
        No sources visible to this identity.
      </div>
    );
  }

  return (
    <div>
      <h2>Catalog</h2>
      <p style={{ color: "var(--fg-muted)", fontSize: 12 }}>
        {catalog.sources.length} source{catalog.sources.length === 1 ? "" : "s"}{" "}
        visible to <strong>{catalog.role}</strong>. Columns are listed as
        the role would see them after governance applies.
      </p>
      {catalog.sources.map((s) => (
        <SourceCard
          key={s.source_id}
          source={s}
          expanded={expandedSource === s.source_id}
          onToggle={() =>
            setExpandedSource((curr) =>
              curr === s.source_id ? null : s.source_id,
            )
          }
        />
      ))}
    </div>
  );
}

function SourceCard({
  source,
  expanded,
  onToggle,
}: {
  source: SourceSchema;
  expanded: boolean;
  onToggle: () => void;
}) {
  return (
    <div className="card">
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
          cursor: "pointer",
        }}
        onClick={onToggle}
      >
        <div>
          <strong>{source.display_name}</strong>{" "}
          <code style={{ color: "var(--fg-muted)", fontSize: 12 }}>
            {source.source_id}
          </code>
        </div>
        <div style={{ fontSize: 12, color: "var(--fg-muted)" }}>
          {source.columns.length} cols · {source.metrics.length} metrics ·{" "}
          {source.dimensions.length} dims
        </div>
      </div>
      {expanded && (
        <div style={{ marginTop: 16 }}>
          {source.metrics.length > 0 && (
            <Section title="Metrics">
              <table>
                <thead>
                  <tr>
                    <th>ID</th>
                    <th>Type</th>
                    <th>Display</th>
                    <th>Unit</th>
                  </tr>
                </thead>
                <tbody>
                  {source.metrics.map((m) => (
                    <tr key={m.id}>
                      <td><code>{m.id}</code></td>
                      <td>{m.type}</td>
                      <td>{m.display_name}</td>
                      <td>{m.unit ?? <span style={{ color: "var(--fg-muted)" }}>—</span>}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </Section>
          )}
          {source.dimensions.length > 0 && (
            <Section title="Dimensions">
              <table>
                <thead>
                  <tr><th>ID</th><th>Column</th><th>Type</th><th>Display</th></tr>
                </thead>
                <tbody>
                  {source.dimensions.map((d) => (
                    <tr key={d.id}>
                      <td><code>{d.id}</code></td>
                      <td><code>{d.column}</code></td>
                      <td>{d.type}</td>
                      <td>{d.display_name}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </Section>
          )}
          {source.columns.length > 0 && (
            <Section title="Columns">
              <table>
                <thead>
                  <tr><th>Name</th><th>Type</th><th>Sensitivity</th><th>Examples</th></tr>
                </thead>
                <tbody>
                  {source.columns.map((c) => (
                    <tr key={c.name}>
                      <td><code>{c.name}</code></td>
                      <td>{c.data_type}</td>
                      <td><span className={`badge ${c.sensitivity}`}>{c.sensitivity}</span></td>
                      <td style={{ fontSize: 11, color: "var(--fg-muted)" }}>
                        {c.example_values?.slice(0, 3).join(", ") ?? "—"}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </Section>
          )}
        </div>
      )}
    </div>
  );
}

function Section({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div style={{ marginBottom: 12 }}>
      <h3 style={{ fontSize: 13, marginBottom: 8 }}>{title}</h3>
      {children}
    </div>
  );
}
