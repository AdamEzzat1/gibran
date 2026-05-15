import { useState, useEffect, useCallback } from "react";
import { getLog, type LogEntry, type LogResponse, ApiError } from "../../api/client";

// Audit Log: filterable table of gibran_query_log. Non-admin sees own
// rows only (the backend scopes); admin sees all. Cursor-paginated
// (next page = backend's next_cursor).

export function AuditLog() {
  const [data, setData] = useState<LogResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [filters, setFilters] = useState({ status: "", role_id: "", user_id: "" });
  const [cursor, setCursor] = useState<string | null>(null);
  const [expanded, setExpanded] = useState<string | null>(null);

  const load = useCallback(async (resetCursor: boolean) => {
    setLoading(true);
    setError(null);
    try {
      const r = await getLog({
        limit: 50,
        cursor: resetCursor ? undefined : (cursor ?? undefined),
        status: filters.status || undefined,
        role_id: filters.role_id || undefined,
        user_id: filters.user_id || undefined,
      });
      setData(r);
    } catch (e) {
      setError(e instanceof ApiError ? e.detail : String(e));
    } finally {
      setLoading(false);
    }
  }, [cursor, filters]);

  useEffect(() => {
    load(true);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [filters]);

  return (
    <div>
      <h2>Audit Log</h2>
      {data?.scoped_to_self && (
        <div style={{ fontSize: 12, color: "var(--fg-muted)", marginBottom: 8 }}>
          Showing only your queries (admin role required for full log).
        </div>
      )}
      <div className="card">
        <div style={{ display: "flex", gap: 8, marginBottom: 12 }}>
          <FilterInput
            placeholder="status (ok/denied/error)"
            value={filters.status}
            onChange={(v) => { setCursor(null); setFilters((f) => ({ ...f, status: v })); }}
          />
          <FilterInput
            placeholder="role_id"
            value={filters.role_id}
            onChange={(v) => { setCursor(null); setFilters((f) => ({ ...f, role_id: v })); }}
          />
          <FilterInput
            placeholder="user_id"
            value={filters.user_id}
            onChange={(v) => { setCursor(null); setFilters((f) => ({ ...f, user_id: v })); }}
          />
        </div>

        {error && <div className="error-banner">{error}</div>}
        {loading && <div className="empty" style={{ padding: 12 }}>Loading…</div>}

        {data && data.rows.length === 0 && (
          <div className="empty">No matching log rows.</div>
        )}
        {data && data.rows.length > 0 && (
          <>
            <table>
              <thead>
                <tr>
                  <th>When</th>
                  <th>User</th>
                  <th>Role</th>
                  <th>Status</th>
                  <th>Rows</th>
                  <th>Duration</th>
                  <th></th>
                </tr>
              </thead>
              <tbody>
                {data.rows.map((row) => (
                  <RowEntry
                    key={row.query_id}
                    row={row}
                    expanded={expanded === row.query_id}
                    onToggle={() =>
                      setExpanded((curr) =>
                        curr === row.query_id ? null : row.query_id,
                      )
                    }
                  />
                ))}
              </tbody>
            </table>
            <div style={{ marginTop: 12, display: "flex", justifyContent: "flex-end" }}>
              {data.next_cursor && (
                <button
                  className="secondary"
                  onClick={() => { setCursor(data.next_cursor); load(false); }}
                  disabled={loading}
                >
                  Next page
                </button>
              )}
            </div>
          </>
        )}
      </div>
    </div>
  );
}

function FilterInput({
  placeholder,
  value,
  onChange,
}: {
  placeholder: string;
  value: string;
  onChange: (v: string) => void;
}) {
  return (
    <input
      type="text"
      placeholder={placeholder}
      value={value}
      onChange={(e) => onChange(e.target.value)}
      style={{ flex: 1 }}
    />
  );
}

function RowEntry({
  row,
  expanded,
  onToggle,
}: {
  row: LogEntry;
  expanded: boolean;
  onToggle: () => void;
}) {
  return (
    <>
      <tr
        className={row.is_break_glass ? "break-glass" : undefined}
        onClick={onToggle}
        style={{ cursor: "pointer" }}
      >
        <td style={{ fontSize: 12, fontFamily: "var(--font-mono)" }}>
          {row.created_at?.split(".")[0]}
        </td>
        <td>{row.user_id}</td>
        <td>{row.role_id}</td>
        <td className={`status-${row.status}`}>{row.status}</td>
        <td className="numeric">{row.row_count ?? "—"}</td>
        <td className="numeric">{row.duration_ms ?? "—"}ms</td>
        <td style={{ fontSize: 11, color: "var(--fg-muted)" }}>
          {expanded ? "▼" : "▶"}
        </td>
      </tr>
      {expanded && (
        <tr>
          <td colSpan={7} style={{ background: "var(--bg-sunken)" }}>
            <div style={{ padding: 8 }}>
              {row.nl_prompt && (
                <div style={{ marginBottom: 8 }}>
                  <strong>Prompt:</strong>{" "}
                  <code>{row.nl_prompt}</code>
                </div>
              )}
              {row.generated_sql && (
                <div style={{ marginBottom: 8 }}>
                  <strong>SQL:</strong>
                  <pre className="sql">{row.generated_sql}</pre>
                </div>
              )}
              {row.deny_reason && (
                <div className="error-banner">
                  Denied: {row.deny_reason}
                </div>
              )}
              <div style={{ fontSize: 11, color: "var(--fg-muted)" }}>
                query_id: <code>{row.query_id}</code>
              </div>
            </div>
          </td>
        </tr>
      )}
    </>
  );
}
