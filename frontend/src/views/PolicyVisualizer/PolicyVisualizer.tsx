import { useState, useEffect } from "react";
import {
  getPolicy,
  listRoles,
  type PolicyResponse,
  type PolicyPreviewSource,
  type Role,
  ApiError,
} from "../../api/client";

// Policy Visualizer: pick a role, see what it can read.
// Admin-only (the backend gates with 403 for non-admin).
// The "two-role comparison" mode is the demo flagship -- pick analyst_west
// next to admin and watch the column lists diverge.

export function PolicyVisualizer() {
  const [roles, setRoles] = useState<Role[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [leftRole, setLeftRole] = useState<string>("");
  const [rightRole, setRightRole] = useState<string>("");
  const [leftData, setLeftData] = useState<PolicyResponse | null>(null);
  const [rightData, setRightData] = useState<PolicyResponse | null>(null);
  const [, setLoading] = useState(false);

  // Roles fetched from /api/roles (admin-only). Picks the first two
  // distinct roles for the side-by-side comparison.
  useEffect(() => {
    listRoles()
      .then(({ roles }) => {
        setRoles(roles);
        if (roles.length >= 1) setLeftRole(roles[0].id);
        if (roles.length >= 2) setRightRole(roles[1].id);
        else if (roles.length === 1) setRightRole(roles[0].id);
      })
      .catch((e) => setError(e instanceof ApiError ? e.detail : String(e)));
  }, []);

  useEffect(() => {
    if (!leftRole) return;
    setLoading(true);
    getPolicy(leftRole)
      .then(setLeftData)
      .catch((e) => setError(e instanceof ApiError ? e.detail : String(e)))
      .finally(() => setLoading(false));
  }, [leftRole]);

  useEffect(() => {
    if (!rightRole) return;
    setLoading(true);
    getPolicy(rightRole)
      .then(setRightData)
      .catch((e) => setError(e instanceof ApiError ? e.detail : String(e)))
      .finally(() => setLoading(false));
  }, [rightRole]);

  if (error) {
    return (
      <div>
        <h2>Policy Visualizer</h2>
        <div className="error-banner">
          {error}
          {error.toLowerCase().includes("admin") && (
            <div style={{ marginTop: 8 }}>
              This view requires a break-glass (admin) role. Change your
              identity to <code>admin</code> via the sidebar.
            </div>
          )}
        </div>
      </div>
    );
  }

  return (
    <div>
      <h2>Policy Visualizer</h2>
      <p style={{ color: "var(--fg-muted)", fontSize: 12 }}>
        Pick two roles to compare what each one is allowed to see.
        Sensitivity badges show the column-level governance result;
        the row-filter is applied to query results, not the schema.
      </p>

      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16 }}>
        <RolePane
          label="Left"
          options={roles}
          value={leftRole}
          onChange={setLeftRole}
          data={leftData}
        />
        <RolePane
          label="Right"
          options={roles}
          value={rightRole}
          onChange={setRightRole}
          data={rightData}
        />
      </div>
    </div>
  );
}

function RolePane({
  label,
  options,
  value,
  onChange,
  data,
}: {
  label: string;
  options: Role[];
  value: string;
  onChange: (v: string) => void;
  data: PolicyResponse | null;
}) {
  return (
    <div>
      <div style={{ marginBottom: 8 }}>
        <label style={{ fontSize: 12, color: "var(--fg-muted)" }}>{label} role</label>
        <select
          value={value}
          onChange={(e) => onChange(e.target.value)}
          style={{ marginTop: 4 }}
        >
          {options.map((o) => (
            <option key={o.id} value={o.id}>
              {o.display_name}{o.is_break_glass ? " (break-glass)" : ""}
            </option>
          ))}
        </select>
      </div>
      {data && (
        <div className="card">
          <h3>{data.target_role_display_name}</h3>
          {Object.keys(data.target_role_attributes).length > 0 && (
            <div style={{ fontSize: 12, color: "var(--fg-muted)", marginBottom: 8 }}>
              attributes:{" "}
              {Object.entries(data.target_role_attributes).map(([k, v]) => (
                <code key={k} style={{ marginRight: 8 }}>
                  {k}={v}
                </code>
              ))}
            </div>
          )}
          {data.previews.map((p) => <SourcePreview key={p.source_id} preview={p} />)}
        </div>
      )}
    </div>
  );
}

function SourcePreview({ preview }: { preview: PolicyPreviewSource }) {
  if (preview.denied) {
    return (
      <div style={{ padding: 8, borderTop: "1px solid var(--border)", marginTop: 8 }}>
        <strong>{preview.source_id}</strong>
        <div className="badge restricted" style={{ marginLeft: 8 }}>denied</div>
        <div style={{ color: "var(--fg-muted)", fontSize: 11, marginTop: 4 }}>
          {preview.reason}
        </div>
      </div>
    );
  }
  return (
    <div style={{ padding: 8, borderTop: "1px solid var(--border)", marginTop: 8 }}>
      <strong>{preview.display_name ?? preview.source_id}</strong>{" "}
      <span style={{ fontSize: 11, color: "var(--fg-muted)" }}>
        {preview.columns?.length ?? 0} cols · {preview.metrics?.length ?? 0} metrics
      </span>
      {preview.columns && preview.columns.length > 0 && (
        <div style={{ marginTop: 8 }}>
          <table>
            <tbody>
              {preview.columns.map((c) => (
                <tr key={c.name}>
                  <td><code style={{ fontSize: 11 }}>{c.name}</code></td>
                  <td style={{ width: 80 }}>
                    <span className={`badge ${c.sensitivity}`}>{c.sensitivity}</span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
