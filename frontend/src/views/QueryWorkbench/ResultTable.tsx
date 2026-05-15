interface Props {
  columns: string[];
  rows: unknown[][];
}

// Simple non-virtualized table. For result sets up to ~1000 rows this
// is fine; virtualization (Tanstack Virtual) is a 4B follow-up if real
// users hit million-row exports.

export function ResultTable({ columns, rows }: Props) {
  if (rows.length === 0) {
    return <div className="empty" style={{ padding: 12 }}>0 rows.</div>;
  }

  function isNumeric(v: unknown): boolean {
    return typeof v === "number" || (typeof v === "string" && /^-?\d+(\.\d+)?$/.test(v));
  }

  function exportCsv() {
    const esc = (v: unknown) => {
      const s = v === null || v === undefined ? "" : String(v);
      if (s.includes(",") || s.includes('"') || s.includes("\n")) {
        return '"' + s.replace(/"/g, '""') + '"';
      }
      return s;
    };
    const lines = [
      columns.map(esc).join(","),
      ...rows.map((row) => row.map(esc).join(",")),
    ];
    const blob = new Blob([lines.join("\n")], { type: "text/csv" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = "gibran-result.csv";
    a.click();
    URL.revokeObjectURL(url);
  }

  return (
    <div>
      <div style={{ marginBottom: 8, display: "flex", justifyContent: "space-between" }}>
        <span style={{ fontSize: 12, color: "var(--fg-muted)" }}>
          {rows.length} row{rows.length === 1 ? "" : "s"}
        </span>
        <button className="secondary" onClick={exportCsv} style={{ padding: "4px 10px", fontSize: 12 }}>
          Export CSV
        </button>
      </div>
      <table>
        <thead>
          <tr>
            {columns.map((c) => <th key={c}>{c}</th>)}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, i) => (
            <tr key={i}>
              {row.map((v, j) => (
                <td
                  key={j}
                  className={isNumeric(v) ? "numeric" : undefined}
                >
                  {v === null || v === undefined ? <span style={{ color: "var(--fg-muted)" }}>null</span> : String(v)}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
