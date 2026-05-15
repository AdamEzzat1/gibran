import { useState, useEffect, useCallback } from "react";
import { ask, type AskResult, ApiError } from "../../api/client";
import { ResultTable } from "./ResultTable";
import { ExamplesPanel } from "./ExamplesPanel";

// Three-pane Workbench: NL input on top, compiled-SQL preview in the
// middle, result table on the bottom. cmd+enter (or ctrl+enter) runs
// the query. The "no pattern matched" empty state surfaces the
// backend's hint string instead of an error.

export function QueryWorkbench() {
  const [prompt, setPrompt] = useState("");
  const [result, setResult] = useState<AskResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const submit = useCallback(async () => {
    if (!prompt.trim()) return;
    setLoading(true);
    setError(null);
    try {
      const r = await ask(prompt.trim());
      setResult(r);
    } catch (e) {
      setResult(null);
      setError(e instanceof ApiError ? e.detail : String(e));
    } finally {
      setLoading(false);
    }
  }, [prompt]);

  // Keyboard shortcut: cmd/ctrl + enter to submit.
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key === "Enter") {
        e.preventDefault();
        submit();
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [submit]);

  return (
    <div className="workbench">
      <section className="card workbench-input">
        <h2>Ask a question</h2>
        <textarea
          placeholder="e.g. 'top 5 region by gross revenue' or 'revenue by month'"
          value={prompt}
          onChange={(e) => setPrompt(e.target.value)}
          rows={3}
        />
        <div style={{ marginTop: 8, display: "flex", gap: 8, alignItems: "center" }}>
          <button
            className="primary"
            onClick={submit}
            disabled={!prompt.trim() || loading}
          >
            {loading ? "Running…" : "Run"}
          </button>
          <span style={{ color: "var(--fg-muted)", fontSize: 12 }}>
            cmd/ctrl + enter to run
          </span>
        </div>
        <ExamplesPanel onPick={(q) => setPrompt(q)} />
      </section>

      <section className="card workbench-preview">
        <h3>Compiled preview</h3>
        {!result && !error && !loading && (
          <div className="empty" style={{ padding: 12 }}>
            Run a question to see the matched pattern and emitted SQL.
          </div>
        )}
        {error && <div className="error-banner">Error: {error}</div>}
        {result && <PreviewPane result={result} />}
      </section>

      <section className="card workbench-result">
        <h3>Result</h3>
        {result?.status === "ok" && result.columns && result.rows && (
          <ResultTable columns={result.columns} rows={result.rows} />
        )}
        {result?.status === "denied" && (
          <div className="error-banner">
            Denied: {result.deny_reason}
            {result.deny_detail && ` — ${result.deny_detail}`}
          </div>
        )}
        {result?.status === "error" && (
          <div className="error-banner">Error: {result.error}</div>
        )}
        {result && !result.matched && (
          <div className="empty">
            <div style={{ marginBottom: 8 }}>No pattern matched.</div>
            <div style={{ color: "var(--fg-muted)", fontSize: 12 }}>
              {result.hint}
            </div>
          </div>
        )}
      </section>
    </div>
  );
}

function PreviewPane({ result }: { result: AskResult }) {
  if (!result.matched) {
    return (
      <div style={{ color: "var(--fg-muted)" }}>
        No pattern matched -- nothing to compile.
      </div>
    );
  }
  return (
    <div>
      <div style={{ marginBottom: 8 }}>
        <strong>Pattern:</strong>{" "}
        <code>{result.pattern_name}</code> matched "
        <em>{result.matched_text}</em>"
      </div>
      {result.compiled_sql && (
        <pre className="sql">{result.compiled_sql}</pre>
      )}
      {result.duration_ms !== undefined && (
        <div style={{ fontSize: 12, color: "var(--fg-muted)", marginTop: 4 }}>
          duration: {result.duration_ms}ms
          {result.row_count !== undefined && ` · ${result.row_count} rows`}
        </div>
      )}
    </div>
  );
}
