import { useState, useEffect } from "react";
import { IdentitySetup } from "./components/IdentitySetup";
import { ErrorBoundary } from "./components/ErrorBoundary";
import { QueryWorkbench } from "./views/QueryWorkbench/QueryWorkbench";
import { CatalogBrowser } from "./views/CatalogBrowser/CatalogBrowser";
import { AuditLog } from "./views/AuditLog/AuditLog";
import { SourceHealth } from "./views/SourceHealth/SourceHealth";
import { PolicyVisualizer } from "./views/PolicyVisualizer/PolicyVisualizer";
import { getIdentity, clearIdentity, type Identity } from "./api/client";

type ViewName = "workbench" | "catalog" | "log" | "health" | "policy";

interface ViewDef {
  name: ViewName;
  label: string;
  admin?: boolean;
}

const VIEWS: ViewDef[] = [
  { name: "workbench", label: "Workbench" },
  { name: "catalog", label: "Catalog" },
  { name: "log", label: "Audit Log" },
  { name: "health", label: "Source Health" },
  { name: "policy", label: "Policy Visualizer", admin: true },
];

export function App() {
  const [identity, setIdentityState] = useState<Identity | null>(getIdentity());
  const [view, setView] = useState<ViewName>("workbench");

  useEffect(() => {
    const onStorage = () => setIdentityState(getIdentity());
    window.addEventListener("storage", onStorage);
    return () => window.removeEventListener("storage", onStorage);
  }, []);

  if (!identity) {
    return <IdentitySetup onSet={setIdentityState} />;
  }

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div style={{ display: "flex", alignItems: "baseline", justifyContent: "space-between" }}>
          <h1>Gibran</h1>
          <span style={{ fontSize: 10, color: "var(--fg-muted)" }}>v0.1</span>
        </div>
        <nav>
          {VIEWS.map((v) => (
            <button
              key={v.name}
              className={view === v.name ? "active" : ""}
              onClick={() => setView(v.name)}
              title={v.admin ? "Admin / break-glass roles only" : undefined}
            >
              {v.label}
              {v.admin && (
                <span style={{ marginLeft: 6, fontSize: 9, opacity: 0.6 }}>
                  ADMIN
                </span>
              )}
            </button>
          ))}
        </nav>
        <div className="identity-card">
          <div>
            <strong>{identity.user}</strong>
          </div>
          <div>role: <code>{identity.role}</code></div>
          {identity.attrs && (
            <div style={{ wordBreak: "break-word" }}>
              attrs: <code>{identity.attrs}</code>
            </div>
          )}
          <button
            className="secondary"
            style={{ marginTop: 8, padding: "4px 8px", fontSize: 11 }}
            onClick={() => {
              clearIdentity();
              setIdentityState(null);
            }}
          >
            change identity
          </button>
        </div>
      </aside>
      <main className="main">
        <ErrorBoundary key={view}>
          {view === "workbench" && <QueryWorkbench />}
          {view === "catalog" && <CatalogBrowser />}
          {view === "log" && <AuditLog />}
          {view === "health" && <SourceHealth />}
          {view === "policy" && <PolicyVisualizer />}
        </ErrorBoundary>
      </main>
    </div>
  );
}
