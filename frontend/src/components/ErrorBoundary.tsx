import { Component, ReactNode } from "react";

interface State {
  error: Error | null;
}

// Top-level error boundary. Wraps the entire app so a render error in
// one view doesn't blank-screen the whole UI. The reset button reloads
// the page (cheap, reliable -- nothing in this UI carries state worth
// preserving across an error).

export class ErrorBoundary extends Component<{ children: ReactNode }, State> {
  state: State = { error: null };

  static getDerivedStateFromError(error: Error): State {
    return { error };
  }

  componentDidCatch(error: Error, info: { componentStack?: string }) {
    // eslint-disable-next-line no-console
    console.error("ErrorBoundary caught:", error, info);
  }

  render() {
    if (this.state.error) {
      return (
        <div style={{ padding: 24 }}>
          <div className="card">
            <h2 style={{ color: "var(--danger)" }}>Something broke.</h2>
            <p>The UI hit an unexpected error rendering this view.</p>
            <pre className="sql" style={{ color: "var(--danger)" }}>
              {this.state.error.message}
            </pre>
            <button className="primary" onClick={() => location.reload()}>
              Reload
            </button>
          </div>
        </div>
      );
    }
    return this.props.children;
  }
}
