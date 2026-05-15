import { useState, useEffect } from "react";
import { getExamples, ApiError } from "../../api/client";

interface Props {
  onPick: (question: string) => void;
}

// Auto-generated example questions from the user's actual catalog.
// Each example is one-click into the Workbench prompt. The pattern
// name is shown on hover so demo viewers can see which NL template
// they're exercising.

export function ExamplesPanel({ onPick }: Props) {
  const [examples, setExamples] = useState<{ question: string; pattern: string }[]>([]);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    getExamples()
      .then((r) => setExamples(r.examples))
      .catch((e) => setErr(e instanceof ApiError ? e.detail : String(e)));
  }, []);

  if (err) return null;  // Quiet failure -- examples are decorative
  if (examples.length === 0) return null;

  return (
    <div style={{ marginTop: 8 }}>
      <div style={{ fontSize: 11, color: "var(--fg-muted)", marginBottom: 6 }}>
        Try these:
      </div>
      <div style={{ display: "flex", flexWrap: "wrap", gap: 6 }}>
        {examples.map((ex, i) => (
          <button
            key={i}
            className="secondary"
            title={`pattern: ${ex.pattern}`}
            onClick={() => onPick(ex.question)}
            style={{
              fontSize: 11,
              padding: "4px 10px",
              borderRadius: 12,
            }}
          >
            {ex.question}
          </button>
        ))}
      </div>
    </div>
  );
}
