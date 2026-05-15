import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { QueryWorkbench } from "./QueryWorkbench";
import { setIdentity } from "../../api/client";

// QueryWorkbench tests: the load-bearing demo view.
// Verifies the input renders, the run button calls /api/ask, the
// preview pane shows the matched pattern + compiled SQL, and the
// result table renders the rows. Network is mocked via fetch stub.

beforeEach(() => {
  // Identity must be set or apiFetch skips the headers; doesn't
  // change correctness but matches real usage.
  setIdentity({ user: "adam", role: "analyst_west", attrs: "region=west" });
  vi.restoreAllMocks();
});

function mockAskAndExamples(askResponse: object, examplesResponse: object = { examples: [], source_id: "orders" }) {
  vi.stubGlobal("fetch", vi.fn().mockImplementation((url: string) => {
    if (url === "/api/ask") {
      return Promise.resolve({ ok: true, json: async () => askResponse });
    }
    if (url.startsWith("/api/examples")) {
      return Promise.resolve({ ok: true, json: async () => examplesResponse });
    }
    return Promise.reject(new Error(`unexpected fetch: ${url}`));
  }));
}

describe("QueryWorkbench", () => {
  it("renders the prompt textarea and Run button", () => {
    mockAskAndExamples({});
    render(<QueryWorkbench />);
    expect(screen.getByPlaceholderText(/top 5 region/i)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /run/i })).toBeInTheDocument();
  });

  it("disables Run when prompt is empty", () => {
    mockAskAndExamples({});
    render(<QueryWorkbench />);
    expect(screen.getByRole("button", { name: /run/i })).toBeDisabled();
  });

  it("calls /api/ask with the typed prompt and shows compiled SQL on success", async () => {
    const askResponse = {
      matched: true,
      pattern_name: "top_n_by_metric",
      matched_text: "top 5 region by gross revenue",
      intent: { source: "orders" },
      source_id: "orders",
      stage: "executed",
      duration_ms: 42,
      status: "ok",
      compiled_sql: 'SELECT "region", SUM(amount) FROM "orders" WHERE ("region" = \'west\')',
      columns: ["region", "revenue"],
      rows: [["west", 100]],
      row_count: 1,
    };
    mockAskAndExamples(askResponse);

    const user = userEvent.setup();
    render(<QueryWorkbench />);

    const textarea = screen.getByPlaceholderText(/top 5 region/i);
    await user.type(textarea, "top 5 region by gross revenue");
    await user.click(screen.getByRole("button", { name: /run/i }));

    // Preview pane shows the pattern name + compiled SQL
    await waitFor(() => {
      expect(screen.getByText(/top_n_by_metric/)).toBeInTheDocument();
    });
    // The full SQL is inside a <pre>; use a function matcher so quoted
    // identifiers don't break substring matches across text nodes.
    expect(
      screen.getByText(
        (_, el) => el?.tagName === "PRE" && el.textContent?.includes("SUM(amount)") === true,
      ),
    ).toBeInTheDocument();
    expect(
      screen.getByText(
        (_, el) => el?.tagName === "PRE" && el.textContent?.includes("'west'") === true,
      ),
    ).toBeInTheDocument();

    // Result table renders the row + columns
    expect(screen.getByText("region")).toBeInTheDocument();
    expect(screen.getByText("revenue")).toBeInTheDocument();
    expect(screen.getByText("west")).toBeInTheDocument();
    expect(screen.getByText("100")).toBeInTheDocument();
  });

  it('shows the "no pattern matched" hint when matched=false', async () => {
    mockAskAndExamples({
      matched: false,
      hint: "No pattern matched your question. Try a simpler shape.",
      prompt: "what is the meaning of life",
      source_id: "orders",
    });

    const user = userEvent.setup();
    render(<QueryWorkbench />);
    await user.type(screen.getByPlaceholderText(/top 5 region/i), "what is the meaning of life");
    await user.click(screen.getByRole("button", { name: /run/i }));

    // "No pattern matched" renders in BOTH the preview pane and the
    // result pane (the preview says "nothing to compile", the result
    // shows the hint). Both are correct -- assert at least one match.
    await waitFor(() => {
      expect(screen.getAllByText(/no pattern matched/i).length).toBeGreaterThanOrEqual(1);
    });
    expect(screen.getByText(/try a simpler shape/i)).toBeInTheDocument();
  });

  it("shows a denial banner when status=denied", async () => {
    mockAskAndExamples({
      matched: true,
      pattern_name: "single_metric",
      matched_text: "gross revenue",
      intent: {},
      source_id: "orders",
      stage: "executed",
      duration_ms: 5,
      status: "denied",
      deny_reason: "column_not_allowed",
      deny_detail: "amount is PII for this role",
      compiled_sql: "SELECT amount FROM orders",
    });
    const user = userEvent.setup();
    render(<QueryWorkbench />);
    await user.type(screen.getByPlaceholderText(/top 5 region/i), "gross revenue");
    await user.click(screen.getByRole("button", { name: /run/i }));

    await waitFor(() => {
      expect(screen.getByText(/denied/i)).toBeInTheDocument();
    });
    expect(screen.getByText(/amount is PII/i)).toBeInTheDocument();
  });

  it("surfaces a network error in the preview pane", async () => {
    vi.stubGlobal("fetch", vi.fn().mockImplementation((url: string) => {
      if (url === "/api/ask") {
        return Promise.resolve({
          ok: false,
          status: 500,
          statusText: "boom",
          json: async () => ({ detail: "engine exploded" }),
        });
      }
      return Promise.resolve({ ok: true, json: async () => ({ examples: [] }) });
    }));

    const user = userEvent.setup();
    render(<QueryWorkbench />);
    await user.type(screen.getByPlaceholderText(/top 5 region/i), "anything");
    await user.click(screen.getByRole("button", { name: /run/i }));

    await waitFor(() => {
      expect(screen.getByText(/engine exploded/i)).toBeInTheDocument();
    });
  });
});
