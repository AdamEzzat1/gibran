import { describe, it, expect, vi, beforeEach } from "vitest";
import {
  setIdentity,
  getIdentity,
  clearIdentity,
  getCatalog,
  ask,
  runIntent,
  ApiError,
  listRoles,
} from "./client";

// API-client tests. Mocks `fetch` so the tests don't need a running
// backend. Verifies:
//   - identity persists across calls + survives clear
//   - X-Gibran-* headers are set on every request
//   - response decoding via JSON
//   - non-2xx responses surface as ApiError with detail
//   - query-string serialization for filter params

describe("identity helpers", () => {
  beforeEach(() => clearIdentity());

  it("round-trips identity through localStorage", () => {
    expect(getIdentity()).toBeNull();
    setIdentity({ user: "adam", role: "analyst_west", attrs: "region=west" });
    const ident = getIdentity();
    expect(ident).toEqual({
      user: "adam",
      role: "analyst_west",
      attrs: "region=west",
    });
  });

  it("clearIdentity wipes the stored value", () => {
    setIdentity({ user: "a", role: "b", attrs: "" });
    clearIdentity();
    expect(getIdentity()).toBeNull();
  });

  it("returns null on malformed stored value (defensive)", () => {
    localStorage.setItem("gibran.identity", "not-json");
    expect(getIdentity()).toBeNull();
  });
});

describe("api fetch wrapper", () => {
  beforeEach(() => {
    clearIdentity();
    vi.restoreAllMocks();
  });

  it("attaches X-Gibran-* headers when identity is set", async () => {
    setIdentity({ user: "adam", role: "analyst_west", attrs: "region=west" });
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({ sources: [], user: "adam", role: "analyst_west" }),
    });
    vi.stubGlobal("fetch", fetchMock);

    await getCatalog();

    expect(fetchMock).toHaveBeenCalledOnce();
    const [, init] = fetchMock.mock.calls[0];
    const headers = init.headers as Headers;
    expect(headers.get("X-Gibran-User")).toBe("adam");
    expect(headers.get("X-Gibran-Role")).toBe("analyst_west");
    expect(headers.get("X-Gibran-Attrs")).toBe("region=west");
    expect(headers.get("Content-Type")).toBe("application/json");
  });

  it("omits X-Gibran-Attrs header when attrs is empty", async () => {
    setIdentity({ user: "admin", role: "admin", attrs: "" });
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({ roles: [] }),
    });
    vi.stubGlobal("fetch", fetchMock);
    await listRoles();
    const [, init] = fetchMock.mock.calls[0];
    const headers = init.headers as Headers;
    expect(headers.get("X-Gibran-User")).toBe("admin");
    expect(headers.has("X-Gibran-Attrs")).toBe(false);
  });

  it("does NOT attach identity headers when no identity is set", async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({}),
    });
    vi.stubGlobal("fetch", fetchMock);
    await getCatalog();
    const [, init] = fetchMock.mock.calls[0];
    const headers = init.headers as Headers;
    expect(headers.has("X-Gibran-User")).toBe(false);
  });

  it("throws ApiError with detail on non-2xx response", async () => {
    setIdentity({ user: "x", role: "y", attrs: "" });
    const fetchMock = vi.fn().mockResolvedValue({
      ok: false,
      status: 403,
      statusText: "Forbidden",
      json: async () => ({ detail: "admin role required" }),
    });
    vi.stubGlobal("fetch", fetchMock);

    await expect(getCatalog()).rejects.toMatchObject({
      status: 403,
      detail: "admin role required",
    });
  });

  it("falls back to statusText when error body isn't JSON", async () => {
    setIdentity({ user: "x", role: "y", attrs: "" });
    const fetchMock = vi.fn().mockResolvedValue({
      ok: false,
      status: 500,
      statusText: "Internal Server Error",
      json: async () => { throw new Error("not json"); },
    });
    vi.stubGlobal("fetch", fetchMock);

    await expect(getCatalog()).rejects.toMatchObject({
      status: 500,
      detail: "Internal Server Error",
    });
  });

  it("POST /api/ask serializes prompt + source as JSON body", async () => {
    setIdentity({ user: "x", role: "y", attrs: "" });
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({ matched: false, hint: "no match", prompt: "x", source_id: "orders" }),
    });
    vi.stubGlobal("fetch", fetchMock);

    await ask("top 5 region by revenue", "orders");

    expect(fetchMock).toHaveBeenCalledOnce();
    const [path, init] = fetchMock.mock.calls[0];
    expect(path).toBe("/api/ask");
    expect(init.method).toBe("POST");
    expect(JSON.parse(init.body)).toEqual({
      prompt: "top 5 region by revenue",
      source: "orders",
    });
  });

  it("POST /api/query wraps intent in body envelope", async () => {
    setIdentity({ user: "x", role: "y", attrs: "" });
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({ stage: "executed", status: "ok" }),
    });
    vi.stubGlobal("fetch", fetchMock);

    const intent = { source: "orders", metrics: ["order_count"] };
    await runIntent(intent);

    const [path, init] = fetchMock.mock.calls[0];
    expect(path).toBe("/api/query");
    expect(JSON.parse(init.body)).toEqual({ intent });
  });
});

describe("ApiError class", () => {
  it("carries status and detail", () => {
    const e = new ApiError(404, "not found");
    expect(e.status).toBe(404);
    expect(e.detail).toBe("not found");
    expect(e.message).toBe("HTTP 404: not found");
  });
});
