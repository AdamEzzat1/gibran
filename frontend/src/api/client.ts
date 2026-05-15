// Typed API client for the gibran UI HTTP backend.
//
// Reads identity from localStorage and injects X-Gibran-* headers on
// every request. The dev-mode identity is set via the IdentitySetup
// component at first launch; later, a JWT-mode replacement would read
// from a session token instead.

export interface Identity {
  user: string;
  role: string;
  attrs: string;
}

const IDENTITY_KEY = "gibran.identity";

export function getIdentity(): Identity | null {
  const raw = localStorage.getItem(IDENTITY_KEY);
  if (!raw) return null;
  try {
    return JSON.parse(raw) as Identity;
  } catch {
    return null;
  }
}

export function setIdentity(ident: Identity): void {
  localStorage.setItem(IDENTITY_KEY, JSON.stringify(ident));
}

export function clearIdentity(): void {
  localStorage.removeItem(IDENTITY_KEY);
}

class ApiError extends Error {
  constructor(public status: number, public detail: string) {
    super(`HTTP ${status}: ${detail}`);
  }
}

async function apiFetch(
  path: string,
  init: RequestInit = {},
): Promise<unknown> {
  const ident = getIdentity();
  const headers = new Headers(init.headers);
  if (ident) {
    headers.set("X-Gibran-User", ident.user);
    headers.set("X-Gibran-Role", ident.role);
    if (ident.attrs) headers.set("X-Gibran-Attrs", ident.attrs);
  }
  headers.set("Content-Type", "application/json");
  const res = await fetch(path, { ...init, headers });
  if (!res.ok) {
    let detail = res.statusText;
    try {
      const body = await res.json();
      detail = (body as { detail?: string }).detail ?? detail;
    } catch {
      // body wasn't JSON; use statusText
    }
    throw new ApiError(res.status, detail);
  }
  return res.json();
}

// ---------------------------------------------------------------------------
// Response types -- mirror the FastAPI endpoint return shapes
// ---------------------------------------------------------------------------

export interface Column {
  name: string;
  display_name?: string;
  data_type: string;
  sensitivity: string;
  description?: string | null;
  example_values?: string[] | null;
}
export interface Dimension {
  id: string;
  column: string;
  display_name: string;
  type: string;
  description?: string | null;
}
export interface Metric {
  id: string;
  display_name: string;
  type: string;
  unit?: string | null;
  description?: string | null;
  depends_on: string[];
}
export interface SourceSchema {
  source_id: string;
  display_name: string;
  columns: Column[];
  dimensions: Dimension[];
  metrics: Metric[];
}
export interface CatalogResponse {
  sources: SourceSchema[];
  user: string;
  role: string;
}

export interface QueryResult {
  stage: string;
  duration_ms?: number;
  status?: "ok" | "denied" | "error";
  compiled_sql?: string;
  columns?: string[];
  rows?: unknown[][];
  row_count?: number;
  error?: string;
  deny_reason?: string | null;
  deny_detail?: string | null;
}

export interface AskResult extends QueryResult {
  matched: boolean;
  pattern_name?: string;
  matched_text?: string;
  intent?: unknown;
  source_id?: string;
  hint?: string;
}

export interface LogEntry {
  query_id: string;
  user_id: string;
  role_id: string;
  nl_prompt: string | null;
  generated_sql: string;
  status: string;
  deny_reason: string | null;
  row_count: number | null;
  duration_ms: number | null;
  is_break_glass: boolean;
  created_at: string | null;
}

export interface LogResponse {
  rows: LogEntry[];
  next_cursor: string | null;
  scoped_to_self: boolean;
}

// ---------------------------------------------------------------------------
// Typed endpoint wrappers
// ---------------------------------------------------------------------------

export async function getCatalog(): Promise<CatalogResponse> {
  return apiFetch("/api/catalog") as Promise<CatalogResponse>;
}

export async function describeSource(sourceId: string): Promise<SourceSchema> {
  return apiFetch(`/api/describe/${encodeURIComponent(sourceId)}`) as Promise<SourceSchema>;
}

export async function ask(prompt: string, source?: string): Promise<AskResult> {
  return apiFetch("/api/ask", {
    method: "POST",
    body: JSON.stringify({ prompt, source }),
  }) as Promise<AskResult>;
}

export async function runIntent(intent: unknown): Promise<QueryResult> {
  return apiFetch("/api/query", {
    method: "POST",
    body: JSON.stringify({ intent }),
  }) as Promise<QueryResult>;
}

export async function explainIntent(intent: unknown): Promise<QueryResult & { schema_preview?: unknown }> {
  return apiFetch("/api/explain", {
    method: "POST",
    body: JSON.stringify({ intent }),
  }) as Promise<QueryResult & { schema_preview?: unknown }>;
}

export async function getLog(params: {
  limit?: number;
  cursor?: string | null;
  user_id?: string;
  role_id?: string;
  status?: string;
} = {}): Promise<LogResponse> {
  const q = new URLSearchParams();
  if (params.limit) q.set("limit", String(params.limit));
  if (params.cursor) q.set("cursor", params.cursor);
  if (params.user_id) q.set("user_id", params.user_id);
  if (params.role_id) q.set("role_id", params.role_id);
  if (params.status) q.set("status", params.status);
  const qs = q.toString();
  return apiFetch(`/api/log${qs ? "?" + qs : ""}`) as Promise<LogResponse>;
}

export interface HealthResponse {
  source_id: string;
  status: "healthy" | "warn" | "block" | "unknown";
  blocking_failures: unknown[];
  warnings: unknown[];
  refreshed_at: string | null;
  recent_runs?: {
    run_id: string;
    rule_id: string;
    rule_kind: string;
    passed: boolean;
    observed_value: unknown;
    ran_at: string | null;
  }[];
  note?: string;
}

export async function getHealth(sourceId: string): Promise<HealthResponse> {
  return apiFetch(`/api/health/${encodeURIComponent(sourceId)}`) as Promise<HealthResponse>;
}

export interface PolicyPreviewSource {
  source_id: string;
  display_name?: string;
  columns?: Column[];
  metrics?: Metric[];
  dimensions?: Dimension[];
  denied?: boolean;
  reason?: string;
}
export interface PolicyResponse {
  target_role: string;
  target_role_display_name: string;
  target_role_attributes: Record<string, string>;
  previews: PolicyPreviewSource[];
  viewed_by: string;
}

export async function getPolicy(roleId: string): Promise<PolicyResponse> {
  return apiFetch(`/api/policy/${encodeURIComponent(roleId)}`) as Promise<PolicyResponse>;
}

export interface Role {
  id: string;
  display_name: string;
  is_break_glass: boolean;
}

export async function listRoles(): Promise<{ roles: Role[] }> {
  return apiFetch("/api/roles") as Promise<{ roles: Role[] }>;
}

export interface ExamplesResponse {
  examples: { question: string; pattern: string }[];
  source_id: string | null;
}

export async function getExamples(sourceId?: string): Promise<ExamplesResponse> {
  const qs = sourceId ? `?source_id=${encodeURIComponent(sourceId)}` : "";
  return apiFetch(`/api/examples${qs}`) as Promise<ExamplesResponse>;
}

export { ApiError };
