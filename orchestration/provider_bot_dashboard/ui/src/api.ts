export type TaskState = "proposed" | "accepted" | "in_progress" | "in_review" | "ready" | "blocked" | "completed" | "dismissed";
export const STATES: TaskState[] = ["proposed", "accepted", "in_progress", "in_review", "ready", "blocked", "completed", "dismissed"];
export type Task = {
  id: string; title: string; category: string; priority: number; state: TaskState;
  assignee_kind?: string | null; assignee_name?: string | null; assignee_profile?: string | null;
  reviewer_required?: boolean; blocked_from_state?: TaskState | null;
  planned_resolution: string; version: number; next_actor?: string; updated_at: string;
};
export type BotHealth = {
  bot: string; outcome: string; retry_class?: string | null; reason_code: string;
  failure_fingerprint?: string | null; duration_ms?: number | null; total_tokens?: number | null;
  deadline_consumed_ms?: number | null; useful_age_seconds?: number | null;
  freshness: "fresh" | "stale" | "missing" | "failed" | "not_due" | string;
  freshness_sla_minutes?: number | null; deadline_at?: string | null;
};
export type LifecycleDurations = Record<string, number | null | { count: number; total_ms: number; average_ms: number | null }>;
export type RunAttempt = {
  ordinal?: number; alias?: string; provider?: string; started_at?: string;
  finished_at?: string; duration_ms?: number; outcome?: string; reason_code?: string;
  fallback_used?: boolean; input_tokens?: number | null; output_tokens?: number | null; total_tokens?: number | null;
  requests?: number; cached_input_tokens?: number | null; cache_write_tokens?: number | null;
  reasoning_tokens?: number | null; cost_micro_usd?: number | null;
  cost_source?: "provider_reported" | "price_book" | "unavailable" | null; pricing_id?: string | null;
};
export type RunUsage = {
  requests: number; input_tokens: number | null; output_tokens: number | null;
  cached_input_tokens: number | null; cache_write_tokens: number | null;
  reasoning_tokens: number | null; total_tokens: number | null; cost_micro_usd: number | null;
  cost_source: "provider_reported" | "price_book" | "unavailable" | null; pricing_id: string | null;
};
export type RunProjection = {
  run_id: string; task_id: string; map_index?: number; try_number: number;
  outcome: string; retry_class: string; reason_code: string; duration_ms: number; deadline_consumed_ms?: number | null;
  deadline_at: string; started_at: string; finished_at: string;
  attempts: RunAttempt[]; attempt_count: number;
  input_tokens?: number | null; output_tokens?: number | null; total_tokens?: number | null;
  requests?: number; cached_input_tokens?: number | null; cache_write_tokens?: number | null;
  reasoning_tokens?: number | null; cost_micro_usd?: number | null;
  cost_source?: "provider_reported" | "price_book" | "unavailable" | null; pricing_id?: string | null;
  model?: string | null; report_schema?: string | null; report_sha256: string; report_bytes: number;
  freshness: string; freshness_sla_minutes?: number | null;
  artifact_digests: string[]; verification_digest?: string | null;
  pr: { provider?: string | null; repository?: string | null; branch?: string | null; number?: number | null; url?: string | null; base_sha?: string | null; head_sha?: string | null };
  review: { verdict?: string | null; commented_at?: string | null };
  merge_state: string; merged_at?: string | null; completed_at?: string | null;
  terminal_reason_code?: string | null; terminal_failure_class?: string | null;
  failure?: { class?: string | null; code?: string | null; fingerprint?: string | null; detail?: string } | null;
  payload?: unknown;
};

export type UsageCounters = {
  requests: number; input_tokens: number; output_tokens: number; cached_input_tokens: number;
  cache_write_tokens: number; reasoning_tokens: number; total_tokens: number; cost_micro_usd: number;
  runs: number; priced_runs: number; unpriced_runs: number;
};
export type UsageSummary = {
  days: number; currency: "USD"; generated_at: string; totals: UsageCounters;
  by_model: (UsageCounters & { model: string })[];
  by_bot: (UsageCounters & { bot: string })[];
  by_day: (UsageCounters & { date: string })[];
  cap: { daily_spend_cap_micro_usd: number | null; spent_today_micro_usd: number; exceeded: boolean };
};
export type BotSummary = {
  name: string; dag_id: string; paused?: boolean | null; latest_status: string;
  latest_reason_code: string; latest_at?: string | null; latest?: RunProjection | null;
};
export type Overview = {
  manager: { status: string; reason_code?: string; age_seconds?: number; executive_summary?: string; stale?: boolean };
  counts: Record<string, number>; actions: Task[]; bot_health: BotHealth[];
  queue: Record<string, number>; queue_depth?: number; lifecycle_durations_ms?: LifecycleDurations;
  provider_cache_age_seconds?: number | null;
};

export function prefix(): string {
  // Airflow dynamically imports bundles, so they are not document.scripts.
  return new URL(document.querySelector("base")?.href || "/", location.origin).pathname.replace(/\/$/, "");
}

export function activityUrl(task?: string): string {
  return `${prefix()}/plugin/bot-activity${task ? `?task=${encodeURIComponent(task)}` : ""}`;
}

export type QueueSummary = { attention_count: number; active_count: number; counts: Record<string, number> };
export const ATTENTION_STATES = ["proposed", "accepted", "blocked", "in_review", "ready"];
export function nextStep(task: Task): string {
  return ({ proposed: "Review recommendation", accepted: "Assign and start work", blocked: "Resolve blocker",
    in_review: "Review the pull request", ready: "Verify and complete", in_progress: "Work in progress",
    completed: "Completed", dismissed: "Archived" })[task.state];
}
export function humanTitle(value: string): string {
  const legacy = value.match(/^Review quarantined legacy package ([a-z_]+)-(\d{8})T/);
  if (legacy) return `Review historical ${label(legacy[1]).toLowerCase()} output (${legacy[2].slice(0, 4)}-${legacy[2].slice(4, 6)}-${legacy[2].slice(6, 8)})`;
  return value.replace(/\b(?:extract__|fetch_)([a-z0-9_]+)(?:\.py)?/g, (_, name: string) => label(name))
    .replace(/\b[a-z][a-z0-9]*(?:_[a-z0-9]+)+\b/g, (name) => label(name));
}
export function relativeTime(value?: string | null): string {
  if (!value) return "Not yet run";
  const seconds = Math.max(0, (Date.now() - Date.parse(value)) / 1000);
  if (!Number.isFinite(seconds)) return "Unknown time";
  if (seconds < 60) return "Just now";
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
  if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ago`;
  return `${Math.floor(seconds / 86400)}d ago`;
}

export async function request<T>(path: string, init: RequestInit = {}): Promise<T> {
  const response = await fetch(`${prefix()}/bot-dashboard/api${path}`, {
    ...init,
    credentials: "same-origin",
    headers: { "Accept": "application/json", ...(init.body ? { "Content-Type": "application/json" } : {}), ...init.headers },
  });
  if (!response.ok) {
    const error = await response.json().catch(() => ({ detail: `HTTP ${response.status}` }));
    throw Object.assign(new Error(typeof error.detail === "string" ? error.detail : error.detail ? "The request was not accepted. Check the entered values." : error.code || `Request failed (HTTP ${response.status})`), { status: response.status });
  }
  return response.json() as Promise<T>;
}

export function label(value: string): string {
  if (value === "pr_reviewer") return "PR reviewer";
  if (value === "dismissed") return "Archived";
  return (value || "Unknown").replaceAll("_", " ").replace(/^./, (letter) => letter.toUpperCase());
}
