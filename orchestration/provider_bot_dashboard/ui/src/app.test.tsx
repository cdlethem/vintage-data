// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { ChakraProvider, defaultSystem } from "@chakra-ui/react";
import { render, screen, waitFor, fireEvent } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import Plugin from "./app";
import { SafeTree } from "./safe-tree";

function nested(depth: number): unknown {
  let value: unknown = "leaf";
  for (let index = 0; index < depth; index += 1) value = { child: value };
  return value;
}

describe("bounded hostile output rendering", () => {
  it("renders HTML and javascript text literally without executable elements", () => {
    const hostile = {
      title: '<img src=x onerror="globalThis.pwned=1">',
      url: "javascript:alert(1)",
      markdown: "<script>globalThis.pwned=1</script>",
    };
    const container = render(
      <ChakraProvider value={defaultSystem}><SafeTree value={hostile} /></ChakraProvider>,
    ).container;
    expect(container.querySelector("img,script,iframe,object,embed,svg")).toBeNull();
    expect(screen.getByText(hostile.title)).toBeInTheDocument();
    expect(screen.getByText(hostile.url)).toBeInTheDocument();
  });

  it("stops rendering deeply nested output", () => {
    render(<ChakraProvider value={defaultSystem}><SafeTree value={nested(20)} /></ChakraProvider>);
    expect(screen.getByText("[Nested data omitted]")).toBeInTheDocument();
  });
});

describe("dashboard lifecycle evidence", () => {
  it("renders specialist freshness, immutable execution identity, and human-only controls", async () => {
    const task = {
      id: "task-1", title: "Update fixture", category: "reliability", priority: 1,
      state: "ready", planned_resolution: "Update one allowed file", version: 9,
      updated_at: "2026-01-01T00:00:00Z", reviewer_required: true,
    };
    const run = {
      run_id: "run-1", task_id: "task-1", try_number: 1, outcome: "succeeded",
      retry_class: "none", reason_code: "ok", duration_ms: 321, deadline_at: "2026-01-01T00:10:00Z",
      started_at: "2026-01-01T00:00:01Z", finished_at: "2026-01-01T00:05:22Z",
      attempts: [{ ordinal: 1, alias: "fixture", provider: "fake", duration_ms: 321, outcome: "succeeded", reason_code: "ok", total_tokens: 42 }],
      attempt_count: 1, input_tokens: 20, output_tokens: 22, total_tokens: 42,
      report_sha256: "a".repeat(64), report_bytes: 300, freshness: "fresh", freshness_sla_minutes: 390,
      artifact_digests: ["b".repeat(64)], verification_digest: "c".repeat(64),
      pr: { provider: "github", repository: "fixture/project", branch: "bot-dashboard/task-1/1-r1", number: 7, url: "https://github.invalid/pr/7", base_sha: "d".repeat(40), head_sha: "e".repeat(40) },
      review: { verdict: "approved", commented_at: "2026-01-01T00:06:00Z" },
      merge_state: "merged", merged_at: "2026-01-01T00:07:00Z", completed_at: "2026-01-01T00:08:00Z",
      payload: { status: "ok" },
    };
    const detail = {
      ...task,
      revisions: [{ revision_number: 1, allowed_path_globs: ["*.txt"], verification_commands: [["python", "-c", "print('ok')"]] }],
      events: [{ sequence: 1, event_type: "change_published" }],
      executions: [{ sequence: 1, stage: "published", pr_number: 7, review_verdict: "approved" }],
      lifecycle_durations_ms: { proposal_to_acceptance: 10, execution_to_pr: 20, review_to_merge: 30 },
    };
    const bots = [
      { name: "source_vetting", dag_id: "bot__source_vetting", latest_status: "succeeded", latest_reason_code: "ok", latest: run },
      { name: "source_scheduling", dag_id: "bot__source_scheduling", latest_status: "succeeded", latest_reason_code: "stale_evidence", latest: { ...run, freshness: "stale", freshness_sla_minutes: 420 } },
      { name: "failure_triage", dag_id: "bot__failure_triage", latest_status: "failed", latest_reason_code: "provider_timeout", latest: { ...run, outcome: "timed_out", freshness: "failed", freshness_sla_minutes: 90 } },
      { name: "analytics_engineer", dag_id: "bot__analytics_engineer", latest_status: "missing", latest_reason_code: "missing_report", latest: null },
      { name: "task_executor_no_change", dag_id: "bot__task_executor", latest_status: "skipped", latest_reason_code: "no_change", latest: { ...run, outcome: "skipped", reason_code: "no_change", freshness: "fresh" } },
      { name: "blocked_worker", dag_id: "bot__task_executor", latest_status: "blocked", latest_reason_code: "policy_blocked", latest: { ...run, outcome: "failed", reason_code: "policy_blocked", freshness: "failed" } },
    ];
    const response = (body: unknown) => new Response(JSON.stringify(body), { status: 200, headers: { "Content-Type": "application/json" } });
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url.includes("/capabilities")) return response({ write_enabled: true, executor_enabled: true });
      if (url.includes("/tasks/task-1")) return response(detail);
      if (url.includes("/tasks?")) return response({ items: [task], next_cursor: null, total: 1 });
      if (url.endsWith("/bots")) return response({ items: bots });
      if (url.endsWith("/bots/source_vetting/runs")) return response({ items: [run] });
      if (url.includes("/bots/source_vetting/runs/run-1")) return response(run);
      throw new Error(`unexpected fixture request: ${url}`);
    });
    vi.stubGlobal("fetch", fetchMock);

    render(<ChakraProvider value={defaultSystem}><Plugin /></ChakraProvider>);
    await waitFor(() => expect(screen.getByText("Bot activity")).toBeInTheDocument());
    fireEvent.click(screen.getByRole("button", { name: "Bots" }));
    await waitFor(() => expect(screen.getByRole("region", { name: "Bot evidence" })).toBeInTheDocument());
    expect(screen.getAllByText(/Evidence: Fresh · SLA 390m/).length).toBeGreaterThan(0);
    expect(screen.getByText(/Evidence: Stale · SLA 420m/)).toBeInTheDocument();
    expect(screen.getByText(/Evidence: Failed · SLA 90m/)).toBeInTheDocument();
    expect(screen.getByText(/No recorded result/)).toBeInTheDocument();
    expect(screen.getByText(/No change/)).toBeInTheDocument();
    expect(screen.getByText(/Policy blocked/)).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Open bot source_vetting" }));
    await waitFor(() => expect(screen.getByRole("region", { name: "Bot runs" })).toBeInTheDocument());
    fireEvent.click(screen.getByRole("button", { name: "Open run run-1" }));
    await waitFor(() => expect(screen.getByRole("region", { name: "Run evidence detail" })).toBeInTheDocument());
    expect(screen.getByText(/Freshness: Fresh · SLA: 390 minutes/)).toBeInTheDocument();
    expect(screen.getByText(/Deadline: 2026-01-01T00:10:00Z · Duration: 321 ms · Attempts: 1/)).toBeInTheDocument();
    expect(screen.getByText(/Tokens: 42 \(20 in \/ 22 out\)/)).toBeInTheDocument();
    expect(screen.getByTitle("b".repeat(64))).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Copy artifact digest" })).toBeInTheDocument();
    expect(screen.getByText(/Pull request: #7 https:\/\/github.invalid\/pr\/7 · Head:/)).toBeInTheDocument();
    expect(screen.getByText(/Review: approved · Merge: Merged/)).toBeInTheDocument();
    expect(screen.getByText(/Completed at 2026-01-01T00:08:00Z/)).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Priority list" }));
    fireEvent.click(await screen.findByRole("button", { name: /Update fixture/ }));
    await waitFor(() => expect(screen.getByRole("region", { name: "Human merge and completion controls" })).toBeInTheDocument());
    expect(screen.getByRole("button", { name: "Complete task after human merge" })).toBeDisabled();
    fireEvent.click(screen.getByText("History and technical evidence"));
    expect(screen.getByRole("region", { name: "Task lifecycle latencies" })).toHaveTextContent("30 ms");
    expect(screen.queryByText(SECRET_FIXTURE)).not.toBeInTheDocument();
    expect(screen.queryByText("/tmp/fixture")).not.toBeInTheDocument();
  });
});


describe("token usage and spend", () => {
  it("renders totals, breakdowns, unpriced rows, cap state, empty periods, and period selection", async () => {
    const response = (body: unknown) => new Response(JSON.stringify(body), { status: 200, headers: { "Content-Type": "application/json" } });
    const counters = { requests: 3, input_tokens: 100, output_tokens: 40, cached_input_tokens: 80, cache_write_tokens: 5, reasoning_tokens: 4, total_tokens: 145, cost_micro_usd: 1200, runs: 2, priced_runs: 1, unpriced_runs: 1 };
    const usage = (days: number) => ({
      days, currency: "USD", generated_at: "2026-09-07T00:00:00Z", totals: counters,
      by_model: [
        { ...counters, model: "gpt-5.6-luna", priced_runs: 2, unpriced_runs: 0 },
        { ...counters, model: "unknown-model", cost_micro_usd: 0, priced_runs: 0, unpriced_runs: 2 },
      ],
      by_bot: [
        { ...counters, bot: "analytics_engineer" },
        { ...counters, bot: "fully_priced_bot", priced_runs: 2, unpriced_runs: 0 },
      ],
      by_day: days === 7 ? [] : [
        { ...counters, date: "2026-09-06" },
        { ...counters, date: "2026-09-07", cost_micro_usd: 0, priced_runs: 0, unpriced_runs: 2 },
      ],
      cap: days === 7 ? { daily_spend_cap_micro_usd: null, spent_today_micro_usd: 0, exceeded: false } : { daily_spend_cap_micro_usd: 1_500, spent_today_micro_usd: 1_200, exceeded: false },
    });
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url.includes("/tasks")) return response({ items: [], next_cursor: null, total: 0 });
      if (url.includes("/capabilities")) return response({ write_enabled: false, executor_enabled: false });
      if (url.includes("/usage?days=7")) return response(usage(7));
      if (url.includes("/usage")) return response(usage(30));
      throw new Error(`unexpected fixture request: ${url}`);
    });
    vi.stubGlobal("fetch", fetchMock);

    render(<ChakraProvider value={defaultSystem}><Plugin /></ChakraProvider>);
    fireEvent.click(screen.getAllByRole("button", { name: "Token usage and spend" }).at(-1)!);
    await waitFor(() => expect(screen.getByRole("region", { name: "Token usage and spend" })).toBeInTheDocument());
    expect(screen.getAllByText(/partial · 1 of 2 runs priced/).length).toBeGreaterThan(0);
    expect(screen.getAllByText("$0.0012").length).toBeGreaterThan(0);
    expect(screen.getByRole("region", { name: "Daily spend cap" })).toHaveTextContent("consumed");
    expect(screen.getByText("Fully priced bot")).toBeInTheDocument();
    expect(screen.getAllByText("not priced").length).toBeGreaterThan(0);
    expect(screen.getByText("$0.0012 of $0.0015 consumed")).toBeInTheDocument();
    fireEvent.change(screen.getByRole("combobox", { name: "Usage period" }), { target: { value: "7" } });
    await waitFor(() => expect(screen.getByText("No daily usage in this period")).toBeInTheDocument());
    expect(screen.getByText("No cap configured.")).toBeInTheDocument();
  });
});
const SECRET_FIXTURE = "fixture-secret-should-never-render";
