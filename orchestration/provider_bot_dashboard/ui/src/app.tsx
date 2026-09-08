import {
  Box, Button, Grid, HStack, Input, NativeSelect, Spinner, Stack, Table, Text, Textarea,
} from "@chakra-ui/react";
import { useEffect, useRef, useState } from "react";
import { BotSummary, LifecycleDurations, RunProjection, STATES, Task, TaskState, UsageSummary, QueueSummary, ATTENTION_STATES, activityUrl, humanTitle, nextStep, relativeTime, label, request } from "./api";
import { EmptyState, CodeValue, LIFECYCLE_PHASES, Metadata, Panel, SectionHeading, StatusBadge } from "./ui-kit";
import { SafeTree } from "./safe-tree";

type Page = { items: Task[]; next_cursor?: string | null; total: number };
type Capabilities = {
  write_enabled: boolean;
  executor_enabled: boolean;
  csrf_token?: string;
  reasons?: string[];
};
type Detail = Task & {
  revisions: Record<string, unknown>[];
  events: Record<string, unknown>[];
  executions: Record<string, unknown>[];
  lifecycle_durations_ms?: LifecycleDurations;
};


function ActivityApp() {
  const [view, setView] = useState<"priority" | "bots" | "usage">("priority");
  const [scope, setScope] = useState("attention");
  const [page, setPage] = useState<Page | null>(null);
  const [summary, setSummary] = useState<QueueSummary | null>(null);
  const [caps, setCaps] = useState<Capabilities | null>(null);
  const [selected, setSelected] = useState<Detail | null>(null);
  const [state, setState] = useState("");
  const [search, setSearch] = useState("");
  const [error, setError] = useState("");
  const [creating, setCreating] = useState(false);
  const [loading, setLoading] = useState(false);
  const [refreshKey, setRefreshKey] = useState(0);
  const generation = useRef(0);
  const states = state || (scope === "attention" ? ATTENTION_STATES.join(",") : scope === "active" ? [...ATTENTION_STATES, "in_progress"].join(",") : "completed,dismissed");
  const query = `/tasks?state=${encodeURIComponent(states)}&search=${encodeURIComponent(search)}`;
  const reload = () => setRefreshKey((value) => value + 1);
  const open = async (id: string) => {
    try {
      const detail = await request<Detail>(`/tasks/${encodeURIComponent(id)}`);
      setSelected(detail); setError("");
      history.replaceState(null, "", activityUrl(id));
    } catch (reason) { setError((reason as Error).message); }
  };
  const close = () => { setSelected(null); history.replaceState(null, "", activityUrl()); };
  const mutate = async <T,>(path: string, body: unknown, method = "POST"): Promise<T> => {
    try {
      const currentCaps = await request<Capabilities>("/capabilities");
      setCaps(currentCaps);
      const value = await request<T>(path, { method, headers: currentCaps.csrf_token ? { "X-Bot-Dashboard-CSRF": currentCaps.csrf_token } : {}, body: JSON.stringify(body) });
      reload(); return value;
    } catch (reason) {
      if ((reason as { status?: number }).status === 409 && selected) await open(selected.id);
      throw reason;
    }
  };
  useEffect(() => {
    const controller = new AbortController();
    const current = ++generation.current;
    setLoading(true); setError(""); setPage(null);
    const timer = setTimeout(() => {
      request<Page>(query, { signal: controller.signal }).then((value) => { if (current === generation.current) setPage(value); })
        .catch((reason) => { if (!controller.signal.aborted) setError(reason.message); })
        .finally(() => { if (!controller.signal.aborted) setLoading(false); });
    }, 200);
    return () => { controller.abort(); clearTimeout(timer); };
  }, [query, refreshKey]);
  useEffect(() => {
    request<QueueSummary>("/summary").then(setSummary).catch(() => setSummary(null));
  }, [refreshKey]);
  useEffect(() => { request<Capabilities>("/capabilities").then(setCaps).catch(() => setCaps(null)); }, []);
  useEffect(() => { const task = new URLSearchParams(location.search).get("task"); if (task) void open(task); }, []);
  const more = async () => {
    if (!page?.next_cursor || loading) return;
    const current = generation.current;
    setLoading(true);
    try {
      const next = await request<Page>(`${query}&cursor=${encodeURIComponent(page.next_cursor)}`);
      if (current === generation.current) setPage({ ...next, items: [...page.items, ...next.items] });
    } catch (reason) { setError((reason as Error).message); }
    finally { if (current === generation.current) setLoading(false); }
  };
  return <Stack padding={{ base: "4", md: "6" }} gap="5" maxW="1440px" marginX="auto" colorPalette="blue">
    <HStack justify="space-between" gap="4" flexWrap="wrap">
      <Box><Text as="h1" fontSize="2xl" fontWeight="semibold">Bot activity</Text><Text color="fg.muted" fontSize="sm" marginTop="1">Review recommendations, follow work, and check bot health.</Text></Box>
      <HStack><Button size="sm" variant="outline" onClick={reload}>Refresh</Button>{caps?.write_enabled && <Button size="sm" onClick={() => { close(); setCreating(true); }}>Create task</Button>}</HStack>
    </HStack>
    <HStack gap="1" borderBottomWidth="1px" borderColor="border" paddingBottom="2" role="navigation" aria-label="Bot activity views">
      <Button size="sm" variant="ghost" bg={view === "priority" ? "bg.emphasized" : undefined} color="fg" aria-label="Priority list" aria-pressed={view === "priority"} onClick={() => { setView("priority"); close(); }}>Action queue{summary ? ` (${summary.attention_count})` : ""}</Button>
      <Button size="sm" variant="ghost" bg={view === "bots" ? "bg.emphasized" : undefined} color="fg" aria-pressed={view === "bots"} onClick={() => { setView("bots"); close(); }}>Bots</Button>
      <Button size="sm" variant="ghost" bg={view === "usage" ? "bg.emphasized" : undefined} color="fg" aria-label="Token usage and spend" aria-pressed={view === "usage"} onClick={() => { setView("usage"); close(); }}>Usage</Button>
    </HStack>
    {caps && !caps.write_enabled && view === "priority" && <Text fontSize="sm" color="fg.muted">View only · Task editing is disabled for this installation.</Text>}
    {error && <Panel role="alert" borderColor="red.muted"><HStack justify="space-between"><Text color="red.fg">{error}</Text><Button size="sm" variant="outline" onClick={reload}>Retry</Button></HStack></Panel>}
    {view === "usage" ? <UsageTab key={refreshKey} /> : view === "bots" ? <BotsTab key={refreshKey} /> : creating ? <CreateTaskForm close={() => setCreating(false)} create={async (body) => { await mutate("/tasks", body); setCreating(false); reload(); }} /> : selected ?
      <TaskDetail key={`${selected.id}:${selected.version}`} task={selected} caps={caps} close={close} mutate={mutate} refresh={() => open(selected.id)} /> : <>
      <HStack gap="3" flexWrap="wrap" justify="space-between">
        <HStack gap="1" bg="bg.subtle" padding="1" borderRadius="lg" flexWrap="wrap">
          {[["attention", "Needs attention"], ["active", "All active"], ["history", "History"]].map(([value, title]) => <Button key={value} size="xs" variant="ghost" color="fg" bg={scope === value ? "bg.panel" : undefined} aria-pressed={scope === value} onClick={() => { setScope(value); setState(""); }}>{title}</Button>)}
        </HStack>
        <HStack gap="2" flexWrap="wrap">
          <Input width={{base:"100%",md:"16rem"}} size="sm" aria-label="Search tasks" placeholder="Search actions…" value={search} onChange={(event) => setSearch(event.target.value)} />
          <NativeSelect.Root width="10rem" size="sm"><NativeSelect.Field aria-label="Filter state" value={state} onChange={(event) => setState(event.target.value)}><option value="">All statuses</option>{STATES.filter((item) => scope === "history" ? ["completed", "dismissed"].includes(item) : scope === "attention" ? ATTENTION_STATES.includes(item) : !["completed", "dismissed"].includes(item)).map((item) => <option key={item} value={item}>{label(item)}</option>)}</NativeSelect.Field></NativeSelect.Root>
        </HStack>
      </HStack>
      <Box><Text fontSize="sm" color="fg.muted">{scope === "attention" ? "Recommendations and blockers awaiting a decision from you." : scope === "history" ? "Completed and archived work. Past evidence and decisions are retained." : "Open recommendations and work already underway."}</Text></Box>
      {!page && loading ? <HStack padding="8" justify="center"><Spinner size="sm" /><Text color="fg.muted">Loading actions…</Text></HStack> : page?.items.length === 0 ?
        <EmptyState title={search || state ? "No tasks match these filters" : scope === "history" ? "No past actions" : "You're all caught up"}>{search || state ? "Try a different status or search term." : scope === "history" ? "Completed and archived tasks will appear here." : "No actions need a decision right now. New recommendations will appear after the bots assess current evidence."}</EmptyState> : page &&
        <Panel padding="0" overflow="hidden"><Stack gap="0" role="list" aria-label="Actions">{page.items.map((task) => <Box key={task.id} role="listitem" borderBottomWidth="1px" borderColor="border">
          <Button variant="ghost" width="100%" height="auto" padding="4" whiteSpace="normal" textAlign="start" justifyContent="start" borderRadius="0" onClick={() => open(task.id)}>
            <HStack width="100%" align="start" gap="4"><StatusBadge value={`P${task.priority}`} tone={task.priority <= 1 ? "warning" : "neutral"} /><Stack gap="1" flex="1" minW="0">
              <Text fontWeight="semibold">{humanTitle(task.title)}</Text>
              <Text color="fg.muted" fontSize="sm" fontWeight="normal" lineClamp="2">{task.planned_resolution || nextStep(task)}</Text>
              <Text fontSize="xs" color="fg.muted" fontWeight="normal">{label(task.category)} · {task.assignee_name || (task.assignee_profile ? label(task.assignee_profile) : "Unassigned")} · {relativeTime(task.updated_at)}</Text>
            </Stack><Stack align="end" gap="2" display={{base:"none",sm:"flex"}}><StatusBadge value={label(task.state)} /><Text fontSize="xs" color="fg.muted" fontWeight="normal">{nextStep(task)}</Text></Stack><Text color="fg.muted">›</Text></HStack>
          </Button></Box>)}</Stack></Panel>}
      {page && page.total > 0 && <HStack justify="space-between"><Metadata>Showing {page.items.length} of {page.total} actions</Metadata>{page.next_cursor && <Button size="sm" variant="outline" disabled={loading} onClick={more}>{loading ? "Loading…" : "Load more"}</Button>}</HStack>}
    </>}
  </Stack>;
}
function formatCost(microUsd: number): string {
  return `$${(microUsd / 1_000_000).toFixed(4)}`;
}

function UsageCost({ row }: { row: UsageSummary["totals"] }) {
  if (row.priced_runs === 0) return <Text>not priced</Text>;
  const partial = row.unpriced_runs > 0;
  return <Stack gap="0">
    <Text>{formatCost(row.cost_micro_usd)}</Text>
    {partial && <Metadata>partial · {row.priced_runs} of {row.runs} runs priced</Metadata>}
  </Stack>;
}

function UsageTab() {
  const [period, setPeriod] = useState(30);
  const [usage, setUsage] = useState<UsageSummary | null>(null);
  const [error, setError] = useState("");
  useEffect(() => {
    setUsage(null); setError("");
    request<UsageSummary>(`/usage?days=${period}`).then(setUsage).catch((reason) => setError(reason.message));
  }, [period]);
  if (error) return <Panel role="alert" aria-label="Usage error"><Text color="red.fg">{error}</Text></Panel>;
  if (!usage) return <Spinner aria-label="Loading token usage" />;
  const cacheShare = usage.totals.total_tokens > 0
    ? Math.round((usage.totals.cached_input_tokens / usage.totals.total_tokens) * 100)
    : 0;
  const maxDay = Math.max(...usage.by_day.map((row) => row.total_tokens), 1);
  return <Stack role="region" aria-label="Token usage and spend" gap="5">
    <HStack justify="space-between" align="start" flexWrap="wrap" gap="3">
      <SectionHeading description="Provider-agnostic model calls, cached input, and spend from live bot runs.">Token usage and spend</SectionHeading>
      <NativeSelect.Root width="9rem"><NativeSelect.Field aria-label="Usage period" value={String(period)} onChange={(event) => setPeriod(Number(event.target.value))}>
        {[7, 30, 90].map((days) => <option key={days} value={days}>{days} days</option>)}
      </NativeSelect.Field></NativeSelect.Root>
    </HStack>
    <Grid templateColumns={{ base: "repeat(2, minmax(0, 1fr))", md: "repeat(4, minmax(0, 1fr))" }} gap="3" role="group" aria-label="Usage totals">
      <Panel><Metadata>Requests</Metadata><Text fontSize="xl" fontWeight="bold">{usage.totals.requests.toLocaleString()}</Text></Panel>
      <Panel><Metadata>Total tokens</Metadata><Text fontSize="xl" fontWeight="bold">{usage.totals.total_tokens.toLocaleString()}</Text></Panel>
      <Panel><Metadata>Spend</Metadata><Box fontSize="xl" fontWeight="bold"><UsageCost row={usage.totals} /></Box><Metadata>{usage.totals.runs} runs · {usage.totals.priced_runs} priced</Metadata></Panel>
      <Panel><Metadata>Cache-read share</Metadata><Text fontSize="xl" fontWeight="bold">{cacheShare}%</Text><Metadata>{usage.totals.cached_input_tokens.toLocaleString()} cached input tokens</Metadata></Panel>
    </Grid>
    <Panel role="region" aria-label="Daily spend cap">
      <SectionHeading description="Caps are enforced before a new run claim and never interrupt an active run.">Daily spend cap</SectionHeading>
      {usage.cap.daily_spend_cap_micro_usd == null || usage.cap.daily_spend_cap_micro_usd === 0
        ? <Text color="fg.muted">No cap configured.</Text>
        : <Stack gap="1"><Text>{formatCost(usage.cap.spent_today_micro_usd)} of {formatCost(usage.cap.daily_spend_cap_micro_usd)} consumed{usage.cap.exceeded ? " · cap reached" : ""}</Text><Box height="2" bg="bg.muted" borderRadius="full" overflow="hidden"><Box height="100%" width={`${Math.min(100, usage.cap.spent_today_micro_usd / usage.cap.daily_spend_cap_micro_usd * 100)}%`} bg={usage.cap.exceeded ? "red.solid" : "blue.solid"} /></Box></Stack>}
    </Panel>
    <Panel role="region" aria-label="Usage by model">
      <SectionHeading description="Reported provider model names, with unpriced rows called out explicitly.">By model</SectionHeading>
      {usage.by_model.length === 0 ? <EmptyState title="No model usage in this period">Run a bot to populate model accounting.</EmptyState> :
        <Box overflowX="auto"><Table.Root size="sm" variant="outline"><Table.Header><Table.Row><Table.ColumnHeader>Model</Table.ColumnHeader><Table.ColumnHeader>Requests</Table.ColumnHeader><Table.ColumnHeader>Total tokens</Table.ColumnHeader><Table.ColumnHeader>Spend</Table.ColumnHeader></Table.Row></Table.Header><Table.Body>{usage.by_model.map((row) => <Table.Row key={row.model}><Table.Cell>{row.model}</Table.Cell><Table.Cell>{row.requests.toLocaleString()}</Table.Cell><Table.Cell>{row.total_tokens.toLocaleString()}</Table.Cell><Table.Cell><UsageCost row={row} /></Table.Cell></Table.Row>)}</Table.Body></Table.Root></Box>}
    </Panel>
    <Panel role="region" aria-label="Usage by bot">
      <SectionHeading description="Usage attributed to each live specialist bot.">By bot</SectionHeading>
      {usage.by_bot.length === 0 ? <EmptyState title="No bot usage in this period">No live runs were recorded.</EmptyState> :
        <Box overflowX="auto"><Table.Root size="sm" variant="outline"><Table.Header><Table.Row><Table.ColumnHeader>Bot</Table.ColumnHeader><Table.ColumnHeader>Runs</Table.ColumnHeader><Table.ColumnHeader>Requests</Table.ColumnHeader><Table.ColumnHeader>Spend</Table.ColumnHeader></Table.Row></Table.Header><Table.Body>{usage.by_bot.map((row) => <Table.Row key={row.bot}><Table.Cell>{label(row.bot)}</Table.Cell><Table.Cell>{row.runs.toLocaleString()}</Table.Cell><Table.Cell>{row.requests.toLocaleString()}</Table.Cell><Table.Cell><UsageCost row={row} /></Table.Cell></Table.Row>)}</Table.Body></Table.Root></Box>}
    </Panel>
    <Panel role="region" aria-label="Daily token trend">
      <SectionHeading description="Token volume by UTC day.">Daily trend</SectionHeading>
      {usage.by_day.length === 0 ? <EmptyState title="No daily usage in this period">There is no token activity to chart.</EmptyState> :
        <Stack gap="2">{usage.by_day.map((row) => <HStack key={row.date} gap="3"><Text width="6rem" fontSize="sm" fontFamily="mono">{row.date}</Text><Box flex="1" height="2" bg="bg.muted" borderRadius="full" overflow="hidden"><Box height="100%" width={`${row.total_tokens / maxDay * 100}%`} bg="blue.solid" /></Box><Stack width="8rem" gap="0" alignItems="flex-end"><Text fontSize="sm">{row.total_tokens.toLocaleString()}</Text><UsageCost row={row} /></Stack></HStack>)}</Stack>}
    </Panel>
  </Stack>;
}

function CreateTaskForm({ close, create }: { close: () => void; create: (body: unknown) => Promise<void> }) {
  const [title, setTitle] = useState("");
  const [category, setCategory] = useState("other");
  const [priority, setPriority] = useState(1);
  const [plan, setPlan] = useState("");
  const [error, setError] = useState("");
  const [saving, setSaving] = useState(false);
  return <Panel role="dialog" aria-label="Create manual task" maxW="42rem" width="100%">
    <SectionHeading description="Add a human-owned action to the manager queue.">Create manual task</SectionHeading>
    <Stack gap="3">
      <Input aria-label="Title" placeholder="Task title" value={title} onChange={(event) => setTitle(event.target.value)} maxLength={200} />
      <NativeSelect.Root><NativeSelect.Field aria-label="Category" value={category} onChange={(event) => setCategory(event.target.value)}>{["reliability", "new_source", "cadence", "load", "storage", "architecture", "credential", "cost", "other"].map((value) => <option key={value} value={value}>{label(value)}</option>)}</NativeSelect.Field></NativeSelect.Root>
      <Input aria-label="Priority" type="number" min={1} value={priority} onChange={(event) => setPriority(Number(event.target.value))} />
      <Textarea aria-label="Planned resolution" placeholder="Planned resolution" value={plan} onChange={(event) => setPlan(event.target.value)} maxLength={20000} />
      {error && <Text role="alert" color="red.fg">{error}</Text>}
      <HStack><Button disabled={saving || !title.trim() || !plan.trim() || priority < 1 || !Number.isInteger(priority)} onClick={async () => { setSaving(true); setError(""); try { await create({ title: title.trim(), category, priority, planned_resolution: plan.trim(), evidence: [] }); } catch (reason) { setError((reason as Error).message); } finally { setSaving(false); } }}>{saving ? "Creating…" : "Create"}</Button><Button variant="outline" onClick={close}>Cancel</Button></HStack>
    </Stack>
  </Panel>;
}

function TaskDetail({ task, caps, close, mutate, refresh }: { task: Detail; caps: Capabilities | null; close: () => void; mutate: <T>(path: string, body: unknown, method?: string) => Promise<T>; refresh: () => Promise<void> | void }) {
  const [plan, setPlan] = useState(task.planned_resolution);
  const [target, setTarget] = useState<TaskState>(task.state);
  const [assignee, setAssignee] = useState("human");
  const transitions: Record<string, string[]> = { proposed: ["accepted", "dismissed", "blocked"], accepted: ["dismissed", "blocked"], in_progress: ["in_review", "ready", "blocked"], in_review: ["in_progress", "ready", "blocked"], ready: ["in_progress", "blocked"], blocked: [...(task.blocked_from_state ? [task.blocked_from_state] : []), "dismissed"], completed: ["accepted"], dismissed: ["accepted"] };
  const [reason, setReason] = useState("");
  const [comment, setComment] = useState("");
  const [evidence, setEvidence] = useState("");
  const latest = (task.revisions.at(-1) || {}) as Record<string, unknown>;
  const [verification, setVerification] = useState(JSON.stringify(latest.verification_commands || [], null, 2));
  const [globs, setGlobs] = useState(((latest.allowed_path_globs as string[] | undefined) || []).join("\n"));
  const [resources, setResources] = useState(((latest.resource_keys as string[] | undefined) || []).join("\n"));
  const [followUps, setFollowUps] = useState(((latest.follow_up_bots as string[] | undefined) || []).join("\n"));
  const [reviewerRequired, setReviewerRequired] = useState(task.reviewer_required !== false);
  const [policyError, setPolicyError] = useState("");
  const [saving, setSaving] = useState(false);
  const apply = async (path: string, body: unknown, method?: string) => {
    if (saving) return;
    setSaving(true); setPolicyError("");
    try { await mutate(path, body, method); await refresh(); }
    catch (reason) { setPolicyError((reason as Error).message); }
    finally { setSaving(false); }
  };
  return <Panel role="dialog" aria-label={`Task: ${task.title}`} maxW="64rem" width="100%" alignSelf="center">
    <HStack justify="space-between" align="start" gap="4"><Box><Text as="h2" fontSize="xl" fontWeight="bold">{humanTitle(task.title)}</Text><HStack marginTop="2" gap="2" flexWrap="wrap"><StatusBadge value={`P${task.priority}`} /><StatusBadge value={label(task.state)} /><Metadata>{label(task.category)} · {task.assignee_name || task.assignee_profile || "Unassigned"}</Metadata></HStack></Box><Button size="sm" variant="outline" onClick={close}>Close</Button></HStack>
    {policyError && <Text role="alert" color="red.fg" marginTop="3">{policyError}</Text>}
    <Stack marginTop="5" gap="4">
      {typeof latest.why_now === "string" && latest.why_now && <Box><SectionHeading>Why this needs attention</SectionHeading><Text fontSize="sm">{latest.why_now}</Text></Box>}
      {typeof latest.expected_benefit === "string" && latest.expected_benefit && <Box><SectionHeading>Expected outcome</SectionHeading><Text fontSize="sm">{latest.expected_benefit}</Text></Box>}
      <Panel bg="bg.subtle"><SectionHeading description="The resolution plan is the human-approved scope for this task.">Planned resolution</SectionHeading><Textarea readOnly={!caps?.write_enabled} value={plan} onChange={(event) => setPlan(event.target.value)} maxLength={20000} /></Panel>
      {caps?.write_enabled && <Panel role="region" aria-label="Human merge and completion controls">
        <SectionHeading description="These actions write to the task record and may require human approval.">Human actions</SectionHeading>
        <Stack gap="3">
          {task.state === "proposed" && <HStack><Button disabled={saving} onClick={() => apply(`/tasks/${task.id}/transition`, { version: task.version, state: "accepted" })}>Accept recommendation</Button><Metadata>Accepting does not start execution.</Metadata></HStack>}
          {task.state === "accepted" && <HStack flexWrap="wrap"><NativeSelect.Root width="12rem"><NativeSelect.Field aria-label="Assign work to" value={assignee} onChange={(event) => setAssignee(event.target.value)}><option value="human">Me</option><option value="junior">Junior bot</option><option value="senior">Senior bot</option><option value="staff">Staff bot</option></NativeSelect.Field></NativeSelect.Root><Button variant="outline" disabled={saving} onClick={() => apply(`/tasks/${task.id}/assign`, { version: task.version, kind: assignee === "human" ? "human" : "bot", profile: assignee === "human" ? null : assignee, reviewer_required: true })}>Assign</Button>
          {task.assignee_kind === "human" && <Button disabled={saving} onClick={() => apply(`/tasks/${task.id}/transition`, { version: task.version, state: "in_progress" })}>Start my work</Button>}
          {task.assignee_kind === "bot" && <Button disabled={saving || !caps.executor_enabled} onClick={() => apply(`/tasks/${task.id}/start`, { version: task.version, idempotency_key: `ui-${task.id}-${task.version}` })}>Start bot work</Button>}
          </HStack>}
          <HStack flexWrap="wrap"><NativeSelect.Root flex="1"><NativeSelect.Field aria-label="Target state" value={target} onChange={(event) => setTarget(event.target.value as TaskState)}>{[task.state, ...(transitions[task.state] || [])].map((value) => <option key={value} value={value}>{label(value)}</option>)}</NativeSelect.Field></NativeSelect.Root><Input flex="2" aria-label="Transition reason" placeholder="Reason when required" value={reason} onChange={(event) => setReason(event.target.value)} maxLength={20000} /><Button disabled={saving || target === task.state} aria-label="Apply human task transition" onClick={() => apply(`/tasks/${task.id}/transition`, { version: task.version, state: target, reason: reason || null })}>Move</Button></HStack>
          <HStack align="start"><Textarea aria-label="Human completion comment" placeholder="Completion comment" value={comment} onChange={(event) => setComment(event.target.value)} /><Textarea aria-label="Human verification evidence" placeholder="Evidence URL or reference" value={evidence} onChange={(event) => setEvidence(event.target.value)} /></HStack>
          {task.state === "ready" && <Button alignSelf="start" aria-label="Complete task after human merge" disabled={saving || !comment || !evidence} onClick={async () => { setSaving(true); try { await mutate(`/tasks/${task.id}/comments`, { version: task.version, comments: [comment] }); await mutate(`/tasks/${task.id}/evidence`, { version: task.version + 1, evidence: [{ label: "Human verification", url: evidence }] }); await mutate(`/tasks/${task.id}/transition`, { version: task.version + 2, state: "completed", reason: reason || "Human verified merge" }); await refresh(); } catch (reason) { setPolicyError((reason as Error).message); } finally { setSaving(false); } }}>Complete after human merge</Button>}
          {task.state !== "ready" && <Metadata>Completion is available after the task reaches Ready.</Metadata>}
          {task.state === "in_progress" && task.executions.some((value) => /no.?change/i.test(String(value.stage || value.terminal_reason_code || ""))) && <Text role="status" aria-label="No change execution state" color="green.fg">Execution made no repository change.</Text>}
        </Stack>
      </Panel>}
      {caps?.write_enabled && <Box as="details"><Box as="summary" cursor="pointer" fontWeight="semibold" paddingY="2">Execution settings</Box><Panel bg="bg.subtle">
        <SectionHeading description="Execution boundaries are submitted as structured policy, not free-form instructions.">Execution policy</SectionHeading>
        <Stack gap="3">
          <Text fontSize="sm" fontWeight="medium">Admitted verification argv (JSON array of arrays)</Text><Textarea aria-label="Verification commands" value={verification} onChange={(event) => setVerification(event.target.value)} />
          <Text fontSize="sm" fontWeight="medium">Allowed path globs (one per line)</Text><Textarea aria-label="Allowed path globs" value={globs} onChange={(event) => setGlobs(event.target.value)} />
          <Text fontSize="sm" fontWeight="medium">Resource keys (one per line)</Text><Textarea aria-label="Resource keys" value={resources} onChange={(event) => setResources(event.target.value)} />
          <Text fontSize="sm" fontWeight="medium">Follow-up bots (one per line)</Text><Textarea aria-label="Follow-up bots" value={followUps} onChange={(event) => setFollowUps(event.target.value)} />
          <HStack flexWrap="wrap">
            <Button size="sm" variant="outline" aria-pressed={reviewerRequired} onClick={() => setReviewerRequired(!reviewerRequired)}>Reviewer {reviewerRequired ? "required" : "optional"}</Button>
            <Button size="sm" onClick={async () => {
              try {
                const commands = JSON.parse(verification);
                setPolicyError("");
                await apply(`/tasks/${task.id}`, {
                  version: task.version, planned_resolution: plan, verification_commands: commands,
                  allowed_path_globs: globs.split("\n").map((value) => value.trim()).filter(Boolean),
                  resource_keys: resources.split("\n").map((value) => value.trim()).filter(Boolean),
                  follow_up_bots: followUps.split("\n").map((value) => value.trim()).filter(Boolean),
                  reviewer_required: reviewerRequired,
                }, "PATCH");
              } catch (reason) {
                setPolicyError(reason instanceof Error ? reason.message : String(reason));
              }
            }}>Save execution policy</Button>
          </HStack>
          {policyError && <Text role="alert" color="red.fg">{policyError}</Text>}
        </Stack>
      </Panel></Box>}
      <Box as="details"><Box as="summary" cursor="pointer" fontWeight="semibold" paddingY="2">History and technical evidence</Box>
      <Panel role="region" aria-label="Task lifecycle latencies">
        <SectionHeading description="Average elapsed time between workflow milestones.">Task lifecycle latencies</SectionHeading>
        <Stack as="ol" gap="2" margin="0" paddingLeft="5">{Object.entries(task.lifecycle_durations_ms || {}).sort(([a], [b]) => {
          const ai = LIFECYCLE_PHASES.indexOf(a as typeof LIFECYCLE_PHASES[number]);
          const bi = LIFECYCLE_PHASES.indexOf(b as typeof LIFECYCLE_PHASES[number]);
          return (ai < 0 ? LIFECYCLE_PHASES.length : ai) - (bi < 0 ? LIFECYCLE_PHASES.length : bi);
        }).map(([phase, value]) => {
          const average = typeof value === "object" && value !== null ? value.average_ms : value;
          return <Box as="li" key={phase}><HStack justify="space-between"><Metadata>{label(phase)}</Metadata><Text>{average == null ? "not observed" : `${average} ms`}</Text></HStack></Box>;
        })}</Stack>
      </Panel>
      <Panel><SectionHeading description="Structured recommendation output is bounded and rendered as text.">Recommendation revisions</SectionHeading><SafeTree value={task.revisions} /></Panel>
      <Panel><SectionHeading description="Execution identity and pull request metadata.">Execution and pull request</SectionHeading><SafeTree value={task.executions} /></Panel>
      <Panel><SectionHeading description="Human and system transitions for this task.">Audit history</SectionHeading><SafeTree value={task.events} /></Panel></Box>
    </Stack>
  </Panel>;
}

function RunEvidence({ run }: { run: RunProjection }) {
  const outcome = run.outcome === "skipped" && /no.?change/i.test(run.reason_code) ? "no_change" : run.outcome;
  return <Stack role="region" aria-label="Run evidence detail" gap="4">
    <Panel>
      <HStack justify="space-between" align="start" gap="3"><Box><Text as="h3" fontSize="lg" fontWeight="bold">{label(outcome)}</Text><Metadata>{label(run.reason_code)} · Attempt {run.try_number}</Metadata></Box><StatusBadge value={label(outcome)} /></HStack>
      {run.outcome === "blocked" && <Text role="status" color="orange.fg" marginTop="3">Blocked pending human action.</Text>}
      {outcome === "no_change" && <Text role="status" color="green.fg" marginTop="3">No repository change was required.</Text>}
    </Panel>
    <Panel>
      <SectionHeading description="Freshness, SLA and execution accounting for this projection.">Run accounting</SectionHeading>
      <Stack gap="1">
        <Text color="fg.muted">Freshness: {label(run.freshness)} · SLA: {run.freshness_sla_minutes == null ? "none" : `${run.freshness_sla_minutes} minutes`}</Text>
        <Text color="fg.muted">Deadline consumption: {run.deadline_consumed_ms == null ? "not reported" : `${run.deadline_consumed_ms} ms`}</Text>
        <Text color="fg.muted">Deadline: {run.deadline_at} · Duration: {run.duration_ms} ms · Attempts: {run.attempt_count}</Text>
        <Text color="fg.muted">Tokens: {run.total_tokens == null ? "unavailable" : `${run.total_tokens} (${run.input_tokens ?? 0} in / ${run.output_tokens ?? 0} out)`}</Text>
      </Stack>
    </Panel>
    <Panel role="region" aria-label="Run token usage">
      <SectionHeading description="Normalized usage reported by the provider for this run.">Token usage</SectionHeading>
      <Stack gap="1">
        <Text>Requests: {run.requests ?? 0} · Total tokens: {run.total_tokens == null ? "unavailable" : run.total_tokens.toLocaleString()}</Text>
        <Text color="fg.muted">Input: {run.input_tokens == null ? "unavailable" : run.input_tokens.toLocaleString()} · Output: {run.output_tokens == null ? "unavailable" : run.output_tokens.toLocaleString()} · Reasoning: {run.reasoning_tokens == null ? "unavailable" : run.reasoning_tokens.toLocaleString()}</Text>
        <Text color="fg.muted">Cache read: {run.cached_input_tokens == null ? "unavailable" : run.cached_input_tokens.toLocaleString()} · Cache write: {run.cache_write_tokens == null ? "unavailable" : run.cache_write_tokens.toLocaleString()}</Text>
        <Text color="fg.muted">Cost: {run.cost_source === "unavailable" || run.cost_micro_usd == null ? "not priced" : formatCost(run.cost_micro_usd)} · Source: {run.cost_source ?? "unavailable"}{run.pricing_id ? ` · ${run.pricing_id}` : ""}</Text>
      </Stack>
    </Panel>
    <Panel>
      <SectionHeading description="Immutable output references are shown as bounded text.">Artifact and verification identity</SectionHeading>
      <Stack gap="2" overflowX="auto">
        <HStack gap="2" flexWrap="nowrap" whiteSpace="nowrap"><Text fontSize="sm">Artifacts:</Text>{run.artifact_digests.length ? run.artifact_digests.map((digest) => <CodeValue key={digest} value={digest} label="artifact digest" copy />) : <Metadata>none</Metadata>}</HStack>
        <HStack gap="2" flexWrap="nowrap" whiteSpace="nowrap"><Text fontSize="sm">Verification:</Text><CodeValue value={run.verification_digest} label="verification digest" copy /></HStack>
        <HStack><Text as="span">Report:</Text><CodeValue value={run.report_sha256} label="report digest" copy /><Text as="span" color="fg.muted">({run.report_bytes} bytes)</Text></HStack>
      </Stack>
    </Panel>
    <Panel>
      <SectionHeading description="Repository, pull request and human review state.">PR and review identity</SectionHeading>
      <Stack gap="1" overflowX="auto">
        <HStack gap="2" flexWrap="nowrap" whiteSpace="nowrap"><Text>Pull request: {run.pr.number == null ? "not published" : `#${run.pr.number} ${run.pr.url || ""}`} · Head:</Text><CodeValue value={run.pr.head_sha} label="PR head SHA" copy /></HStack>
        <HStack gap="2" flexWrap="nowrap" whiteSpace="nowrap"><Metadata>Repository: {run.pr.repository || "not reported"} · Branch: {run.pr.branch || "not reported"} · Base:</Metadata><CodeValue value={run.pr.base_sha} label="base SHA" copy /></HStack>
        <Text>Review: {run.review.verdict || "not reviewed"} · Merge: {label(run.merge_state)}</Text>
        {run.merged_at && <Metadata>Merged at {run.merged_at}</Metadata>}
        {run.completed_at && <Metadata>Completed at {run.completed_at}</Metadata>}
      </Stack>
    </Panel>
    {run.failure && <Box role="alert" aria-label="Bounded redacted failure detail" borderLeftWidth="3px" borderColor="red.solid" bg="red.subtle" padding="3" borderRadius="l1"><Text fontWeight="semibold" color="red.fg">Diagnostic detail</Text><Text marginTop="1">{run.failure.detail || "Failure detail unavailable."}</Text></Box>}
    <Panel><SectionHeading description="Bounded structured report data.">Projected payload</SectionHeading><SafeTree value={run.payload ?? {}} /></Panel>
  </Stack>;
}

function BotsTab() {
  const [bots, setBots] = useState<BotSummary[] | null>(null);
  const [name, setName] = useState("");
  const [runs, setRuns] = useState<RunProjection[] | null>(null);
  const [report, setReport] = useState<RunProjection | null>(null);
  const [error, setError] = useState("");
  const [retry, setRetry] = useState(0);
  const [busy, setBusy] = useState(false);
  const [cursor, setCursor] = useState<string | null>(null);
  const openRun = (run: RunProjection) => { setBusy(true); request<RunProjection>(`/bots/${encodeURIComponent(name)}/runs/${encodeURIComponent(run.run_id)}`).then(setReport).catch((reason) => setError(reason.message)).finally(() => setBusy(false)); };
  const loadRuns = (bot: string, next?: string | null) => {
    setBusy(true); setError("");
    request<{ items: RunProjection[]; next_cursor?: string | null }>(`/bots/${encodeURIComponent(bot)}/runs${next ? `?cursor=${encodeURIComponent(next)}` : ""}`)
      .then((value) => { setName(bot); setRuns((previous) => next ? [...(previous || []), ...value.items] : value.items); setCursor(value.next_cursor || null); })
      .catch((reason) => setError(reason.message)).finally(() => setBusy(false));
  };
  useEffect(() => {
    request<{ items: BotSummary[] }>("/bots")
      .then((value) => setBots(value.items))
      .catch((reason) => setError(reason.message));
  }, [retry]);
  if (error) return <Panel role="alert"><Text color="red.fg">Unable to load bot activity: {error}</Text><Button size="sm" variant="outline" marginTop="3" onClick={() => { setError(""); setRetry((value) => value + 1); if (name) loadRuns(name); }}>Retry</Button></Panel>;
  if (!bots) return <Spinner aria-label="Loading bots" />;
  if (report) {
    return <Stack gap="3"><Button alignSelf="start" size="sm" variant="outline" aria-label="Back to runs" onClick={() => setReport(null)}>Back to runs</Button><RunEvidence run={report} /></Stack>;
  }
  if (runs) {
    return <Stack role="region" aria-label="Bot runs" gap="3">
      <HStack justify="space-between"><Box><Text as="h2" fontSize="lg" fontWeight="bold">Runs for {label(name)}</Text><Metadata>Projected specialist evidence</Metadata></Box><Button size="sm" variant="outline" aria-label="Back to bots" onClick={() => { setRuns(null); setName(""); }}>Back to bots</Button></HStack>
      {runs.length === 0 ? <EmptyState title="No projected runs are available">Try another specialist or wait for the next scheduled run.</EmptyState> :
        <Box overflowX="auto" borderWidth="1px" borderColor="border" borderRadius="l2">
          <Table.Root size="sm" variant="outline">
            <Table.Header><Table.Row><Table.ColumnHeader>Run</Table.ColumnHeader><Table.ColumnHeader>Outcome</Table.ColumnHeader><Table.ColumnHeader>Reason</Table.ColumnHeader><Table.ColumnHeader>Model</Table.ColumnHeader><Table.ColumnHeader>Duration</Table.ColumnHeader><Table.ColumnHeader>Tokens</Table.ColumnHeader><Table.ColumnHeader>Deadline / consumption</Table.ColumnHeader></Table.Row></Table.Header>
            <Table.Body>{runs.map((run) => <Table.Row key={`${run.run_id}:${run.try_number}`} role="button" tabIndex={0} aria-label={`Open run ${run.run_id}`} cursor="pointer" onClick={() => openRun(run)} onKeyDown={(event) => { if (event.key === "Enter" || event.key === " ") { event.preventDefault(); openRun(run); } }}>
              <Table.Cell><Text fontSize="sm" title={run.run_id}>{new Date(run.started_at).toLocaleString()}</Text><Metadata>try {run.try_number}</Metadata></Table.Cell>
              <Table.Cell><StatusBadge value={label(run.outcome)} /></Table.Cell><Table.Cell><Metadata>{label(run.reason_code)}</Metadata></Table.Cell><Table.Cell>{run.model || "—"}</Table.Cell><Table.Cell>{run.duration_ms} ms</Table.Cell><Table.Cell>{run.total_tokens == null ? "—" : run.total_tokens}</Table.Cell><Table.Cell>{run.deadline_at}<Metadata>{run.deadline_consumed_ms == null ? "not reported" : `${run.deadline_consumed_ms} ms consumed`}</Metadata></Table.Cell>
            </Table.Row>)}</Table.Body>
          </Table.Root>
        </Box>}
      {cursor && <Button size="sm" variant="outline" disabled={busy} onClick={() => loadRuns(name, cursor)}>Load more runs</Button>}
    </Stack>;
  }
  return <Stack role="region" aria-label="Bot evidence" gap="3">
    <HStack justify="space-between"><SectionHeading description="Latest runs and evidence freshness. Select a bot to investigate.">Bot health</SectionHeading>{busy && <Spinner size="sm" />}</HStack>
    {bots.filter((bot) => !["task_executor", "pr_reviewer"].includes(bot.name)).every((bot) => bot.paused) && bots.length > 0 && <Panel bg="bg.subtle"><Text fontSize="sm">Scheduled bots are paused. The results below are historical; new recommendations require fresh bot runs.</Text></Panel>}
    {bots.length === 0 ? <EmptyState title="No specialist bots are registered">The bot registry is empty.</EmptyState> : <Panel padding="0" overflowX="auto"><Table.Root size="sm"><Table.Header><Table.Row><Table.ColumnHeader>Bot</Table.ColumnHeader><Table.ColumnHeader>Latest result</Table.ColumnHeader><Table.ColumnHeader>Last run</Table.ColumnHeader><Table.ColumnHeader>Evidence</Table.ColumnHeader></Table.Row></Table.Header><Table.Body>{bots.map((bot) =>
      <Table.Row key={bot.name} _hover={{ bg: "bg.subtle" }}>
        <Table.Cell><Button variant="plain" size="sm" justifyContent="start" aria-label={`Open bot ${bot.name}`} onClick={() => loadRuns(bot.name)}>{label(bot.name)} →</Button><Text fontSize="xs" color="fg.muted">{["task_executor", "pr_reviewer"].includes(bot.name) ? "On demand" : bot.paused ? "Paused" : "Scheduled"}</Text></Table.Cell>
        <Table.Cell><StatusBadge value={bot.latest_status === "missing" ? "Not yet run" : label(bot.latest_status)} /><Metadata>{bot.latest_status !== "missing" ? label(bot.latest_reason_code) : "No recorded result"}</Metadata></Table.Cell>
        <Table.Cell><Text fontSize="sm" title={bot.latest_at || undefined}>{relativeTime(bot.latest_at)}</Text></Table.Cell>
        <Table.Cell>{bot.latest ? <Text fontSize="sm" color="fg.muted">Evidence: {label(bot.latest.freshness)} · {bot.latest.freshness_sla_minutes == null ? "no SLA" : `SLA ${bot.latest.freshness_sla_minutes}m`}</Text> : <Metadata>Not available</Metadata>}</Table.Cell>
      </Table.Row>)}</Table.Body></Table.Root></Panel>}

  </Stack>;
}
export default function Plugin() {
  return <ActivityApp />;
}
