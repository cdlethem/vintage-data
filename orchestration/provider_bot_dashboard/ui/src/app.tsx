import {
  Box, Button, Grid, HStack, Input, NativeSelect, Spinner, Stack, Table, Text, Textarea,
} from "@chakra-ui/react";
import { useEffect, useRef, useState } from "react";
import { BotSummary, LifecycleDurations, RunProjection, STATES, Task, TaskState, UsageSummary, QueueSummary, ATTENTION_STATES, activityUrl, humanTitle, nextStep, botWorkQueued, canRetryBotWork, relativeTime, label, request } from "./api";
import { EmptyState, CodeValue, LIFECYCLE_PHASES, Metadata, Panel, SectionHeading, StatusBadge, Glyph, LoadingState, interfaceStyles } from "./ui-kit";
import { SafeTree } from "./safe-tree";
import { useUnsavedChanges } from "./use-unsaved-changes";
import { Autopilot } from "./autopilot";
import { ConcurrencySettings, ConcurrencyProps, ModelLimitNotice } from "./concurrency";
import { ModelSettings } from "./model-settings";
import { ActivityTimeline, Disclosure, EvidenceList, ExecutionList, ExecutionScope, TechnicalRecords, TicketBrief, CommandEditor } from "./ticket-content";

type Page = { items: Task[]; next_cursor?: string | null; total: number };
type Capabilities = {
  write_enabled: boolean;
  executor_enabled: boolean;
  csrf_token?: string;
  reasons?: string[];
};
type Detail = Task & {
  source_bot?: string;
  created_at?: string;
  revisions: Record<string, unknown>[];
  events: Record<string, unknown>[];
  executions: Record<string, unknown>[];
  lifecycle_durations_ms?: LifecycleDurations;
};


function ActivityApp() {
  const [view, setView] = useState<"priority" | "bots" | "usage" | "models">("priority");
  const [scope, setScope] = useState("attention");
  const [page, setPage] = useState<Page | null>(null);
  const [summary, setSummary] = useState<QueueSummary | null>(null);
  const [caps, setCaps] = useState<Capabilities | null>(null);
  const [selected, setSelected] = useState<Detail | null>(null);
  const [state, setState] = useState("");
  const [search, setSearch] = useState("");
  const [error, setError] = useState("");
  const [creating, setCreating] = useState(false);
  const [dirty, setDirty] = useState(false);
  const [pendingNavigation, setPendingNavigation] = useState<(() => void) | null>(null);
  const navigate = (action: () => void) => { if (dirty) setPendingNavigation(() => action); else action(); };
  const [loading, setLoading] = useState(false);
  const [refreshKey, setRefreshKey] = useState(0);
  const generation = useRef(0);
  const detailGeneration = useRef(0);
  const [opening, setOpening] = useState(false);
  const [failedTask, setFailedTask] = useState("");
  const previousQuery = useRef("");
  const states = state || (scope === "attention" ? ATTENTION_STATES.join(",") : scope === "active" ? [...ATTENTION_STATES, "in_progress"].join(",") : "completed,dismissed");
  const query = `/tasks?state=${encodeURIComponent(states)}&search=${encodeURIComponent(search)}`;
  const reload = () => setRefreshKey((value) => value + 1);
  const open = async (id: string) => {
    const current = ++detailGeneration.current;
    setOpening(true);
    try {
      const detail = await request<Detail>(`/tasks/${encodeURIComponent(id)}`);
      if (current !== detailGeneration.current) return;
      setSelected(detail); setError(""); setFailedTask("");
      history.replaceState(history.state, "", activityUrl(id));
    } catch (reason) { if (current === detailGeneration.current) { setFailedTask(id); setError(`Unable to open this task: ${(reason as Error).message}`); } }
    finally { if (current === detailGeneration.current) setOpening(false); }
  };
  const close = () => { ++detailGeneration.current; setOpening(false); setFailedTask(""); setSelected(null); history.replaceState(history.state, "", activityUrl()); };
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
    setLoading(true); setError("");
    if (previousQuery.current !== query) setPage(null);
    previousQuery.current = query;
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
  useEffect(() => { const task = new URLSearchParams(location.search).get("task"); if (task) void open(task); return () => { ++detailGeneration.current; }; }, []);
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
  const changeView = (next: typeof view) => { setView(next); setCreating(false); setError(""); close(); };
  const clearFilters = () => { setSearch(""); setState(""); };
  return <Stack data-bot-activity="" css={interfaceStyles} padding={{ base: "4", md: "8" }} gap="6" maxW="1320px" width="100%" minW="0" marginX="auto" colorPalette="blue">
    <HStack justify="space-between" gap="4" flexWrap="wrap">
      <HStack gap="3"><Box bg="blue.subtle" color="blue.fg" padding="3" borderRadius="xl" display={{ base: "none", sm: "block" }}><Glyph name="bots" size={24} /></Box><Box><Text as="h1" fontSize={{ base: "2xl", md: "28px" }} letterSpacing="-0.035em" lineHeight="short" fontWeight="semibold">Bot activity</Text><Text color="fg.muted" fontSize="sm" marginTop="1.5">A clear view of your bots, their work, and what happens next.</Text></Box></HStack>
      <HStack gap="2"><Button size="sm" variant="outline" aria-label="Refresh" onClick={() => { reload(); if (selected) void open(selected.id); }}><Glyph name="refresh" />Refresh</Button>{caps?.write_enabled && <Button size="sm" onClick={() => navigate(() => { changeView("priority"); setCreating(true); })}><Glyph name="plus" />Create task</Button>}</HStack>
    </HStack>
    <HStack gap={{ base: "0", md: "3" }} borderBottomWidth="1px" borderColor="border" role="navigation" aria-label="Bot activity views" overflowX="auto" minW="0">
      {([
        ["priority", "Action queue", "queue", "Priority list"],
        ["bots", "Bots", "bots", "Bots"],
        ["usage", "Usage", "usage", "Token usage and spend"],
        ["models", "Models & connections", "settings", "Models & connections"],
      ] as const).map(([value, title, icon, accessible]) => <Button key={value} size="sm" variant="ghost" color={view === value ? "blue.fg" : "fg.muted"} position="relative" borderRadius="0!" borderWidth="0" paddingX={{ base: "2", md: "3" }} height="12" flexShrink="0" aria-label={accessible} aria-pressed={view === value} onClick={() => { if (view !== value || value === "priority" && (selected || creating || opening)) navigate(() => changeView(value)); }}>{view === value && <Box as="span" position="absolute" bottom="0" left="2" right="2" height="2px" bg="blue.fg" />}<Box display={{ base: "none", md: "block" }}><Glyph name={icon} /></Box><Text as="span" display={{ base: "none", md: "inline" }}>{title}</Text><Text as="span" display={{ base: "inline", md: "none" }}>{value === "priority" ? "Queue" : value === "models" ? "Models" : title}</Text>{value === "priority" && summary && <Box as="span" display={{ base: "none", md: "inline" }} bg={view === value ? "blue.subtle" : "bg.muted"} borderRadius="full" paddingX="1.5" fontSize="xs" fontVariantNumeric="tabular-nums">{summary.attention_count}</Box>}</Button>)}
    </HStack>
    <Autopilot writeEnabled={!!caps?.write_enabled} mutate={mutate} refreshKey={refreshKey} />
    {pendingNavigation && <Panel role="alertdialog" aria-labelledby="unsaved-title" aria-describedby="unsaved-description" borderColor="orange.muted" bg="bg.panel">
      <HStack justify="space-between" align="start" gap="4" flexWrap="wrap"><Box><Text id="unsaved-title" fontWeight="semibold">You have unsaved changes</Text><Text id="unsaved-description" color="fg.muted" fontSize="sm" marginTop="1">Keep editing to save your work, or discard these changes before leaving.</Text></Box><HStack gap="2" flexWrap="wrap"><Button size="sm" autoFocus onClick={() => setPendingNavigation(null)}>Keep editing</Button><Button size="sm" variant="outline" borderColor="border" onClick={() => { const action = pendingNavigation; setPendingNavigation(null); setDirty(false); action(); }}>Discard changes</Button></HStack></HStack>
    </Panel>}
    {caps && !caps.write_enabled && view === "priority" && <Text fontSize="sm" color="fg.muted">View only · Task editing is disabled for this installation.</Text>}
    {error && view === "priority" && <Panel role="alert" borderColor="red.muted" bg="red.subtle"><HStack justify="space-between" flexWrap="wrap"><HStack><Glyph name="alert" /><Text color="red.fg">{error}</Text></HStack><Button size="sm" variant="outline" onClick={() => { if (failedTask) void open(failedTask); else reload(); }}>Retry</Button></HStack></Panel>}
    {view === "models" ? <ModelSettings onDirtyChange={setDirty} refreshKey={refreshKey} writeEnabled={!!caps?.write_enabled} executorEnabled={!!caps?.executor_enabled} mutate={mutate} /> : view === "usage" ? <UsageTab refreshKey={refreshKey} /> : view === "bots" ? <BotsTab navigate={navigate} refreshKey={refreshKey} writeEnabled={!!caps?.write_enabled} mutate={mutate} onDirtyChange={setDirty} onModels={() => navigate(() => changeView("models"))} /> : creating ? <CreateTaskForm onDirtyChange={setDirty} close={() => navigate(() => setCreating(false))} create={async (body) => { await mutate("/tasks", body); setCreating(false); reload(); }} /> : opening && !selected ? <LoadingState>Opening task…</LoadingState> : selected ?
      <TaskDetail onModels={() => navigate(() => changeView("models"))} onDirtyChange={setDirty} key={selected.id} task={selected} caps={caps} close={() => navigate(close)} mutate={mutate} refresh={() => open(selected.id)} /> : <>
      <Grid templateColumns="repeat(3, minmax(0, 1fr))" gap={{ base: "2", md: "4" }} role="group" aria-label="Queue overview">
        {([
          ["Needs a decision", summary?.attention_count, "queue", "attention", "", "Recommendations, reviews & blockers"],
          ["Work in progress", summary?.counts.in_progress, "clock", "active", "in_progress", "Accepted work now underway"],
          ["Blocked", summary?.counts.blocked, "alert", "attention", "blocked", "Work that needs intervention"],
        ] as const).map(([title, count, icon, nextScope, nextState, description]) => <Button key={title} variant="outline" borderColor="border" height="auto" display="block" textAlign="start" whiteSpace="normal" padding={{ base: "3", md: "5" }} bg="bg.panel" borderRadius="xl!" aria-label={`${title}: ${summary ? count || 0 : "unavailable"}`} onClick={() => { setScope(nextScope); setState(nextState); setSearch(""); }}>
          <HStack justify="space-between" color={title === "Blocked" && count ? "orange.fg" : "fg.muted"} gap="1"><Text fontSize={{ base: "xs", md: "sm" }}>{title}</Text><Box display={{ base: "none", sm: "block" }}><Glyph name={icon} /></Box></HStack>
          <Text fontSize={{ base: "2xl", md: "3xl" }} letterSpacing="-0.04em" lineHeight="short" fontWeight="semibold" fontVariantNumeric="tabular-nums" marginTop="3">{summary ? (count || 0).toLocaleString() : "—"}</Text>
          <Text color="fg.muted" fontSize="xs" fontWeight="normal" marginTop="2" display={{ base: "none", md: "block" }}>{description}</Text>
        </Button>)}
      </Grid>
      <Stack gap="3">
        <HStack gap="3" flexWrap="wrap" justify="space-between">
          <HStack gap="1" bg="bg.subtle" padding="1" borderRadius="lg" width={{ base: "100%", lg: "auto" }}>
            {[["attention", "Needs attention"], ["active", "All active"], ["history", "History"]].map(([value, title]) => <Button key={value} size="sm" paddingX="2" flex={{ base: "1", lg: "initial" }} minW="0" aria-label={title} variant="ghost" color="fg" bg={scope === value ? "bg.panel" : undefined} boxShadow={scope === value ? "xs" : undefined} aria-pressed={scope === value} onClick={() => { setScope(value); setState(""); }}><Text as="span" display={{ base: "none", md: "inline" }}>{title}</Text><Text as="span" display={{ base: "inline", md: "none" }} fontSize="xs">{value === "attention" ? "Attention" : title}</Text></Button>)}
          </HStack>
          <Stack direction={{ base: "column", sm: "row" }} gap="2" width={{ base: "100%", lg: "auto" }}>
            <Box position="relative" flex="1" minW="0"><Box position="absolute" left="3" top="3" zIndex="1" color="fg.muted" pointerEvents="none"><Glyph name="search" /></Box><Input paddingLeft="9" height="10" width={{ base: "100%", lg: "15rem" }} size="sm" aria-label="Search tasks" placeholder="Search actions…" value={search} onChange={(event) => setSearch(event.target.value)} /></Box>
            <NativeSelect.Root width={{ base: "100%", sm: "10rem" }} size="sm" flexShrink="0"><NativeSelect.Field height="10" aria-label="Filter state" value={state} onChange={(event) => setState(event.target.value)}><option value="">All statuses</option>{STATES.filter((item) => scope === "history" ? ["completed", "dismissed"].includes(item) : scope === "attention" ? ATTENTION_STATES.includes(item) : !["completed", "dismissed"].includes(item)).map((item) => <option key={item} value={item}>{label(item)}</option>)}</NativeSelect.Field></NativeSelect.Root>
          </Stack>
        </HStack>
        <HStack justify="space-between" minH="6" gap="2" flexWrap="wrap"><Text fontSize="sm" color="fg.muted">{scope === "attention" ? "Recommendations and blockers awaiting a decision." : scope === "history" ? "Completed and archived work, with every decision retained." : "Open recommendations and work already underway."}</Text>{(search || state) && <Button variant="plain" size="xs" color="blue.fg" onClick={clearFilters}>Clear filters</Button>}{loading && page && <Text role="status" fontSize="xs" color="fg.muted">Updating…</Text>}</HStack>
      </Stack>
      {!page && loading ? <LoadingState>Loading actions…</LoadingState> : page?.items.length === 0 ?
        <EmptyState title={search || state ? "No tasks match these filters" : scope === "history" ? "No past actions" : scope === "active" ? "No active work" : "You're all caught up"} action={search || state ? <Button variant="outline" size="sm" onClick={clearFilters}>Reset filters</Button> : undefined}>{search || state ? "Try a different status or search term, or reset your filters to see the queue." : scope === "history" ? "Completed and archived tasks will appear here." : scope === "active" ? "New recommendations and work in progress will appear here." : "No actions need a decision right now. New recommendations will appear after the bots assess current evidence."}</EmptyState> : page &&
        <Panel padding="0!" overflow="hidden"><Stack gap="0" role="list" aria-label="Actions">{page.items.map((task) => <Box key={task.id} role="listitem" borderBottomWidth="1px" borderColor="border" _last={{ borderBottomWidth: "0" }}>
          <Button variant="ghost" width="100%" height="auto" padding={{ base: "4", md: "5" }} whiteSpace="normal" textAlign="start" justifyContent="start" borderRadius="0!" onClick={() => open(task.id)} _hover={{ bg: "bg.subtle" }}>
            <HStack width="100%" align="start" gap={{ base: "3", md: "4" }}>
              <Box paddingTop="0.5" title={`Priority ${task.priority}. Lower numbers are higher priority.`}><StatusBadge value={`P${task.priority}`} tone={task.priority <= 1 ? "warning" : "neutral"} /></Box>
              <Stack gap="2" flex="1" minW="0">
                <HStack justify="space-between" align="start" gap="3" flexWrap={{ base: "wrap", md: "nowrap" }}><Text fontWeight="semibold" fontSize="sm" lineHeight="tall" overflowWrap="anywhere">{humanTitle(task.title)}</Text><StatusBadge value={botWorkQueued(task) ? "Queued" : label(task.state)} /></HStack>
                <Text color="fg.muted" fontSize="sm" fontWeight="normal" lineClamp="1" overflowWrap="anywhere">{task.planned_resolution || nextStep(task)}</Text>
                <HStack justify="space-between" gap="2" flexWrap="wrap"><Text fontSize="xs" color="fg.muted" fontWeight="normal">{label(task.category)} · {task.assignee_name || (task.assignee_profile ? `${label(task.assignee_profile)} bot` : "Unassigned")} · <span title={new Date(task.updated_at).toLocaleString()}>{relativeTime(task.updated_at)}</span></Text><Text fontSize="xs" color="blue.fg" fontWeight="medium">{nextStep(task)}</Text></HStack>
              </Stack><Box alignSelf="center" color="fg.muted" display={{ base: "none", sm: "block" }}><Glyph name="arrow" /></Box>
            </HStack>
          </Button></Box>)}</Stack></Panel>}
      {page && page.total > 0 && <HStack justify="space-between"><Metadata>Showing {page.items.length} of {page.total} actions</Metadata>{page.next_cursor && <Button size="sm" variant="outline" disabled={loading} onClick={more}>{loading ? "Loading…" : "Load more"}</Button>}</HStack>}
    </>}
  </Stack>;
}

function formatCost(microUsd: number): string {
  return `$${(microUsd / 1_000_000).toFixed(4)}`;
}

function CountValue({ value }: { value: number }) {
  return <Text as="span" title={value.toLocaleString()}><Text as="span" display={{ base: "inline", md: "none" }}>{new Intl.NumberFormat(undefined, { notation: "compact", maximumFractionDigits: 1 }).format(value)}</Text><Text as="span" display={{ base: "none", md: "inline" }}>{value.toLocaleString()}</Text></Text>;
}

function UsageCost({ row }: { row: UsageSummary["totals"] }) {
  if (row.priced_runs === 0) return <Text>not priced</Text>;
  const partial = row.unpriced_runs > 0;
  return <Stack gap="0">
    <Text>{formatCost(row.cost_micro_usd)}</Text>
    {partial && <Metadata>partial · {row.priced_runs} of {row.runs} runs priced</Metadata>}
  </Stack>;
}

function UsageTab({ refreshKey }: { refreshKey: number }) {
  const [period, setPeriod] = useState(30);
  const [retry, setRetry] = useState(0);
  const [usage, setUsage] = useState<UsageSummary | null>(null);
  const [error, setError] = useState("");
  useEffect(() => {
    setUsage(null); setError("");
    const controller = new AbortController();
    request<UsageSummary>(`/usage?days=${period}`, { signal: controller.signal }).then(setUsage).catch((reason) => { if (!controller.signal.aborted) setError(reason.message); });
    return () => controller.abort();
  }, [period, refreshKey, retry]);
  if (error) return <Panel role="alert" aria-label="Usage error"><Text color="red.fg">{error}</Text><Button variant="outline" size="sm" marginTop="3" onClick={() => setRetry(value => value + 1)}>Retry usage</Button></Panel>;
  if (!usage) return <LoadingState>Loading token usage…</LoadingState>;
  const cacheShare = usage.totals.total_tokens > 0
    ? Math.round((usage.totals.cached_input_tokens / usage.totals.total_tokens) * 100)
    : 0;
  const maxDay = Math.max(...usage.by_day.map((row) => row.total_tokens), 1);
  return <Stack role="region" aria-label="Token usage and spend" gap="5">
    <HStack justify="space-between" align="start" flexWrap="wrap" gap="3">
      <SectionHeading description="Understand usage and spend across your bots.">Token usage and spend</SectionHeading>
      <NativeSelect.Root width="9rem"><NativeSelect.Field aria-label="Usage period" value={String(period)} onChange={(event) => setPeriod(Number(event.target.value))}>
        {[7, 30, 90].map((days) => <option key={days} value={days}>{days} days</option>)}
      </NativeSelect.Field></NativeSelect.Root>
    </HStack>
    <Grid templateColumns={{ base: "repeat(2, minmax(0, 1fr))", md: "repeat(4, minmax(0, 1fr))" }} gap="3" role="group" aria-label="Usage totals">
      <Panel><Metadata>Requests</Metadata><Text fontSize={{ base: "xl", md: "3xl" }} letterSpacing="-0.035em" fontWeight="semibold"><CountValue value={usage.totals.requests} /></Text></Panel>
      <Panel><Metadata>Total tokens</Metadata><Text fontSize={{ base: "xl", md: "3xl" }} letterSpacing="-0.035em" fontWeight="semibold"><CountValue value={usage.totals.total_tokens} /></Text></Panel>
      <Panel><Metadata>Spend</Metadata><Box fontSize={{ base: "xl", md: "3xl" }} letterSpacing="-0.035em" fontWeight="semibold"><UsageCost row={usage.totals} /></Box><Metadata>{usage.totals.runs} runs · {usage.totals.priced_runs} priced</Metadata></Panel>
      <Panel><Metadata>Cache-read share</Metadata><Text fontSize={{ base: "xl", md: "3xl" }} letterSpacing="-0.035em" fontWeight="semibold">{cacheShare}%</Text><Metadata>{usage.totals.cached_input_tokens.toLocaleString()} cached input tokens</Metadata></Panel>
    </Grid>
    <Panel role="region" aria-label="Daily spend cap">
      <SectionHeading description="Caps are enforced before a new run claim and never interrupt an active run.">Daily spend cap</SectionHeading>
      {usage.cap.daily_spend_cap_micro_usd == null || usage.cap.daily_spend_cap_micro_usd === 0
        ? <Text color="fg.muted">No cap configured.</Text>
        : <Stack gap="1"><Text>{formatCost(usage.cap.spent_today_micro_usd)} of {formatCost(usage.cap.daily_spend_cap_micro_usd)} consumed{usage.cap.exceeded ? " · cap reached" : ""}</Text><Box height="2" bg="bg.muted" borderRadius="full" overflow="hidden"><Box height="100%" width={`${Math.min(100, usage.cap.spent_today_micro_usd / usage.cap.daily_spend_cap_micro_usd * 100)}%`} bg={usage.cap.exceeded ? "red.solid" : "blue.solid"} /></Box></Stack>}
    </Panel>
<Panel role="region" aria-label="Daily token trend">
      <SectionHeading description="Token volume by UTC day.">Daily trend</SectionHeading>
      {usage.by_day.length === 0 ? <EmptyState title="No daily usage in this period">There is no token activity to chart.</EmptyState> :
        <Stack gap="4">
          <HStack height="40" align="end" gap={{ base: "0.5", md: "1" }} borderBottomWidth="1px" borderColor="border" role="group" aria-label="Daily token totals">{usage.by_day.map(row => <Box key={row.date} tabIndex={0} role="img" aria-label={`${row.date}: ${row.total_tokens.toLocaleString()} tokens`} title={`${row.date} · ${row.total_tokens.toLocaleString()} tokens · ${row.priced_runs ? formatCost(row.cost_micro_usd) : "not priced"}`} flex="1" minW="0" height={`${Math.max(1, row.total_tokens / maxDay * 100)}%`} bg="blue.solid" opacity="0.8" borderTopRadius="sm" _hover={{ opacity: 1 }} _focusVisible={{ opacity: 1 }} />)}</HStack>
          <HStack justify="space-between"><Text fontSize="xs" color="fg.muted">{usage.by_day[0]?.date}</Text><Text fontSize="xs" color="fg.muted">{usage.by_day.at(-1)?.date}</Text></HStack>
          <Disclosure title="View daily breakdown"><Box overflowX="auto" tabIndex={0}><Table.Root size="sm"><Table.Header><Table.Row><Table.ColumnHeader>Date (UTC)</Table.ColumnHeader><Table.ColumnHeader>Tokens</Table.ColumnHeader><Table.ColumnHeader>Spend</Table.ColumnHeader></Table.Row></Table.Header><Table.Body>{usage.by_day.map(row => <Table.Row key={row.date}><Table.Cell>{row.date}</Table.Cell><Table.Cell>{row.total_tokens.toLocaleString()}</Table.Cell><Table.Cell><UsageCost row={row} /></Table.Cell></Table.Row>)}</Table.Body></Table.Root></Box></Disclosure>
        </Stack>}

    </Panel>
    <Panel role="region" aria-label="Usage by bot">
      <SectionHeading description="Requests and spend across your bots.">By bot</SectionHeading>
      {usage.by_bot.length === 0 ? <EmptyState title="No bot usage in this period">No live runs were recorded.</EmptyState> :
        <Box overflowX="auto" tabIndex={0}><Table.Root size="sm" variant="outline"><Table.Header><Table.Row><Table.ColumnHeader>Bot</Table.ColumnHeader><Table.ColumnHeader>Runs</Table.ColumnHeader><Table.ColumnHeader>Requests</Table.ColumnHeader><Table.ColumnHeader>Spend</Table.ColumnHeader></Table.Row></Table.Header><Table.Body>{usage.by_bot.map((row) => <Table.Row key={row.bot}><Table.Cell>{label(row.bot)}</Table.Cell><Table.Cell>{row.runs.toLocaleString()}</Table.Cell><Table.Cell>{row.requests.toLocaleString()}</Table.Cell><Table.Cell><UsageCost row={row} /></Table.Cell></Table.Row>)}</Table.Body></Table.Root></Box>}
    </Panel>
  </Stack>;
}

function CreateTaskForm({ close, create, onDirtyChange }: { onDirtyChange?: (dirty: boolean) => void; close: () => void; create: (body: unknown) => Promise<void> }) {
  const [title, setTitle] = useState("");
  const [category, setCategory] = useState("other");
  const [priority, setPriority] = useState(1);
  const [plan, setPlan] = useState("");
  const [error, setError] = useState("");
  const [saving, setSaving] = useState(false);
  useUnsavedChanges(!!title || !!plan || category !== "other" || priority !== 1, onDirtyChange);
  return <Panel role="dialog" aria-label="Create manual task" maxW="42rem" width="100%" alignSelf="center">
    <SectionHeading description="Add a human-owned action to the manager queue.">Create manual task</SectionHeading>
    <Stack gap="3">
      <Box><label htmlFor="task-title">Task title</label><Input id="task-title" autoFocus aria-label="Title" placeholder="What needs to change?" value={title} onChange={(event) => setTitle(event.target.value)} maxLength={200} /></Box>
      <Grid templateColumns={{ base: "1fr", sm: "2fr 1fr" }} gap="4"><Box><label htmlFor="task-category">Category</label><NativeSelect.Root><NativeSelect.Field id="task-category" aria-label="Category" value={category} onChange={(event) => setCategory(event.target.value)}>{["reliability", "new_source", "cadence", "load", "storage", "architecture", "credential", "cost", "other"].map((value) => <option key={value} value={value}>{label(value)}</option>)}</NativeSelect.Field></NativeSelect.Root></Box>
      <Box><label htmlFor="task-priority">Priority</label><Input id="task-priority" aria-label="Priority" type="number" min={1} value={priority} onChange={(event) => setPriority(Number(event.target.value))} /><Text fontSize="xs" color="fg.muted" marginTop="1">Lower numbers come first.</Text></Box></Grid>
      <Box><label htmlFor="task-plan">Resolution plan</label><Textarea id="task-plan" minH="40" aria-label="Planned resolution" placeholder="Describe the problem, the intended change, and how you’ll know it’s done." value={plan} onChange={(event) => setPlan(event.target.value)} maxLength={20000} /></Box>
      {error && <Text role="alert" color="red.fg">{error}</Text>}
      <HStack><Button disabled={saving || !title.trim() || !plan.trim() || priority < 1 || !Number.isInteger(priority)} onClick={async () => { setSaving(true); setError(""); try { await create({ title: title.trim(), category, priority, planned_resolution: plan.trim(), evidence: [] }); } catch (reason) { setError((reason as Error).message); } finally { setSaving(false); } }}>{saving ? "Creating…" : "Create"}</Button><Button variant="outline" disabled={saving} onClick={close}>Cancel</Button></HStack>
    </Stack>
  </Panel>;
}

function TaskDetail({ task, caps, close, mutate, refresh, onDirtyChange, onModels }: { onModels: () => void; onDirtyChange?: (dirty: boolean) => void; task: Detail; caps: Capabilities | null; close: () => void; mutate: <T>(path: string, body: unknown, method?: string) => Promise<T>; refresh: () => Promise<void> | void }) {
  const heading = useRef<HTMLParagraphElement>(null);
  useEffect(() => { heading.current?.focus({ preventScroll: true }); }, [task.id]);
  const [plan, setPlan] = useState(task.planned_resolution);
  const [target, setTarget] = useState<TaskState>(task.state);
  const [assignee, setAssignee] = useState(task.assignee_profile || (task.assignee_kind === "human" ? "human" : String(task.revisions.at(-1)?.suggested_executor || "junior")));
  const [editingPlan, setEditingPlan] = useState(false);
  const [section, setSection] = useState("overview");
  const [notice, setNotice] = useState("");
  const [feedbackLocation, setFeedbackLocation] = useState<"actions" | "plan" | "policy" | "activity">("actions");
  useEffect(() => { setTarget(task.state); }, [task.state]);
  const transitions: Record<string, string[]> = { proposed: ["accepted", "dismissed", "blocked"], accepted: ["dismissed", "blocked"], in_progress: ["in_review", "ready", "blocked"], in_review: ["in_progress", "ready", "blocked"], ready: ["in_progress", "blocked"], blocked: [...(task.blocked_from_state ? [task.blocked_from_state] : []), "dismissed"], completed: ["accepted"], dismissed: ["accepted"] };
  const [reason, setReason] = useState("");
  const reasonRequired = ["blocked", "dismissed"].includes(target) || ["blocked", "dismissed", "completed"].includes(task.state);
  const [comment, setComment] = useState("");
  const [activityComment, setActivityComment] = useState("");
  const [evidence, setEvidence] = useState("");
  const latest = (task.revisions.at(-1) || {}) as Record<string, unknown>;
  const [verification, setVerification] = useState(JSON.stringify(latest.verification_commands || [], null, 2));
  const [globs, setGlobs] = useState(((latest.allowed_path_globs as string[] | undefined) || []).join("\n"));
  const [resources, setResources] = useState(((latest.resource_keys as string[] | undefined) || []).join("\n"));
  const [followUps, setFollowUps] = useState(((latest.follow_up_bots as string[] | undefined) || []).join("\n"));
  const [reviewerRequired, setReviewerRequired] = useState(task.reviewer_required !== false);
  const persisted = {
    plan: task.planned_resolution,
    verification: JSON.stringify(latest.verification_commands || [], null, 2),
    globs: ((latest.allowed_path_globs as string[] | undefined) || []).join("\n"),
    resources: ((latest.resource_keys as string[] | undefined) || []).join("\n"),
    followUps: ((latest.follow_up_bots as string[] | undefined) || []).join("\n"),
    reviewerRequired: task.reviewer_required !== false,
    assignee: task.assignee_profile || (task.assignee_kind === "human" ? "human" : String(latest.suggested_executor || "junior")),
  };
  const previous = useRef(persisted);
  useEffect(() => {
    const old = previous.current;
    setPlan(value => value === old.plan ? persisted.plan : value);
    setVerification(value => value === old.verification ? persisted.verification : value);
    setGlobs(value => value === old.globs ? persisted.globs : value);
    setResources(value => value === old.resources ? persisted.resources : value);
    setFollowUps(value => value === old.followUps ? persisted.followUps : value);
    setReviewerRequired(value => value === old.reviewerRequired ? persisted.reviewerRequired : value);
    setAssignee(value => value === old.assignee ? persisted.assignee : value);
    previous.current = persisted;
  }, [task]);
  const queued = botWorkQueued(task);
  const unsavedScope = plan !== persisted.plan || verification !== persisted.verification || globs !== persisted.globs || resources !== persisted.resources || followUps !== persisted.followUps || reviewerRequired !== persisted.reviewerRequired;
  const validVerification = (() => { try { const commands = JSON.parse(verification); return Array.isArray(commands) && commands.every(argv => Array.isArray(argv) && argv.length > 0 && argv.every(arg => typeof arg === "string" && arg.length > 0)); } catch { return false; } })();
  useUnsavedChanges(unsavedScope || assignee !== persisted.assignee || !!activityComment || !!comment || !!evidence || !!reason, onDirtyChange);
  const [policyError, setPolicyError] = useState("");
  const [saving, setSaving] = useState(false);
  const apply = async (path: string, body: unknown, method?: string, success = "Task updated.") => {
    if (saving) return;
    setFeedbackLocation(success === "Comment added." ? "activity" : method === "PATCH" ? (success === "Resolution plan saved." ? "plan" : "policy") : "actions");
    setSaving(true); setPolicyError(""); setNotice("");
    try { await mutate(path, body, method); await refresh(); setNotice(success); return true; }
    catch (reason) { setPolicyError((reason as Error).message); return false; }
    finally { setSaving(false); }
  };
  const feedback = (location: typeof feedbackLocation) => feedbackLocation === location ? <>
    {notice && <Text role="status" color="green.fg">{notice}</Text>}
    {saving && <HStack role="status"><Spinner size="sm" /><Text>Saving…</Text></HStack>}
    {policyError && <Text role="alert" color="red.fg">{policyError}</Text>}
  </> : null;
  return <Box role="dialog" aria-label={`Task: ${task.title}`} width="100%" minW="0">
    <HStack justify="space-between" marginBottom="5"><Button size="sm" variant="ghost" aria-label="Close" onClick={close}>← Action queue</Button><Text color="fg.muted" fontSize="xs">Task <Text as="span" fontFamily="mono">{task.id.slice(0, 8)}</Text></Text></HStack>
    {task.executions.at(-1)?.terminal_reason_code === "model_rate_limited" && <Box marginBottom="5"><ModelLimitNotice onModels={onModels} /></Box>}
    <Grid templateColumns={{ base: "minmax(0, 1fr)", lg: "minmax(0, 1fr) 19rem" }} gap={{ base: "6", lg: "10" }} alignItems="start">
      <Stack gap="6" minW="0">
        <Box><Text as="h2" tabIndex={-1} ref={heading} outline="none" fontSize={{ base: "2xl", md: "30px" }} letterSpacing="-0.035em" lineHeight="1.3" fontWeight="semibold">{humanTitle(task.title)}</Text><HStack marginTop="4" gap="2" flexWrap="wrap"><StatusBadge value={queued ? "Queued" : label(task.state)} /><StatusBadge value={label(task.category)} tone="neutral" /><Text fontSize="xs" color="fg.muted">Updated {relativeTime(task.updated_at).toLowerCase()}</Text><Button size="xs" variant="ghost" display={{ base: "inline-flex", lg: "none" }} onClick={() => document.getElementById("task-actions")?.scrollIntoView({ block: "start" })}>Task actions ↓</Button></HStack></Box>
        <HStack gap="4" borderBottomWidth="1px" borderColor="border" role="navigation" aria-label="Task sections">{[["overview", "Overview"], ["activity", `Activity · ${task.events.length}`], ["execution", "Execution"]].map(([value, title]) => <Button key={value} size="sm" variant="ghost" paddingX="0" borderWidth="0" position="relative" borderRadius="0!" color={section === value ? "fg" : "fg.muted"} aria-pressed={section === value} onClick={() => setSection(value)}>{section === value && <Box as="span" position="absolute" bottom="0" left="0" right="0" height="2px" bg="fg" />}{title}</Button>)}</HStack>
        {section === "overview" && <Stack gap="6">
          <Box>{editingPlan ? <Stack gap="3"><label htmlFor="edit-plan">Resolution plan</label><Textarea id="edit-plan" aria-label="Planned resolution" minH="24rem" fontSize="sm" lineHeight="tall" value={plan} onChange={(event) => setPlan(event.target.value)} maxLength={20000} /><HStack flexWrap="wrap"><Button size="sm" disabled={saving || !plan.trim()} onClick={() => apply(`/tasks/${task.id}`, { version: task.version, planned_resolution: plan }, "PATCH", "Resolution plan saved.")}>Save resolution plan</Button><Button size="sm" variant="outline" disabled={saving} onClick={() => { setPlan(task.planned_resolution); setEditingPlan(false); }}>{plan === task.planned_resolution ? "Done editing" : "Cancel editing"}</Button></HStack>{feedback("plan")}</Stack> : <><TicketBrief value={task.planned_resolution} />{caps?.write_enabled && <Button size="xs" variant="ghost" color="fg.muted" marginTop="4" onClick={() => { setPlan(task.planned_resolution); setEditingPlan(true); }}>Edit resolution plan</Button>}</>}</Box>
          {(["why_now", "expected_benefit", "risk", "rollback", "verification"] as const).some(key => typeof latest[key] === "string" && latest[key]) && <Disclosure title="Context & considerations" description="Why now, expected outcome, risks, and rollback"><Stack gap="5">{[["why_now", "Why now"], ["expected_benefit", "Expected outcome"], ["risk", "Risks"], ["rollback", "Rollback"], ["verification", "Verification"]].map(([key, title]) => typeof latest[key] === "string" && latest[key] ? <Box key={key}><Text as="h3" fontSize="sm" fontWeight="semibold" marginBottom="2">{title}</Text><TicketBrief value={String(latest[key])} /></Box> : null)}</Stack></Disclosure>}
          {Array.isArray(latest.evidence) && latest.evidence.length > 0 && <Disclosure title={`Supporting evidence · ${latest.evidence.length}`} description="Source documents and observations behind this task"><EvidenceList items={latest.evidence} /></Disclosure>}
          <Box borderTopWidth="1px" borderColor="border" paddingTop="5"><ActivityTimeline events={task.events} preview /><Button size="xs" variant="ghost" color="fg.muted" onClick={() => setSection("activity")}>View all activity ({task.events.length}) →</Button></Box>
        </Stack>}
        {section === "activity" && <Stack gap="6">{caps?.write_enabled && <Stack gap="3"><label htmlFor="activity-comment">Add a comment</label><Textarea id="activity-comment" value={activityComment} onChange={event => setActivityComment(event.target.value)} placeholder="Leave an update or share context…" minH="28" maxLength={10000} disabled={saving} /><Button alignSelf="end" size="sm" disabled={saving || !activityComment.trim()} onClick={async () => { if (await apply(`/tasks/${task.id}/comments`, { version: task.version, comments: activityComment.trim().split(/\r?\n/).filter(Boolean) }, undefined, "Comment added.")) setActivityComment(""); }}>Post comment</Button>{feedback("activity")}</Stack>}<ActivityTimeline events={task.events} /><Disclosure title={`Recommendation history · ${task.revisions.length}`} description="Previous versions of this task’s brief"><Stack gap="3">{[...task.revisions].reverse().map((revision, index) => <Disclosure key={index} title={`Revision ${String(revision.revision_number || task.revisions.length - index)}`} description={typeof revision.created_at === "string" ? new Date(revision.created_at).toLocaleString() : undefined}><TicketBrief value={String(revision.action || revision.title || "No brief recorded.")} /></Disclosure>)}</Stack></Disclosure><TechnicalRecords revisions={task.revisions} executions={task.executions} events={task.events} /></Stack>}
        {section === "execution" && <Stack gap="6"><ExecutionList executions={task.executions} /><ExecutionScope revision={latest} reviewerRequired={task.reviewer_required !== false} />
      {caps?.write_enabled && <Box as="details"><Box as="summary" cursor="pointer" fontWeight="semibold" paddingY="2">Edit execution settings</Box><Panel bg="bg.subtle">
        <SectionHeading description="Choose which files this task can change and how its work will be checked.">Execution settings</SectionHeading>
        <Stack gap="3">
          <CommandEditor value={verification} onChange={setVerification} disabled={saving} />{!validVerification && <Text role="status" fontSize="sm" color="orange.fg">Each verification check needs a program and nonempty arguments.</Text>}
          <Box><label htmlFor="allowed-files">Allowed files and patterns · one per line</label><Textarea id="allowed-files" fontFamily="mono" fontSize="xs" aria-label="Allowed path globs" value={globs} onChange={(event) => setGlobs(event.target.value)} /></Box>
          <Box><label htmlFor="reserved-resources">Reserved resources · one per line</label><Textarea id="reserved-resources" fontFamily="mono" fontSize="xs" aria-label="Resource keys" value={resources} onChange={(event) => setResources(event.target.value)} /></Box>
          <Box><label htmlFor="follow-up-bots">Follow-up bots · one per line</label><Textarea id="follow-up-bots" fontSize="sm" aria-label="Follow-up bots" value={followUps} onChange={(event) => setFollowUps(event.target.value)} /></Box>
          <HStack flexWrap="wrap">
            <Button size="sm" variant="outline" aria-pressed={reviewerRequired} onClick={() => setReviewerRequired(!reviewerRequired)}>Reviewer {reviewerRequired ? "required" : "optional"}</Button>
            <Button size="sm" disabled={saving || !validVerification || !plan.trim()} onClick={async () => {
              setFeedbackLocation("policy"); setNotice("");
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
          {feedback("policy")}
        </Stack>
      </Panel></Box>}
      <Box role="region" aria-label="Task lifecycle latencies">
        <SectionHeading description="Average elapsed time between workflow milestones.">Workflow timing</SectionHeading>
        {!Object.keys(task.lifecycle_durations_ms || {}).length && <Metadata>No workflow milestones recorded yet.</Metadata>}<Stack as="ol" gap="2" margin="0" paddingLeft="5">{Object.entries(task.lifecycle_durations_ms || {}).sort(([a], [b]) => {
          const ai = LIFECYCLE_PHASES.indexOf(a as typeof LIFECYCLE_PHASES[number]);
          const bi = LIFECYCLE_PHASES.indexOf(b as typeof LIFECYCLE_PHASES[number]);
          return (ai < 0 ? LIFECYCLE_PHASES.length : ai) - (bi < 0 ? LIFECYCLE_PHASES.length : bi);
        }).map(([phase, value]) => {
          const average = typeof value === "object" && value !== null ? value.average_ms : value;
          return <Box as="li" key={phase}><HStack justify="space-between"><Metadata>{label(phase)}</Metadata><Text>{average == null ? "not observed" : duration(average)}</Text></HStack></Box>;
        })}</Stack>
      </Box>

        </Stack>}
      </Stack>
      <Stack as="aside" id="task-actions" aria-label="Task properties and actions" gap="6" borderLeftWidth={{ base: "0", lg: "1px" }} borderTopWidth={{ base: "1px", lg: "0" }} borderColor="border" paddingLeft={{ base: "0", lg: "6" }} paddingTop={{ base: "5", lg: "0" }} minW="0">
        <Box><Text fontSize="xs" fontWeight="medium" color="fg.muted" marginBottom="4">Properties</Text><Stack as="dl" gap="4">{[
          ["Status", <StatusBadge value={queued ? "Queued" : label(task.state)} />],
          ["Priority", <Text title="Lower numbers are higher priority">P{task.priority}</Text>],
          ["Assignee", <Text>{task.assignee_name || (task.assignee_profile ? `${label(task.assignee_profile)} bot` : task.assignee_kind === "human" ? "Me" : "Unassigned")}</Text>],
          ["Category", <Text>{label(task.category)}</Text>],
          ["Proposed by", <Text>{task.source_bot ? label(task.source_bot) : "—"}</Text>],
          ["Review", <Text>{task.reviewer_required !== false ? "Required" : "Optional"}</Text>],
        ].map(([title, content]) => <Grid key={String(title)} templateColumns="5.5rem minmax(0, 1fr)" gap="2" alignItems="start" fontSize="xs"><Text as="dt" color="fg.muted">{String(title)}</Text><Box as="dd" margin="0">{content}</Box></Grid>)}</Stack></Box>
        <Box borderTopWidth="1px" borderColor="border" paddingTop="5">
      {caps?.write_enabled && <Box role="region" aria-label="Human merge and completion controls" fontSize="sm">
        <SectionHeading description={queued ? undefined : task.state === "proposed" ? "Review the plan and evidence, then accept this recommendation." : nextStep(task)}>Next action</SectionHeading>
        <Stack gap="3">
          {feedback("actions")}
          {task.state === "blocked" && task.executions.at(-1)?.terminal_reason_code === "model_rate_limited" && task.executions.at(-1)?.admission_kind === "pr_reviewer" && <Button disabled={saving || unsavedScope || !caps.executor_enabled} onClick={() => apply(`/tasks/${task.id}/retry-model`, { version: task.version, idempotency_key: `model-${task.id}-${task.version}` }, undefined, "Review retry queued with the current model settings.")}>Retry review</Button>}
          {canRetryBotWork(task) && <Stack gap="2"><Button disabled={saving || unsavedScope || !caps.executor_enabled} onClick={() => apply(`/tasks/${task.id}/start`, { version: task.version, idempotency_key: `ui-${task.id}-${task.version}` }, undefined, "Bot retry queued. The dispatcher will start a new attempt when capacity is available.")}>Retry bot work</Button><Metadata>The previous attempt ended without a pull request. Retry uses the saved task scope and current model settings.</Metadata></Stack>}
          {queued && <Text role="status" color="blue.fg" fontSize="sm">The dispatcher will start this task automatically. Refresh to check progress.</Text>}
          {unsavedScope && <Text role="status" color="orange.fg">Save your resolution plan and execution settings before accepting or starting work.</Text>}
          {task.state === "proposed" && <HStack flexWrap="wrap"><Button disabled={saving || unsavedScope} onClick={() => apply(`/tasks/${task.id}/transition`, { version: task.version, state: "accepted" }, undefined, "Recommendation accepted. Choose an assignee, then start work.")}>Accept recommendation</Button><Metadata>Accepting does not start execution.</Metadata></HStack>}
          {task.state === "accepted" && <HStack flexWrap="wrap"><NativeSelect.Root width="100%" disabled={saving || queued}><NativeSelect.Field aria-label="Assign work to" value={assignee} onChange={(event) => setAssignee(event.target.value)}><option value="human">Me</option><option value="junior">Junior bot</option><option value="senior">Senior bot</option><option value="staff">Staff bot</option></NativeSelect.Field></NativeSelect.Root><Button variant="outline" disabled={saving || queued} onClick={() => apply(`/tasks/${task.id}/assign`, { version: task.version, kind: assignee === "human" ? "human" : "bot", profile: assignee === "human" ? null : assignee, reviewer_required: true }, undefined, `Assigned to ${assignee === "human" ? "you" : `${label(assignee)} bot`}. ${assignee === "human" ? "Select Start my work" : "Select Start bot work"} to begin.`)}>{saving ? "Saving…" : "Save assignment"}</Button>
          {task.assignee_kind === "human" && <Button disabled={saving || unsavedScope || assignee !== "human"} onClick={() => apply(`/tasks/${task.id}/transition`, { version: task.version, state: "in_progress" })}>Start my work</Button>}
          {task.assignee_kind === "bot" && <Button disabled={saving || queued || unsavedScope || !caps.executor_enabled || assignee !== task.assignee_profile} onClick={() => apply(`/tasks/${task.id}/start`, { version: task.version, idempotency_key: `ui-${task.id}-${task.version}` }, undefined, "Bot work queued. The dispatcher will start execution when capacity is available.")}>{queued ? "Bot work queued" : "Start bot work"}</Button>}
          <Metadata>{queued ? "Assignment is locked while this execution is queued." : task.assignee_kind ? `Saved assignment: ${task.assignee_kind === "bot" ? `${label(task.assignee_profile || "")} bot` : task.assignee_name || "you"}. Assignment does not start execution.` : "Save an assignment to enable starting work."}</Metadata>
          {task.assignee_kind === "bot" && !caps.executor_enabled && <Text color="orange.fg">Bot execution is disabled for this installation. Assignment is saved, but work cannot start yet.</Text>}
          </HStack>}
          <Box as="details"><Box as="summary" cursor="pointer" paddingY="2">Other status changes</Box><Stack gap="3"><Box><label htmlFor="target-status">Move to</label><NativeSelect.Root><NativeSelect.Field id="target-status" aria-label="Target state" value={target} onChange={(event) => setTarget(event.target.value as TaskState)}>{[task.state, ...(transitions[task.state] || [])].map((value) => <option key={value} value={value}>{label(value)}</option>)}</NativeSelect.Field></NativeSelect.Root></Box><Box><label htmlFor="transition-reason">Reason {reasonRequired ? "(required)" : "(optional)"}</label><Input id="transition-reason" aria-label="Transition reason" placeholder="Why is the status changing?" value={reason} onChange={(event) => setReason(event.target.value)} maxLength={20000} /></Box><Button alignSelf="start" size="sm" disabled={saving || (reasonRequired && !reason.trim()) || target === task.state || (unsavedScope && ["accepted", "in_progress"].includes(target))} aria-label="Apply human task transition" onClick={() => apply(`/tasks/${task.id}/transition`, { version: task.version, state: target, reason: reason.trim() || null })}>Update status</Button></Stack></Box>
          {task.state === "ready" && <Stack direction={{ base: "column", md: "row" }} align="start"><Textarea aria-label="Human completion comment" placeholder="Completion comment" value={comment} onChange={(event) => setComment(event.target.value)} /><Textarea aria-label="Human verification evidence" placeholder="Evidence URL or reference" value={evidence} onChange={(event) => setEvidence(event.target.value)} /></Stack>}
          {task.state === "ready" && <Button alignSelf="start" aria-label="Complete task after human merge" disabled={saving || !comment || !evidence} onClick={async () => { setFeedbackLocation("actions"); setNotice(""); setPolicyError(""); setSaving(true); try { await mutate(`/tasks/${task.id}/comments`, { version: task.version, comments: [comment] }); await mutate(`/tasks/${task.id}/evidence`, { version: task.version + 1, evidence: [{ label: "Human verification", url: evidence }] }); await mutate(`/tasks/${task.id}/transition`, { version: task.version + 2, state: "completed", reason: reason || "Human verified merge" }); await refresh(); } catch (reason) { setPolicyError((reason as Error).message); } finally { setSaving(false); } }}>Complete after human merge</Button>}
          {task.state === "in_progress" && task.executions.some((value) => /no.?change/i.test(String(value.stage || value.terminal_reason_code || ""))) && <Text role="status" aria-label="No change execution state" color="green.fg">Execution made no repository change.</Text>}
        </Stack>
      </Box>}

        </Box>
        {caps?.write_enabled && !caps.executor_enabled && ["proposed", "accepted"].includes(task.state) && <Box bg="orange.subtle" borderRadius="lg" padding="3"><Text role="status" color="orange.fg" fontSize="xs" lineHeight="tall">Bot execution is disabled. You can review and assign this task, but it will not run until execution is enabled.</Text></Box>}
      </Stack>
    </Grid>
  </Box>;
}

function duration(value?: number | null): string {
  if (value == null) return "—";
  if (value < 1000) return `${value} ms`;
  if (value < 60000) return `${(value / 1000).toFixed(1)}s`;
  return `${Math.floor(value / 60000)}m ${Math.floor(value % 60000 / 1000)}s`;
}
function timestamp(value?: string | null): string {
  if (!value) return "—";
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? value : date.toLocaleString(undefined, { dateStyle: "medium", timeStyle: "short" });
}
function RunEvidence({ run, onModels }: { run: RunProjection; onModels: () => void }) {
  const outcome = run.outcome === "skipped" && /no.?change/i.test(run.reason_code) ? "no_change" : run.outcome;
  return <Stack role="region" aria-label="Run evidence detail" gap="5">
    <HStack justify="space-between" align="start" gap="3" flexWrap="wrap"><Box><Text as="h2" fontSize="2xl" letterSpacing="-0.03em" fontWeight="semibold">Run {label(outcome).toLowerCase()}</Text><Metadata>{timestamp(run.started_at)} · Attempt {run.try_number} · {label(run.reason_code)}</Metadata></Box><StatusBadge value={label(outcome)} /></HStack>
    {run.reason_code === "model_rate_limited" && <ModelLimitNotice onModels={onModels} />}
    {run.outcome === "blocked" && <Text role="status" color="orange.fg">Blocked pending human action.</Text>}
    {outcome === "no_change" && <Text role="status" color="green.fg">No repository change was required.</Text>}
    {run.failure && <Box role="alert" aria-label="Bounded redacted failure detail" borderLeftWidth="3px" borderColor="red.solid" bg="red.subtle" padding="4" borderRadius="lg"><Text fontWeight="semibold" color="red.fg">What went wrong</Text><Text fontSize="sm" marginTop="2" overflowWrap="anywhere">{run.failure.detail || "Failure detail unavailable."}</Text></Box>}
    <Grid templateColumns={{ base: "repeat(2, minmax(0, 1fr))", md: "repeat(4, minmax(0, 1fr))" }} gap="3">{[
      ["Duration", duration(run.duration_ms)], ["Total tokens", run.total_tokens?.toLocaleString() ?? "—"], ["Requests", (run.requests ?? 0).toLocaleString()], ["Spend", run.cost_source === "unavailable" || run.cost_micro_usd == null ? "Not priced" : formatCost(run.cost_micro_usd)],
    ].map(([title, value]) => <Panel key={title}><Metadata>{title}</Metadata><Text fontSize="2xl" fontWeight="semibold" letterSpacing="-0.025em" marginTop="2" fontVariantNumeric="tabular-nums">{title === "Total tokens" && run.total_tokens != null ? <CountValue value={run.total_tokens} /> : value}</Text></Panel>)}</Grid>
    <Grid templateColumns={{ base: "minmax(0, 1fr)", lg: "repeat(2, minmax(0, 1fr))" }} gap="4">
      <Panel><SectionHeading>Run health</SectionHeading><Stack gap="3"><HStack justify="space-between"><Metadata>Evidence</Metadata><StatusBadge value={label(run.freshness)} /></HStack><HStack justify="space-between"><Metadata>Freshness target</Metadata><Text fontSize="sm">{run.freshness_sla_minutes == null ? "Not set" : `${run.freshness_sla_minutes} minutes`}</Text></HStack><HStack justify="space-between"><Metadata>Attempt number</Metadata><Text fontSize="sm">{run.try_number}</Text></HStack><HStack justify="space-between" flexWrap="wrap"><Metadata>Deadline</Metadata><Text fontSize="sm" title={run.deadline_at}>{timestamp(run.deadline_at)}</Text></HStack><HStack justify="space-between"><Metadata>Time consumed</Metadata><Text fontSize="sm">{duration(run.deadline_consumed_ms)}</Text></HStack></Stack></Panel>
      <Panel><SectionHeading>Pull request & review</SectionHeading><Stack gap="3"><EvidenceList items={run.pr.url ? [{ label: run.pr.number ? `Pull request #${run.pr.number}` : "Pull request", url: run.pr.url }] : []} />{!run.pr.url && <Metadata>No pull request published.</Metadata>}<HStack justify="space-between"><Metadata>Review</Metadata><StatusBadge value={run.review.verdict ? label(run.review.verdict) : "Not reviewed"} /></HStack><HStack justify="space-between"><Metadata>Merge</Metadata><StatusBadge value={label(run.merge_state)} /></HStack>{run.merged_at && <Metadata>Merged {timestamp(run.merged_at)}</Metadata>}{run.completed_at && <Metadata>Completed {timestamp(run.completed_at)}</Metadata>}</Stack></Panel>
    </Grid>
    <Panel role="region" aria-label="Run token usage"><SectionHeading>Usage breakdown</SectionHeading><Grid templateColumns={{ base: "repeat(2, minmax(0, 1fr))", md: "repeat(3, minmax(0, 1fr))" }} gap="4">{[
      ["Input", run.input_tokens], ["Output", run.output_tokens], ["Reasoning", run.reasoning_tokens], ["Cache read", run.cached_input_tokens], ["Cache write", run.cache_write_tokens],
    ].map(([name, value]) => <Box key={String(name)}><Metadata>{String(name)}</Metadata><Text fontSize="lg" fontVariantNumeric="tabular-nums">{value == null ? "—" : value.toLocaleString()}</Text></Box>)}</Grid><Text fontSize="xs" color="fg.muted" marginTop="4">Pricing: {run.cost_source ? label(run.cost_source) : "Not available"}</Text></Panel>
    <Disclosure title="Artifact & execution references" description="Digests, repository identity, and the exact run reference"><Stack gap="3" fontSize="sm">
      <HStack flexWrap="wrap"><Metadata>Run</Metadata><CodeValue value={run.run_id} label="run ID" copy /></HStack>
      <HStack flexWrap="wrap"><Metadata>Artifacts</Metadata>{run.artifact_digests.length ? run.artifact_digests.map(digest => <CodeValue key={digest} value={digest} label="artifact digest" copy />) : <Metadata>None</Metadata>}</HStack>
      <HStack flexWrap="wrap"><Metadata>Verification</Metadata><CodeValue value={run.verification_digest} label="verification digest" copy /></HStack>
      <HStack flexWrap="wrap"><Metadata>Report · {run.report_bytes.toLocaleString()} bytes</Metadata><CodeValue value={run.report_sha256} label="report digest" copy /></HStack>
      <Metadata>Repository: {run.pr.repository || "Not reported"}</Metadata><Metadata>Branch: {run.pr.branch || "Not reported"}</Metadata>
      <HStack flexWrap="wrap"><Metadata>Head</Metadata><CodeValue value={run.pr.head_sha} label="PR head SHA" copy /><Metadata>Base</Metadata><CodeValue value={run.pr.base_sha} label="base SHA" copy /></HStack>
    </Stack></Disclosure>
    <Disclosure title="Inspect report data" description="Original structured output from this run"><Box maxH="32rem" overflow="auto" fontSize="xs" fontFamily="mono"><SafeTree value={run.payload ?? {}} /></Box></Disclosure>
  </Stack>;
}

function BotsTab({ refreshKey, onModels, navigate, ...concurrencyProps }: ConcurrencyProps & { onModels: () => void; navigate: (action: () => void) => void }) {
  const [bots, setBots] = useState<BotSummary[] | null>(null);
  const [name, setName] = useState("");
  const [runs, setRuns] = useState<RunProjection[] | null>(null);
  const [report, setReport] = useState<RunProjection | null>(null);
  const [error, setError] = useState("");
  const [retry, setRetry] = useState(0);
  const [busy, setBusy] = useState(false);
  const [cursor, setCursor] = useState<string | null>(null);
  const navigation = useRef(0);
  const openRun = (run: RunProjection) => {
    const current = ++navigation.current;
    setBusy(true); setError("");
    request<RunProjection>(`/bots/${encodeURIComponent(name)}/runs/${encodeURIComponent(run.run_id)}`)
      .then(value => { if (current === navigation.current) setReport(value); })
      .catch(reason => { if (current === navigation.current) setError(reason.message); })
      .finally(() => { if (current === navigation.current) setBusy(false); });
  };
  const loadRuns = (bot: string, next?: string | null) => {
    const current = ++navigation.current;
    setBusy(true); setError(""); setName(bot);
    request<{ items: RunProjection[]; next_cursor?: string | null }>(`/bots/${encodeURIComponent(bot)}/runs${next ? `?cursor=${encodeURIComponent(next)}` : ""}`)
      .then((value) => { if (current === navigation.current) { setRuns((previous) => next ? [...(previous || []), ...value.items] : value.items); setCursor(value.next_cursor || null); } })
      .catch((reason) => { if (current === navigation.current) setError(reason.message); }).finally(() => { if (current === navigation.current) setBusy(false); });
  };
  useEffect(() => {
    const controller = new AbortController();
    request<{ items: BotSummary[] }>("/bots", { signal: controller.signal })
      .then((value) => setBots(value.items))
      .catch((reason) => { if (!controller.signal.aborted) setError(reason.message); });
    if (report) openRun(report);
    else if (name) loadRuns(name);
    return () => { controller.abort(); ++navigation.current; };
  }, [retry, refreshKey]);
  if (error) return <Panel role="alert"><Text color="red.fg">Unable to load bot activity: {error}</Text><Button size="sm" variant="outline" marginTop="3" onClick={() => { setError(""); setRetry((value) => value + 1); }}>Retry</Button></Panel>;
  if (!bots) return <LoadingState>Loading bots…</LoadingState>;
  if (report) {
    return <Stack gap="3"><Button alignSelf="start" size="sm" variant="outline" aria-label="Back to runs" onClick={() => { ++navigation.current; setBusy(false); setReport(null); }}>← Back to runs</Button>{busy && <Text role="status" fontSize="sm" color="fg.muted">Updating run…</Text>}<RunEvidence run={report} onModels={onModels} /></Stack>;
  }
  if (runs) {
    return <Stack role="region" aria-label="Bot runs" gap="3">
      <HStack justify="space-between"><Box><Text as="h2" fontSize="lg" fontWeight="bold">Runs for {label(name)}</Text><Metadata>Recent results, usage, and execution health.</Metadata></Box><Button size="sm" variant="outline" aria-label="Back to bots" onClick={() => { ++navigation.current; setBusy(false); setReport(null); setRuns(null); setName(""); }}>← Back to bots</Button></HStack>
      {busy && <Text role="status" fontSize="sm" color="fg.muted">Loading run details…</Text>}
      {runs.length === 0 ? <EmptyState title="No projected runs are available">Try another specialist or wait for the next scheduled run.</EmptyState> :
        <Box overflowX="auto" borderWidth="1px" borderColor="border" borderRadius="l2">
          <Table.Root size="sm" variant="outline">
            <Table.Header><Table.Row><Table.ColumnHeader>Run</Table.ColumnHeader><Table.ColumnHeader>Outcome</Table.ColumnHeader><Table.ColumnHeader>Reason</Table.ColumnHeader><Table.ColumnHeader>Duration</Table.ColumnHeader><Table.ColumnHeader>Tokens</Table.ColumnHeader><Table.ColumnHeader>Deadline / consumption</Table.ColumnHeader></Table.Row></Table.Header>
            <Table.Body>{runs.map((run) => <Table.Row key={`${run.run_id}:${run.try_number}`} cursor="pointer" _hover={{ bg: "bg.subtle" }} onClick={() => { if (!busy) openRun(run); }}>
              <Table.Cell><Button variant="plain" size="sm" disabled={busy} aria-label={`Open run ${run.run_id}`} title={run.run_id} onClick={event => { event.stopPropagation(); openRun(run); }}>{timestamp(run.started_at)}</Button><Metadata>try {run.try_number}</Metadata></Table.Cell>
              <Table.Cell><StatusBadge value={label(run.outcome)} /></Table.Cell><Table.Cell><Metadata>{label(run.reason_code)}</Metadata></Table.Cell><Table.Cell title={`${run.duration_ms} ms`}>{duration(run.duration_ms)}</Table.Cell><Table.Cell>{run.total_tokens == null ? "—" : run.total_tokens.toLocaleString()}</Table.Cell><Table.Cell>{timestamp(run.deadline_at)}<Metadata>{run.deadline_consumed_ms == null ? "not reported" : `${duration(run.deadline_consumed_ms)} consumed`}</Metadata></Table.Cell>
            </Table.Row>)}</Table.Body>
          </Table.Root>
        </Box>}
      {cursor && <Button size="sm" variant="outline" disabled={busy} onClick={() => loadRuns(name, cursor)}>Load more runs</Button>}
    </Stack>;
  }
  return <Stack role="region" aria-label="Bot evidence" gap="5">
    <ConcurrencySettings refreshKey={refreshKey} {...concurrencyProps} />
    {bots.some(bot => bot.latest_reason_code === "model_rate_limited") && <ModelLimitNotice onModels={onModels} />}
    <HStack justify="space-between"><SectionHeading description="Latest runs and evidence freshness. Select a bot to investigate.">Bot health</SectionHeading>{busy && <Spinner size="sm" />}</HStack>
    {bots.some((bot) => !["task_executor", "pr_reviewer"].includes(bot.name)) && bots.filter((bot) => !["task_executor", "pr_reviewer"].includes(bot.name)).every((bot) => bot.paused) && bots.length > 0 && <Panel bg="bg.subtle"><Text fontSize="sm">Scheduled bots are paused. The results below are historical; new recommendations require fresh bot runs.</Text></Panel>}
    {bots.length === 0 ? <EmptyState title="No specialist bots are registered">The bot registry is empty.</EmptyState> : <Grid templateColumns={{ base: "1fr", md: "repeat(2, minmax(0, 1fr))", xl: "repeat(3, minmax(0, 1fr))" }} gap="4">{bots.map((bot) =>
      <Button key={bot.name} variant="outline" borderColor="border" height="auto" display="block" whiteSpace="normal" textAlign="start" bg="bg.panel" borderRadius="xl!" padding="5" aria-label={`Open bot ${bot.name}`} disabled={busy} onClick={() => navigate(() => loadRuns(bot.name))} _hover={{ bg: "bg.subtle", borderColor: "blue.muted" }}>
        <HStack justify="space-between" gap="3" marginBottom="4"><Box padding="2" bg="bg.subtle" borderRadius="lg" color="fg.muted"><Glyph name="bots" size={20} /></Box><Text fontSize="xs" fontWeight="normal" color="fg.muted">{["task_executor", "pr_reviewer"].includes(bot.name) ? "On demand" : bot.paused ? "Paused" : "Scheduled"}</Text></HStack>
        <Text fontWeight="semibold" marginBottom="3">{label(bot.name)}</Text>
        <HStack gap="2" flexWrap="wrap"><StatusBadge value={bot.latest_status === "missing" ? "Not yet run" : label(bot.latest_status)} /><Text fontSize="xs" fontWeight="normal" color="fg.muted">{bot.latest_status !== "missing" ? label(bot.latest_reason_code) : "No recorded result"}</Text></HStack>
        <Box borderTopWidth="1px" borderColor="border" marginTop="4" paddingTop="3"><Text fontSize="xs" fontWeight="normal" color="fg.muted">{bot.latest ? `Evidence: ${label(bot.latest.freshness)} · ${bot.latest.freshness_sla_minutes == null ? "no SLA" : `SLA ${bot.latest.freshness_sla_minutes}m`}` : "Evidence not available"}</Text><HStack justify="space-between" marginTop="2"><Text fontSize="xs" fontWeight="normal" color="fg.muted" title={bot.latest_at || undefined}>{relativeTime(bot.latest_at)}</Text><HStack color="blue.fg" gap="1"><Text fontSize="xs">View runs</Text><Glyph name="arrow" size={12} /></HStack></HStack></Box>
      </Button>)}</Grid>}


  </Stack>;
}
export default function Plugin() {
  return <ActivityApp />;
}
