import { Box, Button, HStack, NativeSelect, Stack, Text } from "@chakra-ui/react";
import { useEffect, useRef, useState } from "react";
import { label, request } from "./api";
import { Panel, SectionHeading } from "./ui-kit";
import { useUnsavedChanges } from "./use-unsaved-changes";

type Settings = { version: number; maximum: number; limits: Record<string, number>; workload?: { allowed: boolean; reason: string; open_count: number; max_open: number; created_24h: number; completed_24h: number; source_budget_24h: number }; bots: { name: string; running: number; queued: number; applied_limit: number | null }[] };
export type ConcurrencyProps = { refreshKey: number; writeEnabled: boolean; onDirtyChange?: (dirty: boolean) => void; mutate: <T>(path: string, body: unknown, method?: string) => Promise<T> };
export function ConcurrencySettings({ refreshKey, writeEnabled, onDirtyChange, mutate }: ConcurrencyProps) {
  const [poll, setPoll] = useState(0);
  useEffect(() => { const timer = setInterval(() => setPoll(value => value + 1), 15000); return () => clearInterval(timer); }, []);
  const [settings, setSettings] = useState<Settings | null>(null);
  const [draft, setDraft] = useState<Record<string, number>>({});
  const [expanded, setExpanded] = useState(false);
  const [error, setError] = useState("");
  const [notice, setNotice] = useState("");
  const [saving, setSaving] = useState(false);
  const dirty = !!settings && JSON.stringify(draft) !== JSON.stringify(settings.limits);
  const dirtyRef = useRef(dirty); dirtyRef.current = dirty;
  useUnsavedChanges(dirty, onDirtyChange);
  useEffect(() => {
    const controller = new AbortController();
    request<Settings>("/concurrency", { signal: controller.signal }).then(value => {
      if (!Array.isArray(value.bots) || !value.limits) throw new Error("Concurrency settings are unavailable. Refresh to try again.");
      if (!dirtyRef.current) { setSettings(value); setDraft(value.limits); setError(""); }
    }).catch(reason => { if (!controller.signal.aborted) setError(reason.message); });
    return () => controller.abort();
  }, [refreshKey, poll]);
  const save = async () => {
    if (!settings) return;
    setSaving(true); setError(""); setNotice("");
    try {
      const value = await mutate<Settings>("/concurrency", { version: settings.version, limits: draft }, "PUT");
      setSettings(value); setDraft(value.limits);
      setNotice("Concurrency saved. New limits apply as the scheduler refreshes; active runs will finish normally.");
    } catch (reason) { setError((reason as Error).message); }
    finally { setSaving(false); }
  };
  return <Panel><SectionHeading description="Choose how many runs each bot can work on at once. Executors and reviewers share worker capacity. Executive decisions can run on different tickets in parallel.">Concurrency</SectionHeading>
    <Stack gap="4">
      {error && <Box><Text role="alert" color="red.fg" fontSize="sm">{error}</Text><Button size="xs" variant="outline" marginTop="2" onClick={async () => { try { const value = await request<Settings>("/concurrency"); if (!Array.isArray(value.bots)) throw new Error("Concurrency settings unavailable"); setSettings(value); setDraft(value.limits); setError(""); setNotice(""); } catch (reason) { setError((reason as Error).message); } }}>Reload saved limits</Button></Box>}
      {!settings && !error && <Text role="status" fontSize="sm" color="fg.muted">Loading concurrency settings…</Text>}
      {settings?.workload && <Box padding="3" borderWidth="1px" borderRadius="lg" borderColor="border" bg="bg.subtle">
        <Text fontSize="sm" fontWeight="semibold">{settings.workload.allowed ? "Source intake available" : "Prioritizing existing work"}</Text>
        <Text fontSize="sm" marginTop="1">{settings.workload.reason}</Text>
        <Text fontSize="xs" color="fg.muted" marginTop="2">{settings.workload.open_count} open · {settings.workload.created_24h} opened / {settings.workload.completed_24h} completed in 24 hours</Text>
        <Text fontSize="xs" color="fg.muted" marginTop="1">Discovery runs daily; vetting checks twice daily. Intake requires fewer than {settings.workload.max_open} open tickets and room within measured throughput, with up to {settings.workload.source_budget_24h} new source tickets per day. Repairs and completion follow-ups remain active.</Text>
      </Box>}
      {settings?.bots.filter(bot => expanded || ["task_executor", "pr_reviewer", "executive"].includes(bot.name)).map(bot => <HStack key={bot.name} justify="space-between" gap="4" borderBottomWidth="1px" borderColor="border" paddingBottom="3">
        <Box><Text fontSize="sm" fontWeight="medium">{label(bot.name)}</Text><Text fontSize="xs" color="fg.muted" marginTop="1">{`${bot.running} running · ${bot.queued} queued`}</Text>{bot.applied_limit != null && bot.applied_limit !== settings.limits[bot.name] && <Text fontSize="xs" color="blue.fg">Applying limit… scheduler currently allows {bot.applied_limit}</Text>}</Box>
        <NativeSelect.Root size="sm" width="7rem" flexShrink="0" disabled={!writeEnabled || saving}><NativeSelect.Field aria-label={`${label(bot.name)} concurrency`} value={draft[bot.name]} onChange={event => { setDraft({ ...draft, [bot.name]: Number(event.target.value) }); setNotice(""); }}>{Array.from({ length: settings.maximum }, (_, i) => <option key={i + 1} value={i + 1}>{i + 1} {i === 0 ? "run" : "runs"}</option>)}</NativeSelect.Field></NativeSelect.Root>
      </HStack>)}
      <HStack justify="space-between" flexWrap="wrap"><Button size="sm" variant="plain" onClick={() => setExpanded(!expanded)}>{expanded ? "Show core bots" : "Configure all bots"}</Button><HStack>{dirty && <Button size="sm" variant="ghost" disabled={saving} onClick={() => { if (settings) setDraft(settings.limits); setNotice(""); }}>Discard</Button>}<Button size="sm" disabled={!writeEnabled || !dirty || saving} onClick={save}>{saving ? "Saving…" : "Save concurrency"}</Button></HStack></HStack>
      {notice && <Text role="status" fontSize="sm" color="green.fg">{notice}</Text>}
      <Text fontSize="xs" color="fg.muted">Lowering a limit lets active runs finish before new work starts. Provider availability can still limit throughput.</Text>
    </Stack>
  </Panel>;
}
export function ModelLimitNotice({ onModels }: { onModels: () => void }) {
  return <Panel borderColor="orange.muted" bg="orange.subtle"><HStack justify="space-between" align="start" flexWrap="wrap" gap="3"><Box><Text fontWeight="semibold" color="orange.fg">Model rate limit reached</Text><Text fontSize="sm" marginTop="1">The provider could not accept more work. Automatic retries are bounded. Change the affected bot’s model, then retry any blocked task; its approval and review requirements stay in place.</Text></Box><Button size="sm" variant="outline" onClick={onModels}>Change bot model</Button></HStack></Panel>;
}
