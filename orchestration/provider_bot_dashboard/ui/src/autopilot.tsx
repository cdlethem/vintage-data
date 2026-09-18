import { Box, Button, HStack, Link, Stack, Text } from "@chakra-ui/react";
import { useEffect, useState } from "react";
import { activityUrl, label, relativeTime, request } from "./api";
import { Glyph, Metadata, Panel, StatusBadge } from "./ui-kit";

type AutopilotStatus = {
  enabled: boolean; version: number; model: string | null; model_problem: string | null;
  active_decisions?: number; concurrency?: number;
  deciding: boolean; last_checked_at: string | null; last_error: string | null;
  last_decision: { task_id: string; title: string; at: string; action: string; rationale: string; result: string } | null;
};
type Mutate = <T>(path: string, body: unknown, method?: string) => Promise<T>;

export function Autopilot({ writeEnabled, mutate, refreshKey }: { writeEnabled: boolean; mutate: Mutate; refreshKey: number }) {
  const [status, setStatus] = useState<AutopilotStatus | null>(null);
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  const [expanded, setExpanded] = useState(false);
  useEffect(() => {
    const controller = new AbortController();
    const load = () => request<AutopilotStatus>("/autopilot", { signal: controller.signal })
      .then(value => { setStatus(value); setError(""); })
      .catch(reason => { if (!controller.signal.aborted) setError(reason.message); });
    void load();
    const timer = setInterval(load, 30000);
    return () => { controller.abort(); clearInterval(timer); };
  }, [refreshKey]);
  const toggle = async () => {
    if (!status) return;
    setBusy(true); setError("");
    try { setStatus(await mutate<AutopilotStatus>("/autopilot", { enabled: !status.enabled, version: status.version }, "PUT")); }
    catch (reason) {
      setError((reason as Error).message);
      try { setStatus(await request<AutopilotStatus>("/autopilot")); } catch { /* Keep the last known state. */ }
    } finally { setBusy(false); }
  };
  if (!status) return <HStack role="status" color="fg.muted" fontSize="sm"><Glyph name="bots" /><Text>{error ? "Autopilot status unavailable. Refresh to retry." : "Checking Autopilot…"}</Text></HStack>;
  const stale = status.enabled && (!status.last_checked_at || Date.now() - Date.parse(status.last_checked_at) > 10 * 60000);
  return <Panel role="region" aria-label="Autopilot" padding={{ base: "4", md: "5" }} borderColor={status.enabled ? "blue.muted" : "border"}>
    <HStack justify="space-between" align="center" gap="4" flexWrap="wrap">
      <HStack align="start" gap="3" flex="1" minW="200px"><Box padding="2" bg={status.enabled ? "blue.subtle" : "bg.subtle"} color={status.enabled ? "blue.fg" : "fg.muted"} borderRadius="lg"><Glyph name="bots" /></Box><Box>
        <HStack gap="2" flexWrap="wrap"><Text fontWeight="semibold" fontSize="sm">Autopilot</Text><StatusBadge value={status.enabled ? "On" : "Off"} tone={status.enabled ? "success" : "neutral"} />{status.deciding && <Metadata>{(status.active_decisions || 1) > 1 ? `Considering ${status.active_decisions} decisions…` : "Considering a decision…"}</Metadata>}</HStack>
        <Text fontSize="sm" color="fg.muted" marginTop="1">{status.enabled ? "Your executive handles approvals and next steps. Every ticket and decision stays visible." : "You make approval decisions. Turn on to delegate them to the executive."}</Text>
      </Box></HStack>
      <HStack gap="2"><Button variant="ghost" size="sm" aria-expanded={expanded} onClick={() => setExpanded(!expanded)}>{expanded ? "Less detail" : "How it works"}</Button><Button size="sm" variant={status.enabled ? "outline" : "solid"} role="switch" aria-label="Autopilot" aria-checked={status.enabled} disabled={!writeEnabled || busy || !status.enabled && !!status.model_problem} onClick={toggle}>{busy ? "Saving…" : status.enabled ? "Turn off" : "Turn on"}</Button></HStack>
    </HStack>
    {expanded && <Stack gap="3" marginTop="4" paddingTop="4" borderTopWidth="1px" borderColor="border">
      <Text fontSize="sm" color="fg.muted">The executive reviews different tickets in parallel at your configured concurrency and decides whether to approve, delegate, revise, merge, or complete work. Verification, independent review, and repository checks still apply. Turning off stops new decisions; work already started continues. You can still act on any ticket.</Text>
      {status.last_decision && <Box bg="bg.subtle" padding="3" borderRadius="lg"><Metadata>{label(status.last_decision.action)} · {label(status.last_decision.result)} · {relativeTime(status.last_decision.at)}</Metadata><Link display="block" href={activityUrl(status.last_decision.task_id)} fontSize="sm" fontWeight="medium" color="blue.fg" marginTop="1">{status.last_decision.title}</Link><Text fontSize="sm" color="fg.muted" marginTop="2" whiteSpace="pre-wrap">{status.last_decision.rationale}</Text></Box>}
      <Metadata>{status.last_checked_at ? `Queue last checked ${relativeTime(status.last_checked_at)}` : "Waiting for the first executive run"}</Metadata>
    </Stack>}
    {(error || status.model_problem || status.enabled && status.last_error || stale) && <Text role="status" fontSize="sm" color="orange.fg" marginTop="3">{error || status.model_problem || status.last_error || "Waiting for the executive scheduler. Tickets remain available for manual action."}</Text>}
  </Panel>;
}
