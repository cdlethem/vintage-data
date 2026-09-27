import { Box, Button, HStack, Input, Link, Stack, Text } from "@chakra-ui/react";
import { useState, type ReactNode } from "react";
import { label, relativeTime } from "./api";
import { Glyph, Metadata, StatusBadge } from "./ui-kit";
import { SafeTree } from "./safe-tree";

type RecordValue = Record<string, unknown>;
const text = (value: unknown) => typeof value === "string" ? value : "";
const record = (value: unknown): RecordValue => value && typeof value === "object" && !Array.isArray(value) ? value as RecordValue : {};

export function safeHref(value: string): string | undefined {
  try {
    const url = new URL(value);
    return ["https:", "http:"].includes(url.protocol) && !url.username && !url.password ? url.href : undefined;
  } catch { return undefined; }
}

// A deliberately small text renderer. Source HTML is always rendered literally.
function Inline({ value }: { value: string }) {
  return <>{value.split(/(`[^`\n]+`|\*\*[^*\n]+\*\*|\[[^\]\n]+\]\(https?:\/\/[^\s)]+\))/g).map((part, index) => {
    if (/^`[^`\n]+`$/.test(part)) return <Box as="code" key={index} fontFamily="mono" fontSize="0.9em" bg="bg.subtle" paddingX="1" borderRadius="sm" overflowWrap="anywhere">{part.slice(1, -1)}</Box>;
    if (/^\*\*[^*\n]+\*\*$/.test(part)) return <strong key={index}>{part.slice(2, -2)}</strong>;
    const link = part.match(/^\[([^\]]+)\]\((.+)\)$/);
    if (link && safeHref(link[2])) return <Link key={index} href={safeHref(link[2])} target="_blank" rel="noopener noreferrer" color="blue.fg" textDecoration="underline" textUnderlineOffset="3px">{link[1]}</Link>;
    return part;
  })}</>;
}

function Prose({ value, list = false }: { value: string; list?: boolean }) {
  // Separate long, plain prose at sentence boundaries without changing its wording.
  const blocks = value.split(/\n\s*\n/).flatMap(block => {
    if (list && !block.includes("\n")) return block.split(/(?<=;)\s+(?=[A-Za-z])|(?<=[.!?])\s+(?=[A-Z])/);
    if (block.length > 600 && !block.includes("\n")) {
      const sentences = block.split(/(?<=[.!?])\s+(?=[A-Z])/);
      const paragraphs: string[] = [];
      let current = "";
      for (const sentence of sentences) {
        current += (current ? " " : "") + sentence;
        if (current.length >= 260) { paragraphs.push(current); current = ""; }
      }
      if (current) paragraphs.push(current);
      return paragraphs;
    }
    return [block];
  }).filter(Boolean);
  return <Stack gap="3" fontSize="sm" lineHeight="1.8" color="fg">
    {blocks.map((block, index) => {
      const lines = block.split("\n");
      const markedList = lines.every(line => /^\s*(?:[-*]|\d+[.)])\s+/.test(line));
      if (list || markedList) {
        const ordered = !list && lines.every(line => /^\s*\d+[.)]\s+/.test(line));
        return <Stack as={ordered ? "ol" : "ul"} key={index} gap="2" paddingLeft="5" listStyleType={ordered ? "decimal" : "disc"}>{(markedList ? lines : [block]).map((line, i) => <Box as="li" key={i}><Inline value={line.replace(/^\s*(?:[-*]|\d+[.)])\s+/, "")} /></Box>)}</Stack>;
      }
      return <Text key={index} whiteSpace="pre-wrap" overflowWrap="anywhere"><Inline value={block} /></Text>;
    })}
  </Stack>;
}

function Paragraphs({ value, list = false }: { value: string; list?: boolean }) {
  const blocks: { code: boolean; language: string; lines: string[] }[] = [];
  let current = { code: false, language: "", lines: [] as string[] };
  for (const line of value.split("\n")) {
    const fence = line.match(/^\s*```([^`]*)$/);
    if (fence && (!current.code || !fence[1].trim())) {
      if (current.lines.length) blocks.push(current);
      current = { code: !current.code, language: current.code ? "" : fence[1].trim(), lines: [] };
    } else current.lines.push(line);
  }
  if (current.lines.length) blocks.push(current);
  return <Stack gap="3">{blocks.map((block, index) => block.code ? <Box key={index} borderWidth="1px" borderColor="border" bg="bg.subtle" borderRadius="lg" overflow="hidden">{block.language && <Text fontSize="xs" color="fg.muted" paddingX="3" paddingTop="2">{block.language}</Text>}<Box as="pre" margin="0" padding="3" overflowX="auto" tabIndex={0} fontFamily="mono" fontSize="xs" lineHeight="tall"><code>{block.lines.join("\n")}</code></Box></Box> : <Prose key={index} value={block.lines.join("\n")} list={list} />)}</Stack>;
}

export function TicketBrief({ value }: { value: string }) {
  const sections: { title: string; lines: string[] }[] = [{ title: "", lines: [] }];
  let fenced = false;
  for (const line of value.split("\n")) {
    if (line.trim().startsWith("```")) fenced = !fenced;
    const heading = !fenced && (line.match(/^#{1,4}\s+(.+?)\s*#*$/)?.[1] || line.match(/^(Implementation|Done when|Acceptance criteria|Verification|Context|Summary|Problem|Resolution|Scope|Risks?|Rollback|Expected outcome|Requirements|Plan|Definition of done):?\s*$/i)?.[1]);
    if (heading) sections.push({ title: heading, lines: [] });
    else sections.at(-1)!.lines.push(line);
  }
  return <Stack gap="6" data-ticket-brief="">
    {sections.map(({ title, lines }, index) => {
      const body = lines.join("\n").trim();
      if (!body) return null;
      const criteria = /done when|acceptance criteria|definition of done/i.test(title);
      const content = <Paragraphs value={body} list={criteria} />;
      if (/implementation/i.test(title) && body.length > 600) return <Disclosure key={index} title={title} description="Files, implementation steps, and verification requirements">{content}</Disclosure>;
      return <Box key={index}>{title && <HStack gap="2" marginBottom="3">{criteria && <Box color="fg.muted"><Glyph name="check" /></Box>}<Text as="h3" fontSize="sm" fontWeight="semibold">{title}</Text></HStack>}{content}</Box>;
    })}
  </Stack>;
}

export function Disclosure({ title, description, children }: { title: string; description?: string; children: ReactNode }) {
  return <Box as="details" borderWidth="1px" borderColor="border" borderRadius="lg" overflow="hidden">
    <Box as="summary" padding="3!" fontWeight="medium">{title}{description && <Text as="span" display="block" fontSize="xs" fontWeight="normal" marginTop="1" color="fg.muted">{description}</Text>}</Box>
    <Box padding="4" paddingTop="1">{children}</Box>
  </Box>;
}

export function EvidenceList({ items }: { items: unknown[] }) {
  return <Stack gap="3">{items.map((item, index) => {
    const value = record(item);
    const reference = text(value.reference || value.url);
    const href = safeHref(reference);
    return <Box key={index} padding="3" borderWidth="1px" borderColor="border" borderRadius="lg">
      <HStack justify="space-between" align="start"><Text fontSize="sm" fontWeight="medium">{text(value.label) || label(text(value.kind) || "Evidence")}</Text>{href && <Link href={href} target="_blank" rel="noopener noreferrer" color="blue.fg" fontSize="xs" whiteSpace="nowrap">Open source ↗</Link>}</HStack>
      {!!(value.summary || value.observation) && <Text fontSize="sm" color="fg.muted" marginTop="2" lineHeight="tall">{text(value.summary || value.observation)}</Text>}
      {reference && <Text fontFamily="mono" fontSize="xs" color="fg.muted" marginTop="2" overflowWrap="anywhere">{reference}</Text>}
    </Box>;
  })}</Stack>;
}

const eventTitles: Record<string, string> = {
  created: "Created this task", recommendation_created: "Proposed this task", recommendation_revised: "Updated the recommendation",
  executive_decision: "Made an executive decision", executive_error: "Deferred an executive decision",
  specialist_follow_up: "Reported follow-up findings",
  planning_requested: "Requested specialist planning", follow_up_ticket_linked: "Linked a follow-up ticket",
  assigned: "Updated the assignee", task_edited: "Updated the task", comment_added: "Added a comment", evidence_added: "Added evidence",
  execution_admitted: "Queued bot work", execution_finalized: "Finished execution", execution_started: "Started execution", execution_queued: "Queued bot work", change_published: "Published a change",
  queue_reset: "Archived this task", resurface_requested: "Requested another review",
};

function EventContent({ event }: { event: RecordValue }) {
  const payload = record(event.payload);
  const type = text(event.event_type);
  if (type === "planning_requested") return <Stack gap="2"><Metadata>{label(text(payload.bot))} · Read-only planning</Metadata><TicketBrief value={text(payload.request)} /></Stack>;
  if (type === "follow_up_ticket_linked" && /^[0-9a-f-]{36}$/i.test(text(payload.task_id))) return <Link href={`?task=${encodeURIComponent(text(payload.task_id))}`} target="_blank" rel="noopener noreferrer" color="blue.fg">{text(payload.title) || "Open follow-up ticket"} ↗</Link>;
  if (type === "executive_decision") return <Stack gap="2" bg="blue.subtle" padding="3" borderRadius="lg"><HStack flexWrap="wrap"><StatusBadge value={label(text(payload.action))} tone="neutral" /><Metadata>{label(text(payload.result))}</Metadata></HStack><Paragraphs value={text(payload.rationale)} />{!!payload.error && <Text color="orange.fg" fontSize="sm">{text(payload.error)}</Text>}</Stack>;
  if (type === "comment_added") return <Stack gap="2" bg="bg.subtle" padding="3" borderRadius="lg">{(Array.isArray(payload.items) ? payload.items : []).map((item, i) => <Paragraphs key={i} value={text(item)} />)}</Stack>;
  if (type === "specialist_follow_up") return <Stack gap="2" bg="bg.subtle" padding="3" borderRadius="lg"><Metadata>{payload.status === "degraded_evidence" ? "Additional evidence required" : "Follow-up recorded"}</Metadata><TicketBrief value={text(payload.summary)} /></Stack>;
  if (type === "evidence_added" && Array.isArray(payload.items)) return <EvidenceList items={payload.items} />;
  if (type === "task_edited") {
    const after = record(payload.after);
    return <Text fontSize="xs" color="fg.muted">Changed {Object.keys(after).map(key => ({ planned_resolution: "resolution plan", verification_commands: "verification checks", allowed_path_globs: "allowed files", resource_keys: "reserved resources", follow_up_bots: "follow-up bots", reviewer_required: "review requirement" }[key] || label(key).toLowerCase())).join(", ") || "task details"}.</Text>;
  }
  if (type === "assigned") return <StatusBadge value={payload.kind === "bot" ? `${label(text(payload.profile))} bot` : "Human assignee"} tone="neutral" />;
  return payload.reason ? <Paragraphs value={text(payload.reason)} /> : null;
}

export function ActivityTimeline({ events, preview = false }: { events: RecordValue[]; preview?: boolean }) {
  const [limit, setLimit] = useState(preview ? 3 : 8);
  const ordered = [...events].sort((a, b) => Number(b.sequence || 0) - Number(a.sequence || 0));
  return <Stack gap="4" role="region" aria-label="Task activity">
    <HStack justify="space-between"><Text as="h3" fontWeight="semibold" fontSize="sm">Activity</Text><Metadata>{events.length} events</Metadata></HStack>
    {ordered.length === 0 && <Metadata>No activity recorded yet.</Metadata>}
    <Stack as="ol" listStyleType="none" gap="0">{ordered.slice(0, limit).map((event, index) => {
      const actor = event.actor_id === "executive" ? "Executive" : text(event.actor_id) || text(event.actor_kind) || "System";
      const type = text(event.event_type);
      return <HStack as="li" key={String(event.sequence ?? index)} align="stretch" gap="3">
        <Stack align="center" gap="0"><Box borderWidth="1px" borderColor="border" borderRadius="full" width="7" height="7" display="grid" placeItems="center" bg="bg.subtle" fontSize="10px" color="fg.muted">{actor.slice(0, 2).toUpperCase()}</Box><Box width="1px" flex="1" bg="border" marginY="1" /></Stack>
        <Stack flex="1" minW="0" gap="2" paddingBottom="5"><HStack justify="space-between" gap="2" flexWrap="wrap"><Text fontSize="sm"><Text as="span" fontWeight="medium">{actor.length > 32 ? label(text(event.actor_kind) || "System") : actor}</Text><Text as="span" color="fg.muted"> · {type === "state_changed" ? `Moved ${event.from_state ? `from ${label(text(event.from_state))} ` : ""}to ${label(text(event.to_state))}` : eventTitles[type] || label(type)}</Text></Text><Text fontSize="xs" color="fg.muted" title={text(event.created_at)}>{event.created_at ? relativeTime(text(event.created_at)) : ""}</Text></HStack><EventContent event={event} /></Stack>
      </HStack>;
    })}</Stack>
    {!preview && limit < events.length && <Button alignSelf="start" variant="outline" borderColor="border" size="sm" onClick={() => setLimit(value => value + 12)}>Show older activity ({events.length - limit})</Button>}
  </Stack>;
}

export function ExecutionList({ executions }: { executions: RecordValue[] }) {
  return <Stack gap="3">{executions.length === 0 ? <Box bg="bg.subtle" padding="4" borderRadius="lg"><Text fontSize="sm" fontWeight="medium">No execution yet</Text><Metadata>Execution and pull requests will appear here once work starts.</Metadata></Box> : [...executions].reverse().map((execution, index) => <Box key={index} borderWidth="1px" borderColor="border" borderRadius="lg" padding="4">
    <HStack justify="space-between" flexWrap="wrap"><Text fontWeight="medium" fontSize="sm">Attempt {String(execution.sequence || executions.length - index)}</Text><StatusBadge value={label(text(execution.terminal_reason_code || execution.stage || execution.dispatch_state) || "pending")} /></HStack>
    <HStack gap="4" flexWrap="wrap" marginTop="3"><Metadata>Review: {label(text(execution.review_verdict) || "pending")}</Metadata>{execution.pr_number != null && <Metadata>Pull request #{String(execution.pr_number)}</Metadata>}</HStack>
    {safeHref(text(execution.pr_url)) && <Link display="inline-block" marginTop="2" href={safeHref(text(execution.pr_url))} target="_blank" rel="noopener noreferrer" color="blue.fg" fontSize="sm">Open pull request ↗</Link>}
  </Box>)}</Stack>;
}

export function TechnicalRecords({ revisions, executions, events }: { revisions: RecordValue[]; executions: RecordValue[]; events: RecordValue[] }) {
  return <Disclosure title="Inspect raw records" description="Original recommendation, execution, and audit data">
    <Stack gap="3">{[["Recommendation revisions", revisions], ["Execution records", executions], ["Audit records", events]].map(([title, values]) => <Disclosure key={String(title)} title={String(title)}><Box maxH="24rem" overflow="auto" tabIndex={0} fontSize="xs" fontFamily="mono"><SafeTree value={values} /></Box></Disclosure>)}</Stack>
  </Disclosure>;
}

export function ExecutionScope({ revision, reviewerRequired }: { revision: RecordValue; reviewerRequired: boolean }) {
  const commands = Array.isArray(revision.verification_commands) ? revision.verification_commands.filter(Array.isArray) as unknown[][] : [];
  const files = Array.isArray(revision.allowed_path_globs) ? revision.allowed_path_globs : [];
  const followUps = Array.isArray(revision.follow_up_bots) ? revision.follow_up_bots : [];
  return <Stack gap="5" role="region" aria-label="Execution scope">
    <HStack justify="space-between" flexWrap="wrap"><Text fontSize="sm" fontWeight="semibold">Execution scope</Text><StatusBadge value={reviewerRequired ? "Review required" : "Review optional"} tone="neutral" /></HStack>
    <Box><Text fontSize="xs" color="fg.muted" marginBottom="2">Allowed files · {files.length}</Text><Stack gap="1">{files.length ? files.map((path, index) => <Text key={index} fontFamily="mono" fontSize="xs" bg="bg.subtle" padding="2" borderRadius="md" overflowWrap="anywhere">{text(path)}</Text>) : <Metadata>No file boundaries configured.</Metadata>}</Stack></Box>
    <Box><Text fontSize="xs" color="fg.muted" marginBottom="2">Verification checks · {commands.length}</Text><Stack gap="2">{commands.length ? commands.map((argv, index) => <Box key={index} bg="bg.subtle" borderRadius="md" padding="3"><Text fontSize="xs" fontWeight="medium" marginBottom="2">Check {index + 1}</Text><HStack gap="1" flexWrap="wrap">{argv.map((arg, i) => <Text as="code" key={i} fontFamily="mono" fontSize="xs" overflowWrap="anywhere" bg="bg.panel" paddingX="1.5" paddingY="0.5" borderRadius="sm">{text(arg)}</Text>)}</HStack></Box>) : <Metadata>No verification checks configured.</Metadata>}</Stack></Box>
    {!!followUps.length && <Box><Text fontSize="xs" color="fg.muted" marginBottom="2">Follow-up bots</Text><HStack flexWrap="wrap">{followUps.map((bot, index) => <StatusBadge key={index} value={label(text(bot))} tone="neutral" />)}</HStack></Box>}
  </Stack>;
}

export function CommandEditor({ value, onChange, disabled }: { value: string; onChange: (value: string) => void; disabled: boolean }) {
  let commands: string[][];
  try { commands = JSON.parse(value); } catch { commands = []; }
  const update = (next: string[][]) => onChange(JSON.stringify(next, null, 2));
  return <Stack gap="3" role="group" aria-label="Verification commands">
    <Box><Text fontSize="sm" fontWeight="medium">Verification checks</Text><Text fontSize="xs" color="fg.muted">Each check has a program and separate arguments. Spaces inside an argument are preserved.</Text></Box>
    {commands.map((argv, commandIndex) => <Stack key={commandIndex} gap="2" bg="bg.panel" padding="3" borderWidth="1px" borderColor="border" borderRadius="lg">
      <HStack justify="space-between"><Text fontSize="xs" fontWeight="medium">Check {commandIndex + 1}</Text><Button size="xs" variant="ghost" disabled={disabled} onClick={() => update(commands.filter((_, i) => i !== commandIndex))}>Remove check {commandIndex + 1}</Button></HStack>
      {argv.map((arg, argIndex) => <HStack key={argIndex} gap="2"><Input size="sm" fontFamily="mono" fontSize="xs" aria-label={`Check ${commandIndex + 1} ${argIndex === 0 ? "program" : `argument ${argIndex}`}`} value={arg} disabled={disabled} placeholder={argIndex === 0 ? "Program, e.g. python3" : "Argument"} onChange={event => update(commands.map((command, i) => i === commandIndex ? command.map((item, j) => j === argIndex ? event.target.value : item) : command))} />{argIndex > 0 && <Button size="xs" variant="ghost" aria-label={`Remove check ${commandIndex + 1} argument ${argIndex}`} disabled={disabled} onClick={() => update(commands.map((command, i) => i === commandIndex ? command.filter((_, j) => j !== argIndex) : command))}>×</Button>}</HStack>)}
      <Button alignSelf="start" size="xs" variant="ghost" disabled={disabled || argv.length >= 32} onClick={() => update(commands.map((command, i) => i === commandIndex ? [...command, ""] : command))}>+ Add argument</Button>
    </Stack>)}
    <Button size="sm" variant="outline" alignSelf="start" borderColor="border" disabled={disabled || commands.length >= 20} onClick={() => update([...commands, [""]])}>+ Add verification check</Button>
  </Stack>;
}
