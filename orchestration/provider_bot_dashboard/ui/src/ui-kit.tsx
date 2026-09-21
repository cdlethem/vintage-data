import { Badge, Box, Button, HStack, Spinner, Stack, Text } from "@chakra-ui/react";
import { useEffect, useRef, useState, type ReactNode } from "react";

export type StatusTone = "success" | "warning" | "danger" | "neutral" | "info";

export const LIFECYCLE_PHASES = ["proposal_to_acceptance", "acceptance_to_execution", "execution_to_pr", "pr_to_review", "review_to_merge", "merge_to_completion"] as const;
const toneFor = (value: string): StatusTone => {
  const normalized = value.toLowerCase();
  if (["failed", "error", "rejected", "timed out", "timed_out", "invalid"].some((item) => normalized.includes(item))) return "danger";
  if (["fresh", "healthy", "success", "succeeded", "completed", "approved", "merged", "no_change", "no change"].some((item) => normalized.includes(item))) return "success";
  if (["stale", "degraded", "blocked", "pending", "retry", "changes_requested", "changes requested", "in_progress", "queued"].some((item) => normalized.includes(item))) return "warning";
  if (["running", "in progress", "accepted", "in_review", "in review"].some((item) => normalized.includes(item))) return "info";
  return "neutral";
};

const paletteFor: Record<StatusTone, string> = {
  success: "green",
  warning: "orange",
  danger: "red",
  neutral: "gray",
  info: "blue",
};

export function StatusBadge({ value, tone }: { value: string; tone?: StatusTone }) {
  return <Badge colorPalette={paletteFor[tone || toneFor(value)]} variant="subtle" size="sm" borderRadius="full" paddingX="2" paddingY="0.5" fontWeight="medium" whiteSpace="nowrap">{value}</Badge>;
}

export function Panel({ children, ...props }: { children: ReactNode; [key: string]: unknown }) {
  return <Box borderWidth="1px" borderColor="border" borderRadius="xl" bg="bg.panel" padding={{ base: "4", md: "5" }} minW="0" {...props}>{children}</Box>;
}

export function SectionHeading({ children, description }: { children: ReactNode; description?: ReactNode }) {
  return <Box marginBottom="3"><Text as="h2" fontWeight="semibold" fontSize="md" letterSpacing="-0.015em" color="fg">{children}</Text>{description && <Text color="fg.muted" fontSize="sm" marginTop="1" lineHeight="tall">{description}</Text>}</Box>;
}

export function Metadata({ children }: { children: ReactNode }) {
  return <Text color="fg.muted" fontSize="sm" fontWeight="normal" letterSpacing="normal" overflowWrap="anywhere">{children}</Text>;
}

export function CodeValue({ value, label, copy = false }: { value?: string | null; label?: string; copy?: boolean }) {
  const [feedback, setFeedback] = useState("");
  const timer = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);
  useEffect(() => () => clearTimeout(timer.current), []);
  if (!value) return <Text as="span" color="fg.muted">—</Text>;
  const short = value.length > 12 ? `${value.slice(0, 12)}…` : value;
  return <HStack gap="1" display="inline-flex" maxW="100%" verticalAlign="middle" flexWrap="wrap">
    <Text as="code" fontFamily="mono" fontSize="sm" title={value} overflow="hidden" textOverflow="ellipsis" whiteSpace="nowrap">{feedback.startsWith("Copy unavailable") ? value : short}</Text>
    {copy && <Button type="button" variant="ghost" size="xs" aria-label={`Copy ${label || "value"}`} onClick={async () => {
      clearTimeout(timer.current);
      try { await navigator.clipboard.writeText(value); setFeedback("Copied"); }
      catch { setFeedback("Copy unavailable. Select the value to copy it."); }
      timer.current = setTimeout(() => setFeedback(""), 3000);
    }}><Glyph name="copy" />Copy</Button>}
    {feedback && <Text as="span" role="status" fontSize="xs" color="fg.muted">{feedback}</Text>}
  </HStack>;
}

type GlyphName = "queue" | "bots" | "usage" | "settings" | "refresh" | "plus" | "search" | "arrow" | "check" | "clock" | "alert" | "copy";
const paths: Record<GlyphName, ReactNode> = {
  queue: <><rect x="4" y="4" width="16" height="16" rx="3" /><path d="M8 9h8M8 13h5M8 17h3" /></>,
  bots: <><rect x="4" y="7" width="16" height="13" rx="4" /><path d="M12 3v4M8 12v2m8-2v2M9 17h6M1 12v3m22-3v3" /></>,
  usage: <><path d="M4 4v16h16M8 15v-4m5 4V7m5 8V4" /></>,
  settings: <><path d="M4 7h6m4 0h6M4 17h10m4 0h2" /><circle cx="12" cy="7" r="2" /><circle cx="16" cy="17" r="2" /></>,
  refresh: <><path d="M20 7v5h-5M4 17v-5h5M6 7a7 7 0 0 1 12-1l2 6M4 12l2 6a7 7 0 0 0 12-1" /></>,
  plus: <path d="M12 5v14M5 12h14" />,
  search: <><circle cx="10.5" cy="10.5" r="6.5" /><path d="m16 16 4 4" /></>,
  arrow: <path d="m9 5 7 7-7 7" />,
  check: <path d="m5 12 4 4L19 6" />,
  clock: <><circle cx="12" cy="12" r="9" /><path d="M12 7v5l3 2" /></>,
  alert: <><path d="m10 4-8 14a2 2 0 0 0 2 3h16a2 2 0 0 0 2-3L14 4a2.3 2.3 0 0 0-4 0Z M12 9v4m0 3v1" /></>,
  copy: <><rect x="8" y="8" width="12" height="12" rx="2" /><path d="M16 8V4H4v12h4" /></>,
};
export function Glyph({ name, size = 16 }: { name: GlyphName; size?: number }) {
  return <svg width={size} height={size} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true" style={{ flexShrink: 0 }}>{paths[name]}</svg>;
}

export function LoadingState({ children }: { children: string }) {
  return <HStack role="status" justify="center" paddingY="16" gap="3"><Spinner size="sm" /><Text color="fg.muted" fontSize="sm">{children}</Text></HStack>;
}

export function EmptyState({ title, children, action }: { title: string; children?: ReactNode; action?: ReactNode }) {
  return <Panel textAlign="center" paddingY="12">
    <Stack align="center" gap="3" maxW="26rem" marginX="auto">
      <Box padding="3" borderRadius="full" bg="bg.subtle" color="fg.muted"><Glyph name="queue" size={24} /></Box>
      <Text fontWeight="semibold">{title}</Text>
      {children && <Text color="fg.muted" fontSize="sm" lineHeight="tall">{children}</Text>}
      {action}
    </Stack>
  </Panel>;
}

// Scoped to the plugin so Airflow's navigation and other pages keep their own theme.
export const interfaceStyles = {
  "& button, & input, & select, & textarea": { borderRadius: "lg" },
  "& button[data-variant=outline]": { borderColor: "border" },
  "& button": { transition: "background 120ms ease, border-color 120ms ease", fontWeight: "medium" },
  "& :is(button, a, input, select, textarea, summary, [tabindex]):not([tabindex='-1']):focus-visible": { outline: "2px solid", outlineColor: "blue.fg", outlineOffset: "3px" },
  "& input, & select, & textarea": { bg: "bg.panel", minWidth: "0" },
  "& label": { display: "block", fontSize: "sm", fontWeight: "medium", marginBottom: "1.5" },
  "& th": { bg: "bg.subtle", color: "fg.muted", fontSize: "xs", fontWeight: "medium", whiteSpace: "nowrap", paddingY: "3" },
  "& td": { paddingY: "3", fontVariantNumeric: "tabular-nums" },
  "& summary": { borderRadius: "md", color: "fg.muted", fontSize: "sm", padding: "3", cursor: "pointer", _hover: { bg: "bg.subtle", color: "fg" } },
  "& details[open] > summary": { color: "fg", marginBottom: "3" },
  "& [role=dialog]": { overflowWrap: "anywhere" },
  "@media (prefers-reduced-motion: reduce)": { "& *": { transition: "none !important" } },
};
