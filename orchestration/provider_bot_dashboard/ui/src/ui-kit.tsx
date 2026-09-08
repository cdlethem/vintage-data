import { Badge, Box, Button, HStack, Text } from "@chakra-ui/react";
import type { ReactNode } from "react";

export type StatusTone = "success" | "warning" | "danger" | "neutral" | "info";

export const LIFECYCLE_PHASES = ["proposal_to_acceptance", "acceptance_to_execution", "execution_to_pr", "pr_to_review", "review_to_merge", "merge_to_completion"] as const;
const toneFor = (value: string): StatusTone => {
  const normalized = value.toLowerCase();
  if (["fresh", "healthy", "success", "succeeded", "completed", "approved", "merged", "no_change", "no change"].some((item) => normalized.includes(item))) return "success";
  if (["stale", "degraded", "blocked", "pending", "retry", "changes_requested", "changes requested", "in_progress", "queued"].some((item) => normalized.includes(item))) return "warning";
  if (["failed", "error", "rejected", "timed out", "invalid"].some((item) => normalized.includes(item))) return "danger";
  if (["running", "accepted", "in_review", "in review"].some((item) => normalized.includes(item))) return "info";
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
  return <Badge colorPalette={paletteFor[tone || toneFor(value)]} variant="subtle" size="sm" whiteSpace="nowrap">{value}</Badge>;
}

export function Panel({ children, ...props }: { children: ReactNode; [key: string]: unknown }) {
  return <Box borderWidth="1px" borderColor="border" borderRadius="l2" bg="bg.panel" padding="4" {...props}>{children}</Box>;
}

export function SectionHeading({ children, description }: { children: ReactNode; description?: ReactNode }) {
  return <Box marginBottom="3"><Text fontWeight="semibold" color="fg">{children}</Text>{description && <Text color="fg.muted" fontSize="sm">{description}</Text>}</Box>;
}

export function Metadata({ children }: { children: ReactNode }) {
  return <Text color="fg.muted" fontSize="sm">{children}</Text>;
}

export function CodeValue({ value, label, copy = false }: { value?: string | null; label?: string; copy?: boolean }) {
  if (!value) return <Text as="span" color="fg.muted">—</Text>;
  const short = value.length > 12 ? `${value.slice(0, 12)}…` : value;
  return <HStack gap="1" display="inline-flex" maxW="100%" verticalAlign="middle">
    <Text as="code" fontFamily="mono" fontSize="sm" title={value} overflow="hidden" textOverflow="ellipsis" whiteSpace="nowrap">{short}</Text>
    {copy && <Button type="button" variant="ghost" size="xs" aria-label={`Copy ${label || "value"}`} onClick={() => { void navigator.clipboard?.writeText(value); }}>Copy</Button>}
  </HStack>;
}

export function EmptyState({ title, children }: { title: string; children?: ReactNode }) {
  return <Panel textAlign="center" paddingY="8" bg="bg.subtle">
    <Text fontWeight="semibold">{title}</Text>
    {children && <Text color="fg.muted" fontSize="sm" marginTop="1">{children}</Text>}
  </Panel>;
}
