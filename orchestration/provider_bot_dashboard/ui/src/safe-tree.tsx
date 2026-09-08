import { Box, Stack, Text } from "@chakra-ui/react";

export function SafeTree({ value, depth = 0 }: { value: unknown; depth?: number }) {
  if (depth >= 10) return <Text>[Nested data omitted]</Text>;
  if (value === null || ["boolean", "number", "string"].includes(typeof value)) {
    return <Text whiteSpace="pre-wrap" overflowWrap="anywhere">{String(value)}</Text>;
  }
  if (Array.isArray(value)) {
    return <Stack paddingLeft="3">
      {value.slice(0, 100).map((item, index) => <SafeTree key={index} value={item} depth={depth + 1} />)}
      {value.length > 100 && <Text>[{value.length - 100} items omitted]</Text>}
    </Stack>;
  }
  if (typeof value === "object") {
    return <Stack paddingLeft="3">
      {Object.entries(value as Record<string, unknown>).slice(0, 100).map(([key, item]) =>
        <Box key={key}><Text fontWeight="semibold">{key}</Text><SafeTree value={item} depth={depth + 1} /></Box>)}
    </Stack>;
  }
  return <Text>[Unsupported value]</Text>;
}
