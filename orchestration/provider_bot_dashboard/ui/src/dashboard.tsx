import { Box, Button, HStack, Spinner, Text } from "@chakra-ui/react";
import { useEffect, useState } from "react";
import { QueueSummary, activityUrl, request } from "./api";

export default function Plugin() {
  const [data, setData] = useState<QueueSummary | null>(null);
  const [error, setError] = useState(false);
  useEffect(() => {
    const controller = new AbortController();
    const refresh = () => request<QueueSummary>("/summary", { signal: controller.signal })
      .then((value) => { setData(value); setError(false); })
      .catch(() => { if (!controller.signal.aborted) setError(true); });
    void refresh();
    const timer = setInterval(refresh, 60000);
    return () => { controller.abort(); clearInterval(timer); };
  }, []);
  return <HStack role="region" aria-label="Bot action queue" justify="space-between" gap="4" paddingX="4" paddingY="3" borderWidth="1px" borderColor="border" borderRadius="lg" bg="bg.panel" flexWrap="wrap">
    <HStack gap="3">
      <Box width="2" height="2" borderRadius="full" bg={error ? "orange.solid" : data?.attention_count ? "blue.solid" : "green.solid"} />
      <Text fontWeight="semibold" fontSize="sm">Bot actions</Text>
      {error ? <Text role="status" fontSize="sm" color="fg.muted">Unable to check the queue</Text>
        : data ? <Text fontSize="sm" color="fg.muted">{data.attention_count ? `${data.attention_count} ${data.attention_count === 1 ? "action needs" : "actions need"} your attention` : "No actions need your attention"}</Text>
        : <Spinner size="xs" aria-label="Loading bot actions" />}
    </HStack>
    <Button asChild variant="ghost" size="sm" colorPalette="blue"><a href={activityUrl()}>View bot activity →</a></Button>
  </HStack>;
}
