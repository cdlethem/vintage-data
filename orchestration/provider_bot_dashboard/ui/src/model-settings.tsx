import { Box, Button, Grid, HStack, Input, NativeSelect, Spinner, Stack, Text } from "@chakra-ui/react";
import { useEffect, useState } from "react";
import { useUnsavedChanges } from "./use-unsaved-changes";
import { label, request } from "./api";
import { EmptyState, Metadata, Panel, SectionHeading, StatusBadge, LoadingState } from "./ui-kit";

type Provider = { id: string; name: string; base_url: string; has_api_key: boolean; models: string[] };
type Assignment = { role: string; provider_id: string; model: string; current_model?: string; current_provider?: string };
type Settings = { providers: Provider[]; assignments: Assignment[]; roles: string[]; current_models?: { role: string; current_provider: string; current_model: string }[] };
type Mutate = <T>(path: string, body: unknown, method?: string) => Promise<T>;
const roleLabel = (role: string) => role.startsWith("executor_") ? `${label(role.slice(9))} task executor` : label(role);

export function ModelSettings({ writeEnabled, executorEnabled, mutate, refreshKey = 0, onDirtyChange }: { onDirtyChange?: (dirty: boolean) => void; writeEnabled: boolean; executorEnabled: boolean; mutate: Mutate; refreshKey?: number }) {
  const [settings, setSettings] = useState<Settings | null>(null);
  const [assignments, setAssignments] = useState<Assignment[]>([]);
  const [loadError, setLoadError] = useState("");
  const [editingConnection, setEditingConnection] = useState(false);
  const [providerId, setProviderId] = useState("");
  const [name, setName] = useState("");
  const [baseUrl, setBaseUrl] = useState("");
  const [apiKey, setApiKey] = useState("");
  const [tested, setTested] = useState("");
  const [models, setModels] = useState<string[]>([]);
  const [providerNotice, setProviderNotice] = useState("");
  const [providerError, setProviderError] = useState("");
  const [mappingNotice, setMappingNotice] = useState("");
  const [mappingError, setMappingError] = useState("");
  const [busy, setBusy] = useState<"test" | "provider" | "mapping" | null>(null);
  const load = async (initial = false) => {
    try {
      const result = await request<Settings>("/model-settings");
      setSettings(result); setLoadError("");
      if (initial) setAssignments(result.assignments);
    } catch (error) { setLoadError((error as Error).message); }
  };
  useEffect(() => { void load(!settings); }, [refreshKey]);
  const selected = settings?.providers.find(provider => provider.id === providerId);
  const draft = { id: providerId || name.trim().toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, ""), name: name.trim(), base_url: baseUrl.trim().replace(/\/$/, ""), ...(apiKey ? { api_key: apiKey } : {}) };
  // Compare form values in memory only. Credentials are never put in URLs or browser storage.
  const signature = JSON.stringify(draft);
  const valid = !!draft.id && !!draft.name && /^https?:\/\//.test(draft.base_url);
  const chooseProvider = (id: string) => {
    const provider = settings?.providers.find(item => item.id === id);
    setProviderId(id); setName(provider?.name || ""); setBaseUrl(provider?.base_url || ""); setApiKey("");
    setTested(""); setModels(provider?.models || []); setProviderNotice(""); setProviderError("");
  };
  const testProvider = async () => {
    setBusy("test"); setProviderError(""); setProviderNotice(""); setTested("");
    try {
      const result = await mutate<{ models: string[]; message: string }>("/model-settings/test", draft);
      setModels(result.models); setTested(signature); setProviderNotice(`Connection verified. ${result.models.length} models available. Save the connection to use it.`);
    } catch (error) { setProviderError((error as Error).message); }
    finally { setBusy(null); }
  };
  const saveProvider = async () => {
    setBusy("provider"); setProviderError(""); setProviderNotice("");
    try {
      const provider = await mutate<Provider>("/model-settings/providers", draft);
      setApiKey(""); setTested(""); setProviderId(provider.id); setModels(provider.models);
      await load(); setEditingConnection(false); setProviderNotice("Connection saved. Choose which models each agent should use below.");
    } catch (error) { setProviderError((error as Error).message); }
    finally { setBusy(null); }
  };
  const updateAssignment = (role: string, change: Partial<Assignment>) => {
    setMappingNotice("");
    setAssignments(current => {
      const existing = current.find(item => item.role === role) || { role, provider_id: "", model: "" };
      return [...current.filter(item => item.role !== role), { ...existing, ...change }];
    });
  };
  const saveMappings = async () => {
    setBusy("mapping"); setMappingError(""); setMappingNotice("");
    try {
      await mutate("/model-settings", { assignments: assignments.filter(item => item.provider_id).map(({ role, provider_id, model }) => ({ role, provider_id, model })) }, "PUT");
      await load(true); setMappingNotice("Model assignments saved. New runs will use these settings.");
    } catch (error) { setMappingError((error as Error).message); }
    finally { setBusy(null); }
  };
  const normalizeAssignments = (values: Assignment[]) => JSON.stringify(values.filter(item => item.provider_id).map(({ role, provider_id, model }) => ({ role, provider_id, model })).sort((a, b) => a.role.localeCompare(b.role)));
  const providerDirty = (!settings?.providers.length || editingConnection) && (name !== (selected?.name || "") || baseUrl !== (selected?.base_url || "") || !!apiKey);
  useUnsavedChanges(!!providerDirty || !!settings && normalizeAssignments(assignments) !== normalizeAssignments(settings.assignments), onDirtyChange);
  if (!settings) return loadError ? <Panel role="alert"><Text>{loadError}</Text><Button marginTop="3" onClick={() => load(true)}>Retry settings</Button></Panel> : <LoadingState>Loading model settings…</LoadingState>;
  return <Stack gap="5" role="region" aria-label="Model settings">
    <SectionHeading description="Connect a model provider, then choose the model each agent uses for new runs.">Models &amp; connections</SectionHeading>
    {!writeEnabled && <Metadata>View only. Task editing permission is required to change connections or model assignments.</Metadata>}
    {!executorEnabled && <Panel bg="bg.subtle"><SectionHeading>Task execution needs setup</SectionHeading><Text fontSize="sm">Model connections and task execution are separate. You can configure models here, but assigned tasks also need an enabled executor, verified isolation, and a publishing connection before work can start.</Text></Panel>}
    {loadError && <Text role="alert" color="red.fg">{loadError}</Text>}
    <Panel>
      <HStack justify="space-between" align="start" gap="3"><SectionHeading description="Providers available to your bots and task executors.">Provider connections</SectionHeading>{settings.providers.length > 0 && <Button size="sm" variant="outline" borderColor="border" disabled={!writeEnabled || !!busy} onClick={() => { chooseProvider(""); setEditingConnection(true); }}>+ Add connection</Button>}</HStack>
      {settings.providers.length === 0 && <Box marginBottom="4"><EmptyState title="No model providers connected">Add your first connection to choose models for the bots and task executors.</EmptyState></Box>}
      {settings.providers.length > 0 && <Stack gap="2" marginBottom="4">{settings.providers.map(provider => <HStack key={provider.id} justify="space-between" flexWrap="wrap" bg="bg.subtle" padding="4" borderRadius="lg"><Box><Text fontWeight="medium">{provider.name}</Text><Metadata>{provider.base_url} · {provider.models.length} models · {provider.has_api_key ? "API key saved" : "No API key"}</Metadata></Box><HStack><StatusBadge value="Configured" tone="neutral" /><Button size="xs" variant="ghost" disabled={!writeEnabled || !!busy} onClick={() => { chooseProvider(provider.id); setEditingConnection(true); }}>Edit</Button></HStack></HStack>)}</Stack>}
      {providerNotice && !editingConnection && settings.providers.length > 0 && <Text role="status" color="green.fg" fontSize="sm">{providerNotice}</Text>}
      {(!settings.providers.length || editingConnection) && <Stack gap="3">
        <Text fontSize="sm" color="fg.muted">Use an OpenAI-compatible provider or model gateway. Include /v1 in the API URL when required.</Text>
        <Box><label htmlFor="connection-select">Connection to edit</label><NativeSelect.Root disabled={!writeEnabled || !!busy}><NativeSelect.Field id="connection-select" aria-label="Connection to edit" value={providerId} onChange={event => chooseProvider(event.target.value)}><option value="">Add a new connection</option>{settings.providers.map(provider => <option key={provider.id} value={provider.id}>{provider.name}</option>)}</NativeSelect.Field></NativeSelect.Root></Box>
        <Grid templateColumns={{ base: "1fr", md: "1fr 2fr" }} gap="3">
          <Box><label htmlFor="provider-name"><Text fontSize="sm">Connection name</Text></label><Input id="provider-name" disabled={!writeEnabled || !!busy} value={name} placeholder="My model provider" onChange={event => setName(event.target.value)} /></Box>
          <Box><label htmlFor="provider-url"><Text fontSize="sm">API base URL</Text></label><Input id="provider-url" disabled={!writeEnabled || !!busy} value={baseUrl} placeholder="https://provider.example/v1" onChange={event => setBaseUrl(event.target.value)} /></Box>
        </Grid>
        <Box><label htmlFor="provider-key"><Text fontSize="sm">API key</Text></label><Input id="provider-key" type="password" autoComplete="off" disabled={!writeEnabled || !!busy} value={apiKey} placeholder={selected?.has_api_key ? "Leave blank to keep the saved key" : "API key, if required"} onChange={event => setApiKey(event.target.value)} /><Metadata>Keys stay on the server after saving and are never shown again.</Metadata></Box>
        <SectionHeading description="Verify access and load the provider’s model list before saving.">Verify and save</SectionHeading>
        <HStack flexWrap="wrap"><Button variant="outline" disabled={!writeEnabled || !!busy || !valid} onClick={testProvider}>{busy === "test" ? "Testing connection…" : "Test and load models"}</Button><Button disabled={!writeEnabled || !!busy || !valid || tested !== signature} onClick={saveProvider}>{busy === "provider" ? "Saving connection…" : "Save connection"}</Button>{settings.providers.length > 0 && <Button variant="ghost" disabled={!!busy} onClick={() => { chooseProvider(""); setEditingConnection(false); }}>Cancel</Button>}</HStack>
        {providerNotice && <Text role="status" color="green.fg">{providerNotice}</Text>}
        {providerError && <Text role="alert" color="red.fg">{providerError}</Text>}
        {!!models.length && <Metadata>Available models: {models.slice(0, 8).join(", ")}{models.length > 8 ? ` and ${models.length - 8} more` : ""}.</Metadata>}
      </Stack>}
    </Panel>
    <Panel>
      <SectionHeading description="Each bot and executor profile can use a different model. Existing runs keep the settings they started with.">Agent models</SectionHeading>
      <Grid templateColumns={{ base: "1fr", lg: "repeat(2, minmax(0, 1fr))" }} gap="4">{settings.roles.map(role => {
        const assignment = assignments.find(item => item.role === role);
        const provider = settings.providers.find(item => item.id === assignment?.provider_id);
        const current = settings.current_models?.find(item => item.role === role);
        const executor = role.startsWith("executor_") || role === "executive";
        const modelOptions = [...new Set([...(provider?.models || []), ...(assignment?.model ? [assignment.model] : [])])];
        return <Grid key={role} templateColumns="minmax(0, 1fr)" gap="3" borderWidth="1px" borderColor="border" borderRadius="lg" padding="4">
          <Box minW="0"><Text fontWeight="medium">{roleLabel(role)}</Text>{executor && !assignment?.provider_id ? <Metadata>Execution blocked until a provider and model are assigned.</Metadata> : current?.current_model && <Metadata>Current: {current.current_provider ? `${current.current_provider} / ` : ""}{current.current_model}</Metadata>}</Box>
          <NativeSelect.Root disabled={!writeEnabled || !!busy}><NativeSelect.Field aria-label={`Provider for ${roleLabel(role)}`} value={assignment?.provider_id || ""} onChange={event => updateAssignment(role, { provider_id: event.target.value, model: "" })}><option value="">{executor ? "Not configured — execution blocked" : "Use existing configuration"}</option>{settings.providers.map(item => <option key={item.id} value={item.id}>{item.name}</option>)}</NativeSelect.Field></NativeSelect.Root>
          <NativeSelect.Root disabled={!writeEnabled || !!busy || !provider}><NativeSelect.Field aria-label={`Model for ${roleLabel(role)}`} value={assignment?.model || ""} onChange={event => updateAssignment(role, { model: event.target.value })}><option value="">{provider ? "Choose a model" : "Select a provider first"}</option>{modelOptions.map(model => <option key={model} value={model}>{model}</option>)}</NativeSelect.Field></NativeSelect.Root>
        </Grid>;
      })}</Grid>
      <Stack gap="3" marginTop="5" borderTopWidth="1px" borderColor="border" paddingTop="4">
      <Metadata>Junior, senior, and staff select task executor profiles. Each executor profile requires an explicit provider and model mapping before it can run assigned work.</Metadata>
      <Button alignSelf="start" disabled={!writeEnabled || !!busy || assignments.some(item => item.provider_id && !item.model)} onClick={saveMappings}>{busy === "mapping" ? "Saving assignments…" : "Save model assignments"}</Button>
      {mappingNotice && <Text role="status" color="green.fg">{mappingNotice}</Text>}
      {mappingError && <Text role="alert" color="red.fg">{mappingError}</Text>}
      </Stack>
    </Panel>
  </Stack>;
}
