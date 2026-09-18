// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { ChakraProvider, defaultSystem } from "@chakra-ui/react";
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import { ModelSettings } from "./model-settings";
const response = (value: unknown) => new Response(JSON.stringify(value), { headers: { "Content-Type": "application/json" } });
afterEach(() => { cleanup(); vi.unstubAllGlobals(); });
it("connects a provider, clears its secret, and saves executor model assignments", async () => {
  const provider = { id: "my-provider", name: "My provider", base_url: "https://provider.example/v1", has_api_key: true, models: ["model-one"] };
  let connected = false;
  const fetchMock = vi.fn(async () => response({ providers: connected ? [provider] : [], assignments: [], roles: ["executor_senior"] }));
  vi.stubGlobal("fetch", fetchMock);
  const mutate = vi.fn(async (path: string) => {
    if (path.endsWith("/test")) return { models: provider.models, message: "ok" };
    if (path.endsWith("/providers")) { connected = true; return provider; }
    return {};
  });
  render(<ChakraProvider value={defaultSystem}><ModelSettings writeEnabled executorEnabled={false} mutate={mutate as never} /></ChakraProvider>);
  expect(await screen.findByText("No model providers connected")).toBeInTheDocument();
  expect(screen.getByText("Task execution needs setup")).toBeInTheDocument();
  fireEvent.change(screen.getByLabelText("Connection name"), { target: { value: "My provider" } });
  fireEvent.change(screen.getByLabelText("API base URL"), { target: { value: provider.base_url } });
  fireEvent.change(screen.getByLabelText("API key"), { target: { value: "secret-test-value" } });
  expect(screen.getByLabelText("API key")).toHaveAttribute("type", "password");
  expect(screen.getByRole("button", { name: "Save connection" })).toBeDisabled();
  fireEvent.click(screen.getByRole("button", { name: "Test and load models" }));
  expect(await screen.findByText(/Connection verified/)).toBeInTheDocument();
  fireEvent.click(screen.getByRole("button", { name: "Save connection" }));
  expect(await screen.findByText(/Connection saved/)).toBeInTheDocument();
  fireEvent.click(screen.getByRole("button", { name: "Edit" }));
  expect(screen.getByLabelText("API key")).toHaveValue("");
  fireEvent.click(screen.getByRole("button", { name: "Cancel" }));
  expect(localStorage.length).toBe(0);
  expect(sessionStorage.length).toBe(0);
  fireEvent.change(screen.getByLabelText("Provider for Senior task executor"), { target: { value: provider.id } });
  expect(screen.getByRole("button", { name: "Save model assignments" })).toBeDisabled();
  fireEvent.change(screen.getByLabelText("Model for Senior task executor"), { target: { value: "model-one" } });
  fireEvent.click(screen.getByRole("button", { name: "Save model assignments" }));
  await waitFor(() => expect(mutate).toHaveBeenLastCalledWith("/model-settings", { assignments: [{ role: "executor_senior", provider_id: "my-provider", model: "model-one" }] }, "PUT"));
});
it("shows connection failures beside the form and keeps saving disabled", async () => {
  vi.stubGlobal("fetch", vi.fn(async () => response({ providers: [], assignments: [], roles: [] })));
  const mutate = vi.fn(async () => { throw new Error("Provider authentication failed"); });
  render(<ChakraProvider value={defaultSystem}><ModelSettings writeEnabled executorEnabled mutate={mutate} /></ChakraProvider>);
  await screen.findByText("No model providers connected");
  fireEvent.change(screen.getByLabelText("Connection name"), { target: { value: "Provider" } });
  fireEvent.change(screen.getByLabelText("API base URL"), { target: { value: "https://provider.example/v1" } });
  fireEvent.click(screen.getByRole("button", { name: "Test and load models" }));
  expect(await screen.findByRole("alert")).toHaveTextContent("Provider authentication failed");
  expect(screen.getByRole("button", { name: "Save connection" })).toBeDisabled();
});

it("shows unmapped executors as blocked without presenting a CLI fallback", async () => {
  vi.stubGlobal("fetch", vi.fn(async () => response({ providers: [], assignments: [], roles: ["executor_senior", "source_vetting"], current_models: [{ role: "executor_senior", current_provider: "command", current_model: "legacy-executor-model" }, { role: "source_vetting", current_provider: "command", current_model: "existing-specialist-model" }] })));
  const mutate = vi.fn(async () => ({}));
  render(<ChakraProvider value={defaultSystem}><ModelSettings writeEnabled executorEnabled mutate={mutate as never} /></ChakraProvider>);
  expect(await screen.findByRole("option", { name: "Not configured — execution blocked" })).toBeInTheDocument();
  expect(screen.getByText("Execution blocked until a provider and model are assigned.")).toBeInTheDocument();
  expect(screen.queryByText(/legacy-executor-model/)).not.toBeInTheDocument();
  expect(screen.getByRole("option", { name: "Use existing configuration" })).toBeInTheDocument();
  expect(screen.getByText(/Current: command \/ existing-specialist-model/)).toBeInTheDocument();
});
