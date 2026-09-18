// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { ChakraProvider, defaultSystem } from "@chakra-ui/react";
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import { ConcurrencySettings, ModelLimitNotice } from "./concurrency";
const settings = { version: 1, maximum: 16, limits: { task_executor: 1, pr_reviewer: 1, executive: 1 }, bots: ["task_executor", "pr_reviewer", "executive"].map(name => ({ name, running: 1, queued: 3, applied_limit: 1 })) };
afterEach(() => { cleanup(); vi.unstubAllGlobals(); });
it("explains automatic intake holds without hiding resolution capacity", async () => {
  vi.stubGlobal("fetch", vi.fn(async () => new Response(JSON.stringify({ ...settings, workload: {
    allowed: false, reason: "Finish existing work before adding sources.", open_count: 39,
    max_open: 8, created_24h: 18, completed_24h: 17, source_budget_24h: 1,
  } }))));
  render(<ChakraProvider value={defaultSystem}><ConcurrencySettings refreshKey={0} writeEnabled mutate={vi.fn()} /></ChakraProvider>);
  expect(await screen.findByText("Prioritizing existing work")).toBeInTheDocument();
  expect(screen.getByText(/18 opened \/ 17 completed/)).toBeInTheDocument();
  expect(screen.getByLabelText("Task executor concurrency")).toBeEnabled();
});
it("saves execution, review, and parallel executive limits", async () => {
  vi.stubGlobal("fetch", vi.fn(async () => new Response(JSON.stringify(settings))));
  const mutate = vi.fn(async (_path, body) => ({ ...settings, ...body, version: 2 }));
  render(<ChakraProvider value={defaultSystem}><ConcurrencySettings refreshKey={0} writeEnabled mutate={mutate as never} /></ChakraProvider>);
  fireEvent.change(await screen.findByLabelText("Task executor concurrency"), { target: { value: "14" } });
  fireEvent.change(screen.getByLabelText("PR reviewer concurrency"), { target: { value: "2" } });
  fireEvent.click(screen.getByText("Configure all bots"));
  expect(screen.getByLabelText("Executive concurrency")).toBeEnabled();
  fireEvent.change(screen.getByLabelText("Executive concurrency"), { target: { value: "4" } });
  fireEvent.click(screen.getByText("Save concurrency"));
  await waitFor(() => expect(mutate).toHaveBeenCalledWith("/concurrency", { version: 1, limits: { task_executor: 14, pr_reviewer: 2, executive: 4 } }, "PUT"));
  expect(await screen.findByText(/Concurrency saved/)).toBeInTheDocument();
});
it("keeps a draft on conflict and links rate limits to model settings", async () => {
  vi.stubGlobal("fetch", vi.fn(async () => new Response(JSON.stringify(settings))));
  const onModels = vi.fn();
  render(<ChakraProvider value={defaultSystem}><ConcurrencySettings refreshKey={0} writeEnabled mutate={vi.fn(async () => { throw new Error("Settings changed; refresh before saving"); })} /><ModelLimitNotice onModels={onModels} /></ChakraProvider>);
  fireEvent.change(await screen.findByLabelText("Task executor concurrency"), { target: { value: "14" } });
  fireEvent.click(screen.getByText("Save concurrency"));
  expect(await screen.findByRole("alert")).toHaveTextContent("Settings changed");
  expect(screen.getByLabelText("Task executor concurrency")).toHaveValue("14");
  fireEvent.click(screen.getByText("Change bot model"));
  expect(onModels).toHaveBeenCalledOnce();
});
