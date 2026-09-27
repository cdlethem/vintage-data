// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { ChakraProvider, defaultSystem } from "@chakra-ui/react";
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import { Autopilot } from "./autopilot";

const initial = { enabled: true, version: 3, model: "openai-codex/gpt-6-astra", model_problem: null,
  deciding: false, last_checked_at: new Date().toISOString(), last_error: null, last_decision: null };
const response = (value: unknown) => new Response(JSON.stringify(value), { headers: { "Content-Type": "application/json" } });
afterEach(() => { cleanup(); vi.unstubAllGlobals(); });

it("turns off using a versioned setting and keeps the current state until saved", async () => {
  vi.stubGlobal("fetch", vi.fn(async () => response(initial)));
  let resolve: (value: unknown) => void = () => {};
  const mutate = vi.fn(() => new Promise(done => { resolve = done; }));
  render(<ChakraProvider value={defaultSystem}><Autopilot writeEnabled mutate={mutate as never} refreshKey={0} /></ChakraProvider>);
  const toggle = await screen.findByRole("switch", { name: "Autopilot" });
  expect(toggle).toHaveAttribute("aria-checked", "true");
  fireEvent.click(toggle);
  expect(mutate).toHaveBeenCalledWith("/autopilot", { enabled: false, version: 3 }, "PUT");
  expect(toggle).toBeDisabled();
  expect(toggle).toHaveAttribute("aria-checked", "true");
  resolve({ ...initial, enabled: false, version: 4 });
  await waitFor(() => expect(toggle).toHaveAttribute("aria-checked", "false"));
  expect(toggle).toHaveTextContent("Turn on");
});

it("allows switching off even if the model is unavailable and shows the last rationale safely", async () => {
  vi.stubGlobal("fetch", vi.fn(async () => response({ ...initial, model_problem: "Executive connection unavailable",
    last_decision: { task_id: "task-1", title: "Review source", at: new Date().toISOString(), action: "wait", result: "applied", rationale: "<script>unsafe()</script> Need fresh evidence." } })));
  render(<ChakraProvider value={defaultSystem}><Autopilot writeEnabled mutate={vi.fn() as never} refreshKey={0} /></ChakraProvider>);
  expect(await screen.findByRole("switch")).not.toBeDisabled();
  fireEvent.click(screen.getByRole("button", { name: "How it works" }));
  expect(screen.getByText(/Need fresh evidence/)).toBeInTheDocument();
  expect(document.querySelector("script")).toBeNull();
  expect(screen.getByText(/Turning off stops new decisions/)).toBeInTheDocument();
});

it("prevents enabling without a configured model and respects read-only permissions", async () => {
  vi.stubGlobal("fetch", vi.fn(async () => response({ ...initial, enabled: false, model_problem: "Configure the executive first" })));
  const mutate = vi.fn();
  const view = render(<ChakraProvider value={defaultSystem}><Autopilot writeEnabled mutate={mutate as never} refreshKey={0} /></ChakraProvider>);
  expect(await screen.findByRole("switch")).toBeDisabled();
  view.rerender(<ChakraProvider value={defaultSystem}><Autopilot writeEnabled={false} mutate={mutate as never} refreshKey={0} /></ChakraProvider>);
  expect(screen.getByRole("switch")).toBeDisabled();
  expect(mutate).not.toHaveBeenCalled();
});

it("uses role labels without exposing model or provider branding", async () => {
  vi.stubGlobal("fetch", vi.fn(async () => response({ ...initial, deciding: true, active_decisions: 4 })));
  render(<ChakraProvider value={defaultSystem}><Autopilot writeEnabled mutate={vi.fn() as never} refreshKey={0} /></ChakraProvider>);
  await screen.findByRole("switch");
  fireEvent.click(screen.getByRole("button", { name: "How it works" }));
  expect(screen.getByText("Considering 4 decisions…")).toBeVisible();
  expect(document.body.textContent).not.toMatch(/astra|openai|gpt-/i);
});
