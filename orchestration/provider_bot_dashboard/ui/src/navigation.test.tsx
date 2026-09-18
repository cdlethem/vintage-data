// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { ChakraProvider, defaultSystem } from "@chakra-ui/react";
import { act, cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import App from "./app";
const task = { id: "navigation-task", title: "A clear task", category: "reliability", state: "accepted", priority: 1, version: 1, updated_at: new Date().toISOString(), planned_resolution: "Restore the feed.", revisions: [], executions: [], events: [] as Record<string, unknown>[] };
const response = (value: unknown) => new Response(JSON.stringify(value), { headers: { "Content-Type": "application/json" } });
const fixture = (input: string) => {
  if (input.endsWith("/capabilities")) return response({ write_enabled: true, executor_enabled: true, csrf_token: "test-token" });
  if (input.endsWith("/summary")) return response({ attention_count: 1, active_count: 1, counts: { accepted: 1 } });
  if (input.endsWith("/model-settings")) return response({ providers: [], assignments: [], roles: [] });
  if (input.endsWith("/bots")) return response({ items: [] });
  return response({ items: [task], total: 1 });
};
const mount = () => render(<ChakraProvider value={defaultSystem}><App /></ChakraProvider>);
afterEach(() => { cleanup(); vi.unstubAllGlobals(); history.replaceState(null, "", "/"); });
it("opens task creation from settings and returns to the queue on cancel", async () => {
  vi.stubGlobal("fetch", vi.fn(async (input: string) => fixture(input)));
  mount(); await screen.findByRole("button", { name: "Create task" });
  fireEvent.click(screen.getByRole("button", { name: "Models & connections" }));
  await screen.findByRole("region", { name: "Model settings" });
  fireEvent.click(screen.getByRole("button", { name: "Create task" }));
  expect(await screen.findByRole("dialog", { name: "Create manual task" })).toBeVisible();
  expect(screen.getByRole("button", { name: "Create" })).toBeDisabled();
  fireEvent.click(screen.getByRole("button", { name: "Cancel" }));
  expect(await screen.findByRole("list", { name: "Actions" })).toBeVisible();
});
it("does not reopen a delayed task after navigating to another view", async () => {
  let resolveDetail!: (value: Response) => void;
  vi.stubGlobal("fetch", vi.fn((input: string) => input.endsWith("/tasks/navigation-task") ? new Promise<Response>(resolve => { resolveDetail = resolve; }) : Promise.resolve(fixture(input))));
  mount(); fireEvent.click(await screen.findByRole("button", { name: /A clear task/ }));
  expect(screen.getByText("Opening task…")).toBeVisible();
  fireEvent.click(screen.getByRole("button", { name: "Bots" }));
  await act(async () => { resolveDetail(response(task)); });
  expect(await screen.findByRole("region", { name: "Bot evidence" })).toBeVisible();
  expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
  expect(location.search).toBe("");
});
it("posts a comment with the current version and adds it to the readable activity timeline", async () => {
  let current = { ...task };
  const fetchMock = vi.fn(async (input: string, init?: RequestInit) => {
    if (input.endsWith("/tasks/navigation-task/comments")) {
      const body = JSON.parse(String(init?.body));
      current = { ...current, version: 2, events: [{ sequence: 1, event_type: "comment_added", actor_id: "admin", payload: { items: body.comments } }] };
      return response(current);
    }
    if (input.endsWith("/tasks/navigation-task")) return response(current);
    return fixture(input);
  });
  vi.stubGlobal("fetch", fetchMock); history.replaceState(null, "", "/?task=navigation-task");
  mount(); await screen.findByRole("dialog");
  fireEvent.click(screen.getByRole("button", { name: "Activity · 0" }));
  fireEvent.change(screen.getByLabelText("Add a comment"), { target: { value: "Verified the source documentation.\nReady for review." } });
  fireEvent.click(screen.getByRole("button", { name: "Post comment" }));
  expect(await screen.findByText("Comment added.")).toBeVisible();
  expect(within(screen.getByRole("region", { name: "Task activity" })).getByText("Verified the source documentation.")).toBeVisible();
  await waitFor(() => expect(screen.getByLabelText("Add a comment")).toHaveValue(""));
  const [, options] = fetchMock.mock.calls.find(([url]) => url.endsWith("/comments"))!;
  expect(JSON.parse(String(options?.body))).toEqual({ version: 1, comments: ["Verified the source documentation.", "Ready for review."] });
  expect(options?.headers).toMatchObject({ "X-Bot-Dashboard-CSRF": "test-token" });
});

it("protects an edited task when leaving and preserves the draft when editing continues", async () => {
  vi.stubGlobal("fetch", vi.fn(async (input: string) => input.endsWith("/tasks/navigation-task") ? response(task) : fixture(input)));
  history.replaceState(null, "", "/?task=navigation-task");
  mount(); await screen.findByRole("dialog");
  fireEvent.click(screen.getByRole("button", { name: "Edit resolution plan" }));
  fireEvent.change(screen.getByLabelText("Planned resolution"), { target: { value: "Keep this unfinished plan." } });
  fireEvent.click(screen.getByRole("button", { name: "Bots" }));
  expect(screen.getByRole("alertdialog")).toHaveTextContent("You have unsaved changes");
  fireEvent.click(screen.getByRole("button", { name: "Keep editing" }));
  expect(screen.getByLabelText("Planned resolution")).toHaveValue("Keep this unfinished plan.");
  const unloading = new Event("beforeunload", { cancelable: true });
  window.dispatchEvent(unloading);
  expect(unloading.defaultPrevented).toBe(true);
  fireEvent.click(screen.getByRole("button", { name: "Close" }));
  fireEvent.click(screen.getByRole("button", { name: "Discard changes" }));
  expect(await screen.findByRole("list", { name: "Actions" })).toBeVisible();
  const afterDiscard = new Event("beforeunload", { cancelable: true });
  window.dispatchEvent(afterDiscard);
  expect(afterDiscard.defaultPrevented).toBe(false);
});

it("protects new connection credentials from accidental tab navigation", async () => {
  vi.stubGlobal("fetch", vi.fn(async (input: string) => fixture(input)));
  mount(); fireEvent.click(screen.getByRole("button", { name: "Models & connections" }));
  await screen.findByLabelText("Connection name");
  fireEvent.change(screen.getByLabelText("Connection name"), { target: { value: "Unfinished connection" } });
  fireEvent.change(screen.getByLabelText("API key"), { target: { value: "in-memory-draft" } });
  fireEvent.click(screen.getByRole("button", { name: "Priority list" }));
  expect(screen.getByRole("alertdialog")).toBeVisible();
  fireEvent.click(screen.getByRole("button", { name: "Keep editing" }));
  expect(screen.getByLabelText("API key")).toHaveValue("in-memory-draft");
  expect(localStorage.length).toBe(0);
  expect(sessionStorage.length).toBe(0);
});

it("returns from a task to the queue using the queue navigation item", async () => {
  vi.stubGlobal("fetch", vi.fn(async (input: string) => input.endsWith("/tasks/navigation-task") ? response(task) : fixture(input)));
  history.replaceState(null, "", "/?task=navigation-task");
  mount(); await screen.findByRole("dialog");
  fireEvent.click(screen.getByRole("button", { name: "Priority list" }));
  expect(await screen.findByRole("list", { name: "Actions" })).toBeVisible();
  expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
});
