// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { ChakraProvider, defaultSystem } from "@chakra-ui/react";
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import App from "./app";
import Dashboard from "./dashboard";
import { activityUrl, humanTitle, request } from "./api";
const response = (body: unknown, status = 200) => new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });
const task = (id: string) => ({ id, title: `Repair ${id}`, category: "reliability", priority: 1, state: "proposed", planned_resolution: "Restore current data", version: 1, updated_at: new Date().toISOString() });
const mount = (component: React.ReactNode) => render(<ChakraProvider value={defaultSystem}>{component}</ChakraProvider>);
afterEach(() => { cleanup(); vi.unstubAllGlobals(); history.replaceState(null, "", "/"); document.querySelector("base")?.remove(); });
describe("home summary", () => {
  it("shows the full count and one link without task cards", async () => {
    vi.stubGlobal("fetch", vi.fn(async () => response({ attention_count: 27, active_count: 30, counts: { proposed: 27 } })));
    mount(<Dashboard />);
    expect(await screen.findByText("27 actions need your attention")).toBeInTheDocument();
    expect(screen.getAllByRole("link")).toHaveLength(1);
    expect(screen.getByRole("link")).toHaveAttribute("href", "/plugin/bot-activity");
    expect(screen.queryByText("Specialist evidence")).not.toBeInTheDocument();
  });
  it("does not report a clear queue when the API fails", async () => {
    vi.stubGlobal("fetch", vi.fn(async () => response({detail:"Unavailable"},503)));
    mount(<Dashboard />);
    expect(await screen.findByText("Unable to check the queue")).toBeInTheDocument();
    expect(screen.queryByText("No actions need your attention")).not.toBeInTheDocument();
    expect(screen.getByRole("link")).toBeInTheDocument();
  });
});
describe("action browsing", () => {
  it("defaults to attention, paginates, and searches history", async () => {
    const fetchMock = vi.fn(async (input: string) => {
      if (input.includes("/summary")) return response({attention_count:26,active_count:26,counts:{proposed:26}});
      if (input.includes("/capabilities")) return response({write_enabled:false,executor_enabled:false});
      const url = new URL(input, "http://localhost");
      if (url.searchParams.get("state") === "completed,dismissed") return response({ items:[], total:0 });
      if (url.searchParams.has("cursor")) return response({items:[task("last")],total:26,next_cursor:null});
      return response({items:Array.from({length:25},(_,i)=>task(String(i))),total:26,next_cursor:"page2"});
    });
    vi.stubGlobal("fetch", fetchMock); mount(<App />);
    expect(await screen.findByText("Showing 25 of 26 actions")).toBeInTheDocument();
    expect(fetchMock.mock.calls.some(([url])=>new URL(url,"http://localhost").searchParams.get("state") === "proposed,accepted,blocked,in_review,ready")).toBe(true);
    fireEvent.click(screen.getByRole("button",{name:"Load more"}));
    expect(await screen.findByText("Showing 26 of 26 actions")).toBeInTheDocument();
    expect(screen.queryByRole("button",{name:"Load more"})).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole("button",{name:"History"}));
    expect(await screen.findByText("No past actions")).toBeInTheDocument();
    fireEvent.change(screen.getByRole("textbox",{name:"Search tasks"}),{target:{value:"holiday"}});
    await waitFor(()=>expect(fetchMock.mock.calls.some(([url])=>url.includes("search=holiday"))).toBe(true));
  });
  it("reports bot API failures instead of an empty registry", async () => {
    vi.stubGlobal("fetch",vi.fn(async (input:string)=> input.endsWith("/bots") ? response({detail:"Service unavailable"},503) : response({items:[],total:0})));
    mount(<App />); fireEvent.click(screen.getByRole("button",{name:"Bots"}));
    expect(await screen.findByRole("alert")).toHaveTextContent("Unable to load bot activity");
    expect(screen.queryByText("No specialist bots are registered")).not.toBeInTheDocument();
  });
});
describe("Airflow integration", () => {
  it("uses the deployment base path without script tags", async () => {
    const base=document.createElement("base");base.href="/airflow/";document.head.append(base);
    const fetchMock=vi.fn(async(_input:string)=>response({}));vi.stubGlobal("fetch",fetchMock);
    await request("/summary");
    expect(fetchMock.mock.calls[0][0]).toBe("/airflow/bot-dashboard/api/summary");
    expect(activityUrl("a b")).toBe("/airflow/plugin/bot-activity?task=a%20b");
  });
  it("makes source and legacy titles readable", () => {
    expect(humanTitle("Repair fetch_nager_date.py")).toBe("Repair Nager date");
    expect(humanTitle("Review quarantined legacy package source_vetting-20260907T024917Z-3e2eb020")).toBe("Review historical source vetting output (2026-09-07)");
  });
});
