// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { ChakraProvider, defaultSystem } from "@chakra-ui/react";
import { cleanup, fireEvent, render, screen, within } from "@testing-library/react";
import { useState } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActivityTimeline, CommandEditor, EvidenceList, TicketBrief, safeHref } from "./ticket-content";
import { CodeValue } from "./ui-kit";
const mount = (component: React.ReactNode) => render(<ChakraProvider value={defaultSystem}>{component}</ChakraProvider>);
afterEach(() => { cleanup(); vi.unstubAllGlobals(); });

describe("readable task content", () => {
  it("shows planning requests and opens linked tickets without losing current edits", () => {
    const id = "da704fb4-26f6-44b9-ad3a-3fb4cbb75c00";
    mount(<ActivityTimeline events={[
      { sequence: 1, event_type: "planning_requested", actor_id: "executive", payload: { bot: "analytics_engineer", request: "Plan the missing recall trends." } },
      { sequence: 2, event_type: "follow_up_ticket_linked", actor_id: "analytics_engineer", payload: { task_id: id, title: "Add recall trends" } },
    ]} />);
    expect(screen.getByText("Plan the missing recall trends.")).toBeVisible();
    const link = screen.getByRole("link", { name: /Add recall trends/ });
    expect(link).toHaveAttribute("href", `?task=${id}`);
    expect(link).toHaveAttribute("target", "_blank");
  });
  it("shows specialist findings as readable evidence rather than an empty event", () => {
    mount(<ActivityTimeline events={[{ sequence: 1, event_type: "specialist_follow_up", actor_id: "source_scheduling", payload: { status: "degraded_evidence", summary: "A live extraction record is still required.", report_sha256: "internal-digest" } }]} />);
    expect(screen.getByText(/Reported follow-up findings/)).toBeVisible();
    expect(screen.getByText("Additional evidence required")).toBeVisible();
    expect(screen.getByText("A live extraction record is still required.")).toBeVisible();
    expect(screen.queryByText("internal-digest")).not.toBeInTheDocument();
  });
  it("keeps long implementation details expandable and acceptance criteria readable", () => {
    const implementation = "Add the source extractor. ".repeat(35);
    const { container } = mount(<TicketBrief value={`A bounded daily feed.\n\nImplementation\n${implementation}\n\nDone when\nRecords have stable IDs; malformed responses fail clearly; tests pass.`} />);
    expect(screen.getByText("A bounded daily feed.")).toBeVisible();
    expect(screen.getByRole("heading", { name: "Done when" })).toBeVisible();
    expect(screen.getByText("Records have stable IDs;")).toBeVisible();
    const details = container.querySelector("details")!;
    expect(details).not.toHaveAttribute("open");
    fireEvent.click(within(details).getByText("Implementation"));
    expect(details).toHaveAttribute("open");
    expect(details).toHaveTextContent("Add the source extractor.");
  });
  it("renders hostile HTML literally and only makes safe external references clickable", () => {
    const { container } = mount(<><TicketBrief value={'<img src=x onerror="alert(1)"> **Scope** `example.py` [Docs](https://example.com/docs)'} /><EvidenceList items={[{ label: "Unsafe", reference: "javascript:alert(1)" }, { label: "Docs", reference: "https://example.com/docs" }]} /></>);
    expect(container.querySelector("img,script,iframe")).toBeNull();
    expect(screen.getAllByRole("link")).toHaveLength(2);
    expect(screen.getByText("javascript:alert(1)")).toBeInTheDocument();
    expect(safeHref("https://user:password@example.com")).toBeUndefined();
    expect(safeHref("//example.com")).toBeUndefined();
  });
  it("shows a human timeline without fingerprints or duplicated before-and-after documents", () => {
    mount(<ActivityTimeline events={[
      { sequence: 1, event_type: "state_changed", actor_id: "admin", from_state: "proposed", to_state: "accepted", fingerprint: "internal-fingerprint" },
      { sequence: 2, event_type: "task_edited", actor_id: "admin", payload: { before: { planned_resolution: "old-record-text" }, after: { planned_resolution: "new-record-text" } } },
      { sequence: 3, event_type: "comment_added", actor_id: "admin", payload: { items: ["Ready for review."] } },
    ]} />);
    expect(screen.getByText(/Moved from Proposed to Accepted/)).toBeVisible();
    expect(screen.getByText("Changed resolution plan.")).toBeVisible();
    expect(screen.getByText("Ready for review.")).toBeVisible();
    expect(screen.queryByText("internal-fingerprint")).not.toBeInTheDocument();
    expect(screen.queryByText("new-record-text")).not.toBeInTheDocument();
  });
  it("preserves exact command arguments including whitespace and punctuation when editing", () => {
    function Editor() {
      const [value, setValue] = useState(JSON.stringify([["python3", "-c", "print('a b; c')"]]));
      return <><CommandEditor value={value} onChange={setValue} disabled={false} /><output data-testid="serialized">{value}</output></>;
    }
    mount(<Editor />);
    fireEvent.change(screen.getByLabelText("Check 1 program"), { target: { value: "python" } });
    expect(JSON.parse(screen.getByTestId("serialized").textContent!)).toEqual([["python", "-c", "print('a b; c')"]]);
    fireEvent.click(screen.getByRole("button", { name: "+ Add argument" }));
    fireEvent.change(screen.getByLabelText("Check 1 argument 3"), { target: { value: "a file with spaces.txt" } });
    expect(JSON.parse(screen.getByTestId("serialized").textContent!)[0][3]).toBe("a file with spaces.txt");
  });
  it("confirms successful copies and handles clipboard failure without unhandled rejection", async () => {
    const writeText = vi.fn().mockResolvedValue(undefined);
    vi.stubGlobal("navigator", { clipboard: { writeText } });
    mount(<CodeValue value="0123456789abcdef" label="digest" copy />);
    fireEvent.click(screen.getByRole("button", { name: "Copy digest" }));
    expect(await screen.findByRole("status")).toHaveTextContent("Copied");
    expect(writeText).toHaveBeenCalledWith("0123456789abcdef");
    writeText.mockRejectedValueOnce(new Error("Denied"));
    fireEvent.click(screen.getByRole("button", { name: "Copy digest" }));
    expect(await screen.findByText(/Copy unavailable/)).toBeInTheDocument();
  });
});

it("preserves list items, literal code blocks, and unmatched inline markup", () => {
  const { container } = mount(<TicketBrief value={'Summary\n`unfinished marker\n\nDone when\n- The feed is valid.\n- Tests pass.\n\nVerification\n```python\n# A code comment, not a ticket heading\nprint("<script>literal</script>")\n```'} />);
  expect(screen.getByText("`unfinished marker")).toBeVisible();
  const list = screen.getByRole("list");
  expect(within(list).getAllByRole("listitem")).toHaveLength(2);
  expect(screen.getByText("# A code comment, not a ticket heading", { exact: false }).closest("pre")).toBeVisible();
  expect(container.querySelector("script")).toBeNull();
  expect(container.querySelector("pre")).toHaveTextContent('print("<script>literal</script>")');
});

it("labels executive activity by role rather than the underlying model", () => {
  mount(<ActivityTimeline events={[{ sequence: 1, event_type: "executive_decision", actor_id: "executive",
    payload: { action: "accept", result: "applied", model: "openai-codex/gpt-6-astra", rationale: "Evidence supports proceeding." } }]} />);
  expect(screen.getByText("Executive")).toBeVisible();
  expect(screen.getByText("Evidence supports proceeding.")).toBeVisible();
  expect(document.body.textContent).not.toMatch(/astra|openai|gpt-/i);
});
