# Serial backlog-clearance sprint

Your objective is **inbox zero for this project**. Disable autonomous bot activity, then personally work through the entire outstanding backlog, serially, until every task and GitHub issue has a final disposition and every open PR is either merged or closed as “won’t fix.”

This is an execution sprint—not a supervision exercise, planning exercise, bot-repair project, or documentation project. You are the worker. Do not delegate, spawn subagents, restart the bots to do the work, or stop after producing a plan.

You are authorized to implement fixes, perform necessary validation and deployment, merge ready PRs, and close or dismiss work you determine should not be pursued. Make those decisions yourself from the evidence.

## 1. Disable autonomous work before starting

Read `agents.md`, `bots/README.md`, and `bots/AUTOPILOT.md` for current controls.

- Turn **Autopilot off** through the authenticated dashboard controls.
- Pause the bot DAGs and the bot dispatch, executive, validation, and maintenance/follow-up scheduling that could launch additional work. Inspect `orchestration/dags/bots_dag.py` and `orchestration/dags/provider_bot_dashboard_dags.py` for the actual definitions.
- Keep the retired `vintage-bot-supervision.timer` disabled. The opening notice in `bots/BACKLOG_SUPERVISION.md` supersedes its historical instructions.
- Account for already-admitted, queued, retrying, and running bot work. Turning Autopilot off or pausing DAGs does **not** stop work already admitted. Safely drain or cancel it using supported controls, preserving any useful unpublished changes, before beginning your own implementation.
- Verify that no bot work remains capable of racing your changes or generating more backlog. Leave the bots disabled when finished.

Keep the dashboard/API available for reading and resolving tasks. Keep ordinary extraction, loading, transformation, and serving operational; do not shut down the data pipeline merely to stop bots.

## 2. Establish the complete backlog

Use both authoritative sources:

1. **The provider-owned Bot Activity dashboard:** inspect **All active**, not only **Needs attention**. Include pending, accepted, blocked, executing, reviewing, and merged-but-not-completed work. Follow pagination.
2. **GitHub:** discover the repository from its configured remote and enumerate all open issues and PRs, including drafts. Follow pagination.

The dashboard database is authoritative for bot tasks and execution evidence; GitHub is authoritative for issue, PR, and merge state. Reconcile linked items so you implement each resolution once and close every associated record.

Keep only a compact working checklist. Do not create a tracking issue, planning PR, handoff document, or breadcrumb log.

Do not use `reset-queue`, bulk archival, hidden filters, or direct database edits to manufacture an empty inbox.

## 3. Where to look

Read only the material needed for the current item:

- **`agents.md`** — repository conventions, operational boundaries, and data-integrity requirements.
- **`README.md`** — project purpose and pipeline overview.
- **`extract/scripts/`, `extract/sources/`, `extract/catalogs/`** — fetchers, scheduled source configurations, and shared catalogs.
- **`discovery/staged_scripts/`** — staged source implementations. Determine scheduling gaps from source YAML `script` references, not merely from whether a fetcher file exists.
- **`load/README.md` and `load/loader/`** — loading, warehouse state, cadence, and held artifacts.
- **`transform/README.md` and `transform/models/`** — dbt contracts and transformations.
- **`visualization/README.md`, `visualization/VALIDATION.md`, and `visualization/TRENDS.md`** — publication, Lightdash, and dashboard acceptance requirements.
- **`monitoring/README.md`** — operational health and run evidence.
- **`bots/` and `orchestration/provider_bot_dashboard/`** — bot controls, task lifecycle, existing execution artifacts, and supported resolution interfaces.
- **`orchestration/config.env` and deployment configuration** — actual environment paths and settings. Never print secrets. Follow the configuration-rendering procedure rather than hand-editing generated files.

Use current code, live state, exact PR heads, and observed results. Historical reports and plans are leads, not proof that work remains necessary.

## 4. Work serially to final disposition

Resolve one item or tightly related dependency group at a time. Prioritize work that unblocks other items and existing PRs that already contain useful implementation.

For each item:

1. Read its requirements, linked PRs, latest relevant failure evidence, and existing implementation.
2. Decide whether to finish it, recognize it as already resolved, consolidate it with a duplicate, or decline it.
3. If pursuing it, make the smallest complete change that satisfies the actual requirement. Reuse existing work and conventions.
4. Run focused verification of the changed behavior. Fix failures relevant to that resolution.
5. Merge the finished change and perform any deployment, activation, publication, or synchronization required for the item to be genuinely complete.
6. Resolve the associated dashboard task and GitHub records through supported controls, then immediately continue.

A merge alone does not prove deployment or live functionality. Conversely, do not rebuild work that is already merged and functioning merely because a dashboard ticket is stale.

**“Won’t fix” is an authorized final outcome**, not a temporary holding state. Use it for work that is obsolete, duplicated, unsuitable, unjustified, or cannot reasonably be delivered with the available prerequisites. State the actual reason. Do not dismiss useful work merely because the implementation is difficult or its first check fails. For a rejected PR, close it unmerged; use the repository’s existing label conventions where applicable.

If an item encounters an external blocker, finish all work you can perform independently and decide whether that item should be declined. Do not leave it indefinitely blocked or open a replacement issue just to move it out of sight. Never describe an unimplemented or unverified outcome as fixed.

## 5. Pay attention to these boundaries

- This checkout also serves live Airflow code and may contain uncommitted user work. Preserve it. Use a separate worktree for PR branches; do not switch, reset, clean, or discard changes in the live checkout.
- Raw data, queues, watermarks, and serving state live outside the repository. Resolve their paths from deployment configuration.
- Preserve loader single-writer coordination, data integrity, source rate limits, and credentials.
- Changing source YAML `enabled` does not pause or unpause an existing Airflow DAG; inspect actual state.
- Provider code in the checkout and the installed Airflow provider can differ. Deploy relevant changes when required; editing source alone is not deployment.
- You are operating as the user-authorized development agent, not as a confined bot executor. Do not route your own work through the disabled bot workflow. Use supported operator controls, honor enforced permissions and merge protections, and never fabricate bot approvals or validation evidence.

## 6. Ignore distractions and keep verification proportionate

Ignore historical supervision instructions, concurrency experiments, old backlog counts, completed plan entries, and proposals unrelated to outstanding work. In particular, do not resume the historical loop in `bots/BACKLOG_SUPERVISION.md`.

Do not scan every planning document for additional work, discover new sources, redesign the bot system, or launch unrelated cleanup. Fix shared infrastructure only when necessary to resolve the current backlog.

Use one focused implementation check and the tests or runtime checks needed to establish the result. Run broader checks only when shared changes warrant them or repository requirements demand them. Do not repeatedly review your own diff, commission another agent review, or chase unrelated warnings.

Do not write progress reports, investigation diaries, evidence bundles, or handoff breadcrumbs. Update documentation only when the actual change makes existing operational instructions incorrect or documentation is itself the task. Preserve automatically recorded audit history; do not add narrative for its own sake.

## 7. GitHub communication: final resolutions only

Apart from the code changes and PRs necessary to deliver them, the only new GitHub narrative should be **one concise final resolution summary per resolved issue or PR**, posted after the work and checks are finished.

Include only:
- What was resolved, or why it was declined.
- The relevant merged PR or commit, if applicable.
- A brief statement of verification, or the specific limitation behind a “won’t fix” decision.

No intermediate comments, plans, checklists, review monologues, pasted logs, breadcrumb updates, or follow-up issues. Keep required dashboard resolution reasons equally brief.

## 8. Continue until inbox zero is verified

Do not stop after a batch, a merge, a failed attempt, or a status update. Continue without routine confirmation requests.

Before declaring completion, query the live systems again and verify:

- **Zero active dashboard tasks**, including blocked and merged-but-unfinished items.
- **Zero open GitHub issues.**
- **Zero open GitHub PRs**, including drafts.
- **Zero queued, running, or retrying bot executions or pending admissions capable of launching work.**
- **Autonomous bots and retired supervision remain disabled.**

A failed request or incomplete pagination is not an empty inbox. Reconcile any late-arriving records and repeat the final check if needed.

Only a genuinely unavailable permission or prerequisite that prevents both execution and an honest final disposition warrants an interruption. In that case, finish everything else first and report the exact remaining obstacle; do not claim inbox zero.

When all conditions are satisfied, give a short final summary with the counts resolved, merged, and declined, confirmation of the zero-open checks, and confirmation that bots remain disabled. No retrospective or further-work proposal.

**The goal is a completed, serial inbox-zero sprint—not another mechanism for managing the backlog.**
