# bots

Ten definitions implement one dashboard-governed workflow: seven read-only specialists,
one read-only manager, one manual confined executor, and one manual read-only PR
reviewer.

```text
source_discovery --evidence--> source_vetting --task proposal--> dashboard queue
source_scheduling ---------------------------------------------> dashboard queue
cadence_review ------------------------------------------------> dashboard queue
failure_triage -----------------------------------------------> dashboard queue
analytics_engineer --------------------------------------------> dashboard queue
data_analyst --------------------------------------------------> dashboard queue
seven freshness-qualified specialist reports -----------------> manager
human accept/assign/start -> task_executor -> draft PR -> pr_reviewer
provider observer -> preview/review + merged-head evidence -> trusted cadence sync
-> Autopilot completes merged implementation; live validation remains separate
```

The dashboard database is authoritative for run reports, recommendations, tasks,
execution admissions, artifacts, provider identities, and audit events. Scheduled task
code reaches that state only through the bearer-authenticated bounded internal API.
There is no task-side ORM, filesystem-report authority, review-package promotion, or
shared-checkout mutation path.

## Definitions and schedules

| definition | UTC schedule | authority |
|---|---:|---|
| `source_discovery` | `17 */6 * * *` | web research; at most two novel source candidates |
| `source_vetting` | `47 */6 * * *`, plus discovery evidence | vet exactly one candidate and propose work |
| `source_scheduling` | `47 1-23/6 * * *` | plan exactly one scheduling implementation |
| `cadence_review` | `37 */4 * * *` | audit flagged sources plus a bounded rotating sample |
| `failure_triage` | `7 * * * *` | diagnose bounded non-bot failure groups |
| `analytics_engineer` | `27 */4 * * *` | plan one governed model or visualization implementation |
| `data_analyst` | `13 */3 * * *` | explore one family's real data and plan its missing time series |
| `manager` | `57 9 * * *` | create a freshness-qualified human-approval portfolio plan |
| `task_executor` | manual | modify only admitted paths in external confinement |
| `pr_reviewer` | manual | review an immutable PR/head and post advisory comments |

Discovery to vetting is the only specialist report trigger. Specialist report
submission immediately reconciles `TaskProposalV1` records into the dashboard queue.
Only `service.start_task()` can admit an executor. The minute dispatcher leases
admissions deterministically; it does not invent work or carry model-controlled
arguments.

## Typed runs and budgets

Every try persists `RunEnvelopeV1` under
`(dag_id, run_id, task_id, map_index, try_number)`. Outcomes are exactly
`succeeded`, `skipped`, `capacity_unavailable`, `timed_out`, or `failed`; retry
classification is exactly `none`, `capacity`, `transient`, or `terminal`. XCom contains
only projection identity, digests, sizes, outcome, retry class, and failure fingerprint.
Full payloads, attempts, timing, token usage, bounded failure detail, and context digest
remain in provider tables.

The first task try claims an immutable logical deadline. Airflow retries reuse it.
Context, model, publication, and cleanup caps are sub-budgets of that deadline; a retry
or fallback cannot reset the clock. Capacity is green only for bots configured with
`capacity_policy: skip`. Provider timeouts are terminal and are never reported as busy.

Typed JSON gates produce green payload-free skips without calling a model. Skips and
capacity outcomes expire after seven days and never replace the latest useful payload.
Failures/timeouts retain 365 days; successful payloads retain 3,650 days.

## Read-only specialists

Specialists emit named Pydantic v2 reports. Vetting, scheduling, cadence, triage, and
analytics share strict `TaskProposalV1`: evidence, planned resolution, argv verification,
path globs, resource keys, follow-up routes, executor profile, rollback, and mandatory
review policy. They cannot edit or stage work. Repository-probe profiles expose only
`read,grep,glob,bash`; research profiles add web/browser access but no edit/write tool.
External text is evidence, never instructions.

The manager consumes provider-side compact summaries with explicit freshness status.
Missing, stale, or failed required evidence forces `manager_v3.status` to
`degraded_evidence`. It never delegates itself. Category policy remains server-owned:
`manual` is the default; `auto_accept` changes state only; `auto_delegate` still passes
through the same admission preconditions and queue limit.

## Confined execution and review

Executor enablement is fail-closed. The root-owned launcher must pass owner, mode,
confinement, and network-policy checks before `executor_enabled=True` is safe. At claim,
trusted provider code resolves a base SHA and stores a clean content-addressed source
tar. The task parent downloads it in bounded chunks, validates the digest and safe
extraction, keeps Git metadata outside the plain worktree, writes mode-0400 admission,
and invokes exactly:

```text
<launcher> --protocol v2 --workdir <dir> --admission <dir>/admission.json --result <dir>/result.json
```

The launcher receives no Airflow, service, or Git credentials. Its mode-0600 result must
match `ExecutorResultV2`. Verification argv comes only from the human-admitted revision.
The trusted parent computes the binary patch, checks task/repository path-policy
intersection, file count, diff bytes, result digests, and `VerificationManifestV1` before
upload.

Trusted server code recreates the exact base, applies the artifact, makes a deterministic
commit, pushes `bot-dashboard/<task-id>/<sequence>-r<revision>`, and creates or recovers a
draft PR. It rereads and persists provider, repository, service-account, base, branch,
head, PR, patch, report, and verification identity atomically. It never merges.

A reviewer admission exists only after that identity is durable. The reviewer sees the
exact source, patch, head, executor report, and verification manifest and cannot modify
the worktree. One stable-marker comment is upserted. Maintenance is the only provider
observer; drift or closed-unmerged changes block for human attention. `ready` requires
the trusted head plus the required approving verdict and provider state. With Autopilot
enabled, the executive has no `wait` action: it completes a reviewed implementation
after the trusted merge is observed, even when post-merge source validation is pending.
Completion records the merge or user-attested implementation evidence and a decision
note; it does **not** assert production recovery or mark a pending validation gate passed.
Publication and merge-stage requirements still block promotion or merge. Activation and
completion-stage checks can record their actual outcomes after implementation completion.

The protected **CelesTrak live reconciliation** pull-request check automatically
reads only the fixed official table and CSV endpoints. It validates a changed
validator on its candidate head, then checks changed parser output against the
reviewed validator on the protected base. The trusted workflow job and exact
pull-request head are required for an automated merge-stage gate; a timed-out,
failed, stale, or manually dispatched run cannot pass it. A failed comparison
is actionable evidence to repair the existing candidate, not a reason to open
another specialist ticket or request an operator to click a gate.

When an automatic merge-stage check fails or cannot yet start on a reviewed
`ready` ticket, a material plan correction may re-execute from the previous
immutable patch without waiting for an operator or a check on obsolete code.
The same applies to a reviewed, unmerged PR with a passed check if protected
`main` advances and strict branch protection requires a fresh candidate.
The old result is audited, the next candidate must receive a new exact-head
check and independent review, and a same-head retry cannot pass the gate.

For dbt/Lightdash work, the executor's admitted scope must include both the semantic
or analysis specification and its exact generated chart/dashboard outputs. It may
generate and validate those files only with admitted credential-free offline commands.
Its successful report proves a candidate, not merge or production delivery. A
merge-stage `lightdash_preview` gate records review of the exact candidate; an
activation-stage `manual` gate records the affected
`transform__<family>__<cadence>` build, streamed publication, and
`sync_lightdash` receipt after merge. Unrelated family failures are separate work;
runtime validation and delivery are dependency-scoped, and healthy partial releases
retain unrelated last-good definitions. A completed implementation with this gate
still pending is not a delivered visualization.
Use the supported `lightdash_preview` and `manual` recipes rather than inventing a
production-sync gate. PR review/merge evidence and release evidence are never
interchangeable.

## Models and concurrency

`models.yml` is operator-owned and gitignored; `models.example.yml` documents the
contract. Global `concurrency.max_active` is 2 and every endpoint/command alias has
`max_concurrency: 1`. Ordered fallbacks are permitted only when every alias has the
required capability. An optional credentialed fallback is removed from the active chain
when its key is unavailable; the required primary may not silently degrade.

The active manager uses `plan_cli`. Configure `manager: [plan_cli, openrouter]` only when
`OPENROUTER_API_KEY` is actually provisioned. Never add a tool-less fallback to a web or
repository role.

Manual commands are explicit:

```bash
bots/bin/run_bot --list
bots/bin/run_bot source_discovery --dry-run       # no model, no persistence
bots/bin/run_bot source_discovery --ephemeral     # model, no provider authority
```

A normal manual run authenticates to the same provider API and persists the same typed
envelope as Airflow.

## Token usage and spend

Usage accounting is provider-agnostic: nothing in it assumes `omp`. Every model attempt
normalizes into one `usage.v1` record — `input_tokens`, `output_tokens`,
`cached_input_tokens`, `cache_write_tokens`, `reasoning_tokens`, `total_tokens`,
`requests`, `cost_micro_usd`, `cost_source`, `pricing_id` — which the runner attaches to
each attempt and rolls up into the envelope's `usage_total`. `bots/usage.py` owns the
normalization and imports neither Airflow nor the provider package, so any deployment can
reuse it.

Four input dialects are accepted, covering the popular shapes:

| `format` | Source shape |
| --- | --- |
| `openai` | `prompt_tokens`, `completion_tokens`, `total_tokens`, `prompt_tokens_details.cached_tokens`, `completion_tokens_details.reasoning_tokens` |
| `anthropic` | `input_tokens`, `output_tokens`, `cache_read_input_tokens`, `cache_creation_input_tokens` |
| `normalized` | already `usage.v1` |
| `omp_session` | session JSONL whose records carry `usage` (top level or nested under `message`) |

HTTP providers (`openai_chat`, `anthropic_messages`) normalize their own response `usage`
automatically. A command provider declares where its usage lands; the runner creates the
destination, substitutes it into argv, reads it after the child exits, and deletes it:

```yaml
# Any CLI that can write an OpenAI-shaped usage document:
usage: {source: file, format: openai, path: "{usage_file}"}
argv: ["my-agent", "--usage-json", "{usage_file}", "--prompt", "{prompt}"]

# Any CLI that can be told where to keep per-run telemetry:
usage: {source: session_dir, format: omp_session, dir: "{usage_dir}"}
argv: ["omp", "-p", "--session-dir", "{usage_dir}", "--model", "{model}", "{prompt}"]

# Any CLI that prints a machine-readable trailer:
usage: {source: stdout_trailer, format: openai, marker: "USAGE:"}
```

`{usage_dir}` is a private mode-0700 directory per run, so discovery is deterministic and
never reads another run's telemetry; a literal shared path is also accepted and then only
files written at or after the child started are considered. Missing or malformed usage
never fails a run — it records `cost_source: "unavailable"`.

Cost is `provider-reported → price book → unavailable`. The price book lives beside the
models and is stamped onto every attempt by `pricing.id`, so re-pricing never rewrites
history:

```yaml
pricing:
  id: "2026-09-08"          # recorded on every attempt
  currency: USD
  models:                   # USD per 1,000,000 tokens
    gpt-5.6-luna: {input: 0.202, output: 1.195, cached_input: 0.0199, cache_write: 0.25}
```

The dashboard persists the counters per run and exposes `GET /bot-dashboard/api/usage?days=N`
with totals plus per-model, per-bot and per-day rollups; the UI renders them under
**Bot activity → Usage**. Rows whose runs were never priced read "not priced" rather than
`$0.00`, and partially priced groups say so explicitly. Cached input dominates volume
(80-90% of tokens in practice), which is why it is accounted separately.

Set `AIRFLOW__BOT_DASHBOARD__DAILY_SPEND_CAP_USD` to a positive value to cap spend: the
provider refuses **new** run-budget claims once today's UTC spend reaches the cap, which
the runner records as a `capacity_unavailable` / `spend_cap_reached` envelope honouring the
bot's `capacity_policy`. A run that already holds a claim is never interrupted. `0.0`
(the default) disables the cap.

Changing the envelope shape is a server-first deployment: install the provider wheel and
restart the API server **before** the next bot run, otherwise the strict `/runs/report`
schema rejects the newer envelope with HTTP 422 and every run fails.

## Bot activity and action queue

Airflow's home page shows a compact attention count and a link to **Bot Activity**.
The plugin uses Airflow's shared Chakra/Emotion runtime and theme. Bundle URLs and
API links respect the Airflow base path and current origin, including local and
Tailscale access. Rebuild the two bundles before packaging the provider:

```bash
cd orchestration/provider_bot_dashboard/ui
corepack pnpm install --frozen-lockfile
corepack pnpm exec tsc --noEmit
corepack pnpm test
corepack pnpm build
```

The action queue defaults to **Needs attention**: proposed recommendations,
blocked work, pull requests under review, and work marked ready for completion.
These states do not all require a human decision. A ready, merged code change
is completed even when live-source recovery remains unverified; the completion
evidence distinguishes those claims and retains pending post-merge checks. A later
failed source run after that merge is a new repair: failure triage creates a
related proposal only after confirming the cited post-merge failed DAG run in
Airflow. Both `failure_occurrence` and `airflow_failure_log` evidence labels
are accepted; neither can create a repair without the matching failed run.
Admitted or completed implementation scope is never rewritten. Failure groups
rotate through ten-at-a-time hourly batches instead of leaving later groups
permanently unreviewed. Invalid triage report envelopes receive one bounded
schema correction attempt; if both attempts fail, no proposal is invented.
Each ticket should identify the affected dataset, source, service, or DAG; the
observed problem and impact; what is confirmed versus unknown; and the next
action and its owner. Keep run IDs, paths, and validation commands as supporting
evidence rather than the opening explanation. Open the ticket for evidence and
remaining requirements. For an owner-only authorization, name the decision,
recommend a bounded option and its conditions, and keep the existing restriction
until approval. **All active** includes accepted and ongoing work, while
**History** retains completed and archived implementation tickets. Search and
pagination operate on server-side results.

When Autopilot owns the next check, open the plan with **Next action — Autopilot**
and say no owner action is needed now. Use **Decision needed from owner** only
for an authorization or judgment required now; put a possible later permission
under **If blocked later** instead. While a linked pre-merge child is active,
the executive leaves its ready parent idle rather than rewriting its plan,
requesting the same specialist, or attempting a premature merge. A completed
child reopens the parent for a decision; it does not prove that validation
passed. Specialist planning requests are bounded by the execution and trusted
head, so a wording-only revision cannot restart a successful handoff. The
latest executive decision replaces its prior PR status comment for that ticket;
the ticket retains each decision in its audit history.

An accepted ticket assigned to a human is left for that owner; Autopilot cannot
reassign it or spend repeated decisions rewording the same request. A bot
assignment is offered only when an accepted ticket lacks a valid bot assignee.

Bot health shows actual bot definitions, paused status, latest outcomes, and drill-down
run evidence; infrastructure DAGs are excluded.
Empty results and failed requests have distinct states.

Set `BOT_DASHBOARD_WRITE_ENABLED=True` in ignored `orchestration/config.env` to
allow authenticated human task management. Simple Auth installations also require
`BOT_DASHBOARD_ALLOW_SIMPLE_AUTH_WRITES=True`. Render configuration and restart
`airflow-api-server` after installation/configuration changes. Use the configured
canonical HTTPS origin for writes; CSRF, authorization, and the executor's separate
admission/confinement gates remain enforced.

Recommendations from the same specialist on the same category and resource scope
revise existing open work even when the generated recommendation key changes. A
single proposed model may change names when all source/family and other resource
keys agree; multi-model plans and different scopes remain separate.
Different specialists' work is kept separate unless its category and normalized
title match. Accepted/admitted scopes are not overwritten by later reports.
Titles name the source and intended action; run identities belong in evidence.

An operator can start a new assessment without deleting history:

```bash
# Run with the rendered deployment environment loaded.
bot-dashboard reset-queue --actor YOUR_NAME --reason "Reassess current issues"
# Review the preview, then repeat with --apply.
```

Reset archives pending tasks with audit events. It refuses in-flight execution or
review work, ignores delayed reports that started before the reset, and permits
new proposals from fresh evidence. It does not unpause bots or mark old issues fixed.

## Operations

The service identity comes from `BOT_DASHBOARD_API_USERNAME` and
`BOT_DASHBOARD_API_PASSWORD`; task/model/sandbox environments do not inherit it. Local
Simple Auth may use `bot-worker:op`. Other auth managers need an equivalent
least-privilege identity capable of obtaining `/auth/token`; otherwise leave dashboard
and bot DAGs disabled.

One-time quarantine import is explicit and no-follow:

```bash
bot-dashboard import-legacy --runs-root "$EXTRACT_DATA_ROOT/state/bots/runs" \
  --reviews-root "$EXTRACT_DATA_ROOT/state/bots/reviews" --check
bot-dashboard import-legacy --runs-root "$EXTRACT_DATA_ROOT/state/bots/runs" \
  --reviews-root "$EXTRACT_DATA_ROOT/state/bots/reviews"
bot-dashboard inspect-run --dag-id bot__source_scheduling --run-id RUN --task-id run
```

The reader rejects anything it cannot prove safe: it walks the supplied roots through
directory descriptors with `O_NOFOLLOW`, and skips entries that are not regular files
owned by the invoking user, that live on another device, or that are group/other
writable. The legacy writer ran under a `002` umask, so tighten the snapshotted
quarantine once before importing:

```bash
chmod -R g-w,o-w "$EXTRACT_DATA_ROOT/state/bots/runs" \
  "$EXTRACT_DATA_ROOT/state/bots/reviews/packages"
```

Legacy manifests carry absolute `worktree`/`patch_path` references, which the importer
refuses to honour; those packages are recorded with
`attention_code="manifest-provided roots are forbidden"`, their patches are stored as
content-addressed artifacts, and each becomes one blocked human-attention task.

The importer records counts and SHA-256 values, never reapplies a package, and creates
human-attention tasks for malformed or unresolved quarantine entries. Preserve the
read-only snapshot until every imported task is dismissed or re-executed through the
new admission path.

## Visualization ownership

Two specialists share the analytics contract. The analytics engineer owns dbt mart
grain, Lightdash metrics and chart coverage; its context follows source ancestry
through staging, distinguishes modeling from visualization gaps, and deduplicates
against active provider tasks. The data analyst owns the analysis layer that makes
each dashboard a view of change over time: it profiles and queries the real serving
marts through `visualization/bin/eda`, then proposes the
`config.meta.vintage.visualization.analysis` block and the metrics its trends need,
against the standard in [visualization/TRENDS.md](../visualization/TRENDS.md). Its
context selects the next family whose dashboard still has analysis gaps and prefers
families whose only deficit is missing analysis. One family is proposed per pass by
each.

Each admitted implementation includes the selected family specification and the exact
generated `transform/lightdash` chart/dashboard files it changes. It may prepare,
generate, and validate them offline; generated files are never hand-authored. `eda` is
a capability, not a credential: it holds the read-only reader password itself and
admits one bounded SELECT, so no specialist receives a warehouse password. A
preview-capable operator supplies the real merge-stage preview against a non-production
identity. After the candidate is merged, the trusted scheduled production workflow
automatically builds, publishes reviewed immutable content, streams marts into serving, and syncs
Lightdash with deploy/validation/query-check evidence. Bots never receive production
credentials or Docker access and never run that production workflow. See
[the visualization workflow](../visualization/README.md).
