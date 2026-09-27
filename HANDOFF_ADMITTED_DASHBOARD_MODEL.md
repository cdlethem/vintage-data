# Handoff: Reconstruct Three Lost Functions

> Copy this file to a fresh agent/session. Full incident/fix history: commits
> `9d0f72d4`, `f94b5e0e`, `970b8a9d` on `main` (`git log --oneline -5` from repo
> root; read those commit messages first).

## Background

An unattended agent ran `git checkout` in this repo, which doubles as Airflow's
live DAGS_FOLDER. Because the autopilot/executive feature was never committed,
this silently reverted several tracked files to old versions. Most was already
recovered (see commits above) from a still-running wheel build and from precise
test-driven reconstruction. Three pieces remain missing. Fix all three in one pass.

## Step 0 — check for the real original code before reconstructing anything

This exact feature was built by the persistent Codex thread
`01a09981-2860-72c0-9314-5b759dd70b5b` (see
`~/.local/state/vintage-bot-supervision/state.json`). Its session log is at
`~/.codex/sessions/2026/09/13/rollout-2026-09-13T01-42-53-01a09981-2860-72c0-9314-5b759dd70b5b.jsonl`
and it may have continued into later rollout files under `~/.codex/sessions/2026/09/**`.

Search these (and any other session/history files under `~/.codex/`, e.g.
`thread_history_1.sqlite`) for the literal strings `_resume_published`,
`_sandbox_attempt`, and `def dashboard_model` — these are JSONL tool-call logs,
so a file write/patch containing the original source may be recoverable
verbatim. If found, use that source as-is (verify it against the specs below and
current call sites first; the codebase has moved on since these were written).
If genuinely not recoverable, design new implementations from scratch using the
specs below — do not block on this.

## Gap 1 — `bot_runner.dashboard_model` (bots/bot_runner.py)

Missing entirely. Routes a dashboard-mapped specialist model through OMP without
giving the sandboxed model raw provider credentials. Exact contract from
`bots/test_dashboard_models.py` (already in the repo — do not modify its
assertions):

```python
def dashboard_model(cfg, models_cfg, control, stack: ExitStack, deadline_at) -> dict | None:
    # control.model_settings() -> {"providers": [...], "assignments": [...]}
    # cfg["name"] is the role to look up in assignments; no match -> return None
    # assignment present but its provider_id missing from providers -> raise
    #   BotError("... mapped model provider is missing ...")
    # otherwise: use model_gateway.model_gateway(provider, model, ...) as a context
    #   manager (from bots/model_gateway.py, already intact) to get a local
    #   gateway socket; ExitStack-manage it so it closes when the caller's stack
    #   unwinds. Build a home dir under `stack` (auto-cleaned on exit) containing
    #   .omp/agent/models.yml pointing at the gateway (placeholder model id
    #   containing "gateway-placeholder", no real api key ever written to disk
    #   or into the returned dict — see test's assertNotIn checks).
    # Returned dict merges the matched local model template's argv/max_concurrency/
    #   usage config (from models_cfg["models"][models_cfg["default"]] i.e. the
    #   "cli" template) with model="gateway/<mapped model>", inherit_env=False,
    #   env=<fresh HOME, no inherited OLD_KEY-style vars>.
```

Read `bots/test_dashboard_models.py` in full for the exact assertions (env
isolation, home dir lifecycle via ExitStack, argv/max_concurrency/usage
passthrough). Read `bots/model_gateway.py` (intact) for the gateway context
manager's real signature. Read `bots/bot_runner.py`'s existing
`resolve_model` / `_model_aliases` / `_inference_slot` functions (intact) for the
surrounding conventions to match.

Two related regressions in the same file, same revert, fix alongside:

- `_failure_detail` no longer includes the offending path in its message
  (`bots/test_path_diagnostics.py` expects
  `self.assertIn(path, _failure_detail(...))`).
- `_inference_slot` currently acquires a local concurrency lock even for
  dashboard-managed models; `bots/test_dashboard_models.py`'s
  `test_managed_remote_models_use_scheduler_capacity_not_local_model_locks`
  expects `_inference_slot({}, {"_dashboard_managed": True})` to skip
  `_acquire_one` entirely (dashboard-mapped models are capacity-limited by the
  Airflow scheduler pool, not a local in-process lock).

Wire the callsite back in: `run()`'s model-selection block should call
`dashboard_model(cfg, models_cfg, control, model_resources, deadline_at)` when
not `ephemeral`, falling back to `resolve_bot_models` when it returns None —
this exact call shape is described in `bots/README.md`'s "Bot concurrency and
provider capacity" section and referenced in commit `9d0f72d4`'s message.

## Gap 2 — `admitted_runner._resume_published` (bots/admitted_runner.py)

Missing entirely. Resumes an executor admission whose evidence (patch + report +
manifest) was already verified/published in a prior attempt that crashed before
finalizing, without re-running the sandbox or calling any model. Exact call
signature and behavioral contract from `bots/test_admitted_envelope.py`
(already in the repo — read it in full, it is the spec):

```python
def _resume_published(admission, client, cfg, runner, dashboard_module,
                      identity, started_at, deadline_at, context_digest):
    # 1. Fetch client.get_artifact(admission["executor_report_sha256"]) then
    #    client.get_artifact(admission["patch_sha256"]) — in that order (see
    #    the fixture's `client.get_artifact.side_effect = [report, patch_bytes]`).
    # 2. Verify both digests against the admission's own fields; raise
    #    dashboard_module.ControlPlaneError("digest_invalid", "terminal") on
    #    mismatch, BEFORE calling submit_run or finalize_execution.
    # 3. Verify admission["verification_manifest"]'s identity fields
    #    (task_id, execution_id, revision, base_sha, patch_sha256) match the
    #    admission's own fields; raise ControlPlaneError("manifest_invalid",
    #    "terminal") on mismatch, before finalize_execution.
    # 4. If admission["pr_number"] is already set: the work is already
    #    published: skip client.publish_execution entirely, use the admission's
    #    existing pr_number/pr_url/trusted_head_sha, reason_code
    #    "published_report_recovered".
    # 5. If admission["pr_number"] is None: call
    #    client.publish_execution(identity["dag_id"], identity["run_id"], {...})
    #    with the SAME body shape run() already uses for a fresh executor
    #    publish (status, patch_sha256, changed_paths, verification_manifest,
    #    report_sha256) — mirror that existing code in run() exactly, reading
    #    changed_paths/status from the parsed report payload and patch_sha256
    #    from the manifest. Use the returned pr_number/pr_url/trusted_head_sha
    #    for finalize_execution's publication dict. Pick a distinct reason_code
    #    for this branch (not tested by name, but must differ from
    #    "published_report_recovered" — e.g. "pending_publication_resumed").
    # 6. subprocess.run must NEVER be called in either branch (both tests
    #    assert this) — no sandbox, no git.
    # 7. Build the run envelope via runner._envelope(...) (bot_runner._envelope,
    #    already intact) with attempts=[], outcome="succeeded", retry_class="none",
    #    payload=<parsed report JSON>, payload_schema=cfg["output"]["schema"],
    #    selected_model=admission["model_role"], then client.submit_run(envelope) —
    #    it must pass report_schemas.validate_run_envelope unchanged.
    # 8. client.finalize_execution(identity["dag_id"], identity["run_id"],
    #    "executor", {"projection": <submit_run's return>, "publication": {...},
    #    "result_artifact_sha256": ...}) — mirror run()'s existing finalize call
    #    shape exactly (same file, ~15 lines from the end of run()).
    # 9. Return runner.RunResult(projection, outcome, retry_class, reason_code).
```

Read `admitted_runner.run()`'s existing executor-publish block (the `else:`
branch under `if kind == "pr_reviewer":`) closely — this function should reuse
that exact publish_execution/finalize_execution call shape, not invent a new one.

Wire the callsite: this needs to be invoked from `run()` (or a thin dispatcher
above it) when an admission indicates "already has verified evidence, no new
sandbox attempt needed" — check the `ExecutorAdmissionV2` schema
(`report_schemas.py`, already restored/intact) for whatever field signals this
(likely something checking `admission.get("verification_manifest")` is present
and complete before deciding whether to run the sandbox at all). Confirm with a
scoped read of `report_schemas.py`'s `ExecutorAdmissionV2` and `execution.py`'s
`claim_execution` before assuming the exact trigger condition — do not guess
this part, verify it against the actual schema/service code that produces such
an admission.

## Gap 3 — `admitted_runner._sandbox_attempt` (bots/admitted_runner.py)

Missing entirely; `run()` currently builds an equivalent dict inline (search for
`"sandbox_succeeded"` in `run()`'s attempts.append block — around 15 lines).
Extract that inline block into:

```python
def _sandbox_attempt(model_role: str, started_at: datetime, duration_ms: int) -> dict:
    ...
```

Per `bots/test_admitted_envelope.py`'s
`test_attempt_counters_come_from_normalized_usage`, it must call
`usage_tools.normalize(<counts>, format="openai")` and derive
`input_tokens`/`output_tokens`/`total_tokens` from that call's return value
(not hardcode them) — same as the current inline code already does with `{}`.

I checked `bots/sandbox/worker.py` and `launcher.py`: neither currently reports
real per-attempt token usage back to the parent, so `<counts>` is almost
certainly still `{}` here too (zero usage) — this extraction's value is DRY and
a wiring point for a future real usage source, not a new capability. Do not
invent a fake usage source. If you find evidence in the recovered Codex session
history that the original passed something other than `{}`, use that; otherwise
keep `{}` and say so in your summary. Replace the inline block in `run()` with a
call to this function.

## Constraints (apply to all three)

- This repo's checkout is Airflow's live DAGS_FOLDER. Never run `git checkout`,
  `switch`, `restore`, `reset`, `clean`, or `pull --force` here. Commit your own
  finished work with a plain `commit` on the current branch only.
- Run:

  ```
  PYTHONPATH=<repo>:<repo>/bots <repo>/orchestration/.venv/bin/python -m pytest \
      bots/test_admitted_envelope.py bots/test_dashboard_models.py \
      bots/test_path_diagnostics.py -q
  ```

  All must pass.
- Mocks in this test suite use bare `Mock()` in places, which auto-vivifies any
  attribute/call and will NOT catch a wrong URL path, wrong argument order, or a
  wrong keyword name. Two such bugs already slipped through this exact test suite
  once (see commit `970b8a9d`). After tests pass, additionally reproduce each
  fixed path live against the running system (or a throwaway script using the
  real `provider_dashboard.DashboardClient` / `bot_runner` module) — do not
  trust a green mocked test alone as proof for these three.
- Do not touch `bots/AUTOPILOT.md`'s Astra-only language re-appearing — that
  allowlist was intentionally removed; any mapped model is valid now.
