# monitoring

Deterministic health analysis of extract output. `digest.py` reads
`$EXTRACT_DATA_ROOT`, source ymls, the effective cadence plan, and
`source_types.json`; it computes evidence before any model is involved.

## Evidence produced

For every source the digest derives the effective expected gap, staleness,
success/failure streaks, record-count trends, and the last-two-run signal:

- **feed** — fraction of ids in the latest run absent from the previous run;
- **status** — fraction of common ids whose configured values changed;
- **snapshot** — staleness and record-count stability.

Known-normal behavior belongs in `source_types.json`: weekends, release
calendars, quiet communities, and small periodic batches. These notes prevent a
model from treating expected silence as an incident.

```
../orchestration/.venv/bin/python digest.py
../orchestration/.venv/bin/python digest.py --json
../orchestration/.venv/bin/python digest.py --triage
```

`--triage` prints only `WATCH`/`PROBLEM` rows. `--json` provides the compact
per-source objects used by `bots/agent_context.py cadence`.

## Consumers

Monitoring judgment is split by responsibility rather than duplicated:

1. **`bot__cadence_review`** runs at `37 */4 * * *`. Its compact context
   contains every flagged source plus a bounded rotating sample. It is
   read-only and emits cadence recommendations or strict task proposals; it
   never changes source configuration.
2. **`bot__failure_triage`** runs at minute 7 hourly and calls a model only for
   bounded, normalized, previously unreviewed non-bot failure groups. Bot
   workflow failures stay in deterministic dashboard health cards. Triage is
   read-only and proposes admitted work instead of repairing the checkout.
3. **`bot__manager`** reads the latest useful provider-stored reports daily.
   Missing, stale, or failed specialist evidence is explicit and forces a
   `degraded_evidence` plan whose actions remain human approval pending.

Report submission stores typed outcomes, retry classes, durations, deadline
consumption, token usage, and normalized failure fingerprints in the dashboard
database. Payload-free skips do not replace useful evidence. The dashboard
shows freshness and missing-evidence status, execution queue stages, and
provider cache age without expanding model prompts or XCom.

The former `bot__pipeline_check` and package-promoting `bot__reviewer` are
removed. `task_executor` is the only writer and runs only after human
admission in external confinement; `pr_reviewer` is the sole read-only
reviewer. Neither monitoring consumer mutates the shared checkout.

## Triage rules

- `PROBLEM`: invalid/missing cron, stale output, three or more consecutive
  failures, missing configured value keys, or a fast source that has never
  produced.
- `WATCH`: isolated recovered failures or a fast feed/status source with no
  novelty/drift in the current comparison.
- A failure streak is different from scattered upstream timeouts. The former
  is an outage; the latter is evidence for observation unless it materially
  loses data.

Monitoring reads the cadence plan as well as the declared yml. A source slowed
to twelve hours is not incorrectly marked stale against its original
fifteen-minute cron.
