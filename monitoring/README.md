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

`--triage` prints only `WATCH`/`PROBLEM` rows. `--json` provides compact
per-source objects for scripts and agents.

Task failures themselves are handled by the self-healing bots
([`bots/README.md`](../bots/README.md)), which read them from Airflow directly;
the digest covers what Airflow cannot see, such as stale or silently empty sources.

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
