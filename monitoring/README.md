# monitoring

Deterministic health analysis of the extract pipeline's output. The expensive
judgment — *is this flag real?* — belongs to the `pipeline_check` bot
(`bots/pipeline_check/`); everything objective happens here, in ordinary code,
first.

## Two stages

1. **`digest.py` — deterministic triage (this directory).** Reads the landed
   data (`$EXTRACT_DATA_ROOT`), the source ymls, and `source_types.json`. For
   every source it derives the expected gap from the yml's five-field UTC cron
   schedule, computes staleness, run/failure counts, id-novelty (feeds),
   value-drift (status sources), and record-count trends, then classifies the
   source as `OK` / `WATCH` / `PROBLEM`. No model involved. `--triage` prints
   just the verdict line plus the flagged sources.

2. **`bot__pipeline_check` — model only on the flags.** The bot takes the
   triage output as its context. If triage says `OK` it writes an OK report and
   **calls no model at all**. Otherwise it sends only the flagged sources and a
   focused prompt (`bots/pipeline_check/prompt.md`) to whichever model alias
   this machine configures, asking for DISMISS / WARN / INVESTIGATE per source.

```
../orchestration/.venv/bin/python digest.py           # full per-source digest
../orchestration/.venv/bin/python digest.py --triage  # verdict + flags only
../orchestration/.venv/bin/python ../bots/bin/run_bot pipeline_check --dry-run
../orchestration/.venv/bin/python ../bots/bin/run_bot pipeline_check
```


## What "healthy" means per source

`source_types.json` encodes each source's behavioral `type` and known quirks;
the source yml `schedule` is the sole cadence authority:

- **feed** — new ids should appear across a real interval; `id_novelty` ~0 on a
  fast feed is the warning (but quiet hours/weekends are normal — see `notes`).
- **status** — same ids each run, values drift; a frozen `value_change_fraction`
  on a source that should move is the warning.
- **snapshot** — periodic full snapshot; only staleness and record-count
  stability matter.

`notes` records behavior that looks alarming but is normal (weekend markets,
hourly publish cycles, weekly release batches, small quiet projects). The model
leans on these to dismiss false alarms. **Keep them updated as you learn each
source** — they are the institutional memory that lets a small model reason
correctly.

## Triage rules (why a flag fires)

- `PROBLEM`: invalid or missing cron schedule (`schedule_error`), `stale`,
  `consecutive_failures >= 3` (down now), `value_keys_missing` (the drift
  check is misconfigured), or a fast source that has never produced
  (`no_data_yet` with a sub-daily cron-derived gap).
- `WATCH`: isolated failures that already recovered (`failed_runs >= 1` but
  `consecutive_failures = 0`), or a fast feed/status source showing no
  novelty/drift this cycle.
- Note the distinction: **consecutive** trailing failures mean an outage;
  scattered failures among successes are transient upstream flakiness and only
  warrant a WATCH — a source polling every 10 min will collect a few timeouts a
  day at zero real cost.

## Scheduling

The check runs as an Airflow DAG (`bot__pipeline_check`, every 30 minutes) like
every other recurring job in this repo — no separate timer. It is cheap when
healthy: the model is invoked only on a flag, and a busy local model server
makes the run skip rather than queue.

Which model answers is not this directory's business: the bot names a *model
alias* and `bots/models.yml` (gitignored, per-machine) maps that alias to
OpenRouter, an Anthropic key, a local llama-server, or a CLI agent. See
[`bots/README.md`](../bots/README.md).

Reports land in `bots/runs/pipeline_check/` (gitignored).
