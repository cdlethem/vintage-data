# monitoring

Recurring sanity checks on the extract pipeline's output, designed to be run
by a **small local model** on a schedule. The expensive judgment is kept small
by doing all the objective work deterministically first.

## Two stages

1. **`digest.py` — deterministic triage.** Reads the landed data
   (`$EXTRACT_DATA_ROOT`), the source ymls, and `source_types.json`, and for
   every source computes staleness, run/failure counts, id-novelty (feeds),
   value-drift (status sources), and record-count trends — then classifies each
   as `OK` / `WATCH` / `PROBLEM`. No model involved. `--triage` prints just the
   verdict line plus the flagged sources.

2. **`run_check.sh` — model only on the flags.** If triage says everything is
   `OK`, it writes an OK report with **no model call**. Otherwise it hands the
   model *only* the flagged sources and a focused prompt (`PROMPT.md`), asking
   it to judge each against its known-normal `notes` and label it
   DISMISS / WARN / INVESTIGATE. Small input, small output — a local model
   finishes in a couple of minutes.

```
python3 digest.py                 # full per-source digest (all 27)
python3 digest.py --triage        # verdict + flagged sources only
./run_check.sh --dry-run          # what the model would receive
./run_check.sh                    # triage + model assessment -> reports/
```

## What "healthy" means per source

`source_types.json` encodes each source's `type` and known quirks:

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

- `PROBLEM`: `stale`, or `consecutive_failures >= 3` (down now), or
  `value_keys_missing` (the drift check is misconfigured), or a fast source
  that has never produced (`no_data_yet` with a sub-daily gap).
- `WATCH`: isolated failures that already recovered (`failed_runs >= 1` but
  `consecutive_failures = 0`), or a fast feed/status source showing no
  novelty/drift this cycle.
- Note the distinction: **consecutive** trailing failures mean an outage;
  scattered failures among successes are transient upstream flakiness and only
  warrant a WATCH — a source polling every 10 min will collect a few timeouts a
  day at zero real cost.

## Scheduling

`systemd/install.sh` installs a timer that runs the check every 30 minutes
(`extract-monitor.timer`). It's cheap when healthy (triage only). The model
call happens solely when a source is flagged, and pre-flights the llama-server
so a saturated model makes the check skip rather than queue.

The model is omp's `local` provider — the **Qwen3.8-27B on :8080**
(`~/.omp/agent/models.yml`). Override with `MODEL=`, `MAXT=`, `SLOTS_URL=`.

Reports land in `reports/` (gitignored).
