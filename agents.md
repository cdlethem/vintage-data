# Extract pipeline: scheduling & operations notes

Durable notes for agents doing four kinds of work here: (1) promoting fetchers
from `staged_scripts/` into the scheduled pipeline, (2) debugging why a
scheduled source isn't producing data, (3) making a source fetch only new or
changed data instead of re-downloading a near-identical payload, (4) operating
the load layer. Last full pass: 2026-09-04 — all 133 staged scripts are
scheduled (0 unscheduled backlog at time of writing) and six sources have been
converted to incremental fetching. Scripts arrive over time; always recompute
the backlog rather than trusting this file's counts.

## Pipeline at a glance

```
pipelines/extract/sources/*.yml        one config per scheduled source instance
  -> parsed by orchestration/dags/extract_dags.py   (DAG factory, one DAG per yml)
  -> each DAG runs orchestration/include/extract_runner.py
       subprocess: python pipelines/extract/scripts/fetch_X.py [args...]
       stdout = NDJSON, each line carrying source/fetched_at/id
  -> orchestration/include/sinks.py (LocalSink) writes the raw file + manifest

  -> orchestration/dags/load_dag.py (load__raw, every 15 min) submits a job
  -> $EXTRACT_LOAD_QUEUE (filesystem queue), claimed serially by
  -> extract-loader.service = pipelines/load/loader (the only warehouse writer)
  -> ~/dev/data/warehouse/extract.duckdb : raw.<source> insert-only + _load.*
```

Raw output: `$EXTRACT_DATA_ROOT/raw/source=<name>/dt=<date>/<name>_<ts>.ndjson`
(default root `~/dev/data/extract`, currently `/home/colin/dev/data/extract`).
Every run also writes `<file>.ndjson.meta.json` (success manifest, written
even for a valid zero-record run — the empty staged file is just dropped) or
`<file>.ndjson.fail.json` (failure marker, written instead of raising silently
so monitoring sees failures without inferring them from staleness).

Three sibling trees live under `$EXTRACT_DATA_ROOT` and never touch each other:
`raw/` (the sink, and the only thing the load layer reads), `state/` (watermarks,
Task 3) and `_load_queue/` (load jobs, Task 4).

### DAG shapes: two factories

`extract_dags.py` is the default and should stay the simple shape: one yml,
one DAG, one `run` task. A yml is claimed by another factory only when it sets
`dag_factory: <name>`; `extract_dags.py` skips those, so exactly one factory
ever builds a given config.

`job_boards_dag.py` is the second shape and the template for future ones: it
globs `sources/job_boards_*.yml` (each `dag_factory: job_boards`, one per ATS
provider) and composes them into a single DAG whose `providers` TaskGroup runs
one task per config in parallel. Providers fail independently — a Workday
outage cannot discard Greenhouse's records — and because each provider is
still a normal source config, it keeps its own `source=` partition, manifest,
cadence/rate-limit metadata and monitoring baseline. Adding a provider is a
new yml plus an adapter, with no factory edit.

Reach for a dedicated factory when one logical source fans out to upstreams
with genuinely independent availability. Do not add per-source branching to
`extract_dags.py`.

### Job-board catalog (data, not code)

`fetch_job_boards.py` holds no company list. The watchlist is
`pipelines/extract/catalogs/job_boards.json` (1,829 boards, 53 HQ countries,
35 industries), and provider endpoints/pagination/throttling live once in
`pipelines/extract/scripts/job_boards_lib/`, shared with the tooling.

- Adding companies is a data change; adding an ATS is one adapter in
  `job_boards_lib/adapters.py` plus one `job_boards_<provider>.yml`.
- Every catalog entry MUST be live-verified first:
  `python3 tools/verify_job_boards.py candidates.ndjson` prints only tokens
  that really returned open postings. Drop the rest; never guess a token.
- `python3 tools/build_job_board_catalog.py` merges
  `discovery/verified_*.json` into the catalog. It validates ISO-3166 country
  codes and the fixed industry vocabulary, and **fails on any new
  company/country/industry disagreement between sources** rather than letting
  file order decide. Resolve it explicitly in that script's `OVERRIDES`.
- A 429 means "unknown", not "dead" — retry later; do not delete the entry.

## Task 1: schedule new fetchers from staged_scripts/

### Finding the real backlog

Never infer "scheduled" from `pipelines/extract/scripts/` file existence.
Parse every `pipelines/extract/sources/*.yml`, collect each config's `script`
value into a set, and subtract that set from the `fetch_*.py` filenames in
`staged_scripts/`. (`len(yml files) != len(distinct scripts)` is normal — one
script can back several instances, e.g. `gbfs_citibike.yml` /
`gbfs_divvy.yml` both use `fetch_gbfs.py`.)

```python
import yaml
from pathlib import Path
root = Path(".")
staged = sorted((root/"staged_scripts").glob("fetch_*.py"))
cfg_scripts = {yaml.safe_load(p.read_text())["script"]
               for p in (root/"pipelines/extract/sources").glob("*.yml")}
unscheduled = [p.name for p in staged if p.name not in cfg_scripts]
```

### Promotion convention

`staged_scripts/` is retained, not moved — every fetcher there keeps its
original copy permanently. Promoting means:

1. Copy the fetcher **unchanged** into `pipelines/extract/scripts/`.
2. Add `pipelines/extract/sources/<name>.yml`, key order:
   `name`, `script`, `args`, `schedule` (with trailing `# UTC` comment),
   `enabled`, `retries`, `timeout_minutes`, `sink`, blank line, then the
   vetting-metadata comment, `cadence_note`, `rate_limit`, `licence`,
   `attribution_required`.
3. One yml per source *instance*. Reuse one script across several ymls only
   when the instances are independently useful (see gbfs example above).
4. `name` becomes the DAG id as `extract__<name>` — must be unique across all
   ymls, or Airflow will show duplicate/ambiguous DAG rows.
5. `schedule` is a standard 5-field cron (`minute hour dom month dow`).
   **Double-check field count** — `M H * * *` is daily (5 fields); a stray
   extra `*` silently becomes 6 fields and is a different (nonsensical)
   schedule. `M-59/step * * * *` staggers a sub-hourly source off :00 so
   many sources don't fire in the same second.
6. Nothing polls faster than every 5 minutes. Stagger the minute offset
   across sources sharing a cadence.
7. `args` items are passed as literal positional CLI tokens to the script,
   including flags (`extract_runner.run()` does `[str(a) for a in args]` and
   execs `python fetch_X.py *args`, no shell parsing). Use `args` to pin a
   *safe* parameter set for the scheduled instance even if it differs from
   the script's own default — see the drivebc gotcha below for a concrete
   case where this mattered.
8. DAG factory required keys are only `name`, `script`, `schedule`
   (`orchestration/dags/extract_dags.py`, `REQUIRED_KEYS`). It also verifies
   the referenced script file exists under `pipelines/extract/scripts/`.
   Everything else has defaults (`retries` default 1, `timeout_minutes`
   default 10, `max_active_runs=1` always).

### Verifying a newly scheduled source

Do this in small batches (4-way concurrency is plenty) so a failure is
attributable to one source and upstreams aren't hammered:

1. Run the copied script with exactly the yml's `args`. Require exit 0
   within `timeout_minutes`.
2. Exit 0 + empty stdout is a valid result (some sources legitimately have
   nothing new). Don't treat it as failure.
3. For non-empty stdout: `extract_runner._check_envelope` only validates the
   **first** nonblank line has `source`/`fetched_at`/`id` — match that bar
   in ad-hoc verification too (parsing every line of a multi-million-row
   file just to verify is wasteful and, if you materialize all lines in a
   list, can use a lot of memory — stream + sample instead).
4. stderr output is not failure by itself — several scripts log attribution
   notices or non-fatal warnings to stderr while still exiting 0.
5. On a real failure, reproduce that single invocation directly, inspect the
   HTTP status/body, and fix root cause (bad default arg, bad header, wrong
   endpoint) — don't paper over it by loosening retries/timeouts or
   swallowing the error.
6. After editing scripts/configs, load the DAG factory in the Airflow
   environment and confirm no import errors, then confirm the five services
   are still active and dag-processor logs are clean (see below).
7. Recompute the unscheduled backlog immediately before declaring the task
   done — scripts can arrive mid-task.

## Task 2: debugging the running pipeline

### Environment / topology

- Airflow env vars: `orchestration/airflow.env` (checked in) +
  `orchestration/airflow.secrets.env` (gitignored, source it too).
- Python: `orchestration/.venv/bin/python` / `.../bin/airflow` (uv-managed
  venv, see `orchestration/setup/03_airflow_venv.sh`). `PYTHONPATH` must
  include `orchestration/include` for `extract_runner`/`sinks` imports —
  `airflow.env` sets this.
- Five systemd services should all report `active`:
  `airflow-api-server`, `airflow-scheduler`, `airflow-dag-processor`,
  `airflow-worker@1`, `airflow-worker@2`.
  ```
  systemctl is-active airflow-api-server airflow-scheduler \
    airflow-dag-processor airflow-worker@1 airflow-worker@2
  ```
- To list what Airflow has actually discovered:
  ```
  cd orchestration
  set -a; . ./airflow.env; . ./airflow.secrets.env; set +a
  .venv/bin/airflow dags list -o plain
  ```
  (`-o csv` is not a valid choice for this subcommand; use `plain`/`table`/
  `json`/`yaml`.) Don't be alarmed by a dag_id appearing twice in this
  output — Airflow keeps a row per `(dag_id, bundle_version)`, so re-editing
  a yml after an initial parse leaves a stale row alongside the current one.
  It's cosmetic; the scheduler uses the latest version. `is_paused=False`
  confirms the DAG is actually scheduled, not just imported.

### Logs

- DAG-parse errors (factory-level, e.g. malformed yml, missing script file):
  `orchestration/airflow_home/logs/dag_processor/<date>/dags-folder/extract_dags.py.log`.
  A bad yml is caught by `extract_dags.py`'s per-file try/except and appears
  here as `skipping source config <file>` with a traceback; every *other*
  yml still loads. **Healthy sibling DAGs are not proof every yml parsed** —
  grep this log directly for `skipping source config` when you need that
  guarantee.
- Per-run task logs (proves the actual runner→sink path worked, not just
  DAG discovery):
  `orchestration/airflow_home/logs/dag_id=extract__<name>/run_id=<run_id>/task_id=run/attempt=<n>.log`.
  Look for the `extract_runner` line `"<name>: N records, B bytes -> <path>"`
  and the PythonOperator `"Done. Returned value was: {... 'exit_code': 0 ...}"`.
- Raw data + manifests land under `$EXTRACT_DATA_ROOT/raw/source=<name>/dt=<date>/`.
  A `.fail.json` next to a run's expected filename means that run failed —
  read it directly, it's the `RuntimeError` message plus context, no need to
  dig through Airflow logs.

### Common upstream failure modes seen while scheduling

- **Transient 5xx**: some public APIs blip under momentary load (observed
  on BC's Open511 endpoint) — a bare retry moments later succeeds. Airflow's
  `retries: 1` / 5-minute retry_delay already covers this; don't change
  config for a one-off blip, just confirm it clears.
  Not all 5xx are transient — `fetch_drivebc_events.py`'s default
  `--limit 1000` deterministically 500s on this upstream (repeatable, not
  load-related) while `--limit 500` and below succeed. Fixed by pinning
  `args: ["--limit", "500"]` in `drivebc_events.yml` rather than touching
  the script — the script's own default is fine for other callers/limits.
- **429 from probing too fast**: hitting an endpoint repeatedly by hand
  (e.g. while debugging) can trip rate limiting that the source's actual
  15-minute scheduled cadence never would. Back off and retest at a normal
  pace before concluding the source is broken.
- **Header sensitivity**: a couple of sources (`fetch_gazette_insolvency.py`,
  `fetch_globe_measurements.py`) get HTTP 500/406 if an `Accept` header is
  sent explicitly; the scripts already omit it deliberately — don't "fix"
  that by adding one back.
- **Large/slow sources**: `fetch_irs_exempt_organizations.py` (bare = one full
  ~150MB regional partition), `fetch_malaysia_pricecatcher.py` (scans the full
  current-month CSV) and `fetch_netflix_top10.py` (streams full history TSV)
  all do a large download/scan even though most only *emit* the newest
  slice. Give these generous `timeout_minutes` (20-30) and don't be
  surprised if a manual verification run takes noticeably longer than the
  five-request-per-run sources. (`fetch_imdb_ratings.py` used to belong here;
  it now skips the download entirely when the dump is unchanged — see Task 3.)
- **Fan-out fetchers (2026-09-04 batch)**: several newer scripts discover their
  own target list and then make one request per target, so a bare run is the
  broad-coverage run and `timeout_minutes` must match the fan-out, not a single
  request. Measured live 2026-09-04:
  - `fetch_openactive_feeds.py` — discovers every feed of one type from
    `status.openactive.io` (149 Slot feeds / 131 providers). Bare
    `--max-pages 3 --max-items 1500` took 11 minutes for 146k records; give it
    30 `timeout_minutes` and schedule per `--feed-type`
    (`Slot`, `FacilityUse`, `SessionSeries`, `ScheduledSession`,
    `CourseInstance`, `Event`) rather than per provider. 7 of 149 Slot feeds
    return publisher-side 500/403 (they carry ⚠️ on the dashboard); the script
    logs and skips them and only fails if *every* feed fails.
  - `fetch_queensland_hospital.py` — 40 facility pages from the sitemap, ~50s.
  - `fetch_catsa_wait_times.py` — 17 airport pages from the wait-times index.
  - `fetch_tpims_parking.py` — both state feeds in one run; positional
    `regions` narrows it.
  - `fetch_ri_shelter_stats.py` — five species reports in one run.
  - `fetch_melbourne_pedestrians.py` — paginates the rolling window
    newest-first and stops at its stored watermark, so a run costs one page
    when nothing new has been published (see Task 3).
- **TLS/verb quirks in that batch**: `fetch_hk_cremation_sessions.py` needs
  `OP_LEGACY_SERVER_CONNECT` (set in-script; certificate verification stays
  on) or the handshake fails with `UNSAFE_LEGACY_RENEGOTIATION_DISABLED`, and
  `fetch_tokyo_mou_detentions.py` must POST the APCIS form fields
  (`Mode/MOU/Auth/Src/Type/Month/Year/SaveFile`) — a bare GET returns only the
  selection surface with no detention rows.

## Task 3: fetching only new data (state & watermarks)

Most fetchers here are stateless by design and that is usually right: if an
upstream only publishes a rolling window or a "latest" document, re-reading it
is the only way to observe change. But a source that re-emits a mostly
identical dataset every run wastes the upstream's bandwidth and buries the
actual signal in duplicates. Where the publisher exposes a change mechanism,
use it.

### The convention

State lives **outside the repo**, beside the raw data:
`$EXTRACT_DATA_ROOT/state/<name>.json` (gzipped TSV when it holds a whole
keyset, as imdb does). A stateful script MUST:

1. Take `--state-file` (override the path) and `--no-state` (run stateless,
   for one-off pulls and for reproducing a bug without touching production
   state). Neither flag belongs in the yml `args` — the defaults are correct
   for the scheduled instance.
2. Resolve the default path from `$EXTRACT_DATA_ROOT` with the same
   `~/dev/data/extract` fallback the sink uses, so a script run by hand and
   the same script run by a worker share one watermark. Verified: the worker
   writes `state/crossref.json`, and the next hand-run honours it.
3. Write state **only after every record has been printed**, and write it
   atomically (`.tmp` + `os.replace`). This is deliberately at-least-once: a
   run that dies mid-stream re-fetches its window next time rather than
   skipping it. Losing a window is unrecoverable; a duplicate is not.
4. Treat a cold start (no state) as the backfill: emit the full available
   history/dataset once. Keep a `--full` style flag if re-baselining is
   plausible (imdb has one).
5. Never bound a run with an arbitrary record/page cap. The watermark is what
   bounds it. A cap silently truncates a busy window and, worse, makes the
   truncation invisible — the run still exits 0. Where the upstream genuinely
   needs bounding, bound it by *wall clock* and persist a resumable cursor
   (openactive) so the next run continues instead of re-starting.

Pick the mechanism the upstream actually offers, in this order of preference:

- **Conditional request** — publisher sends `Last-Modified`/`ETag`: store it,
  send `If-Modified-Since`/`If-None-Match`, and treat `304` as a valid
  zero-record run. This is the only pattern that avoids the download itself.
- **Server-side change filter** — a `since`/`from-*` query parameter. Watch
  the *resolution*: CrossRef's `from-created-date` only resolves to the hour,
  so the filter is the coarse floor and the exact timestamp must still be
  applied client-side.
- **Sorted-descending walk** — no filter, but sortable by update time: walk
  newest-first and stop at the first record older than the watermark.
- **Keyset diff** — dump-only publisher with no change signal at all: keep the
  previous keyset and emit only rows whose values moved. Costs a full scan
  (unavoidable) but stops the re-emission.

**A day-granular watermark is not enough on its own.** If the timestamp is a
date (or a second shared by many records), also persist the ids already
emitted *at exactly that boundary value* — otherwise you must choose between
dropping records that arrive later on the same day and re-emitting the whole
day. Both clinicaltrials (1,112 ids on the boundary date) and crossref (up to
162 DOIs sharing one second) need this.

### Sources converted so far (2026-09-04), with measured effect

| source | mechanism | cold run | immediate re-run |
| --- | --- | --- | --- |
| `imdb_ratings` | `If-Modified-Since` + keyset diff | 1,713,237 records | 0 (`304`, no download) |
| `clinicaltrials` | sorted-desc walk + boundary ids | 2,000 records | 0, zero overlap |
| `crossref_new_dois` | hour filter + exact ts + boundary ids | 1,905 records | 0, zero overlap |
| `melbourne_pedestrians` | `sensing_datetime` watermark | 2,596 records | 0 |
| `swpc_space_weather` | per-feed `ts` watermark | 58 (kp_index) | 0 |
| `openactive_feeds` | persisted RPDE `next` cursors | 392,796 records | resumes mid-feed |

`clinicaltrials` was also *lossy* before this: it pulled a fixed 20 newest
studies every 4 hours (~120/day) against ~1,100 studies that actually update
per day. The watermark made it both cheaper per record and complete.

### Verifying a stateful source

Run it twice against a throwaway `--state-file` in a temp dir. The second run
must emit ~0 records and cost ~1 request. Then prove the watermark is exact,
not just "quiet": rewind the state (drop half the boundary ids, or set an
older timestamp) and confirm **exactly** the withheld records come back and
nothing older leaks. A watermark that suppresses too much looks identical to a
working one until you audit for gaps.

Do not conclude from `0 records` alone that a watermark works — confirm the
upstream really has nothing newer. CrossRef indexes with a ~20 minute lag, so
a run at 13:32 legitimately sees nothing after 13:12; cross-check with a
`--no-state` pull before believing either outcome.

### Gotchas found while doing this

- **ClinicalTrials v2 has no date filter.** v1's
  `filter=LastUpdatePostDate:<date>` now returns HTTP 400, as does any
  `filter=Field:value` form (`filter.overallStatus` is a separate, real
  parameter). Docstrings written against v1 will mislead you. `sort=` still
  works, which is why the client-side walk is the answer there.
- **State is outside the repo, so it survives `git checkout`.** Deleting a
  state file re-baselines the source — which re-emits the full dataset into
  RAW, and RAW is insert-only (see Task 4). Delete deliberately, not as a
  debugging reflex.
- **`--no-state` is the right debugging tool**, precisely because it cannot
  corrupt the watermark of a scheduled source.
- Several docstrings *describe* a watermark the code never implemented
  (`fetch_swpc_space_weather.py` and `fetch_openfda.py` both said "newer than
  your stored watermark"). Treat a docstring as intent, not as evidence;
  check the code.

### Remaining candidates (not yet converted)

Ranked by waste removed. Each still re-fetches a mostly-identical payload:

- `fetch_openfda.py` — docstring already specifies a `report_date`/
  `receivedate` watermark with descending sort; currently a fixed 14-day
  trailing window. (Returned 0 records live, so verify the endpoint before
  assuming the window is the problem.)
- `fetch_irs_exempt_organizations.py`, `fetch_netflix_top10.py`,
  `fetch_malaysia_pricecatcher.py` — full-file downloads that emit only the
  newest slice; all three are keyset-diff or `If-Modified-Since` candidates
  exactly like imdb.
- `fetch_usaspending.py` — has a `Last Modified Date` field and a
  `time_period` filter; a watermark replaces the trailing window.
- `fetch_wayback_cdx.py`, `fetch_gh_archive.py` — append-only upstreams
  (capture timestamps / hourly archive files), so a cursor is exact.
- `fetch_chess_archives.py` — its own docstring notes only the current
  month's archive needs re-polling; the monthly archive list is the cursor.

## Task 4: the load layer (raw files -> warehouse)

Full design notes: `pipelines/load/README.md`, which has a step-by-step of what
one job does plus a troubleshooting section. What an agent needs on hand:

```
sink files -> orchestration/dags/load_dag.py (submits a job, waits)
           -> $EXTRACT_LOAD_QUEUE/{pending,running,done}/*.json
           -> extract-loader.service = pipelines/load/loader (the only writer)
           -> ~/dev/data/warehouse/extract.duckdb : raw.<source> + _load.*
```

### Rules that explain most surprising behaviour

- **One writer, always.** DuckDB's file lock is exclusive: while the service
  holds a connection nothing else can open the file, not even read-only. The
  service closes it after `service.idle_release_s` of quiet, so read-only access
  (`pipelines/load/bin/loader sql`, or `duckdb -readonly` if you install the
  standalone CLI — it is not on this box) works between loads, and
  `loader status` degrades to "warehouse unreadable right now" rather than
  hanging. Never add a second
  process that writes; submit a job instead.
- **Never run `run-once` while the service is up** — it will sit in `connect()`
  retrying the lock until `lock_timeout_s`. Use
  `pipelines/load/bin/loader submit --wait`.
- **Backfill is not a mode.** A job loads every sink file absent from the
  `_load.files` ledger, so the first run and the 15-minute run are one code path.
- **Insert-only, no dedupe.** To re-load a file you must delete *both* its
  ledger row and its rows from `raw.<source>` (`WHERE _source_file = ...`), with
  the service stopped. Deleting only the ledger row gives you a duplicate copy.
- **Schema is never user-supplied.** Types are inferred from `sample_lines`
  records per file; new keys become columns, conflicting types widen
  (`integer -> double -> string`), and `_payload` always holds the verbatim
  record, so nothing is ever unrecoverable.
- **All-null is not evidence.** A key seen only as `null` is *tentative*: it
  creates nothing and widens nothing. This rule exists because the first
  backfill widened `hackernews.score` to VARCHAR purely because one batch had no
  scores. If you touch `loader/schema.py`, keep it.
- **A failed file is not a stuck pipeline.** `_load.files.status='failed'` with
  `attempts`; retried until `max_attempts` (3), then skipped so one poison file
  can't make every run red.
- The loader has its own venv (`pipelines/load/.venv`, driver + yaml only).
  Airflow's venv deliberately has no warehouse driver; don't add one there.
- Use the `pipelines/load/bin/loader` wrapper, not `python -m loader` — it puts
  the package on PYTHONPATH (so it works from any cwd) and sources `airflow.env`,
  so the CLI can't end up pointed at a different warehouse than the service.

### Debugging a red `load__raw` run

The task raises exactly three failures, and only the third is about data:

1. `loader service heartbeat is missing / Ns old` — the writer is down or wedged
   and nothing was attempted. `systemctl status extract-loader`,
   `journalctl -u extract-loader -n 50`. Not a data problem; don't go looking at
   sources.
2. `load job ... did not finish within Ns` — still queued or running. Check
   `ls $EXTRACT_LOAD_QUEUE/running` and the journal; usually a huge file or a
   lock it can't get (`warehouse locked by another process; retrying in ...` in
   the journal means someone left a `duckdb` shell open — DuckDB's underlying
   error names the holding PID).
3. `loaded N file(s) but M failed` — the rest of the batch landed fine. Go to
   `SELECT path, attempts, error FROM _load.files WHERE status='failed'`.

Three places hold the full job result: the Airflow task log (pretty-printed
JSON), `$EXTRACT_LOAD_QUEUE/done/<job_id>.json` (kept 7 days), and `_load.jobs`
(forever). The counters account for every file — `files_seen`, `files_skipped`
(ledger / out of attempts / disabled source), `files_loaded`, `files_failed`, and
`files_pending_after` when `max_files` cut the batch short. A file younger than
`min_age_s` with no `.meta.json` yet is deliberately left for the next run.

### Debugging a schema question

`pipelines/load/bin/loader inspect <source>` shows what inference would produce
without writing anything — that is the first command for any "why is this column
a VARCHAR" question. `_load.schema_changes` then says when the live table last
moved and why:

```sql
SELECT changed_at, table_name, column_name, change, from_type, to_type
FROM _load.schema_changes WHERE change <> 'create' ORDER BY changed_at DESC;
```

A widen is usually real drift, not a bug. Observed live: `queensland_hospital`'s
`patients_waiting` is an integer except for the 27 rows where the publisher sends
`"-"`, so the column is VARCHAR and that is correct. A missing column normally
means the key was all-null in the sample (tentative), appears only nested, or
arrived after the sample window — it is still in `_payload`, and `sample_lines`
for that source is the lever. Column names are lowercased, non-alphanumerics
become `_`, a leading `_` is dropped (reserved for lineage columns), and
collisions get `_2`.

### Gotchas that have already cost time here

- `duckdb` returns `TIMESTAMPTZ` in the session timezone; `SET timezone='UTC'`
  before comparing anything to the sink's ISO strings (`loader sql --utc` does
  this for you).
- `loader sql` is read-only by design. Ledger surgery (deleting a `_load.files`
  row to re-load a file) needs the service stopped and a write connection from
  the loader's venv.
- The duckdb Python client needs `pytz` installed to hand `TIMESTAMPTZ` values
  back to Python — it is in `requirements.txt` for that reason alone; don't prune
  it as unused.
- A new `load__raw` DAG is created **paused** (Airflow pauses DAGs at creation);
  the yml `schedule` has nothing to do with it. Unpause once.
- Restarting the service mid-job is safe: the orphaned job is requeued at startup
  (`requeued N job(s) orphaned by a restart`) and skips whatever already
  committed, because each file's rows and its ledger row share one transaction.
  `systemctl stop` finishes the file in flight first, hence `TimeoutStopSec=600`.
- `_payload` roughly doubles a source's footprint. For a source that re-lands the
  same bulk file every run (`openactive_feeds` re-lands ~550MB), `keep_payload:
  false` or `schema_detection: payload_only` is the fix — and DuckDB never
  returns freed space to the filesystem, so the existing file won't shrink.

## Historical backfill operations (2026-09-04)

`tools/backfill.py` is the operator runner. It reads optional `backfill:` blocks
in source ymls, walks units oldest-first, and writes ordinary sink files plus
manifests under the true historical `dt=YYYY-MM-DD` partition. It skips any
unit whose manifest already exists, so it is safe to interrupt and restart:

```bash
python3 tools/backfill.py --plan
python3 tools/backfill.py --concurrency 4
pipelines/load/bin/loader submit --wait
```

Units within a source are sequential and paced; `--concurrency` only runs
different sources in parallel. Failed units retry with quadratic backoff and
stop that source after five consecutive failures. The loader service discovers
backfill `.ndjson` files exactly like scheduled output and `_load.files` makes
each committed path idempotent. Keep `extract-loader` active; never use
`run-once` while it is running.

The current configured scope is 11,766 units before smoke-test units are
excluded on resume:

| source | verified history | projected raw |
|---|---|---:|
| `malaysia_pricecatcher` | 2022-01 to 2026-09, monthly | 35.9 GB |
| `infodengue` | 2010 onward, one national sweep | 4.1 GB |
| `globe_measurements` | 1995-01-01 onward, daily | 220 MB |
| `carbon_intensity` | 2018-01 onward, monthly | ~30 MB |
| `uk_police` | 2023-08 to 2026-07, Leicester monthly | ~22 MB |

Expected added raw volume is approximately **40.2 GB** and warehouse growth
approximately another **40 GB** because `_payload` and RAW rows are retained.
This is well below the current local free space (~1.7 TB), but loader throughput
is the operational bottleneck: 400 files per scheduled load pass. Let the
service drain continuously rather than manually loading the same files.

The two probes already committed (one unit each for carbon, GLOBE and Malaysia)
are intentionally treated as completed by the resume logic. If a probe is not
representative, remove its sink file and ledger row before rerunning; RAW is
insert-only and the loader does not deduplicate `(source, id)`.

Sources already emitting their complete available history on every scheduled
run do not need a separate backfill (for example `nsidc_sea_ice`,
`smithsonian_volcanism`, `noaa_catch_quotas`, and `netflix_top10`). GOV.UK
prison-estate historical slugs before 2026 returned 404 and were not guessed.
TB-scale archives (`gh_archive`, `wspr_live`, full `stac_imagery`,
`sensor_community`) remain intentionally unconfigured; estimate and get an
object-store decision before attempting them.
