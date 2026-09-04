# extract

A local, production-shaped data pipeline monorepo. It polls public APIs on schedules,
lands raw newline-delimited JSON, and is built to grow into a full ELT stack:

- **extract** (live): source → raw NDJSON files, orchestrated by Airflow
- **load** (live): raw files → a warehouse RAW schema, through a single-writer service
- **transform** (planned): dbt models over the warehouse

The point of the project is pedagogical: aspiring data engineers rarely have access to
real, frequently-changing data sources to practice on. Everything here is free, keyless,
and polls often enough to produce interesting time series within days.

## Architecture

```
pipelines/extract/sources/*.yml        one config per scheduled source instance
        │
        ▼  parsed by
orchestration/dags/extract_dags.py     DAG factory: one Airflow DAG per yml
        │
        ▼  each DAG runs
orchestration/include/extract_runner.py    subprocess: python fetch_X.py [args...]
        │                                  stdout = NDJSON records
        ▼  written through
orchestration/include/sinks.py         Sink interface (LocalSink today; S3/GCS later)
        │
        ▼
~/dev/data/extract/raw/source=<name>/dt=YYYY-MM-DD/<name>_<ts>.ndjson  (+ .meta.json)
        │
        ▼  scanned by
orchestration/dags/load_dag.py         one DAG, submits a job and waits
        │
        ▼  through a filesystem queue, claimed serially by
extract-loader.service                 the single writer (pipelines/load/loader/)
        │                              schema inferred, never user-supplied
        ▼
~/dev/data/warehouse/extract.duckdb    raw.<source> (insert-only) + _load.* ledger
```

Airflow 3.3 runs natively under systemd: api-server (UI on :8082), scheduler,
dag-processor, and two Celery workers backed by Postgres (metadata) and Redis (broker).
The UI is reachable only via localhost and `tailscale serve`.

## The extract contract

Every script in `pipelines/extract/scripts/` is a self-contained, stdlib-only Python
file with one behavior:

```
python fetch_<source>.py [positional args...]   →   NDJSON records on stdout
```

Each record carries the envelope `source`, `fetched_at` (UTC ISO 8601, stamped once per
run), and `id` (source-natural key). stderr is for attribution notices and warnings.
A non-zero exit is a failure; **exit 0 with empty stdout is a valid empty result**
(a 404 from many of these APIs means "no matches", not an error).

Scripts carry no state, write no files, and know nothing about scheduling or storage —
those concerns live entirely in the orchestration layer. Deduplication across runs on
`(source, id)` is deliberately deferred to the load stage.

## Adding a source

1. Write (or promote) a `fetch_<source>.py` into `pipelines/extract/scripts/` that
   honors the contract above.
2. Add a `pipelines/extract/sources/<name>.yml` naming the script, its positional
   args, and a cron schedule.

That's it — no new DAG file. The factory picks the yml up on the next dag-processor
parse (~1 min). A malformed yml skips only itself: the error lands in the
dag-processor log and every other DAG is unaffected.

One script can back many scheduled instances: `gbfs_divvy.yml` and `gbfs_citibike.yml`
both point at `fetch_gbfs.py` with different args. Prefer parameterizing an existing
protocol client over writing a new script when the API is the same shape.

Note one Airflow nuance: `enabled: false` in a yml only pauses a DAG **on first
creation**. Flipping it later does not re-pause an already-created DAG — pause it in
the UI or with `airflow dags pause`.

## Scheduling philosophy

Schedules are hand-set from each source's observed cadence and documented rate limits
(carried as comments in each yml), staggered across minutes so runs don't herd.
Conservative floors: nothing polls faster than every 5 minutes.

There are no Airflow pools: pools throttle *concurrency*, but every constraint here is
a *rate*, and the cron schedule is the rate limiter. Each source is its own DAG with
`max_active_runs=1`, so per-API concurrency is already ≤ 1. A pool becomes worth adding
only when two DAGs share one rate-limited backend.

Future direction: each run writes a `.meta.json` manifest including its record count —
the raw material for tuning polling frequency algorithmically with "did I pick up new
data?" as the objective. Answering that properly needs `(source, id)` dedupe, which
belongs to the load stage.

## The load stage

One DAG (`load__raw`) scans the sink every 15 minutes and hands the new files to a
single dedicated writer process — the DAG itself never opens the warehouse. Work
crosses between them as job files in `$EXTRACT_LOAD_QUEUE`, so Airflow needs no
warehouse driver and concurrent runs cannot race on DDL.

The writer infers each source's schema from a sample of its records (no schema is ever
user-supplied), evolves the table additively when a source's shape changes, and inserts
— never updates. Every row carries lineage columns (`_batch_id`, `_source_file`,
`_file_row_num`, `_load_id`, `_loaded_at`, `_content_hash`, `_payload`) so any value
traces back to the extract run that produced it and the load job that landed it.

Because "what to load" is answered by a ledger inside the warehouse itself
(`_load.files`), backfill is not a special mode: a job always loads every sink file the
ledger hasn't seen. Swapping DuckDB for BigQuery or Snowflake is one `Destination`
subclass and one line of yml.

```bash
pipelines/load/bin/loader status -v            # service, queue, sink, per-table rows
pipelines/load/bin/loader inspect <source>     # inferred schema, writes nothing
pipelines/load/bin/loader sql --utc "SELECT ..."   # read-only query
```

[`pipelines/load/README.md`](pipelines/load/README.md) has the full design, a
step-by-step of what one job does, and a troubleshooting section — start there when a
`load__raw` run goes red, and note that its first failure mode ("heartbeat missing") is
about the service being down, not about data.

## Operations

Bring-up from a clean box (idempotent, rerunnable):

```bash
orchestration/setup/setup.sh      # or run the numbered steps individually
```

| unit | role |
|---|---|
| `airflow-api-server` | UI + REST + Execution API on 127.0.0.1:8082 |
| `airflow-scheduler` | schedules task instances |
| `airflow-dag-processor` | parses DAG files (separate process in Airflow 3) |
| `airflow-worker@1`, `@2` | Celery workers (concurrency 4 each) |
| `extract-loader` | the load layer's single writer; owns the warehouse connection |

All units restart on failure (`Restart=on-failure`) and start on boot, ordered after
Postgres and Redis. Logs: `journalctl -u airflow-scheduler -f` etc.; task logs are in
the UI. The loader is the one component whose own log is the primary record —
`journalctl -u extract-loader -f` shows every file it inserts and every schema change it
makes, and `pipelines/load/bin/loader status -v` summarises the warehouse without
needing Airflow at all. The admin login is `admin`; the password is generated at first api-server start
into `orchestration/airflow_home/simple_auth_manager_passwords.json.generated`.

Web UI over the tailnet: `https://<host>.<tailnet>.ts.net` (via `tailscale serve`;
the server itself binds only 127.0.0.1).

## Repo layout

```
orchestration/    Airflow deployment: DAG factory, runner, sinks, env config,
                  systemd units, clean-box setup scripts
pipelines/
  extract/        scripts/ (the fetchers) + sources/ (per-instance yml configs)
  load/           loader/ (destination-agnostic load layer) + config/load.yml
  transform/      placeholder — dbt project
```

Raw data lands **outside the repo** at `$EXTRACT_DATA_ROOT`
(default `~/dev/data/extract`), hive-partitioned (`source=<name>/dt=<date>`) so
DuckDB/dbt/Spark can discover it directly. Three sibling trees live there and never
touch each other: `raw/` (the sink, and the only one the load layer reads), `state/`
(per-source watermarks) and `_load_queue/` (load jobs). The warehouse itself is outside
both, at `$EXTRACT_WAREHOUSE` (default `~/dev/data/warehouse/extract.duckdb`).
