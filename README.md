# extract

A local, production-shaped data pipeline monorepo. It polls public APIs on schedules,
lands raw newline-delimited JSON, and is built to grow into a full ELT stack:

- **extract** (this stage, live): source → raw NDJSON files, orchestrated by Airflow
- **load** (planned): raw files → data warehouse
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
```

Airflow 3.3 runs natively under systemd: api-server (UI on :8081), scheduler,
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

## Operations

Bring-up from a clean box (idempotent, rerunnable):

```bash
orchestration/setup/setup.sh      # or run the numbered steps individually
```

| unit | role |
|---|---|
| `airflow-api-server` | UI + REST + Execution API on 127.0.0.1:8081 |
| `airflow-scheduler` | schedules task instances |
| `airflow-dag-processor` | parses DAG files (separate process in Airflow 3) |
| `airflow-worker@1`, `@2` | Celery workers (concurrency 4 each) |

All units restart on failure (`Restart=on-failure`) and start on boot, ordered after
Postgres and Redis. Logs: `journalctl -u airflow-scheduler -f` etc.; task logs are in
the UI. The admin login is `admin`; the password is generated at first api-server start
into `orchestration/airflow_home/simple_auth_manager_passwords.json.generated`.

Web UI over the tailnet: `https://<host>.<tailnet>.ts.net` (via `tailscale serve`;
the server itself binds only 127.0.0.1).

## Repo layout

```
orchestration/    Airflow deployment: DAG factory, runner, sinks, env config,
                  systemd units, clean-box setup scripts
pipelines/
  extract/        scripts/ (the fetchers) + sources/ (per-instance yml configs)
  load/           placeholder — raw files → warehouse
  transform/      placeholder — dbt project
```

Raw data lands **outside the repo** at `$EXTRACT_DATA_ROOT`
(default `~/dev/data/extract`), hive-partitioned (`source=<name>/dt=<date>`) so
DuckDB/dbt/Spark can discover it directly.
