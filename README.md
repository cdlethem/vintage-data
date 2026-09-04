# vintage-data

A production-shaped data platform monorepo. It polls public APIs on schedules,
lands raw newline-delimited JSON, and is designed to grow across the data lifecycle:

- **extract** (live): source → raw NDJSON files, orchestrated by Airflow
- **load** (live): raw files → a warehouse RAW schema, through a single-writer service
- **transform** (planned): dbt models over the warehouse

The point of the project is pedagogical: aspiring data engineers rarely have access to
real, frequently-changing data sources to practice on. Everything here is free, keyless,
and polls often enough to produce interesting time series within days.

## Architecture

```
extract/sources/*.yml                  one config per scheduled source instance
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
~/.local/share/vintage-data/extract/raw/source=<name>/dt=YYYY-MM-DD/<name>_<ts>.ndjson  (+ .meta.json)
        │
        ▼  scanned by
orchestration/dags/load_dag.py         one DAG, submits a job and waits
        │
        ▼  through a filesystem queue, claimed serially by
extract-loader.service                 the single writer (load/loader/)
        │                              schema inferred, never user-supplied
        ▼
~/.local/share/vintage-data/warehouse/extract.duckdb    raw.<source> (insert-only) + _load.* ledger
```

Airflow 3.3 runs natively under systemd: api-server, scheduler, dag-processor,
and N Celery workers backed by Postgres (metadata) and Redis (broker). Host,
port and worker count come from `orchestration/config.env`; the UI binds
loopback only unless you publish it deliberately.

## The extract contract

Every script in `extract/scripts/` is a self-contained, stdlib-only Python
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

1. Write (or promote) a `fetch_<source>.py` into `extract/scripts/` that honors
   the contract above.
2. Add an `extract/sources/<name>.yml` naming the script, its positional args,
   and a cron schedule.

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
load/bin/loader status -v            # service, queue, sink, per-table rows
load/bin/loader inspect <source>     # inferred schema, writes nothing
load/bin/loader sql --utc "SELECT ..."   # read-only query
```

[`load/README.md`](load/README.md) has the full design, a
step-by-step of what one job does, and a troubleshooting section — start there when a
`load__raw` run goes red, and note that its first failure mode ("heartbeat missing") is
about the service being down, not about data.

## Configuration

Everything specific to one machine — paths, service account, ports, model
choice, contact details — lives in **two gitignored files**, both copied from a
tracked example. No tracked file contains a home directory, a username, or a
vendor.

| file | copy from | holds |
|---|---|---|
| `orchestration/config.env` | `orchestration/config.example.env` | service user, data root, warehouse path, ports, worker count, user agent |
| `bots/models.yml` | `bots/models.example.yml` | which model each bot alias resolves to |
| `orchestration/airflow.secrets.env` | generated by `setup/02_database.sh` | DB password, JWT secret, any model API keys |

`orchestration/setup/render_config.sh` renders `orchestration/airflow.env` and
every systemd unit from `config.env` plus the tracked `*.template` files, so the
checkout location is discovered, never configured. `render_config.sh --check`
fails if a generated file is stale. Editing a rendered file by hand is pointless
— re-render instead.

```bash
cp orchestration/config.example.env orchestration/config.env
$EDITOR orchestration/config.env          # service user, data root, ports
cp bots/models.example.yml bots/models.yml
$EDITOR bots/models.yml                   # OpenRouter, a local server, or a CLI
orchestration/setup/setup.sh              # idempotent; rerun any time
```

Step 01 installs Postgres and Redis with `apt`; on any other platform install
them yourself and run with `SKIP_SYSTEM_PACKAGES=1`. Nothing else assumes a
distribution. `PUBLISH_TAILSCALE=1` optionally publishes the UI to a tailnet
(`tailscale serve`); by default the UI binds loopback only.

## Operations

| unit | role |
|---|---|
| `airflow-api-server` | UI + REST + Execution API on `$AIRFLOW_API_HOST:$AIRFLOW_API_PORT` |
| `airflow-scheduler` | schedules task instances |
| `airflow-dag-processor` | parses DAG files (separate process in Airflow 3) |
| `airflow-worker@1..N` | Celery workers (`AIRFLOW_WORKERS`, concurrency `AIRFLOW_WORKER_CONCURRENCY`) |
| `extract-loader` | the load layer's single writer; owns the warehouse connection |

All units restart on failure (`Restart=on-failure`) and start on boot, ordered after
Postgres and Redis. Logs: `journalctl -u airflow-scheduler -f` etc.; task logs are in
the UI. The loader is the one component whose own log is the primary record —
`journalctl -u extract-loader -f` shows every file it inserts and every schema change it
makes, and `load/bin/loader status -v` summarises the warehouse without
needing Airflow at all. The admin login is `admin`; the password is generated at first
api-server start into
`orchestration/airflow_home/simple_auth_manager_passwords.json.generated`.

## Repository layout

The top level is organized by independently evolving platform capability, not by
one pipeline's execution order:

```
extract/          source contracts, fetchers, source configs, and catalogs
load/             destination-agnostic warehouse loader and its configuration
transform/        dbt project (planned)
discovery/        pre-production source research, evidence, and staged fetchers
bots/             recurring agent runs (definitions, prompts, model providers)
orchestration/    Airflow deployment, cross-phase DAGs, runners, and sink adapters
monitoring/       deterministic health analysis of landed data
tools/            repository-level operator and development utilities
```

Lifecycle phases remain siblings so each can own its dependencies and release
surface. Cross-cutting concerns stay outside those phase directories.
`orchestration/dags/` is therefore the home for both phase-specific DAG factories
and future end-to-end DAGs that order extract, load, and dbt tasks.

Add later capabilities as peers when they become real—for example `analysis/`,
`apps/`, `apis/`, or `mcp/`—rather than nesting them under `transform/` or
`orchestration/`. Create a shared package only after code has multiple consumers;
do not make a phase directory the accidental owner of a shared contract.

Raw data lands **outside the repo** at `$EXTRACT_DATA_ROOT`
(default `~/.local/share/vintage-data/extract`), hive-partitioned (`source=<name>/dt=<date>`) so
DuckDB/dbt/Spark can discover it directly. Three sibling trees live there and never
touch each other: `raw/` (the sink, and the only one the load layer reads), `state/`
(per-source watermarks) and `_load_queue/` (load jobs). The warehouse itself is outside
both, at `$EXTRACT_WAREHOUSE` (default `~/.local/share/vintage-data/warehouse/extract.duckdb`).

## Bots

Recurring **agent** work is a first-class layer, not a cron line hidden in a
shell script. A bot is a directory (`bots/<name>/bot.yml` + `prompt.md`) that
the same Airflow scheduler runs as `bot__<name>`, and **which model answers is
configuration, not code**: a bot names a model *alias*, and
`bots/models.yml` (gitignored, per-machine) maps aliases onto OpenRouter, OpenAI,
Anthropic, a local llama-server/vLLM/Ollama, or any CLI agent.

```bash
P=orchestration/.venv/bin/python
$P bots/bin/run_bot --list
$P bots/bin/run_bot pipeline_check --dry-run            # assembled prompt only
$P bots/bin/run_bot pipeline_check --model openrouter   # override the alias
```

Live today: `pipeline_check` (every 30 min) — `monitoring/digest.py --triage`
classifies every source deterministically and the model is asked only about
flagged ones, so a healthy pipeline costs zero tokens.
[`bots/README.md`](bots/README.md) has the definition format, the provider
contract, and the two planned bots (script staging, schedule tuning).

## Swapping infrastructure

Local DuckDB and a local filesystem are the *development* choices; each sits
behind a seam, and this is honestly what each swap costs:

| choice | seam | cost of swapping |
|---|---|---|
| **warehouse** (DuckDB → BigQuery/Snowflake/Postgres) | `load/loader/destinations/` — `Destination` ABC + `REGISTRY`, selected by `destination:` in `load/config/load.yml` | one subclass (type mapping, "NDJSON file → relation of JSON records", and a single-transaction `load_file`) plus one registry entry. Nothing above the destination layer — discovery, inference, queue, service loop, ledger semantics — changes. `load/config/load.yml` already carries sketch configs for both. |
| **model provider** (local → OpenRouter/OpenAI/Anthropic/CLI) | `bots/providers.py` + `bots/models.yml` | already zero-code: edit one alias. A new wire protocol is one function plus a `REGISTRY` entry. |
| **sink** (local files → S3/GCS/Azure) | `orchestration/include/sinks.py` — `Sink` ABC + `REGISTRY`, selected by `EXTRACT_SINK` or a source's `sink:` key | **two changes, not one.** The write path is a `Sink` subclass whose `commit()` publishes a staged object. The read path is the honest gap: `load/loader/discovery.py` lists a local directory and `SinkFile.path` is a `pathlib.Path` that `duckdb_dest` hands to `read_json_auto`. Object storage needs discovery to become a listing interface (prefix listing) and file identity to become a URI. Both are contained — one module each — but neither exists yet, and the sink registry alone will not get you there. |
| **metadata DB / broker** | `POSTGRES_*` and `CELERY_BROKER_URL` in `config.env`; the connection string in `airflow.secrets.env` | point them at a managed Postgres/Redis and skip `setup/02_database.sh`; nothing else reads those values. |
| **secrets** | `orchestration/airflow.secrets.env`, loaded by every unit | replace with Airflow's secrets backend or a systemd credential drop-in; no code reads secrets directly, only env vars. |

The rule the repo tries to hold: a deployment choice belongs in a config file
with one implementation per choice behind a registry, and anything a *machine*
decides (paths, accounts, ports, model, contact address) never enters git.
