# load

Raw NDJSON files → a warehouse `RAW` schema, insert-only, with row-level lineage.

The extract stage lands one NDJSON file per run in the sink. This stage moves those
files into a warehouse without asking anyone to write a schema, and does it through a
**single writer**: exactly one process ever holds the destination connection, and it
applies jobs one at a time.

```
$EXTRACT_DATA_ROOT/raw/source=<name>/dt=<date>/<name>_<ts>.ndjson     the sink
        │
        │  Airflow DAG (load__raw) submits a job — it never opens the warehouse
        ▼
$EXTRACT_LOAD_QUEUE/{pending,running,done}/*.json                     the queue
        │
        ▼  claimed one at a time by
extract-loader.service  →  loader.service.LoaderService                the writer
        │                    scan → diff vs ledger → infer schema → insert
        ▼
raw.<source>       insert-only tables, lineage columns on every row
_load.files        the ledger: which sink file produced which rows
_load.jobs         every load job, with counts and timings
_load.schema_changes   every column added or widened, and when
```

## Why a service and a queue

DuckDB takes an exclusive file lock: one writer, or readers, never both. Rather than
fight that, the design leans on it — the single-writer service *is* the concurrency
model, and it is the same model that keeps a real warehouse from having twenty Airflow
workers racing on DDL. Airflow submits a job file and waits for a result file, so:

* the Airflow venv needs no warehouse driver and no credentials;
* two concurrent DAG runs cannot corrupt anything — the second job simply queues;
* swapping DuckDB for BigQuery changes one class and one line of yml.

The service closes the connection after `service.idle_release_s` of quiet, so you can
open the warehouse yourself between loads.

## Automatic schema detection

No source needs a schema. For each batch the loader samples records (`sample_lines`
per file), infers a **canonical** type per top-level key, and asks the destination to
map those to native DDL:

| canonical | DuckDB | inferred from |
|---|---|---|
| `string` | `VARCHAR` | strings, and any key with conflicting types |
| `integer` | `BIGINT` | JSON ints |
| `double` | `DOUBLE` | JSON floats, or ints + floats together |
| `boolean` | `BOOLEAN` | JSON booleans |
| `date` / `timestamp` | `DATE` / `TIMESTAMPTZ` | ISO-8601 strings (`detect_temporal`) |
| `json` | `JSON` | nested objects and arrays |

Three rules make that safe to run unattended:

1. **Nothing is lost.** Every row also keeps the verbatim record in `_payload`, so a
   key that was absent from the sample is still recoverable downstream.
2. **Schemas only grow.** A new key becomes a new column; a key whose type outgrew its
   column widens in place (`integer → double → string`). Columns are never dropped or
   narrowed, so history stays readable.
3. **A bad value never fails a batch.** Every cast is a try-cast: a value that doesn't
   fit its column lands as `NULL`, with the truth still in `_payload`.
4. **All-null is not evidence.** A key the sample only ever saw as `null` is *tentative*:
   it doesn't create a column and it never widens one. Without this, a chatty source's
   quiet hour would pin a good `BIGINT` column to `VARCHAR` — which is exactly what the
   first backfill of `hackernews.score` did before the rule existed.

Widening is recorded in `_load.schema_changes`. Where a warehouse can't alter a column
type in place (BigQuery), `Destination.widen_column` returns `False`, the old column
stays, and the values arrive as `NULL` — visible in `schema_changes` as
`widen_skipped`.

Check what would be inferred, without writing anything:

```bash
pipelines/load/bin/loader inspect gbfs_citibike
```

## Lineage columns

Every RAW row carries where it came from and when it arrived:

| column | meaning |
|---|---|
| `_row_id` | `md5(source_file:line_number)` — stable identity for a physical record |
| `_source` | sink source name (= the extract yml `name`, = the table name) |
| `_batch_id` | the extract run: the NDJSON file's stem, e.g. `gbfs_divvy_20260904T061500Z` |
| `_source_file` | absolute path of the file the row came from |
| `_file_row_num` | 1-based line number within that file |
| `_dt` | sink partition date |
| `_extract_started_at` | when the extract run started (from its `.meta.json` manifest) |
| `_load_id` | the load job that inserted the row → joins `_load.jobs` |
| `_loaded_at` | insert time |
| `_content_hash` | `md5` of the normalized record — the handle for `(source, id)` dedupe |
| `_payload` | the original record, verbatim |

So a single row traces back to its batch, its file, its extract run and its load job:

```sql
SELECT r._row_id, r._batch_id, f.rows_loaded, f.status, j.started_at AS load_started
FROM raw.gbfs_divvy r
JOIN _load.files f ON f.path = r._source_file
JOIN _load.jobs  j ON j.load_id = r._load_id
LIMIT 5;
```

## Insert-only, and why backfill is not a special case

RAW is append-only: the loader never updates or deletes a row. What makes reruns safe
is the **ledger** — `_load.files` records every file it has loaded, in the same
transaction as the rows themselves. A job's work is therefore always "every sink file
the ledger has not seen", which means the first run (a few thousand files) and the
15-minute incremental run (a handful) are literally the same code path.

A file that fails is recorded with `status = 'failed'` and an attempt count; it is
retried on later runs until `max_attempts`, then left alone so one poison file can't
make every run red.

Dedupe on `(source, id)` is deliberately *not* done here — RAW keeps every observation,
including re-fetched overlaps, and the transform stage decides what "current" means.

## Configuration

Everything lives in [`config/load.yml`](config/load.yml) — destination, paths, DAG
schedule, and the inference defaults. Per-source overrides go under `sources:` and are
merged over `defaults:`; a source with no block is loaded on pure defaults, which is
how 164 sources work with an empty `sources:` map.

```yaml
sources:
  imdb_ratings:
    sample_lines: 2000              # wide records, worth a deeper sample
  openactive_feeds:
    schema_detection: payload_only  # shape varies per publisher; keep _payload only
  noisy_source:
    enabled: false
```

`${VAR:-default}` interpolation means the same file works under systemd, in Airflow and
from a shell. The paths come from `orchestration/airflow.env`.

## Changing destination

Everything above `loader/destinations/` is warehouse-agnostic. A new destination is one
subclass of `Destination` — map the canonical types, say how an NDJSON file becomes a
relation of JSON records, implement the ledger writes — plus one entry in
`destinations/REGISTRY` and one line in the yml. The discovery, inference, queue,
service loop, DAG and lineage columns do not change.

## What one job actually does

Worth knowing before debugging anything, because most confusing behaviour is one of
these steps doing exactly what it should:

1. **Claim.** The service renames the oldest `pending/*.json` into `running/` — the
   rename *is* the claim. One job at a time, always.
2. **Connect.** Opens the warehouse, retrying while another process holds the file lock
   (up to `lock_timeout_s`), then ensures the `raw` / `_load` schemas exist.
3. **Discover.** Walks the sink for `source=*/dt=*/*.ndjson`. Files younger than
   `min_age_s` **with no `.meta.json` yet** are skipped — they may still be committing.
4. **Diff.** Reads the whole ledger and drops files that are already `loaded`, that have
   failed `max_attempts` times, or whose source is `enabled: false`. Whatever is left is
   sorted **oldest first** and capped at `max_files`, so a truncated run always leaves
   the newest data for the next one.
5. **Infer, per source.** Samples `sample_lines` records from each of that source's
   files in the batch, unions the observations, and syncs the table (create / add
   column / widen), logging each change to `_load.schema_changes`.
6. **Insert, per file.** One `INSERT … SELECT … FROM read_ndjson_objects(file)` per
   file, with the ledger row written **in the same transaction**. A file is therefore
   either fully loaded and recorded, or neither — which is what makes every retry safe.
7. **Publish.** Writes `done/<job_id>.json` and deletes the `running/` file. The waiting
   Airflow task picks that result up and logs it.

Row order matters for `_row_id`: `row_number()` matches the physical line number because
DuckDB reads a file in insertion order by default. A destination that parallelises file
reads would need its own line-number source.

## Operating it

```bash
# all CLI commands work from anywhere; the wrapper sources orchestration/airflow.env
# so it always points at the same warehouse and queue as the service
pipelines/load/bin/loader status -v      # service, queue, sink, per-table rows
pipelines/load/bin/loader submit --wait  # one incremental pass
pipelines/load/bin/loader backfill       # everything the ledger hasn't seen (uncapped)
pipelines/load/bin/loader inspect gbfs_citibike   # inferred schema, writes nothing
pipelines/load/bin/loader run-once --sources gbfs_divvy --max-files 5   # no service

# query it read-only; the service releases the lock after service.idle_release_s
pipelines/load/bin/loader sql --utc "SELECT count(*) FROM raw.gbfs_divvy"
pipelines/load/bin/loader sql - < some_query.sql

# the service
systemctl status extract-loader
journalctl -u extract-loader -f
```

`loader sql` exists because the standalone `duckdb` CLI is a separate download while
this venv already has the driver — if you do install the CLI, `duckdb -readonly
~/dev/data/warehouse/extract.duckdb` is equivalent. `--utc` matters more than it looks:
DuckDB renders `TIMESTAMPTZ` in the session timezone by default, so without it
`_loaded_at` won't line up with the sink's ISO strings.

A newly created `load__raw` DAG is **paused**, because Airflow pauses DAGs at creation by
default. Unpause it once (`airflow dags unpause load__raw`, or the UI toggle); the yml's
`schedule` has nothing to do with it.

## Troubleshooting

### The DAG task failed

The three failures the task raises are all specific, and only one of them is about data:

| message | meaning | first move |
|---|---|---|
| `loader service heartbeat is missing / Ns old` | the writer is down or wedged; nothing was attempted | `systemctl status extract-loader`, `journalctl -u extract-loader -n 50` |
| `load job … did not finish within Ns` | the job is still queued or running — a very large file, or a lock it can't get | `ls $EXTRACT_LOAD_QUEUE/running`, then the journal |
| `loaded N file(s) but M failed` | per-file errors; the rest of the batch did land | query `_load.files` below |

The full job result is in three places: the Airflow task log (pretty-printed),
`$EXTRACT_LOAD_QUEUE/done/<job_id>.json` (7 days), and `_load.jobs` (forever).

### Loads stall, or `status` says the warehouse is unreadable

DuckDB's lock is exclusive — one writer *or* readers, never both — so the two processes
block each other and both say so plainly:

* journal shows `warehouse locked by another process; retrying in 0.5s` → something else
  holds the file, almost always a reader someone left open. DuckDB's underlying error
  names the culprit exactly — `Conflicting lock is held in /path/to/python (PID 12345)
  by user colin`. After `lock_timeout_s` (120s) the job fails, having written nothing;
  close the reader and the next run recovers on its own.
* `loader status` / `loader sql` print `warehouse unreadable right now (…)` after about
  20s → the service is mid-job. Read-only commands wait only briefly on purpose: "the
  writer is busy" is itself the answer. Try again after `service.idle_release_s`.

This is also why `run-once` must not be used while the service is up: it will sit in
`connect()` retrying the lock. Use `submit --wait`.

### A file has a malformed line

One unparseable line fails its whole file by default: that is a broken fetcher,
and it should be loud rather than silently short a few rows. The file is retried
until `max_attempts`, after which it is no longer picked up — and because those
rows are simply missing from RAW, the job result now reports `files_abandoned`
separately from `files_skipped`, and the service logs a warning naming the files.
Watch for it:

```sql
SELECT path, attempts, error FROM _load.files
WHERE status = 'failed' AND attempts >= 3;
```

If the source is upstream junk you cannot fix, set `on_malformed_lines: skip` for
it. The file then loads, and each bad line still occupies its line number as a row
with a NULL `_payload` — so `_file_row_num` keeps matching the file and
`WHERE _payload IS NULL` finds exactly what was lost.

### A file failed

```sql
SELECT path, source, attempts, error, loaded_at
FROM _load.files WHERE status = 'failed' ORDER BY loaded_at DESC;
```

It is retried on every later run until `attempts` reaches `max_attempts` (3), then it
stops being picked up — one poison file can't make every run red. After fixing the
cause, forget the file so it looks new again (the writer must be stopped, since this is
a write):

```bash
systemctl stop extract-loader   # deleting is a write, so the writer must be stopped
pipelines/load/.venv/bin/python -c "import duckdb, sys; duckdb.connect(sys.argv[1]).execute(
    'DELETE FROM _load.files WHERE path = ?', [sys.argv[2]])" \
  ~/dev/data/warehouse/extract.duckdb '/…/source=x/dt=…/x_….ndjson'
systemctl start extract-loader
```

(`loader sql` is deliberately read-only, so it cannot do this.)

### Re-loading a file that already loaded

RAW is insert-only and the loader will not deduplicate for you, so you must remove
**both** the rows and the ledger entry, or you get a second copy:

```sql
DELETE FROM raw.<source>  WHERE _source_file = '<path>';
DELETE FROM _load.files   WHERE path         = '<path>';
```

`_row_id` is deterministic (`md5(file:line)`), so if you do end up with duplicates they
are easy to find: `GROUP BY _row_id HAVING count(*) > 1`.

### Nothing is loading, but there are new files

Compare the counters in the job result — they account for every file:

* `files_skipped` — already in the ledger, out of attempts, or the source is
  `enabled: false`
* `files_seen` minus everything else — younger than `min_age_s` with no manifest yet;
  they load on the next run
* `files_pending_after: true` — `max_files` cut the batch short; the next run continues

### A column isn't what I expected

Start with `bin/loader inspect <source>`, which shows the inference result without
touching the warehouse, then `_load.schema_changes` for when the live table last moved:

```sql
SELECT changed_at, table_name, column_name, change, from_type, to_type
FROM _load.schema_changes WHERE change <> 'create' ORDER BY changed_at DESC;
```

* **`VARCHAR` where you expected a number** — the key had conflicting types in the
  sample, so it widened. Real example from this repo: `queensland_hospital.patients_waiting`
  is an integer except for the 27 rows where the publisher sends `"-"`.
* **The column doesn't exist** — the key was `null` in every sampled record (*tentative*:
  no basis to pick a type, so nothing is created), or it only ever appears nested, or it
  showed up after the sample window. It is still in `_payload`; raise `sample_lines` for
  that source if you want it promoted sooner.
* **The name differs from the JSON key** — names are lowercased, non-alphanumerics
  become `_`, a leading `_` is dropped (that prefix is reserved for lineage columns), and
  a collision gets `_2`. The original key is recoverable from `_payload`.
* **Everything is `VARCHAR`/`JSON` and nothing is typed** — the source is set to
  `schema_detection: payload_only`.

### Timestamps look shifted

DuckDB renders `TIMESTAMPTZ` in the session timezone, not UTC. `SET timezone = 'UTC';`
before querying, or read `_payload` for the string the source actually sent.

### The service was restarted mid-job

Nothing to repair. A job left in `running/` is moved back to `pending/` at startup
(`requeued N job(s) orphaned by a restart` in the journal), and because every loaded file
was committed with its ledger row, the requeued job simply skips what already landed.
`systemctl stop` is graceful: the service finishes the file in flight first, which is why
the unit allows a 10-minute `TimeoutStopSec`.

### The warehouse is bigger than expected

`_payload` keeps a verbatim copy of every record, which roughly doubles a source's
footprint. For a big source that re-lands the same bulk file every run, set
`keep_payload: false` (drops the column) or `schema_detection: payload_only` (keeps only
the payload and the envelope) in its `sources:` block. Note that DuckDB does not return
freed space to the filesystem, so an existing file won't shrink.
