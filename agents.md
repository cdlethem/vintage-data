# Project guidance

Public, keyless data sources flow through Airflow extract DAGs → raw NDJSON →
the loader → DuckDB → dbt marts → optional Lightdash/Postgres serving.
Keep lifecycle capabilities as sibling directories; cross-phase runners and DAGs
belong in `orchestration/`.

## Read only what the task needs

- [load/README.md](load/README.md): warehouse, schema inference, cadence, holds,
  and loader troubleshooting.
- [transform/README.md](transform/README.md): dbt contracts, incremental models,
  source generation, validation, and build operations.
- [visualization/README.md](visualization/README.md) and
  [visualization/TRENDS.md](visualization/TRENDS.md): Lightdash operations and
  required dashboard analysis standards.
- [bots/README.md](bots/README.md): provider dashboard authority, task admission,
  confined execution, review, and model configuration.
- [monitoring/README.md](monitoring/README.md): source health and run evidence.

Treat current code/configuration as evidence of behavior. Recompute source counts,
scheduling backlogs, and operational status; do not preserve snapshots here.

## Environment and operations

- Machine settings live in ignored `orchestration/config.env`; secrets live in
  `orchestration/airflow.secrets.env`. Change templates/config and run
  `orchestration/setup/render_config.sh`; never hand-edit generated `airflow.env`
  or systemd units. Keep credentials out of logs, commits, and bot contexts.
- Read `EXTRACT_DATA_ROOT`, `EXTRACT_WAREHOUSE`, and `EXTRACT_LOAD_QUEUE` from
  the deployment environment. Raw files, watermarks, queues, and serving state
  live outside the checkout; do not hardcode this machine's paths or worker count.
- Use component wrappers/venvs: `load/bin/loader`, `transform/bin/dbt`,
  `visualization/bin/viz`, and `orchestration/.venv/bin/python` for Airflow code.
- Diagnose DAG imports in the `airflow-dag-processor` journal and individual runs
  in Airflow task logs and raw `.meta.json`/`.fail.json` sidecars. Healthy sibling
  DAGs do not prove every source config parsed.
- Loader code changes require an `extract-loader` service restart to take effect.

## Extract and source changes

- Fetchers emit NDJSON with `source`, UTC `fetched_at` stamped once per run, and
  source-natural `id`. Diagnostics go to stderr. Exit 0 with empty stdout is valid.
  Preserve structured `VINTAGE_RUN_SUMMARY` evidence for partial/failed runs.
- Promote by copying from `discovery/staged_scripts/` into `extract/scripts/`;
  retain the staged original. Determine backlog from YAML `script` references,
  not from files already present in `extract/scripts/`.
- Add one `extract/sources/<name>.yml` per instance, with a unique name, literal
  CLI-token `args`, five-field UTC cron, and rate/licence/attribution metadata.
  Poll no faster than every five minutes and stagger offsets. Use existing
  factory conventions (`dag_factory` for grouped providers), not source-specific
  branches in `extract_dags.py`. `enabled` sets initial pause state only.
- Verify the configured invocation and DAG imports. Stream/sample large outputs;
  reproduce failures without hiding them behind retries or arbitrary record caps.
- Stateful fetchers support `--state-file` and `--no-state`. Use temporary state
  for debugging; deleting production state re-baselines the source. Commit state
  atomically after output, preserve timestamp-boundary IDs where needed, and use
  resumable cursors for bounded runs. Verify replay/gap behavior, not just a quiet
  second run.
- Job-board companies belong in `extract/catalogs/job_boards.json`; shared ATS
  adapters live in `extract/scripts/job_boards_lib/`. Live-verify candidate tokens
  with `tools/verify_job_boards.py`; a 429 is unknown, not evidence to delete a board.

## Data integrity

- Submit raw-load jobs with `load/bin/loader submit --wait`; never run `run-once`
  alongside the loader service. Production transforms use the coordinated runner.
- RAW is insert-only with no cross-run deduplication. File rows and ledger entries
  commit together; deleting only a ledger entry duplicates data on reload.
  Schema is inferred and evolves additively; all-null values are not type evidence.
- Never mutate held artifacts. Inspect with `load/bin/loader candidates`; release
  through `load/bin/loader release` with hash-verified evidence and an audit trail.
- Automatic cadence is opt-in through source YAML bounds. Compare bounded recent
  scheduled batches with volatile fields excluded; missing data and backfills must
  not drive slowdown. Airflow reads the published plan, not the warehouse.
- Inspect historical scope with `python3 tools/backfill.py --plan` before running
  a backfill; estimate storage and upstream cost from current configuration.

## Transform, visualization, and bots

- Develop with `--target dev`. Only base views call `source()`; governed `fct_*`
  models are contracted tables/incrementals with declared grain and exactly one
  cadence tag. Use `sync_raw_sources` for generated source/base declarations.
- Validate dbt changes with `transform/bin/dbt parse --target dev`,
  `transform/bin/validate_project transform/target/manifest.json`, and a targeted
  dev build including ancestors (`--select +<model>`). Data tests belong on
  physical models only; bound large checks to recent/new data.
- Keep model SQL in DuckDB dialect. Lightdash uses generated Postgres serving
  metadata and immutable snapshots; retry publication from its existing batch.
  Serving schema changes need explicit migration. Use the visualization docs for
  content validation and query checks; follow `TRENDS.md` for analysis changes.
- For the bot runtime, provider dashboard state is authoritative. Preserve task
  admission, credential-free confined execution, trusted artifact/PR publication,
  and advisory review boundaries. Bots do not receive production Lightdash
  credentials or Docker access. These runtime roles do not restrict ordinary
  user-authorized development in this checkout.
