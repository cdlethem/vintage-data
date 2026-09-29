# transform

Production dbt-duckdb project over the insert-only `raw` schema in
`$EXTRACT_WAREHOUSE`. It preserves raw lineage, gives every raw relation a thin base
view, and exposes documented physical `fct_*` relations for analysis.

## Runtime

```bash
orchestration/setup/07_transform.sh
transform/bin/dbt debug --target dev
```

`transform/bin/dbt` is the only dbt entrypoint. It loads the rendered deployment
environment, uses the invoking checkout's project/profile files, and uses the main
checkout's `transform/.venv` when called inside a bot worktree.

The tracked profile has two targets:

- `dev` writes to `$DBT_DEV_WAREHOUSE` (default `transform/target/dev.duckdb`) and
  attaches `$EXTRACT_WAREHOUSE` read-only as catalog `extract`;
- `prod` opens `$EXTRACT_WAREHOUSE` directly and writes governed schemas there.

Both targets use `EXTRACT_WAREHOUSE_THREADS` and
`EXTRACT_WAREHOUSE_MEMORY_LIMIT`. Only Airflow or an explicit operator command should
use `--target prod`.

## Model contract

- `models/base/base_<source>.sql`: one thin **view** for every `raw.<source>`;
  only this layer may call `source()`.
- `models/staging/**/stg_*.sql`: optional views for multi-step or reused
  normalization.
- `models/marts/**/fct_<entity>.sql`: governed tables or incremental models.
- `fct_<entity>__daily|weekly`: optional physical aggregates when repeated analysis
  of atomic facts would be wasteful.

Production schemas are `transform_base`, `transform_staging`, and
`transform_marts`. Final facts must have explicit columns, complete descriptions and
data types, an enforced contract, `meta.grain`, at least one base ancestor, and exactly
one cadence tag: `twice_hourly`, `hourly`, or `daily`.

Use tables by default. Incremental models need a stable `unique_key`, a filter anchored
on append-only `_loaded_at` lineage, a replay overlap, and deterministic deduplication
before upsert. SCD2 facts expose `valid_from`, nullable `valid_to`, and `is_current`.
Keep enough complete periods in incremental daily/weekly aggregates to repair late
observations.

## Raw coverage and policy

```bash
transform/bin/sync_raw_sources --check
transform/bin/sync_raw_sources --check --json   # bot-safe drift report; exits 0 on drift
transform/bin/sync_raw_sources --write-all
transform/bin/dbt parse --target dev
transform/bin/validate_project transform/target/manifest.json
```

`sync_raw_sources` reads columns from `information_schema` and row/load metadata from
`_load.files`; it never counts raw tables. It rewrites the generated
`models/base/_raw_sources.yml` and generated base SQL while preserving a base file once
its generated marker is removed. The strict plain check exits nonzero on missing,
stale, or drifted coverage. Connection/query faults always fail.

`validate_project` enforces layering, naming, materialization, contracts,
documentation, cadence dependency order, and the physical-only test rule.

## Tests

A data test belongs only on a table/incremental model when its failure is plausible,
would allow bad downstream data to land silently, and should block the build. Bound
checks on large models to the new or recent window. Never attach a data test to a raw
source, base view, or staging view; doing so reprocesses the source and policy validation
rejects it.

## Scheduled builds

`jobs.yml` supplies schedules and limits for independent
`transform__<family>__<cadence>` DAGs. Families follow
`models/marts/<family>/`; a family has a DAG for each cadence used by its marts.

| Cadence | UTC schedule | family-scoped selector |
|---|---|---|
| `twice_hourly` | `7,37 * * * *` | selected family marts tagged `twice_hourly`, plus ancestors |
| `hourly` | `17 * * * *` | selected family marts tagged `hourly`, plus ancestors |
| `daily` | `47 3 * * *` | selected family marts tagged `daily`, plus ancestors |

Each run prepares a separate dbt project containing its dependency closure, parses
and policy-validates that scope, then builds. Unrelated SQL/YAML, base-view test,
or contract errors do not block healthy families. Violations inside a dependency
still fail the consuming family. Shared project configuration/macros remain shared
infrastructure; the strict whole-project validator remains available for review.
Invalid family definitions must remain visible as failures, not silently disappear
from scheduling.

A single flock at `$EXTRACT_DATA_ROOT/state/transform/dbt.lock` serializes DuckDB
writes. It is not a success dependency between DAGs: failure releases the lock and
other families proceed. The wrapper retries only explicit DuckDB lock failures
before any model starts, for up to 120 seconds, never query/model failures.

When `LIGHTDASH_ENABLED=1`, a successful family build streams its accepted marts
into the serving database under the same transform lock, records only catalog
metadata for that batch, and then runs `sync_lightdash`. Only successfully built
marts enter serving.
Sync combines the accepted update with unrelated families' last successful
definitions; a partial release must never replace the project with just one family
or expose an unbuilt sibling. Content/query checks apply to the changed scope.
Publication and deployment locks serialize shared serving writes; failures remain
visible and retryable without rebuilding unrelated families.

Developer commands (these operate on the full checkout; scheduled production uses
the isolated family runner):

```bash
transform/bin/dbt build --target dev --select +fct_bike_station_status_daily
transform/bin/dbt docs generate --target dev
transform/bin/dbt docs serve --target dev
```

