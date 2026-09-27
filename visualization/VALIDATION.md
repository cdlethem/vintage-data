# Integration evidence

Verified on 2026-09-07 on x86-64 Linux using the pinned Lightdash 2.140.0
server/CLI, Postgres 18.6 and MinIO images in `images.env`.

- Parsed the complete dbt project and passed its manifest policy.
- Covered 163/163 mart models with 326 charts and 162 family dashboards.
- Checked every content file against the pinned native JSON schemas and checked
  deterministic regeneration without differences.
- Compiled all 163 explores against actual Postgres serving relations.
- Bootstrapped a local OSS organization/account, deployed a project, uploaded all
  content and completed Lightdash content/catalog validation without errors.
- Executed all 326 saved charts through the v2 asynchronous query API: zero failures.
- Created a preview, uploaded/validated its content and verified its project UUID
  differs from the retained deployment project UUID.
- Passed 12 snapshot/Postgres tests, including exact decimal/Unicode/null/timestamp
  round trips, updates/deletes/empty tables, idempotence/stale retries, schema-change
  rejection and rollback of both earlier tables and ledger on a later COPY failure.
- Passed 4 content/bundle tests and 19 bot-context/transform-runner tests.
- Rendered configuration twice from a different checkout with spaces in its path
  and state directory; the second `--check` was clean. Shell syntax and diff
  whitespace checks passed.

The database integration fixtures are synthetic rows derived from every mart's
contract, explicitly marked in their manifest and confined to a disposable `/tmp`
stack on separate ports. This verifies wiring, types and executable queries; it
is not a production data reconciliation or visual browser review. The production
feature flag remains opt-in and the production warehouse was not rebuilt or
published during these integration checks. Follow README setup/publication steps
for the live installation.

Failures found and fixed during integration: an upstream OSS-only migration issue
in the initially considered Lightdash 2.119.0 release (upgrade pin to 2.140.0),
registration request shape, profile template quoting, local Postgres SSL settings,
required internal worker enablement, overlong PostgreSQL field aliases, and JSON
grouping expressions. Static Lightdash validation did not catch the latter query
errors; retain `viz query-check` in upgrade and release verification.

## Automatic metadata synchronization

Verified on 2026-09-21 using an isolated Postgres/Lightdash stack on separate
ports and synthetic DuckDB marts; production data and the production project
were not deployed.

- Exercised the trusted `publish_marts` → `sync_lightdash` callables against
  the real services: added a second mart and updated an existing chart title.
  Lightdash exposed the new charts and all 10 saved-chart queries succeeded.
- Retrying the same batch skipped deployment. A fresh dbt invocation with
  unchanged semantics also skipped deployment. An older batch was rejected
  after a newer batch published.
- Passed 37 focused tests, including the disposable Postgres integration cases,
  immutable content, deployment receipts, subprocess JSON output, and isolated
  Compose environment overrides.
- Imported all three production transform DAGs and confirmed synchronization
  depends on successful build and mart publication.

## Source-family failure isolation

The cadence-wide DAG layout above was superseded later on 2026-09-21 by independent
`transform__<family>__<cadence>` DAGs. Verification used a disposable DuckDB,
Postgres, and Lightdash deployment; no production build or deployment was invoked.

- Imported the real DAG factory: 167 family/cadence DAGs, no legacy cadence-wide
  definitions, and separate build/publication/sync chains.
- A healthy family parsed, passed strict policy validation, built, and passed its
  data tests despite unrelated malformed SQL/YAML and a forbidden base-view test.
  The invalid families failed when selected directly.
- Exercised the real runner and worker subprocesses after a WHO-style policy
  failure: the shared lock was released and a healthy family built and published.
- Adopted a verified legacy deployment with no aggregate receipt or content
  snapshot. Made the unrelated family's serving table unavailable, then deployed
  the healthy family's update. Both explores and the unrelated original charts
  remained; all five changed-family chart queries succeeded.
- Same-batch retry and a fresh dbt invocation with unchanged semantic metadata
  skipped deployment. Execution timestamps, temporary compile paths, and dbt user
  IDs did not change the semantic fingerprint.
- Passed 66 focused tests: 12 transform project tests, 12 orchestration tests,
  33 serving tests including disposable Postgres cases, and 9 bot-context tests.
- Ran the migrated disposable `visualization/smoke.py` utility and published its
  two-family fixture through scoped captures.
- Reproduced and fixed first-sync adoption when the current batch introduces a
  new mart into the serving ledger. The retained catalog is preserved; an
  unrelated schema mismatch still rejects adoption. All seven CLI tests passed
  after this regression correction.

## Streamed publication without export files

Verified on 2026-09-22 with an isolated Postgres/Lightdash stack on separate
ports (`/tmp/vintage-stream-*`, ports 15433/18083) and, where stated, on the
live installation after its controlled cutover.

- Streamed every accepted mart straight from DuckDB into Postgres in one
  transaction. Exact round trips held for decimal `123456789012345678.12`,
  Unicode, quotes, commas, tabs, newlines, carriage returns, backslashes,
  literal `\N`, SQL NULL, timezone-normalized microseconds, booleans, JSON
  containing `NaN`, floating `inf`/`nan` and date/timestamp infinities.
  Unsupported types and overlong identifiers still fail before serving changes,
  and an empty source empties its destination.
- A second mart's COPY failure (a real Postgres numeric parse error) rolled back
  the first mart's rows and both ledger entries; a repaired retry under a fresh
  invocation updated both. Replaying a published identity skipped it; an older
  identity was refused. A batch whose single superseded sibling was newer than
  the ledger returned `stale` without reading, transferring or relabeling the
  other mart.
- Published 1,000,000 generated rows of ~1 KiB text through the real publisher:
  source and destination counts and an md5-derived aggregate checksum matched,
  peak publisher RSS was 393 MiB, throughput ~300k rows/s, no `.csv` or other
  payload file appeared, and the state directory grew by 1.8 KiB of metadata.
- Interrupted a real publication while `COPY "incoming_mart" FROM STDIN` was
  active: serving table and ledger stayed at their previous version, the worker
  process group was reaped, no large file existed, and the transform lock was
  reacquirable afterwards. A SIGKILLed parent did not release that lock while
  its inherited child lived; the lock freed when the child exited. A committed
  publication whose parent lost the result republished from source.
- Ran three families end to end through the real runner, worker, publisher and
  Lightdash deploy: explores accumulated 1 → 2 → 3 while unrelated families'
  last-good metadata was retained. A forced deploy failure left the receipt and
  catalog unchanged and a retry advanced both without rebuilding or exporting.
  A forced publication failure exited non-zero, left the ledger untouched and
  never reached sync. Downgrading a current batch's metadata to `schema_version:
  1` with absent payload files still validated, reported `current` publication
  state and composed the aggregate manifest.
- Imported the real DAG factory: 168 family/cadence DAGs, every one exactly
  `build_and_publish -> sync_lightdash`, unchanged schedules, two retries at
  five minutes, `max_active_runs=1`, and combined timeouts of the configured
  build allowance plus 25 minutes.
- Fixture cutover: a state root with current, pending, superseded, partial,
  modern, interrupted `.capture-*` and `failed-batches` payloads plus a symlink
  and a stray file. The dry run mutated nothing, malformed or traversing payload
  metadata failed before any deletion, `--apply` removed only the authorized
  payloads, a second apply removed zero bytes, and unknown paths were reported
  untouched.
- Live installation: 38 scheduled `build_and_publish` tasks succeeded with no
  failures before cleanup, including `fct_sensor_community_measurement` at
  175,743,294 rows. Under the transform, deployment and legacy capture locks,
  `viz discard-export-data` reported 334 payload files and 516.9 GiB with no
  unknown paths; `--apply` removed exactly those (free space 136 GiB → 652 GiB,
  batches directory 518 GiB → 189 MiB) and a second apply removed zero bytes.
  Afterwards `viz status` reported 165 published models, every ledger, aggregate
  and receipt reference resolved, the aggregate manifest composed 164 models,
  and no `.csv` file remained. Scheduled runs resumed with the batches directory
  growing only in megabytes. Dashboard health was not re-reviewed visually.
- Passed 33 serving tests (including disposable Postgres cases) and 21
  orchestration/bot-context tests.

Failure found and fixed during this work: a 128 MiB DuckDB reader budget without
row-id chunking exhausted memory on wide marts (27- and 34-column families) in
production because the driver materializes a whole result. The transfer now
reads fixed row-id spans, which row-group pruning keeps cheap anywhere in a
mart, with a bounded buffer pool and four threads. Afterwards the two affected
families published successfully, and from 16:00 UTC production recorded 121
successful and 17 failed `build_and_publish` tasks and 104 successful and 9
failed `sync_lightdash` tasks. Of those 26 failures, 24 have no task-side error:
the Celery worker rejected their queued workloads with `Invalid auth token`
after long queue waits (unrelated `extract__*` tasks failed the same way). The
other two failed closed by design: an `open311` mart schema change awaiting a
serving migration, and `sensor_community` saved-chart queries exceeding 120
seconds against its 175.7 million-row serving table, which kept Lightdash on its
last-good release. Before this cutover, no `build` task succeeded between 08:00
and 13:59 UTC; their logs record no task error.
