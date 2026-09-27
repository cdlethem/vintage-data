# Lightdash

Optional open-source Lightdash serves the dbt mart contracts and checked-in charts.
DuckDB remains the transformation warehouse. A successful dbt build streams its
completed marts straight into Postgres under the same transform lock, without
writing an export file. Lightdash connects as a read-only Postgres user.
Ordinary local DuckDB files are not a supported Lightdash warehouse connection.

## Reproducible deployment

The server and CLI are pinned together at 2.140.0. `images.env` pins runtime images
by digest; `package-lock.json` locks the CLI dependency tree and `requirements.txt`
locks Python dependencies with hashes. The CLI runs in its Node 24 container, so
host Node versions do not affect deployment. Runtime state lives in
`LIGHTDASH_STATE_ROOT`, outside the checkout. Linux with Docker Compose v2, `uv`,
and Python 3.13 is required; the integration suite was exercised on x86-64 Linux.
No Lightdash Cloud account or enterprise license is required.
Git worktrees reuse the main checkout's pinned visualization environment while
executing their own code. Plain confined workspaces can use a preprovisioned
interpreter via `VINTAGE_VISUALIZATION_PYTHON`; install the locked Python requirements
there before admitting validation commands. Offline `validate`, `content` and
`bundle` commands do not load the deployment secret file.

Set `LIGHTDASH_ENABLED=1` in the ignored `orchestration/config.env`. Set state root,
ports and public URL there if the defaults do not fit this machine; see
`orchestration/config.example.env`. Run from the checkout root:

```bash
orchestration/setup/08_lightdash.sh
visualization/bin/viz bootstrap --email admin@vintage-data.local
```

Setup renders the existing deployment templates, creates/preserves secrets, builds
the pinned CLI image, starts Postgres/MinIO/Lightdash, and installs
`vintage-lightdash.service`. It also restarts active Airflow workers so they pick
up the current publication flag and serving credentials; changing an environment
file alone does not update running workers. Set `SKIP_SYSTEMD=1` for a Compose-only
installation, and refresh any externally managed workers yourself.
Reruns preserve database state and credentials. Docker must already be installed
and accessible to the configured service user. The default UI is
`http://127.0.0.1:8083`; Postgres binds only to loopback port 5433. For a remote
browser, use an SSH tunnel or configure the public URL and a TLS reverse proxy,
including secure-cookie/proxy settings.

When `PUBLISH_TAILSCALE=1`, setup exposes Lightdash separately at HTTPS port
`LIGHTDASH_TAILSCALE_HTTPS_PORT` while Airflow retains HTTPS 443. Lightdash stays
bound to loopback; Tailscale handles TLS and tailnet access. Set `LIGHTDASH_URL`
to the complete tailnet URL and enable secure cookies and trusted proxy headers.

The bootstrap administrator password and deployment API token are stored only in
ignored `orchestration/airflow.secrets.env` (mode 0600). Read the password locally
when signing in; do not copy secrets into project files, PRs, or bot task contexts.
The application, publisher, and reader have separate database roles/databases.
The reader cannot access the application database or mutate the serving database.
Changing configured passwords or database names does not migrate an existing
Postgres volume: rotate roles deliberately or initialize a new state directory.

## Initial data and project publication

Enablement adds publication and Lightdash release to subsequent successful
production family builds. Each `transform__<family>__<cadence>` DAG is independently
retryable. Initial coverage requires successful builds for the desired families
and cadences; bots and confined workspaces do not run production operations.

For a fresh installation with no retained project identity, publish the first
successful family build, then initialize the project explicitly:

```bash
visualization/bin/viz sync --batch <published-batch-id> --create
```

Creation is allowed only when the serving ledger belongs to that initial batch.
Later scheduled family syncs update the retained project. Existing installations
must adopt a verified last-deployed local bundle as their initial aggregate.
The serving ledger must match that baseline outside the current batch; newly
published current-batch models may extend it. Missing or inconsistent baseline
evidence is an error, never permission to replace the catalog with one family.

The normal production entrypoint is the scheduled DAG:

```text
build_and_publish -> sync_lightdash
```

`build_and_publish` builds the family, then — still holding the transform lock —
streams every accepted mart from DuckDB into freshly created Postgres tables and
swaps them into the serving schema in one transaction. Runtime checks must not
read or validate unrelated checkout content. Only the batch's catalog metadata
(manifest, run results, reviewed presentation content, declared column types,
exact row counts, sequence) is written to
`$LIGHTDASH_STATE_ROOT/batches/<batch-id>`; no table data is retained on disk.
A failed transfer is retried by rebuilding from raw data, not by replaying a
saved export.

After publication, `sync_lightdash` checks that the batch is current and combines
its accepted models with unrelated models' last successful metadata. Lightdash
deploy replaces the explore catalog, so deploying one family alone is forbidden:
the aggregate must preserve unrelated explores, including older working releases
of failed families. Unbuilt models cannot enter the aggregate. Mixed-cadence
dashboard content must reflect only accepted, published model definitions.

The deployment lock serializes aggregate changes. Validation and saved-chart query
checks apply to the changed scope, not every other source's current health.
Success records the accepted release; failures remain visible and retryable. A
transfer failure retries the whole build; a sync failure retries only the
metadata deployment. A failed source does not prevent another source publishing.
Shared warehouse/service outages and real shared dependencies remain shared risks.

Developer and confined-bot work is limited to preparing the reviewed candidate:
parse/validate its semantic specification, run `viz content` to regenerate its
admitted chart/dashboard files, and validate the generated diff offline. A
preview-capable operator reviews the exact candidate before merge. Neither that
preview nor offline dbt success is production delivery. After merge, the successful
trusted family build/batch/release receipt is the metadata/content activation
evidence; another family's failure is not a prerequisite for completing it.
Direct deployment or Docker operation remains outside confined bot execution.

The serving bundle changes adapter and relation/type metadata only; it never rewrites
or executes the DuckDB model SQL on Postgres. It retains the original manifest for
provenance and exposes only published mart models. Its trusted release checks the
serving catalog and uploads/checks the changed content while retaining unrelated
accepted definitions. Missing required tables or columns fail the affected release.
The retained project UUID is updated rather than creating one project per source.

## Content and ownership

Each governed mart has dbt descriptions, grain, dimensions, units and metrics in
its existing YAML. `config.meta.vintage.visualization` records the ranked
dimension, metric, date window and detail columns; its `analysis` block records
the time series that are the point of the project. `visualization/TRENDS.md` is
the binding standard for that block and the acceptance checklist for a finished
dashboard. `transform/lightdash/` contains native Lightdash charts-as-code.
Every mart has time series over its own analytic time axis, a domain-specific
ranked comparison, and a bounded detail table. These are observations over an
explicit time window, not claims about a current census. Distinct entities can
overlap between categories; observation-weighted averages can be biased by
polling frequency. Snapshot totals, balances, mixed currencies, and rates must
not be summed. Source attribution and interpretation appear in chart/dashboard
descriptions.

A trend's time axis is normally the source's own event time, not the extractor's
observation timestamp: collection history begins when collection began, while
event history usually reaches back years. Trends on `source_loaded_at`,
`extract_started_at`, `_dt` or `source_date` measure the collector and are
rejected. Only chart shapes verified to render in the pinned Lightdash are
generated — `line`, pivoted `line` bounded by `columnLimit`, and stacked `bar`.
Unstacked `area` and stacked `line`/`area` drop Lightdash's auto-expanded pivot
series and render empty, so the generator refuses them. Dashboards carry the
date-zoom control, restricted to grains every trend axis on that dashboard
declares, and `analysis.controls` becomes a filter chip per sliceable dimension.

Edit metadata and presentation specifications together, regenerate with
`viz content`, and review the YAML diff. `viz check-analysis` validates an
`analysis` block offline against one family YAML with no manifest, warehouse or
credential, and is the fast inner loop. `viz validate --json` separates
**issues** — contract breaches such as unknown fields, undeclared grains, or a
declared trend absent from a dashboard, which block deployment — from **gaps**,
which are analysis-completeness deficits such as a mart with no time series, no
dimensional trend, a collection-time-only axis, or a single analytic metric.
Gaps are scheduled work and do not block deployment. A missing chart remains
work even if its source already has a mart. Semantic dimension aliases keep
generated SQL identifiers within PostgreSQL's 63-byte limit while preserving
column names and display labels. `viz query-check` executes every saved chart
via the asynchronous API; static content validation alone does not detect every
warehouse query error. Do not skip this check on a Lightdash or content upgrade.
Hand-authored charts are supported, but generated files are overwritten by
`viz content`; adopt a UI edit into the specification or keep it as a separately
named chart. `viz export` downloads into an external scratch directory for
review, never directly over tracked content.

Two specialists share this contract. The analytics engineer owns mart grain,
semantic metadata and contract issues. The data analyst (`bots/data_analyst`) owns
the analysis layer: it profiles and queries the real serving data through
`visualization/bin/eda`, decides what actually changes over time in a family, and
proposes the `analysis` block and the metrics its trends need. Its deterministic
context selects the next family with gaps, prefers families whose only deficit is
missing analysis, suppresses active tasks by resource key, and reconsiders completed
work if gaps remain. Both propose one coherent family at a time through the existing
manager/admission/executor/reviewer lifecycle.

The admitted implementation changes the family specification and the exact generated
chart/dashboard paths it affects. The executor may run the offline generation and
validation loop, but never hand-edits generated YAML. `eda` is a capability, not a
credential: it loads the reader password itself and admits one bounded read-only
SELECT, so specialists and confined executors still receive no production credentials
or Docker control. A preview-capable operator runs `viz preview` against a configured
non-production identity for the merge-stage review; its evidence attaches to the
candidate head. The trusted scheduled cadence, not an operator/bot sandbox, later
deploys and query-checks the immutable post-merge batch. A trend that renders empty
in the browser is not done, whatever static validation reports.

## Publication, recovery and monitoring

Each batch directory holds manifest/run-results, a verified snapshot of reviewed
generated content, declared column types, exact row counts, publication time and a
monotonic sequence — kilobytes of catalog state, never table data. Values move as
DuckDB's own text serialization through a `COPY ... FROM STDIN` stream, read in
fixed row-id spans so memory stays bounded, preserving nulls, decimals, JSON text, microsecond timestamps and
date/timestamp infinities. DuckDB JSON is served as text because it may contain
values such as `NaN` that PostgreSQL JSON rejects; Lightdash dimensions cast these
fields to display text. The transfer uses one read-only DuckDB transaction with UTC
time and refuses invalid coverage/schema or unreviewed generated content.

Publication streams into freshly created Postgres tables, compares the rows it sent
against what Postgres received, then drops each previous table and renames its
replacement into place — freeing the old files at commit rather than leaving a
mart of dead rows — and updates all affected marts and `_publish.models`
in one transaction under the serving advisory lock. The batch's metadata directory is
renamed into place and fsynced before that commit, so the ledger never references
absent metadata. Readers see committed versions. Deleted and empty-source rows are
reflected; unaffected cadence tables stay in place. Older or repeated batches cannot
overwrite newer data: a batch with any superseded model is refused whole, without
reading the warehouse. `sync_lightdash` follows a successful publication only: it
refuses a stale sequence, serializes deployments, uses the published manifest/content
rather than the checkout, and atomically records a successful deploy/validation/query
check receipt. The receipt makes an unchanged release a safe skip. A failure rolls
back or leaves no success receipt, remains visible in the Airflow task/failure-triage
path, and a sync can retry without rebuilding dbt:

```bash
visualization/bin/viz status
visualization/bin/compose logs --tail 100 lightdash
```

`status` reports per-model row counts, publication times and source age. Airflow
exposes build/publication and synchronization failures to the existing
failure-triage workflow. Compare source age with each model's cadence; a fresh
publication of old source data is not fresh data. Schema fingerprint changes fail
closed: review an explicit serving schema migration, back up first, and then
rebuild and republish; do not erase the ledger to bypass this check.

Back up both Postgres databases (application metadata/users and serving data),
external bundles/batches, MinIO state, and the secret file. For a consistent whole
installation backup, stop the Compose stack and snapshot/copy the entire external
state directory, then restart it. Database-native online backups are also possible;
retain the matching application image version and encryption secret for restoration.
Do not downgrade an image against an already migrated application database; restore
the pre-upgrade backup into a separate state directory. Restore previous reviewed
content by activating a reviewed historical immutable release through the trusted
sync workflow. There is no data rollback snapshot: recovering older table contents
means rebuilding those models from retained raw data and publishing again. Batch
metadata and dbt-run artifacts are retained, not automatically pruned; they are
small, so monitor disk use rather than scheduling deletions. Installations upgraded
from the retired export path remove their leftover payloads once, under the
transform, deployment and legacy capture locks:

```bash
visualization/bin/viz discard-export-data          # dry run: counts and bytes
visualization/bin/viz discard-export-data --apply
```

It deletes only CSV payloads a valid legacy `batch.json` lists, plus interrupted
`.capture-*` directories and `failed-batches` children; metadata, bundles and
anything unexpected are left untouched and reported. `compose down` preserves
bind-mounted state.

## Verification

```bash
visualization/.venv/bin/python -m unittest visualization.test_project visualization.test_publisher visualization.test_cli
orchestration/.venv/bin/python -m unittest bots.test_visualization_context orchestration.test_transform_runner orchestration.test_transform_dags
```

Postgres integration tests opt in with `VINTAGE_TEST_ENV_FILE` pointing at a
**disposable** stack's environment file. `smoke.py --env-file ... --manifest ...`
bootstraps that stack and publishes clearly marked synthetic contract fixtures for
all marts; it only accepts `/tmp` state and a non-default test database port.
Never use this fixture runner for production data. Tests cover real streamed COPY
round trips, nulls/Unicode/decimals/timestamps/infinities, deletes/empty tables,
stale and replayed batches, whole-batch rollback, schema rejection, retired-export
removal, coverage and bot task suppression. Run deployment/upload and
warehouse validation against the fixture stack when upgrading Lightdash.

The JSON schemas in `schemas/` come from Lightdash tag `2.140.0`,
`packages/common/src/schemas/json/chart-as-code-1.0.json` and
`dashboard-as-code-1.0.json`; their upstream MIT license is included.
See [self-hosting](https://docs.lightdash.com/self-host/customize-deployment),
[CLI](https://docs.lightdash.com/references/lightdash-cli), and
[upstream source](https://github.com/lightdash/lightdash/tree/2.140.0).
Version 2.140.0 includes the OSS migration fix absent from 2.119.0, which failed
when initializing without enterprise AI tables during integration testing.

## Later Postgres-only migration

Keep this replication boundary until mart SQL, incremental behavior, source JSON
access and timestamp semantics have adapter-compatible implementations. All current
mart SQL uses DuckDB constructs. Move warehouse/loading and dbt incrementally only
after fixture parity and real-data reconciliation, preserving unique keys, grain,
source ancestry and serving names. Then remove snapshot replication and point the
same Lightdash semantic/content layer at the Postgres-built marts.
