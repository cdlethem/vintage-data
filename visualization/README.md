# Lightdash

Optional open-source Lightdash serves the dbt mart contracts and checked-in charts.
DuckDB remains the transformation warehouse. A successful dbt build captures its
completed marts under the transform lock; a separate retryable Airflow task copies
that immutable batch into Postgres. Lightdash connects as a read-only Postgres user.
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
`vintage-lightdash.service`. Set `SKIP_SYSTEMD=1` for a Compose-only installation.
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

Enablement adds publication to subsequent successful cadence builds. For the
first deployment, run a full production build through the trusted runner so every
mart has a serving relation. This uses the same lock, policies, and capture path as
Airflow (and may take time on a populated warehouse):

```bash
orchestration/.venv/bin/python - <<'PY'
import sys
sys.path.insert(0, 'orchestration/include')
import deployment
deployment.load_env()
from transform_runner import run, publish_marts
for cadence in ('twice_hourly', 'hourly', 'daily'):
    publish_marts(run(cadence))
PY
```

The scheduled DAGs are the normal production entrypoint. A failed dbt build does
not publish a partial batch.
Then parse the current checkout and deploy metadata and reviewed content:

```bash
transform/bin/dbt parse --target dev
visualization/bin/viz validate
visualization/bin/viz content --check
visualization/bin/viz compile
visualization/bin/viz deploy --create
visualization/bin/viz query-check
visualization/bin/viz status
```

The serving bundle changes adapter and relation/type metadata only; it never
rewrites or executes the DuckDB model SQL on Postgres. It retains the original
manifest for provenance and exposes only mart models. Deployment checks the real
serving catalog, uploads charts/dashboards, and validates content. Missing tables
or columns fail deployment. The created project UUID is retained in external
state; later `deploy` updates that project. Metadata/content deployment is an
explicit operator action after review, separate from routine data refresh.

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
semantic metadata and contract issues. The data analyst (`bots/data_analyst`)
owns the analysis layer: it profiles and queries the real serving data through
`visualization/bin/eda`, decides what actually changes over time in a family,
and proposes the `analysis` block and the metrics its trends need. Its
deterministic context selects the next family with gaps, prefers families whose
only deficit is missing analysis, suppresses active tasks by resource key, and
reconsiders completed work if gaps remain. Both propose one coherent family at a
time through the existing manager/admission/executor/reviewer lifecycle.
`eda` is a capability, not a credential: it loads the reader password itself and
admits one bounded read-only SELECT, so specialists and confined executors still
receive no production credentials or Docker control. An operator runs
`viz preview` against a configured project for review, checks its charts, and
deploys after review. Preview identities are explicit, preventing uploads from
falling back to the production project. A trend that renders empty in the
browser is not done, whatever validation reports.

## Publication, recovery and monitoring

Each batch contains manifest/run-results, declared column types, exact row counts,
CSV checksums, capture time, and a monotonic sequence. CSV preserves quoted literal
`\\N`, nulls, decimals, JSON text and microsecond timestamps. DuckDB JSON is
served as text because it may contain values such as `NaN` that PostgreSQL JSON
rejects; Lightdash dimensions cast these fields to display text. Capture uses one read-only
DuckDB transaction. Publication checks checksums, copies into typed temporary
Postgres tables, and updates all affected marts and `_publish.models` in one
transaction under an advisory lock. Readers see committed versions. Deleted and
empty-source rows are reflected; unaffected cadence tables stay in place. Older
or repeated batches cannot overwrite newer data. A failed publication rolls back
and can be retried without rebuilding dbt:

```bash
visualization/bin/viz publish --batch "$LIGHTDASH_STATE_ROOT/batches/<batch-id>"
visualization/bin/viz status
visualization/bin/compose logs --tail 100 lightdash
```

`status` reports per-model row counts, capture/publication times and source snapshot
age. Airflow exposes capture/build failures and separate publication failures to
the existing failure-triage workflow. Compare snapshot age with each model's cadence;
a fresh publication of an old snapshot is not fresh source data. Schema fingerprint
changes fail closed: review an explicit serving schema migration, back up first,
and then recapture; do not erase the ledger to bypass this check.

Back up both Postgres databases (application metadata/users and serving data),
external bundles/batches, MinIO state, and the secret file. For a consistent whole
installation backup, stop the Compose stack and snapshot/copy the entire external
state directory, then restart it. Database-native online backups are also possible;
retain the matching application image version and encryption secret for restoration.
Do not downgrade an image against an already migrated application database; restore
the pre-upgrade backup into a separate state directory. Restore previous reviewed
content through `viz deploy`; data rollback requires an explicit new publication
from a reviewed historical snapshot because stale-batch retries are intentionally
ignored. Batches and dbt-run artifacts are retained, not automatically pruned;
monitor disk use and archive/remove only batches no longer needed for retries or
recovery. `compose down` preserves bind-mounted state.

## Verification

```bash
visualization/.venv/bin/python -m unittest visualization.test_project visualization.test_publisher
orchestration/.venv/bin/python -m unittest bots.test_visualization_context orchestration.test_transform_runner
```

Postgres integration tests opt in with `VINTAGE_TEST_ENV_FILE` pointing at a
**disposable** stack's environment file. `smoke.py --env-file ... --manifest ...`
bootstraps that stack and publishes clearly marked synthetic contract fixtures for
all marts; it only accepts `/tmp` state and a non-default test database port.
Never use this fixture runner for production data. Tests cover real COPY round trips,
nulls/Unicode/decimals/timestamps, deletes/empty tables, stale retries, corruption,
schema rejection, coverage and bot task suppression. Run deployment/upload and
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
