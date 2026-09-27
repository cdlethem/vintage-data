# Confined execution and model connections

The dashboard is the authority for task acceptance, assignment and start. Model
credentials live in encrypted Airflow Connections; assignments live in Airflow
metadata. A trusted run parent opens a short-lived proxy pinned to one provider
and model. Only its Unix socket enters an executor's network namespace. The bot
cannot read the provider key, Git credential, Airflow credential or host auth store.

## Install the executor

Run Airflow with a service account on a Linux host with bubblewrap, systemd user
services, delegated cgroups and unprivileged user namespaces. Enable lingering
for that account (`sudo loginctl enable-linger SERVICE_USER`) so its worker does
not depend on an interactive login. Install `bubblewrap` through your OS package
manager. Root is needed only to protect the immutable runtime installation.

Prepare a versioned runtime with the installed OMP executable:

```bash
python3 bots/sandbox/install.py prepare --output /tmp/vintage-runtime-build --omp /path/to/omp
sudo python3 bots/sandbox/install.py install --source /tmp/vintage-runtime-build --destination /opt/vintage-bot-runtime-v1
/opt/vintage-bot-runtime-v1/launcher.py --check
```

The staging step installs the repository's pinned Python requirements, pytest,
and Airflow/Pendulum/YAML dependencies needed to instantiate extraction DAGs
offline. Preserve the resolved package inventory and the OMP binary
version/digest with your release. Deploy into a new versioned directory for upgrades;
do not rewrite an installation used by running tasks. The installer and launcher
reject writable/untrusted runtime files. Configure memory, process, CPU and duration
limits in `runtime.json` as root before running the confinement probe.

Changes to `bots/sandbox/worker.py` (including executor/reviewer writing
instructions) do not update a running sandbox. The launcher uses a root-owned
copy in its versioned `/opt/vintage-bot-runtime-vN` directory. Prepare and
install a new version, run its confinement checks, then point
`BOT_DASHBOARD_SANDBOX_LAUNCHER` at that version and reload the affected Airflow
services after existing work drains. Do not edit or overwrite the live runtime.

In `orchestration/config.env`, set the launcher path and Git host allowlist. Keep
execution disabled until the probe below passes. Render using
`orchestration/setup/render_config.sh`. Install the rendered
`generated/systemd-user/airflow-bot-worker.service` into the service account's
`~/.config/systemd/user/`, then run `systemctl --user daemon-reload` and
`systemctl --user enable --now airflow-bot-worker` as that account. It listens on
the dedicated `bot_dashboard_executor` Celery queue; ordinary extract workers
continue on their existing queue.

A probe uses the same launcher invocation as a task plus `--probe`, with an
operator-created private run directory, mode-0400 admission, workdir, and an active
trusted `model.sock`. It asserts host home, Docker, Git and service sockets are
absent, direct Internet and host-service connections fail, and the scoped model
socket is reachable. `bots/sandbox/probe.py` constructs this fixture from a saved
provider mapping. After it passes, set:

```text
BOT_DASHBOARD_EXECUTOR_ENABLED=True
BOT_DASHBOARD_CONFINEMENT_ASSERTION=rootless-userns-cgroup-v1
BOT_DASHBOARD_NETWORK_POLICY_ASSERTION=deny-all-except-model-gateway-v1
```

Re-render and restart the API, DAG processor and bot worker so all components read
the same settings. The dedicated executor pool must exist with the desired
concurrency (the standard dashboard setup creates `bot_dashboard_executor`).
No unchecked fallback runs tasks outside the namespace.

## Connect models in the UI

Open **Bot activity → Models & connections**. Enter a provider name, an
OpenAI-compatible API base URL ending in `/v1` where appropriate, and its API key.
Test the connection to discover models, then save. Map each specialist and each
junior/senior/staff execution profile, plus the PR reviewer, to a provider/model.
New runs use saved mappings; unmapped ordinary specialists retain their operator
configuration. Executors require explicit mappings. Keys are never returned to the
browser, embedded in an admission, or passed to the model process. Rotating a key
uses the same provider ID. A different endpoint requires explicit re-entry of the key.

Public HTTPS providers are supported by default. For a private/on-host gateway,
the operator must set `BOT_DASHBOARD_MODEL_ALLOWED_HOSTS` to exact `host:port`
entries and re-render; for example `127.0.0.1:19092`. The allowlist is a deployment
setting rather than a model-controlled URL. Redirects are rejected. The initial
provider adapter supports OpenAI Chat Completions with streaming and tool calls;
providers using other native wire protocols need a compatible gateway.

For existing OMP OAuth accounts, OMP's separate auth broker and auth gateway can
provide that compatible endpoint. Run `omp auth-broker serve` and
`OMP_AUTH_BROKER_URL=http://127.0.0.1:BROKER_PORT omp auth-gateway serve` as managed
services with private auth storage; connect only the gateway URL/token in the UI.
The broker and its refresh credentials remain outside every bot sandbox. Use
service-owned accounts and your normal secret rotation policy in production.

Git publishing is configured separately: see [GIT_SETUP.md](GIT_SETUP.md).
Publishing creates draft PRs and advisory reviews; merging and production data
publication retain their human review steps.

## Verification and operations

Runs are unattended: automatic approval applies within the admitted tool/path policy,
standard input is closed, and missing prerequisites become structured blockers rather
than permission prompts. The installed worker executes admitted argv itself, hashes bounded observations,
and writes the strict protocol-v2 result. The trusted launcher computes the changed
path list using host-side clean Git metadata; the parent rechecks the actual patch,
path intersection, size limits, verification digest and result schema before upload.
The reviewer sees a read-only patched tree and cannot change it.

Keep executor checks offline: use deterministic HTTP fixtures, development parse,
and content checks. Do not admit production Airflow or warehouse commands. The
runtime includes `python3`, `pytest`, `dbt`, Airflow's DAG factory dependencies,
and visualization dependencies; direct `dbt ... --project-dir transform
--profiles-dir transform --target dev` works without shared-checkout wrappers
or credentials.

Live public-source checks use the separate `bot_dashboard__validation` lane.
Its fixed catalog runs exact candidate extractors in a read-only namespace with
public-only egress and no provider, Git, Airflow, or warehouse credentials.
Enable the installed public capability in `orchestration/config.env` with
`BOT_DASHBOARD_VALIDATION_CAPABILITIES='["public-network-readonly"]'`, then render
and reload the API, DAG processor, and confined worker. Gates retain exact-head
evidence, bounded attempts, and independent review. Warehouse-backed builds,
Lightdash previews, migrations, and production activation are not authorized by
enabling this public capability.

Failed model tests expose status codes without upstream response bodies or keys.
Unpublished terminal failures can use **Retry bot work** after their prerequisite is
fixed; the previous attempt remains in history. If reporting fails after a PR was
published, the Airflow retry validates its current identity and saved artifacts,
then resumes reporting and review without running the agent or publishing again.

A nonzero launcher exit keeps its `sandbox_exit_<status>` reason code and records
bounded stdout (2 KiB) and stderr (4 KiB) tails in the run's failure detail.
Authenticated URLs and credential fields are redacted; admission data and output
that cannot be safely redacted are omitted. These are diagnostics for that run,
not proof that the launcher caused a different revision failure. A model-reported
`blocked` result instead has its own verification report. Older attempts that
discarded launcher output cannot be diagnosed retroactively from an exit code.
Do not treat a no-change diagnostic or a passing compilation check as a repaired
handoff; require a scoped candidate, review, and trusted merge evidence.

Check **Models & connections** first for missing mappings, then task execution
history for admission/verification/publication errors. Provider changes apply to new
runs; an admitted run fails if its provider endpoint changes before launch. Deployment
upgrades should stop admission, drain the bot worker, then switch runtime versions.
