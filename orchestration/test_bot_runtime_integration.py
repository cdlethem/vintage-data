"""Permanent Airflow-task-runner boundary harness for bot workflows.

The harness deliberately uses an HTTP control-plane double.  It exercises the
same bearer-only ``DashboardClient`` calls that workers make, while keeping the
provider database and every Airflow home inside temporary directories.
"""

from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import textwrap
import threading
import unittest
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse


REPO_ROOT = Path(__file__).resolve().parents[1]
PYTHON = REPO_ROOT / "orchestration" / ".venv" / "bin" / "python"
PASSWORD = "integration-only-service-password"


class _ControlPlane(BaseHTTPRequestHandler):
    server_version = "BotWorkflowTest/1"

    def log_message(self, _format, *_args):
        # Credentials, report bodies, and bearer tokens must never enter task
        # output (or the server's inherited stderr).
        return

    def _json(self, status: int, value: object):
        body = json.dumps(value, separators=(",", ":")).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _body(self):
        length = int(self.headers.get("Content-Length", "0"))
        return json.loads(self.rfile.read(length) or b"{}")

    def do_POST(self):  # noqa: N802 - stdlib protocol hook
        path = urlparse(self.path).path
        if path == "/auth/token":
            body = self._body()
            if body.get("username") != "bot-worker" or body.get("password") != PASSWORD:
                self._json(401, {"detail": "invalid credentials"})
                return
            self._json(200, {"access_token": "test-token"})
            return
        if self.headers.get("Authorization") != "Bearer test-token":
            self._json(401, {"detail": "bearer required"})
            return
        body = self._body()
        state = self.server.state
        with state["lock"]:
            state["routes"].append(path)
            if state.get("fail_once", {}).get(path, 0):
                state["fail_once"][path] -= 1
                self._json(503, {"detail": "transient test failure"})
                return
            if path.endswith("/runs/claim-budget"):
                identity = body["identity"]
                key = tuple(identity[k] for k in ("dag_id", "run_id", "task_id", "map_index"))
                claim = state["claims"].setdefault(
                    key,
                    {
                        "deadline_at": "2099-01-01T00:01:00+00:00",
                        "claim_id": f"claim-{len(state['claims']) + 1}",
                    },
                )
                self._json(200, claim)
                return
            if path.endswith("/runs/report"):
                envelope = body["envelope"]
                state["reports"].append(envelope)
                projection = {
                    "report_projection_id": len(state["reports"]),
                    "envelope_digest": "d" * 64,
                    "envelope_bytes": len(json.dumps(envelope, separators=(",", ":"))),
                    "outcome": envelope["outcome"],
                    "retry_class": envelope["retry_class"],
                    "failure_fingerprint": None,
                    "execution": envelope["identity"],
                }
                self._json(200, projection)
                return
            if path.endswith("/dispatch/claim"):
                self._json(200, {"items": state.get("dispatch_items", [])})
                return
            if path.endswith("/maintenance/run"):
                self._json(200, state.get("maintenance", {"follow_up_bots": [], "metrics": {}}))
                return
            if path.endswith("/executions/claim"):
                state["execution_claims"].append(body)
                self._json(200, {"execution_id": "execution-1", "deadline_at": body["deadline_at"]})
                return
        self._json(404, {"detail": "unknown test route"})


class _Server(ThreadingHTTPServer):
    allow_reuse_address = True

    def __init__(self):
        super().__init__(("127.0.0.1", 0), _ControlPlane)
        self.state = {
            "lock": threading.RLock(),
            "fail_once": {},
            "routes": [],
            "claims": {},
            "reports": [],
            "execution_claims": [],
            "dispatch_items": [],
            "maintenance": {"follow_up_bots": [], "metrics": {}},
        }
        self.thread = threading.Thread(target=self.serve_forever, daemon=True)
        self.thread.start()

    @property
    def base_url(self):
        return f"http://127.0.0.1:{self.server_port}"

    def close(self):
        self.shutdown()
        self.server_close()
        self.thread.join(timeout=5)


class AirflowRuntimeIntegrationTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="bot-airflow-test-")
        self.root = Path(self.tmp.name)
        self.home = self.root / "airflow"
        self.home.mkdir()
        self.db = self.home / "airflow.db"
        self.server = _Server()
        self.env = os.environ.copy()
        self.env.update(
            {
                "AIRFLOW_HOME": str(self.home),
                "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
                "AIRFLOW__CORE__EXECUTOR": "SequentialExecutor",
                "AIRFLOW__CORE__DAGS_FOLDER": str(self.root / "dags"),
                "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": f"sqlite:///{self.db}",
                "AIRFLOW__API__BASE_URL": self.server.base_url,
                "BOT_DASHBOARD_API_USERNAME": "bot-worker",
                "BOT_DASHBOARD_API_PASSWORD": PASSWORD,
                # Never contend with the deployment's shared inference slots.
                "BOTS_LOCKS_DIR": str(self.root / "locks"),
                "PYTHONPATH": os.pathsep.join(
                    [str(REPO_ROOT / "bots"), str(REPO_ROOT / "orchestration" / "dags"), str(REPO_ROOT)]
                ),
            }
        )
        self._init_airflow_database()
        self._write_fixture_bot()

    def tearDown(self):
        self.server.close()
        self.tmp.cleanup()

    def _run(self, args, *, check=False):
        return subprocess.run(
            [str(PYTHON), *args],
            cwd=REPO_ROOT,
            env=self.env,
            text=True,
            capture_output=True,
            check=check,
        )

    def _init_airflow_database(self):
        migrated = self._run(["-m", "airflow", "db", "migrate"])
        self.assertEqual(migrated.returncode, 0, migrated.stdout + migrated.stderr)
        provider_migration = self._run(
            [
                "-c",
                "from airflow.settings import Session; from airflow.providers.vintage.bot_dashboard.db_manager import BotDashboardDBManager; s=Session(); BotDashboardDBManager(s).create_db_from_orm(); s.close()",
            ]
        )
        self.assertEqual(provider_migration.returncode, 0, provider_migration.stdout + provider_migration.stderr)
        self.assertTrue(self.db.exists())
    def _write_fixture_bot(self):
        self.bot = self.root / "source_discovery"
        self.bot.mkdir()
        (self.bot / "prompt.md").write_text("Return a JSON report only.\n")
        (self.bot / "gate_context.py").write_text(
            "import json; print(json.dumps({'context_schema_version': 1, 'context_kind': 'fixture', 'generated_at': '2026-01-01T00:00:00Z', 'as_of': '2026-01-01T00:00:00Z', 'errors': [], 'pending_count': 0, 'truncated': False, 'total_count': 0, 'returned_count': 0}))\n"
        )
        (self.bot / "bot.yml").write_text(
            textwrap.dedent(
                f"""
                name: source_discovery
                description: deterministic no-work integration fixture
                schedule: manual
                enabled: true
                model: fixture_model
                prompt: prompt.md
                timeout_minutes: 3
                context_budget_minutes: 1
                model_budget_minutes: 1
                cleanup_margin_seconds: 5
                capacity_policy: skip
                retry_on: [transient]
                requires_capabilities: []
                context:
                  FIXTURE_CONTEXT:
                    command: [{sys.executable!r}, {str(self.bot / 'gate_context.py')!r}]
                    cwd: .
                    timeout_s: 10
                triggers: []
                gate:
                  context: FIXTURE_CONTEXT
                  path: pending_count
                  operator: equals
                  value: 0
                  reason_code: gate_no_work
                output:
                  format: json
                  schema: source_discovery_v2
                """
            ).strip()
            + "\n"
        )
    def _write_timeout_fixture(self):
        bot = self.root / "failure_triage"
        bot.mkdir()
        counter = self.root / "model_invocations"
        model = self.root / "slow_model.py"
        model.write_text(
            "from pathlib import Path\n"
            "import time\n"
            f"p=Path({str(counter)!r}); p.write_text(str(int(p.read_text() or '0')+1) if p.exists() else '1')\n"
            "time.sleep(1)\n"
        )
        (bot / "prompt.md").write_text("Return a failure report.\\n")
        (bot / "bot.yml").write_text(
            textwrap.dedent(
                """
                name: failure_triage
                description: timeout fixture
                schedule: manual
                enabled: true
                model: slow_model
                prompt: prompt.md
                timeout_minutes: 3
                context_budget_minutes: 1
                model_budget_minutes: 1
                cleanup_margin_seconds: 5
                capacity_policy: retry
                retry_on: [capacity, transient]
                requires_capabilities: []
                context: {}
                triggers: []
                output:
                  format: json
                  schema: failure_triage_v2
                """
            ).strip()
            + "\n"
        )
        models = self.root / "models.yml"
        models.write_text(
            textwrap.dedent(
                f"""
                default: slow_model
                concurrency:
                  max_active: 1
                models:
                  slow_model:
                    provider: command
                    argv: [{sys.executable!r}, {str(model)!r}]
                    timeout_s: 0.1
                    inherit_env: false
                    pass_env: []
                    capabilities: []
                    max_concurrency: 1
                bots:
                  failure_triage: slow_model
                """
            ).strip()
            + "\n"
        )
        self.env["BOTS_MODELS_CONFIG"] = str(models)
        return bot, counter

    def _write_runner_dag(self, dag_id: str, bot: Path) -> Path:
        return self._write_dag(
            f"{dag_id}.py",
            textwrap.dedent(
                f"""
                import sys
                sys.path.insert(0, {str(REPO_ROOT / 'orchestration' / 'dags')!r})
                from airflow import DAG
                from airflow.providers.standard.operators.python import PythonOperator
                from bots_dag import _run
                with DAG({dag_id!r}, schedule=None, start_date=__import__('pendulum').datetime(2026,1,1,tz='UTC'), catchup=False) as dag:
                    run = PythonOperator(task_id='run', python_callable=_run, op_kwargs={{'bot_dir': {str(bot)!r}}})
                """
            ),
        )

    def _write_dag(self, name: str, body: str) -> Path:
        dag_dir = self.root / "dags"
        dag_dir.mkdir(exist_ok=True)
        path = dag_dir / f"{name}.py"
        path.write_text(body)
        return path

    def _run_dag(self, dag_id: str, dag_file: Path, *, expect_success=True):
        result = self._run(
            [
                "-m",
                "airflow",
                "dags",
                "test",
                dag_id,
                "2026-01-01T00:00:00+00:00",
                "-f",
                str(dag_file),
            ]
        )
        if expect_success:
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        return result

    def test_gate_task_runner_persists_body_free_skip_and_safe_xcom(self):
        dag = self._write_dag(
            "fixture_gate_dag",
            textwrap.dedent(
                f"""
                import sys
                sys.path.insert(0, {str(REPO_ROOT / 'orchestration' / 'dags')!r})
                from airflow import DAG
                from airflow.providers.standard.operators.python import PythonOperator
                from bots_dag import _run
                with DAG('fixture_gate', schedule=None, start_date=__import__('pendulum').datetime(2026,1,1,tz='UTC'), catchup=False) as dag:
                    run = PythonOperator(task_id='run', python_callable=_run, op_kwargs={{'bot_dir': {str(self.bot)!r}}})
                """
            ),
        )
        result = self._run_dag("fixture_gate", dag)
        combined = result.stdout + result.stderr
        reports = self.server.state["reports"]
        self.assertEqual(len(reports), 1)
        with sqlite3.connect(self.db) as db:
            xcom_rows = db.execute(
                "SELECT value FROM xcom WHERE dag_id = ? AND task_id = ?",
                ("fixture_gate", "run"),
            ).fetchall()
        self.assertTrue(xcom_rows)
        xcom_bytes = b"".join(
            value if isinstance(value, bytes) else str(value).encode()
            for (value,) in xcom_rows
        )
        self.assertNotIn(b"sensitive-report-body", xcom_bytes)
        self.assertNotIn(str(self.bot).encode(), xcom_bytes)
        self.assertNotIn(PASSWORD.encode(), xcom_bytes)
        for forbidden in (b'"payload"', b'"context"', b'"path"'):
            self.assertNotIn(forbidden, xcom_bytes)
        self.assertEqual(reports[0]["outcome"], "skipped")
        self.assertEqual(reports[0]["reason_code"], "gate_no_work")
        self.assertIsNone(reports[0]["payload"])
        for forbidden in ("create_session", "Direct database access via the ORM", "sensitive-report-body", PASSWORD):
            self.assertNotIn(forbidden, combined)
        projections = []
        for (value,) in xcom_rows:
            text = value.decode() if isinstance(value, bytes) else str(value)
            try:
                decoded = json.loads(text)
            except json.JSONDecodeError:
                continue
            if isinstance(decoded, dict):
                projections.append(decoded)
        self.assertTrue(projections, xcom_bytes)
        projection = projections[0]
        self.assertEqual(
            set(projection),
            {
                "report_projection_id",
                "envelope_digest",
                "envelope_bytes",
                "outcome",
                "retry_class",
                "failure_fingerprint",
                "execution",
                "reason_code",
            },
        )
        self.assertEqual(projection["outcome"], "skipped")
        self.assertEqual(projection["reason_code"], "gate_no_work")
        self.assertEqual(projection["retry_class"], "none")
    def test_command_deadline_is_timed_out_without_second_invocation_or_green_run(self):
        bot, counter = self._write_timeout_fixture()
        dag = self._write_runner_dag("timeout_fixture", bot)
        result = self._run_dag("timeout_fixture", dag, expect_success=False)
        self.assertNotEqual(result.returncode, 0)
        self.assertTrue(counter.exists(), result.stdout + result.stderr)
        self.assertEqual(len(self.server.state["reports"]), 1)
        self.assertEqual(self.server.state["reports"][0]["outcome"], "timed_out")
        self.assertNotEqual(self.server.state["reports"][0]["retry_class"], "capacity")
        self.assertIn("DagRun failed", result.stdout + result.stderr)
    def test_transient_claim_and_independent_execution_deadlines_cross_http_boundary(self):
        self.server.state["fail_once"]["/bot-dashboard/api/internal/runs/claim-budget"] = 1
        dag = self._write_dag(
            "deadline_boundary.py",
            textwrap.dedent(
                f"""
                from datetime import datetime, timezone
                from airflow import DAG
                from airflow.providers.standard.operators.python import PythonOperator
                from bots.provider_dashboard import DashboardClient, ControlPlaneError

                def boundary():
                    client = DashboardClient.from_environment()
                    identity = {{'dag_id': 'specialist', 'run_id': 'r1', 'task_id': 'run', 'map_index': -1}}
                    try:
                        client.claim_budget(identity, 60)
                    except ControlPlaneError as exc:
                        assert exc.retry_class == 'transient'
                        client.claim_budget(identity, 60)
                    executor_deadline = datetime(2099, 1, 1, tzinfo=timezone.utc)
                    reviewer_deadline = datetime(2099, 1, 2, tzinfo=timezone.utc)
                    client.claim_execution(dag_id='bot__task_executor', run_id='r1', conf={{'execution_id': 'e1'}}, kind='executor', deadline_at=executor_deadline)
                    client.claim_execution(dag_id='bot__pr_reviewer', run_id='r2', conf={{'execution_id': 'e1'}}, kind='reviewer', deadline_at=reviewer_deadline)

                with DAG('deadline_boundary', schedule=None, start_date=__import__('pendulum').datetime(2026,1,1,tz='UTC'), catchup=False) as dag:
                    run = PythonOperator(task_id='run', python_callable=boundary, retries=1)
                """
            ),
        )
        result = self._run_dag("deadline_boundary", dag)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        claim_routes = [
            route for route in self.server.state["routes"]
            if route.endswith("/runs/claim-budget")
        ]
        self.assertEqual(len(claim_routes), 2)
        self.assertEqual(self.server.state["claims"][("specialist", "r1", "run", -1)]["deadline_at"], "2099-01-01T00:01:00+00:00")
        execution_claims = self.server.state["execution_claims"]
        self.assertEqual(len(execution_claims), 2)
        self.assertEqual(execution_claims[0]["deadline_at"], "2099-01-01T00:00:00+00:00")
        self.assertEqual(execution_claims[1]["deadline_at"], "2099-01-02T00:00:00+00:00")

    def test_dispatch_and_maintenance_use_internal_http_and_deterministic_triggers(self):
        self.server.state["dispatch_items"] = [
            {"trigger_dag_id": "bot__task_executor", "conf": {"execution_id": "e1", "revision": 2}}
        ]
        self.server.state["maintenance"] = {
            "follow_up_bots": [
                {"trigger_dag_id": "bot__analytics_engineer", "conf": {"source": "e1"}}
            ],
            "metrics": {"observed": 1},
        }
        dag = self._write_dag(
            "control_plane_dag",
            textwrap.dedent(
                f"""
                import sys
                sys.path.insert(0, {str(REPO_ROOT / 'orchestration' / 'dags')!r})
                from airflow import DAG
                from airflow.providers.standard.operators.python import PythonOperator
                import provider_dashboard as source
                with DAG('control_plane', schedule=None, start_date=__import__('pendulum').datetime(2026,1,1,tz='UTC'), catchup=False) as dag:
                    dispatch = PythonOperator(task_id='dispatch', python_callable=lambda: source.claim_pending(20))
                    maintenance = PythonOperator(task_id='maintenance', python_callable=source.run_maintenance)
                """
            ),
        )
        result = self._run_dag("control_plane", dag)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("/bot-dashboard/api/internal/dispatch/claim", self.server.state["routes"])
        self.assertIn("/bot-dashboard/api/internal/maintenance/run", self.server.state["routes"])
        self.assertEqual(self.server.state["dispatch_items"][0]["conf"], {"execution_id": "e1", "revision": 2})
        self.assertEqual(self.server.state["maintenance"]["follow_up_bots"][0]["conf"], {"source": "e1"})
        self.assertNotIn("create_session", result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()
