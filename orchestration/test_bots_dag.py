"""Airflow boundary contracts for generated bot DAGs."""

from __future__ import annotations

import importlib
import sys
import unittest
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

# ``dags`` is an Airflow DAG folder, not an installed Python package.  Keep
# this test runnable from the repository root as ``orchestration.test_bots_dag``.
_DAGS_ROOT = Path(__file__).resolve().parent / "dags"
if str(_DAGS_ROOT) not in sys.path:
    sys.path.insert(0, str(_DAGS_ROOT))

import bots_dag  # noqa: E402


_IDENTITY = {
    "dag_id": "bot__fixture",
    "run_id": "manual__fixture",
    "task_id": "run",
    "map_index": -1,
    "try_number": 1,
}


def _result(*, outcome: str, retry_class: str = "none", reason_code: str = "ok"):
    return SimpleNamespace(
        outcome=outcome,
        retry_class=retry_class,
        reason_code=reason_code,
        xcom=lambda: {
            "report_projection_id": 17,
            "envelope_digest": "a" * 64,
            "envelope_bytes": 128,
            "outcome": outcome,
            "retry_class": retry_class,
            "reason_code": reason_code,
            "execution": _IDENTITY,
        },
    )


class RunTranslationTest(unittest.TestCase):
    def setUp(self):
        self.context = {
            "ti": SimpleNamespace(
                dag_id=_IDENTITY["dag_id"],
                task_id="run",
                map_index=-1,
                try_number=1,
            ),
            "dag_run": SimpleNamespace(run_id=_IDENTITY["run_id"]),
        }
        self.cfg = {
            "name": "fixture",
            "capacity_policy": "skip",
            "dir": "/tmp/fixture",
        }

    def _run_with(self, result):
        with (
            mock.patch.object(bots_dag.bot_runner, "load_bot", return_value=self.cfg),
            mock.patch.object(bots_dag, "get_current_context", return_value=self.context),
            mock.patch.object(bots_dag.bot_runner, "run", return_value=result) as run,
        ):
            value = bots_dag._run("/tmp/fixture")
        run.assert_called_once_with(self.cfg, identity=_IDENTITY)
        return value

    def test_success_skipped_and_capacity_skip_are_bounded_green_projection(self):
        for outcome, reason in (
            ("succeeded", "model_succeeded"),
            ("skipped", "gate_no_work"),
            ("capacity_unavailable", "provider_busy"),
        ):
            with self.subTest(outcome=outcome):
                projection = self._run_with(
                    _result(outcome=outcome, reason_code=reason)
                )
                self.assertEqual(projection["outcome"], outcome)
                self.assertNotIn("payload", projection)
                self.assertNotIn("context", projection)
                self.assertNotIn("path", projection)
                self.assertLessEqual(len(repr(projection)), 4096)

    def test_selected_capacity_or_transient_retry_is_airflow_retry(self):
        for retry_class in ("capacity", "transient"):
            with self.subTest(retry_class=retry_class), self.assertRaises(
                bots_dag.AirflowException
            ) as raised:
                self._run_with(
                    _result(
                        outcome="failed",
                        retry_class=retry_class,
                        reason_code=f"{retry_class}_selected",
                    )
                )
            self.assertEqual(str(raised.exception), f"{retry_class}_selected")

    def test_terminal_outcomes_are_non_retryable_airflow_failures(self):
        for outcome in ("timed_out", "failed"):
            with self.subTest(outcome=outcome), self.assertRaises(
                bots_dag.AirflowFailException
            ):
                self._run_with(
                    _result(outcome=outcome, reason_code="deadline_exhausted")
                )


class GateAndDagFactoryTest(unittest.TestCase):
    def test_pending_work_runs_its_own_typed_gate(self):
        cfg = {
            "name": "analytics",
            "gate": {
                "context": "ANALYTICS_CONTEXT",
                "path": "selected_source",
                "operator": "equals",
                "value": None,
                "reason_code": "no_source",
            },
        }
        with (
            mock.patch.object(bots_dag.bot_runner, "load_bot", return_value=cfg),
            mock.patch.object(
                bots_dag.bot_runner, "work_pending", return_value=True
            ) as work_pending,
        ):
            self.assertTrue(bots_dag._has_work("/bots/analytics"))
        work_pending.assert_called_once_with(cfg)

    def test_no_pending_work_suppresses_trigger(self):
        cfg = {"name": "analytics", "gate": {"operator": "equals"}}
        with (
            mock.patch.object(bots_dag.bot_runner, "load_bot", return_value=cfg),
            mock.patch.object(
                bots_dag.bot_runner, "work_pending", return_value=False
            ) as work_pending,
        ):
            self.assertFalse(bots_dag._has_work("/bots/analytics"))
        work_pending.assert_called_once_with(cfg)
    def test_generated_dags_have_retry_deadline_and_executor_routing(self):
        configs = {
            "source_discovery": {
                "name": "source_discovery",
                "schedule": "17 */6 * * *",
                "enabled": True,
                "timeout_minutes": 30,
                "triggers": ["source_vetting"],
            },
            "source_vetting": {
                "name": "source_vetting",
                "schedule": "47 */6 * * *",
                "enabled": True,
                "timeout_minutes": 35,
                "triggers": [],
            },
            "task_executor": {
                "name": "task_executor",
                "schedule": "manual",
                "enabled": True,
                "timeout_minutes": 60,
                "triggers": [],
            },
            "pr_reviewer": {
                "name": "pr_reviewer",
                "schedule": "manual",
                "enabled": True,
                "timeout_minutes": 45,
                "triggers": [],
            },
        }
        paths = [Path(f"/fixture/{name}/bot.yml") for name in configs]

        def fake_load(path):
            name = Path(path).parent.name
            return {**configs[name], "dir": str(Path("/fixture") / name)}

        with (
            mock.patch.object(bots_dag.provider_dashboard, "enabled", return_value=True),
            mock.patch.object(bots_dag.bot_runner, "load_models", return_value={}),
            mock.patch.object(bots_dag.bot_runner, "discover", return_value=paths),
            mock.patch.object(bots_dag.bot_runner, "load_bot", side_effect=fake_load),
            mock.patch.object(bots_dag.bot_runner, "resolve_bot_models", return_value=[]),
        ):
            generated = importlib.reload(bots_dag)

        for name, cfg in configs.items():
            dag = getattr(generated, f"bot__{name}")
            task = dag.get_task("run")
            self.assertEqual(task.retries, 1)
            self.assertEqual(
                task.execution_timeout,
                timedelta(minutes=cfg["timeout_minutes"] + 2),
            )
            if name in {"task_executor", "pr_reviewer"}:
                self.assertEqual(task.pool, "bot_dashboard_executor")
                self.assertEqual(task.queue, "bot_dashboard_executor")
            else:
                self.assertEqual(task.pool, "default_pool")
                self.assertEqual(task.queue, "default")

        discovery = generated.bot__source_discovery
        self.assertEqual(
            [task.task_id for task in discovery.tasks if task.task_id.startswith("trigger__")],
            ["trigger__source_vetting"],
        )
        gate = discovery.get_task("has_work__source_vetting")
        self.assertEqual(gate.op_kwargs["bot_dir"], str(_DAGS_ROOT.parent.parent / "bots" / "source_vetting"))
        self.assertEqual(
            discovery.get_task("trigger__source_vetting").upstream_task_ids,
            {"has_work__source_vetting"},
        )


if __name__ == "__main__":
    unittest.main()
