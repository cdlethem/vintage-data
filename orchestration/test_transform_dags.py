"""Regression tests for isolated transform DAG construction."""

from __future__ import annotations

import importlib.util
import pathlib
import sys
import unittest
from unittest import mock

INCLUDE = pathlib.Path(__file__).resolve().parent / "include"
sys.path.insert(0, str(INCLUDE))

import transform_runner


class TransformDagTest(unittest.TestCase):
    def test_one_dag_per_discovered_family_and_cadence(self):
        jobs = {
            "hourly": {"schedule": "17 * * * *", "timeout_minutes": 50},
            "daily": {"schedule": "47 3 * * *", "timeout_minutes": 240},
        }
        families = [
            {"family": "healthy", "cadences": ["hourly", "daily"]},
            # An extractor failure remains scheduled for that family rather than
            # preventing construction of its healthy sibling's DAGs.
            {"family": "broken", "cadences": ["hourly"], "error": "bad jinja"},
        ]
        path = pathlib.Path(__file__).resolve().parent / "dags" / "transform_dags.py"
        spec = importlib.util.spec_from_file_location("transform_dags_fixture", path)
        assert spec and spec.loader
        module = importlib.util.module_from_spec(spec)
        with mock.patch.object(transform_runner, "load_jobs", return_value=jobs), mock.patch.object(
            transform_runner, "discover_families", return_value=families
        ):
            spec.loader.exec_module(module)
        self.assertEqual(
            {"transform__healthy__hourly", "transform__healthy__daily", "transform__broken__hourly"},
            {
                name
                for name, value in vars(module).items()
                if name.startswith("transform__") and getattr(value, "dag_id", None) == name
            },
        )
        dag = module.transform__healthy__hourly
        self.assertEqual(
            {task.task_id: sorted(task.downstream_task_ids) for task in dag.tasks},
            {"build_and_publish": ["sync_lightdash"], "sync_lightdash": []},
        )
        build = dag.get_task("build_and_publish")
        self.assertEqual(build.op_kwargs, {"job": "hourly", "family": "healthy"})
        # The build allowance plus the retired publication task's allowance.
        self.assertEqual(build.execution_timeout.total_seconds(), (50 + 25) * 60)
        self.assertEqual(dag.get_task("sync_lightdash").execution_timeout.total_seconds(), 30 * 60)
        self.assertEqual(dag.default_args["retries"], 2)


if __name__ == "__main__":
    unittest.main()
