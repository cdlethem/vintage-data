"""Focused tests for immutable sync release and receipt behavior."""
from __future__ import annotations

import io
import contextlib
import json
import os
import pathlib
import subprocess
import tempfile
import unittest
from unittest import mock

from visualization import cli, project


class SyncReceiptTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = pathlib.Path(self.tmp.name)
        self.batch = self.root / "batch"
        self.batch.mkdir()
        self.manifest = {
            "metadata": {"vintage_scope": {"family": "demo", "cadence": "daily"}},
            "nodes": {
                "model.vintage_data.fct_demo": {
                    "name": "fct_demo", "resource_type": "model", "package_name": "vintage_data",
                    "original_file_path": "models/marts/demo/fct_demo.sql",
                    "config": {"tags": ["daily"], "enabled": True}, "columns": {},
                }
            },
        }
        (self.batch / "manifest.json").write_text(json.dumps(self.manifest))
        (self.batch / "batch.json").write_text(json.dumps({
            "batch_id": "a" * 64, "vintage_scope": self.manifest["metadata"]["vintage_scope"],
            "tables": [{"table": "fct_demo"}],
        }))
        self.info = {"database": "serving", "schema": "marts"}
        self.release = project.digest({"manifest": "semantic", "content": "content"})
        cli.release_state_path(self.root).parent.mkdir()
        cli.release_state_path(self.root).write_text(json.dumps({
            "schema_version": 1, "project_uuid": "project-one",
            "models": {"fct_demo": {"batch_id": "a" * 64}},
        }))
        self.env = mock.patch.dict(os.environ, {
            "LIGHTDASH_PROJECT": "project-one",
            "LIGHTDASH_URL": "http://lightdash.test",
            "LIGHTDASH_API_KEY": "fixture-token",
        }, clear=False)
        self.env.start()
        self.addCleanup(self.env.stop)


    def sync_patches(self):
        return (
            mock.patch.object(cli, "load_environment"),
            mock.patch.object(cli, "immutable_batch", return_value=self.batch),
            mock.patch.object(cli.publisher, "state_root", return_value=self.root),
            mock.patch.object(cli.publisher, "deployment_lock", return_value=contextlib.nullcontext()),
            mock.patch.object(cli.publisher, "publication_state", return_value={"current": ["fct_demo"], "stale": [], "missing": []}),
            mock.patch.object(cli.publisher, "serving_model_ledger", return_value={"fct_demo": {"batch_id": "a" * 64, "sequence": 1}}),
            mock.patch.object(cli.content, "write_content", return_value=[]),
            mock.patch.object(cli, "write_scoped_upload", return_value={"fixture"}),
            mock.patch.object(cli.project, "content_digest", return_value="content"),
            mock.patch.object(cli.project, "serving_manifest_digest", return_value="semantic"),
            mock.patch.object(cli.project, "bundle", return_value=self.info),
        )

    def invoke(self):
        with mock.patch.object(cli.sys, "argv", ["viz", "sync", "--batch", "a" * 64]):
            cli.main()

    def test_matching_current_receipt_skips_deployment(self):
        receipt = cli.receipt_path(self.root)
        receipt.parent.mkdir(exist_ok=True)
        receipt.write_text(json.dumps({
            "status": "ok",
            "release_sha256": self.release,
            "project_uuid": "project-one",
            "deployment_config": cli.deployment_config("project-one", self.info),
        }))
        with contextlib.ExitStack() as stack:
            for patch in self.sync_patches():
                stack.enter_context(patch)
            run = stack.enter_context(mock.patch.object(cli.subprocess, "run"))
            self.invoke()
        run.assert_not_called()

    def test_prior_release_receipt_is_retained_on_failed_retry(self):
        receipt = cli.receipt_path(self.root)
        receipt.parent.mkdir(exist_ok=True)
        receipt.write_text(json.dumps({
            "status": "ok",
            "release_sha256": "b" * 64,
            "project_uuid": "project-one",
            "deployment_config": cli.deployment_config("project-one", self.info),
        }))
        with contextlib.ExitStack() as stack:
            for patch in self.sync_patches():
                stack.enter_context(patch)
            run = stack.enter_context(mock.patch.object(
                cli.subprocess,
                "run",
                side_effect=subprocess.CalledProcessError(1, ["compose"]),
            ))
            with self.assertRaises(subprocess.CalledProcessError):
                self.invoke()
        run.assert_called_once()
        self.assertTrue(receipt.exists())
    def test_noisy_deploy_output_stays_off_worker_json_stdout(self):
        destination = self.root / "bundles" / self.release
        destination.mkdir(parents=True)
        (destination / "deployment.json").write_text(json.dumps({"project_uuid": "project-one"}))
        stdout, stderr = io.StringIO(), io.StringIO()
        with contextlib.ExitStack() as stack:
            for patch in self.sync_patches():
                stack.enter_context(patch)
            stack.enter_context(mock.patch.object(
                cli.subprocess,
                "run",
                return_value=subprocess.CompletedProcess(["compose"], 0, "verbose deploy log\n", "warning\n"),
            ))
            stack.enter_context(mock.patch("visualization.verify_queries.verify", return_value={"ok": True, "queried": 1, "failures": []}))
            stack.enter_context(mock.patch("sys.stdout", stdout))
            stack.enter_context(mock.patch("sys.stderr", stderr))
            self.invoke()
        self.assertNotIn("verbose deploy log", stdout.getvalue())
        self.assertIn("verbose deploy log", stderr.getvalue())
        self.assertTrue(cli.receipt_path(self.root).is_file())


class AggregateServingStateTest(unittest.TestCase):
    @staticmethod
    def mart(name: str, family: str, cadence: str) -> dict:
        return {
            "name": name, "resource_type": "model", "package_name": "vintage_data",
            "original_file_path": f"models/marts/{family}/{name}.sql",
            "config": {"tags": [cadence], "enabled": True}, "columns": {},
        }

    def test_aggregate_keeps_last_good_mixed_cadence_models_and_omits_unbuilt_siblings(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            alpha = root / "alpha"
            beta = root / "beta"
            for path, batch_id, cadence, nodes, table in (
                (alpha, "a" * 64, "hourly", {"model.a": self.mart("fct_demo_hourly", "demo", "hourly")}, "fct_demo_hourly"),
                (beta, "b" * 64, "daily", {
                    "model.b": self.mart("fct_demo_daily", "demo", "daily"),
                    "model.future": self.mart("fct_demo_future", "demo", "daily"),
                }, "fct_demo_daily"),
            ):
                path.mkdir()
                manifest = {"metadata": {"vintage_scope": {"family": "demo", "cadence": cadence}}, "nodes": nodes}
                (path / "manifest.json").write_text(json.dumps(manifest))
                (path / "batch.json").write_text(json.dumps({
                    "batch_id": batch_id, "vintage_scope": manifest["metadata"]["vintage_scope"],
                    "tables": [{"table": table}],
                }))
            state = {"schema_version": 1, "project_uuid": "project", "models": {
                "fct_demo_hourly": {"batch_id": "a" * 64},
                "fct_demo_daily": {"batch_id": "b" * 64},
            }}
            with mock.patch.object(cli, "immutable_batch", side_effect=lambda value: {"a" * 64: alpha, "b" * 64: beta}[value]):
                aggregate = cli.aggregate_manifest(root, state)
            self.assertEqual(
                {node["name"] for node in project.mart_nodes(aggregate).values()},
                {"fct_demo_hourly", "fct_demo_daily"},
            )

    def test_existing_project_requires_matching_bundle_baseline(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            batch = root / "batch"
            batch.mkdir()
            manifest = {"metadata": {"vintage_scope": {"family": "beta", "cadence": "daily"}}, "nodes": {
                "model.beta": self.mart("fct_beta", "beta", "daily"),
            }}
            (batch / "manifest.json").write_text(json.dumps(manifest))
            (batch / "batch.json").write_text(json.dumps({
                "batch_id": "b" * 64, "vintage_scope": manifest["metadata"]["vintage_scope"],
                "tables": [{"table": "fct_beta"}],
            }))
            with self.assertRaisesRegex(ValueError, "reliable local aggregate baseline"):
                cli.release_state(root, "existing-project", {"fct_alpha": {"batch_id": "old", "sequence": 1}}, batch)

    def test_adopts_verified_legacy_bundle_without_content_snapshot(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            legacy_manifest = {"metadata": {}, "nodes": {
                "model.alpha": self.mart("fct_alpha", "alpha", "daily"),
            }}
            release = project.digest(legacy_manifest)
            bundle = root / "bundles" / release
            bundle.mkdir(parents=True)
            (bundle / "source-manifest.json").write_text(json.dumps(legacy_manifest))
            (bundle / "bundle.json").write_text(json.dumps({
                "release_sha256": release, "source_manifest_sha256": project.digest(legacy_manifest),
                "database": "vintage_serving", "schema": "transform_marts", "mart_count": 1,
            }))
            (bundle / "deployment.json").write_text(json.dumps({"project_uuid": "existing-project"}))
            (bundle / "validation.json").write_text(json.dumps({"project_uuid": "existing-project", "status": "ok"}))
            batch = root / "batch"
            batch.mkdir()
            current = {"metadata": {"vintage_scope": {"family": "beta", "cadence": "daily"}}, "nodes": {
                "model.beta": self.mart("fct_beta", "beta", "daily"),
            }}
            (batch / "manifest.json").write_text(json.dumps(current))
            (batch / "batch.json").write_text(json.dumps({
                "batch_id": "b" * 64, "vintage_scope": current["metadata"]["vintage_scope"],
                "tables": [{"table": "fct_beta"}],
            }))
            serving = {
                "fct_alpha": {"batch_id": "old", "sequence": 1, "schema_sha256": project.digest([])},
                "fct_beta": {"batch_id": "b" * 64, "sequence": 2, "schema_sha256": project.digest([])},
            }
            state = cli.release_state(root, "existing-project", serving, batch)
            aggregate = cli.aggregate_manifest(root, state)
            self.assertEqual(
                {node["name"] for node in project.mart_nodes(aggregate).values()},
                {"fct_alpha"},
            )
            serving["fct_alpha"]["schema_sha256"] = project.digest(["unrelated schema change"])
            with self.assertRaises(ValueError):
                cli.release_state(root, "existing-project", serving, batch)
class ServingManifestDigestTest(unittest.TestCase):
    def test_dbt_invocation_metadata_does_not_change_release(self):
        first = {"metadata": {"invocation_id": "one", "generated_at": "now", "invocation_started_at": "first", "run_started_at": "first", "user_id": "first-runner"}, "nodes": {"model.x": {"created_at": 1, "name": "x", "compiled_path": "target/one.sql", "build_path": "target/run/one.sql"}}}
        second = {"metadata": {"invocation_id": "two", "generated_at": "later", "invocation_started_at": "second", "run_started_at": "second", "user_id": "second-runner"}, "nodes": {"model.x": {"created_at": 2, "name": "x", "compiled_path": "target/two.sql", "build_path": "target/run/two.sql"}}}
        self.assertEqual(project.serving_manifest_digest(first), project.serving_manifest_digest(second))
        semantic_column = json.loads(json.dumps(first))
        semantic_column["nodes"]["model.x"]["columns"] = {"created_at": {"name": "changed"}}
        self.assertNotEqual(project.serving_manifest_digest(first), project.serving_manifest_digest(semantic_column))
        semantic_metric = json.loads(json.dumps(first))
        semantic_metric["nodes"]["model.x"]["config"] = {"meta": {"metrics": {"user_id": {"type": "count"}}}}
        self.assertNotEqual(project.serving_manifest_digest(first), project.serving_manifest_digest(semantic_metric))
        second["nodes"]["model.x"]["name"] = "changed"
        self.assertNotEqual(project.serving_manifest_digest(first), project.serving_manifest_digest(second))


@unittest.skipUnless(os.environ.get("VINTAGE_TEST_ENV_FILE"), "set VINTAGE_TEST_ENV_FILE to an isolated test stack")
class DiscardExportDataCommandTest(unittest.TestCase):
    """The operator command takes every lock and defaults to a dry run."""

    def invoke(self, *arguments):
        stdout = io.StringIO()
        with mock.patch.object(cli.sys, "argv", ["viz", "discard-export-data", *arguments]), \
                mock.patch("sys.stdout", stdout), self.assertRaises(SystemExit) as exit:
            cli.main()
        return exit.exception.code, json.loads(stdout.getvalue())

    def test_dry_run_then_apply_removes_only_the_retired_payload(self):
        values = dict(
            line.split("=", 1) for line in
            pathlib.Path(os.environ["VINTAGE_TEST_ENV_FILE"]).read_text().splitlines()
            if line and not line.startswith("#"))
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory) / "state"
            batch = root / "batches" / ("a" * 64)
            batch.mkdir(parents=True)
            (batch / "fct_demo.csv").write_text("x" * 4096)
            (batch / "manifest.json").write_text(json.dumps({"nodes": {}}))
            (batch / "batch.json").write_text(json.dumps({
                "schema_version": 1, "batch_id": "a" * 64, "sequence": 1,
                "captured_at": "2026-09-07T00:00:00+00:00",
                "tables": [{"table": "fct_demo", "file": "fct_demo.csv", "sha256": "0" * 64,
                            "columns": [], "schema_sha256": "abc", "rows": 1}]}))
            environment = {
                "LIGHTDASH_STATE_ROOT": str(root),
                "EXTRACT_DATA_ROOT": directory,
                "LIGHTDASH_PG_HOST": "127.0.0.1",
                "LIGHTDASH_PG_PORT": values["LIGHTDASH_PG_PORT"],
                "LIGHTDASH_SERVING_DB": values.get("LIGHTDASH_SERVING_DB", "vintage_serving"),
                "LIGHTDASH_PUBLISHER_PASSWORD": values["LIGHTDASH_PUBLISHER_PASSWORD"],
            }
            with mock.patch.dict(os.environ, environment), mock.patch.object(cli, "load_environment"):
                code, report = self.invoke()
                self.assertEqual((code, report["candidate_files"], report["removed_files"]), (0, 1, 0))
                self.assertTrue((batch / "fct_demo.csv").is_file())
                code, report = self.invoke("--apply")
            self.assertEqual((code, report["removed_files"]), (0, 1))
            self.assertFalse((batch / "fct_demo.csv").exists())
            self.assertTrue((batch / "batch.json").is_file())
            self.assertTrue((root / "deployment.lock").is_file())
            self.assertTrue((root / "capture.lock").is_file())
