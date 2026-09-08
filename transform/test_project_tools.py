"""Focused contract tests for raw synchronization and manifest policy."""

from __future__ import annotations

import pathlib
import sys
import tempfile
import unittest

import duckdb

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from scripts.sync_raw_sources import Column, RawSource, audit, inspect_warehouse, write_all
from scripts.validate_project import validate_manifest


def _source(name: str = "demo", columns: tuple[Column, ...] | None = None) -> RawSource:
    return RawSource(
        name=name,
        columns=columns or (Column("id", "VARCHAR"), Column("_loaded_at", "TIMESTAMP")),
        rows_loaded=3,
        last_loaded_at="2026-09-05T00:00:00",
    )


def _model(
    name: str,
    path: str,
    materialized: str,
    dependencies: list[str],
    *,
    tags: list[str] | None = None,
    final: bool = False,
) -> dict:
    return {
        "name": name,
        "resource_type": "model",
        "package_name": "vintage_data",
        "original_file_path": path,
        "depends_on": {"nodes": dependencies},
        "tags": tags or [],
        "description": "A governed model." if final else "A base view.",
        "meta": {"grain": "id"} if final else {},
        "columns": {
            "id": {
                "name": "id",
                "description": "Stable identifier.",
                "data_type": "VARCHAR",
            }
        },
        "config": {
            "materialized": materialized,
            "tags": tags or [],
            "meta": {"grain": "id"} if final else {},
            "contract": {"enforced": final},
        },
    }


def _valid_manifest() -> dict:
    source_uid = "source.vintage_data.raw.demo"
    base_uid = "model.vintage_data.base_demo"
    fact_uid = "model.vintage_data.fct_demo"
    return {
        "sources": {
            source_uid: {
                "name": "demo",
                "source_name": "raw",
                "resource_type": "source",
                "package_name": "vintage_data",
            }
        },
        "nodes": {
            base_uid: _model(
                "base_demo", "models/base/base_demo.sql", "view", [source_uid]
            ),
            fact_uid: _model(
                "fct_demo",
                "models/marts/demo/fct_demo.sql",
                "table",
                [base_uid],
                tags=["hourly"],
                final=True,
            ),
        },
    }


class RawSourceSyncTest(unittest.TestCase):
    def test_write_all_creates_source_declaration_and_base(self):
        with tempfile.TemporaryDirectory() as tmp:
            project = pathlib.Path(tmp)
            changed = write_all(project, [_source()])
            self.assertEqual(
                changed,
                ["models/base/_raw_sources.yml", "models/base/base_demo.sql"],
            )
            self.assertTrue(audit(project, [_source()])["ok"])
            sql = (project / "models/base/base_demo.sql").read_text()
            self.assertIn("source('raw', 'demo')", sql)
            self.assertIn('"_loaded_at"', sql)

    def test_existing_custom_base_is_preserved(self):
        with tempfile.TemporaryDirectory() as tmp:
            project = pathlib.Path(tmp)
            base = project / "models/base/base_demo.sql"
            base.parent.mkdir(parents=True)
            base.write_text("select 'custom' as id\n")
            write_all(project, [_source()])
            self.assertEqual(base.read_text(), "select 'custom' as id\n")
            self.assertTrue(audit(project, [_source()])["ok"])

    def test_schema_drift_reports_actual_and_declared_columns(self):
        with tempfile.TemporaryDirectory() as tmp:
            project = pathlib.Path(tmp)
            write_all(project, [_source()])
            changed = _source(columns=(Column("id", "BIGINT"),))
            result = audit(project, [changed])
            self.assertFalse(result["ok"])
            self.assertEqual(result["schema_drift"][0]["source"], "demo")
            self.assertEqual(
                result["schema_drift"][0]["actual_columns"],
                [{"name": "id", "data_type": "BIGINT"}],
            )

    def test_inspection_uses_load_ledger_metadata(self):
        with tempfile.TemporaryDirectory() as tmp:
            database = pathlib.Path(tmp) / "fixture.duckdb"
            connection = duckdb.connect(str(database))
            connection.execute("create schema raw")
            connection.execute("create table raw.demo(id varchar, _loaded_at timestamp)")
            connection.execute("create schema _load")
            connection.execute(
                "create table _load.files(table_name varchar, rows_loaded bigint, "
                "loaded_at timestamptz, status varchar)"
            )
            connection.execute(
                "insert into _load.files values "
                "('demo', 2, '2026-09-05 00:00:00+00', 'loaded'), "
                "('demo', 1, '2026-09-05 01:00:00+00', 'loaded')"
            )
            connection.close()
            inspected = inspect_warehouse(database)
            self.assertEqual(inspected[0].rows_loaded, 3)
            self.assertIn("2026-09-05", inspected[0].last_loaded_at or "")

    def test_invalid_raw_relation_name_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            database = pathlib.Path(tmp) / "fixture.duckdb"
            connection = duckdb.connect(str(database))
            connection.execute("create schema raw")
            connection.execute('create table raw."Bad Name"(id integer)')
            connection.execute("create schema _load")
            connection.execute(
                "create table _load.files(table_name varchar, rows_loaded bigint, "
                "loaded_at timestamptz, status varchar)"
            )
            connection.close()
            with self.assertRaisesRegex(ValueError, "not valid dbt model names"):
                inspect_warehouse(database)


class ManifestPolicyTest(unittest.TestCase):
    def test_valid_graph_passes(self):
        self.assertEqual(validate_manifest(_valid_manifest()), [])

    def test_only_base_model_may_consume_raw_source(self):
        manifest = _valid_manifest()
        source_uid = "source.vintage_data.raw.demo"
        manifest["nodes"]["model.vintage_data.fct_demo"]["depends_on"]["nodes"].append(source_uid)
        errors = validate_manifest(manifest)
        self.assertTrue(any("only models/base" in error for error in errors))

    def test_base_must_be_view(self):
        manifest = _valid_manifest()
        manifest["nodes"]["model.vintage_data.base_demo"]["config"]["materialized"] = "table"
        errors = validate_manifest(manifest)
        self.assertTrue(any("base models must be materialized as views" in error for error in errors))

    def test_final_requires_physical_materialization_and_one_cadence(self):
        manifest = _valid_manifest()
        fact = manifest["nodes"]["model.vintage_data.fct_demo"]
        fact["config"]["materialized"] = "view"
        fact["tags"] = ["hourly", "daily"]
        fact["config"]["tags"] = ["hourly", "daily"]
        errors = validate_manifest(manifest)
        self.assertTrue(any("table or incremental" in error for error in errors))
        self.assertTrue(any("exactly one cadence tag" in error for error in errors))

    def test_data_test_may_not_target_view(self):
        manifest = _valid_manifest()
        manifest["nodes"]["test.vintage_data.not_null_base_demo_id"] = {
            "name": "not_null_base_demo_id",
            "resource_type": "test",
            "package_name": "vintage_data",
            "depends_on": {"nodes": ["model.vintage_data.base_demo"]},
        }
        errors = validate_manifest(manifest)
        self.assertTrue(any("data tests may target only" in error for error in errors))


if __name__ == "__main__":
    unittest.main()
