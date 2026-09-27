"""Focused contract tests for raw synchronization and manifest policy."""

from __future__ import annotations

import pathlib
import sys
import tempfile
import unittest

import yaml
import duckdb

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from scripts.family_project import FamilyProjectError, create, discover
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


class FamilyProjectTest(unittest.TestCase):
    def _fixture(self, root: pathlib.Path) -> None:
        (root / "models/base").mkdir(parents=True)
        (root / "models/marts/healthy").mkdir(parents=True)
        (root / "models/marts/broken").mkdir(parents=True)
        (root / "dbt_project.yml").write_text(
            "name: vintage_data\nversion: '1.0'\nconfig-version: 2\nprofile: vintage_data\nmodel-paths: [models]\n"
        )
        (root / "profiles.yml").write_text(
            "vintage_data:\n  target: dev\n  outputs: {dev: {type: duckdb, path: ':memory:'}}\n"
        )
        (root / "models/base/_raw_sources.yml").write_text(
            "version: 2\nsources:\n"
            "  - name: raw\n    schema: raw\n    tables:\n"
            "      - name: alias_template\n"
            "        columns: &shared_columns\n"
            "          - {name: id, data_type: varchar}\n"
            "      - name: demo\n        columns: *shared_columns\n"
            "      - name: unrelated\n"
            "        columns: [{name: id, data_type: varchar}]\n"
        )
        (root / "models/base/base_demo.sql").write_text(
            "select id from {{ source('raw', 'demo') }}\n"
        )
        (root / "models/marts/healthy/fct_good.sql").write_text(
            "{{ config(materialized='table', tags=['hourly'], contract={'enforced': true}) }} "
            "select * from {{ ref('base_demo') }}\n"
        )
        (root / "models/marts/healthy/fct_future.sql").write_text(
            "{{ config(tags=['daily']) }} select * from {{ ref('base_demo') }}\n"
        )
        (root / "models/marts/healthy/_models.yml").write_text(
            "version: 2\nmodels:\n"
            "- name: fct_good\n  description: good\n  columns: [{name: id, description: id, data_type: varchar}]\n"
            "- name: fct_future\n  description: future\n  columns: [{name: id, description: id, data_type: varchar}]\n"
        )
        (root / "models/marts/broken/fct_bad.sql").write_text("{{ ref(\n")
        (root / "models/marts/broken/_models.yml").write_text("models: [\n")

    def test_selected_family_ignores_unrelated_sql_yaml_and_policy(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp) / "project"
            destination = pathlib.Path(tmp) / "isolated"
            self._fixture(root)
            result = create(root, "healthy", "hourly", destination)
            self.assertEqual(result["selected_models"], ["fct_good"])
            self.assertEqual(result["dependency_models"], ["base_demo"])
            self.assertEqual(result["raw_sources"], ["demo"])
            self.assertTrue((destination / "models/marts/healthy/fct_good.sql").exists())
            self.assertFalse((destination / "models/marts/healthy/fct_future.sql").exists())
            self.assertFalse((destination / "models/marts/broken").exists())
            self.assertIn("fct_good", (destination / "models/marts/healthy/_models.yml").read_text())
            self.assertNotIn("fct_future", (destination / "models/marts/healthy/_models.yml").read_text())
            raw_schema = (destination / "models/base/_raw_sources.yml").read_text()
            raw_document = yaml.safe_load(raw_schema)
            tables = raw_document["sources"][0]["tables"]
            self.assertEqual([table["name"] for table in tables], ["demo"])
            self.assertEqual(tables[0]["columns"], [{"name": "id", "data_type": "varchar"}])
            self.assertNotIn("name: unrelated", raw_schema)

    def test_broken_family_is_discoverable_and_fails_in_its_own_project(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp) / "project"
            self._fixture(root)
            families = {entry["family"]: entry for entry in discover(root)}
            self.assertEqual(families["healthy"]["cadences"], ["daily", "hourly"])
            self.assertEqual(families["broken"]["cadences"], ["twice_hourly", "hourly", "daily"])
            self.assertIn("error", families["broken"])
            with self.assertRaises(FamilyProjectError):
                create(root, "broken", "hourly", pathlib.Path(tmp) / "broken")


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

    def test_retired_source_cannot_be_declared_or_modelled(self):
        # A family whose raw table, base view and mart are all otherwise valid is
        # still rejected once its source is retired.
        errors = validate_manifest(_valid_manifest(), retired={"demo"})
        self.assertTrue(any("raw source 'demo' is retired" in error for error in errors))
        self.assertTrue(any(error.startswith("base_demo: belongs to a source retired") for error in errors))
        self.assertEqual(validate_manifest(_valid_manifest(), retired={"other"}), [])


if __name__ == "__main__":
    unittest.main()
