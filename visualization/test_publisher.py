"""Streaming publication tests use an explicitly selected disposable Postgres."""
import copy
import json
import os
import pathlib
import tempfile
import unittest
import uuid
from unittest import mock
from decimal import Decimal

import duckdb

from visualization import project, publisher

COLUMNS = [
    ("id", "varchar"),
    ("amount", "decimal(20,2)"),
    ("text_value", "varchar"),
    ("observed_at", "timestamp with time zone"),
    ("observed_on", "date"),
    ("payload", "json"),
    ("ratio", "double"),
    ("flag", "boolean"),
]
DDL = "id varchar, amount decimal(20,2), text_value varchar, observed_at timestamptz, observed_on date, payload json, ratio double, flag boolean"
AWKWARD = 'tab\there,\\N,quote"\nnewline\r carriage back\\slash Unicode café'


def mart(name: str, columns=COLUMNS) -> tuple[str, dict]:
    uid = "model.vintage_data." + name
    return uid, {
        "unique_id": uid, "resource_type": "model", "package_name": "vintage_data",
        "name": name, "alias": name, "schema": "transform_marts",
        "original_file_path": f"models/marts/fixture/{name}.sql",
        "config": {"materialized": "table", "tags": ["daily"]},
        "columns": {key: {"name": key, "data_type": data_type} for key, data_type in columns},
    }


def fixture(*names: str) -> tuple[dict, dict]:
    nodes = dict(mart(name) for name in names)
    manifest = {
        "metadata": {"invocation_id": "one", "adapter_type": "duckdb",
                     "vintage_scope": {"family": "fixture", "cadence": "daily"}},
        "nodes": nodes, "sources": {},
    }
    results = {"metadata": {"invocation_id": "one"},
               "results": [{"unique_id": uid, "status": "success"} for uid in nodes]}
    return manifest, results


class AcceptanceTest(unittest.TestCase):
    """Selection and validation refuse a publication before any transfer."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = pathlib.Path(self.tmp.name)
        self.manifest, self.results = fixture("fct_fixture")

    def test_failed_or_mismatched_build_cannot_publish(self):
        self.results["results"][0]["status"] = "error"
        with self.assertRaisesRegex(ValueError, "fully verified marts"):
            publisher.publish(self.manifest, self.results, self.root / "warehouse.duckdb", self.root / "state")
        self.assertFalse((self.root / "state").exists())
        self.results["results"][0]["status"] = "success"
        self.results["metadata"]["invocation_id"] = "different"
        with self.assertRaisesRegex(ValueError, "same dbt invocation"):
            publisher.publish(self.manifest, self.results, self.root / "warehouse.duckdb", self.root / "state")
        self.assertFalse((self.root / "state").exists())

    def test_successful_mart_is_published_despite_failed_sibling(self):
        uid, node = mart("fct_unbuilt_sibling")
        self.manifest["nodes"][uid] = node
        self.results["results"].append({"unique_id": uid, "status": "error"})
        _, accepted = publisher.accepted_marts(self.manifest, self.results)
        self.assertEqual([node["name"] for node in accepted.values()], ["fct_fixture"])

    def test_failed_owned_test_disqualifies_its_mart(self):
        mart_id = next(iter(self.manifest["nodes"]))
        self.manifest["nodes"]["test.vintage_data.fct_fixture_contract"] = {
            "resource_type": "test", "depends_on": {"nodes": [mart_id]},
        }
        self.results["results"].append(
            {"unique_id": "test.vintage_data.fct_fixture_contract", "status": "fail"})
        with self.assertRaisesRegex(ValueError, "fully verified marts"):
            publisher.publish(self.manifest, self.results, self.root / "warehouse.duckdb", self.root / "state")

    def test_unsupported_type_and_long_identifiers_fail(self):
        with self.assertRaises(ValueError):
            project.pg_type("struct(a integer)")
        with self.assertRaises(ValueError):
            project.identifier("a" * 64)
        _, node = mart("fct_broken", [("id", "struct(a integer)")])
        with self.assertRaises(ValueError):
            publisher.serving_tables({node["unique_id"]: node})


@unittest.skipUnless(os.environ.get("VINTAGE_TEST_ENV_FILE"), "set VINTAGE_TEST_ENV_FILE to an isolated test stack")
class StreamingPublicationTest(unittest.TestCase):
    def setUp(self):
        import psycopg

        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = pathlib.Path(self.tmp.name)
        self.state = self.root / "state"
        self.db = self.root / "warehouse.duckdb"
        suffix = uuid.uuid4().hex[:12]
        self.name = "fct_fixture_" + suffix
        self.sibling = "fct_sibling_" + suffix
        self.manifest, self.results = fixture(self.name, self.sibling)
        with duckdb.connect(str(self.db)) as con:
            con.execute("SET TimeZone='UTC'")
            con.execute("CREATE SCHEMA transform_marts")
            for name in (self.name, self.sibling):
                con.execute(f"CREATE TABLE transform_marts.{name}({DDL})")
            con.execute(
                f"INSERT INTO transform_marts.{self.name} VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                ["one", Decimal("123456789012345678.12"), AWKWARD,
                 "2026-09-07 01:02:03.123456+02", "2026-09-07", '{"a": [1, null], "b": NaN}', 1.5, True])
            con.execute(f"INSERT INTO transform_marts.{self.name}(id) VALUES ('two')")
            con.execute(
                f"INSERT INTO transform_marts.{self.name} SELECT 'three', NULL, '', "
                "'infinity'::TIMESTAMPTZ, '-infinity'::DATE, NULL, 'inf'::DOUBLE, false")
            con.execute(
                f"INSERT INTO transform_marts.{self.name} SELECT 'four', NULL, NULL, "
                "'-infinity'::TIMESTAMPTZ, 'infinity'::DATE, NULL, 'nan'::DOUBLE, NULL")
            con.execute(f"INSERT INTO transform_marts.{self.sibling}(id) VALUES ('sibling')")
        values = dict(
            line.split("=", 1) for line in
            pathlib.Path(os.environ["VINTAGE_TEST_ENV_FILE"]).read_text().splitlines()
            if line and not line.startswith("#"))
        self.con = psycopg.connect(
            host="127.0.0.1", port=int(values["LIGHTDASH_PG_PORT"]),
            dbname=values.get("LIGHTDASH_SERVING_DB", "vintage_serving"), user="mart_publisher",
            password=values["LIGHTDASH_PUBLISHER_PASSWORD"], autocommit=True)

    def tearDown(self):
        from psycopg import sql

        for name in (self.name, self.sibling):
            self.con.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier("transform_marts", name)))
        if self.con.execute("SELECT to_regclass('_publish.models')").fetchone()[0]:
            self.con.execute("DELETE FROM _publish.models WHERE model = ANY(%s)", ([self.name, self.sibling],))
        self.con.close()

    def publish(self, invocation="one", manifest=None, results=None, **kwargs):
        manifest = copy.deepcopy(manifest or self.manifest)
        results = copy.deepcopy(results or self.results)
        manifest["metadata"]["invocation_id"] = invocation
        results["metadata"]["invocation_id"] = invocation
        return publisher.publish(manifest, results, self.db, self.state, connection=self.con, **kwargs)

    def rows(self, name):
        return self.con.execute(
            f"SELECT id, amount, text_value, observed_at::text, observed_on::text, payload, ratio, flag"
            f" FROM transform_marts.{name} ORDER BY id").fetchall()

    def sequence(self, name):
        return self.con.execute("SELECT sequence FROM _publish.models WHERE model=%s", (name,)).fetchone()

    def frozen(self, name):
        """Comparable rows: NaN never equals itself."""
        return [tuple("nan" if isinstance(value, float) and value != value else value for value in row)
                for row in self.rows(name)]

    def only(self, name):
        """A single-mart manifest and matching run results."""
        manifest = copy.deepcopy(self.manifest)
        uid = "model.vintage_data." + name
        manifest["nodes"] = {uid: manifest["nodes"][uid]}
        results = {"metadata": {"invocation_id": "one"}, "results": [{"unique_id": uid, "status": "success"}]}
        return manifest, results

    def test_every_value_survives_the_stream_and_nothing_is_exported(self):
        result = self.publish()
        self.assertEqual(result["published"], sorted([self.name, self.sibling]))
        rows = {row[0]: row[1:] for row in self.rows(self.name)}
        self.assertEqual(rows["one"][0], Decimal("123456789012345678.12"))
        self.assertEqual(rows["one"][1], AWKWARD)
        self.assertEqual(rows["one"][2], "2026-09-06 23:02:03.123456+00")
        self.assertEqual(rows["one"][3], "2026-09-07")
        self.assertEqual(json.loads(rows["one"][4].replace("NaN", '"NaN"')), {"a": [1, None], "b": "NaN"})
        self.assertEqual((rows["one"][5], rows["one"][6]), (1.5, True))
        self.assertEqual(rows["two"], (None,) * 7)
        self.assertEqual((rows["three"][1], rows["three"][2], rows["three"][3]), ("", "infinity", "-infinity"))
        self.assertEqual((rows["three"][5], rows["three"][6]), (float("inf"), False))
        self.assertEqual((rows["four"][2], rows["four"][3]), ("-infinity", "infinity"))
        self.assertNotEqual(rows["four"][5], rows["four"][5])  # NaN
        self.assertEqual([], list(self.state.rglob("*.csv")))
        info = json.loads((pathlib.Path(result["path"]) / "batch.json").read_text())
        self.assertEqual(info["schema_version"], 2)
        self.assertEqual({row["table"]: row["rows"] for row in info["tables"]}, {self.name: 4, self.sibling: 1})
        self.assertFalse(any({"file", "sha256"} & set(row) for row in info["tables"]))
        self.assertEqual(
            sorted(path.name for path in pathlib.Path(result["path"]).iterdir()),
            ["batch.json", "manifest.json", "run_results.json"])

    def test_replay_is_skipped_and_an_older_batch_cannot_overwrite(self):
        first = self.publish()
        replay = self.publish()
        self.assertEqual(replay["skipped"], sorted([self.name, self.sibling]))
        self.assertEqual(replay["path"], first["path"])
        self.assertEqual(replay["published"], [])
        with duckdb.connect(str(self.db)) as con:
            con.execute(f"DELETE FROM transform_marts.{self.name} WHERE id <> 'one'")
            con.execute(f"UPDATE transform_marts.{self.name} SET amount=42.01")
        self.assertEqual(self.publish("newer")["published"], sorted([self.name, self.sibling]))
        self.assertEqual([row[1] for row in self.rows(self.name)], [Decimal("42.01")])
        stale = self.publish()
        self.assertEqual(stale["stale"], sorted([self.name, self.sibling]))
        self.assertEqual([row[1] for row in self.rows(self.name)], [Decimal("42.01")])
        with duckdb.connect(str(self.db)) as con:
            con.execute(f"DELETE FROM transform_marts.{self.name}")
        self.publish("empty")
        self.assertEqual(self.rows(self.name), [])

    def test_one_superseded_sibling_freezes_the_whole_stale_batch(self):
        self.publish()
        manifest, results = self.only(self.sibling)
        self.publish("sibling-only", manifest=manifest, results=results)
        current = self.sequence(self.name)
        with duckdb.connect(str(self.db)) as con:
            con.execute(f"UPDATE transform_marts.{self.name} SET id='rewritten' WHERE id='one'")
        outcome = self.publish()
        self.assertEqual(outcome["stale"], [self.sibling])
        self.assertEqual(outcome["published"], [])
        # The unsuperseded sibling was never read, transferred or relabeled.
        self.assertEqual(self.sequence(self.name), current)
        self.assertEqual([row[0] for row in self.rows(self.name)], ["four", "one", "three", "two"])

    def test_a_failed_transfer_rolls_back_every_table_and_the_ledger(self):
        import psycopg

        self.publish()
        before = (self.sequence(self.name), self.sequence(self.sibling))
        original = self.frozen(self.name)
        with duckdb.connect(str(self.db)) as con:
            con.execute(f"DELETE FROM transform_marts.{self.name}")
            # A real upstream type breach: the declared contract still says
            # decimal, so only Postgres can reject the transferred text.
            con.execute(f"ALTER TABLE transform_marts.{self.sibling} ALTER amount TYPE varchar")
            con.execute(f"UPDATE transform_marts.{self.sibling} SET amount='not-a-decimal'")
        with self.assertRaises(psycopg.errors.InvalidTextRepresentation):
            self.publish("broken")
        self.assertEqual(self.frozen(self.name), original)
        self.assertEqual((self.sequence(self.name), self.sequence(self.sibling)), before)
        with duckdb.connect(str(self.db)) as con:
            con.execute(f"UPDATE transform_marts.{self.sibling} SET amount=NULL")
            con.execute(f"ALTER TABLE transform_marts.{self.sibling} ALTER amount TYPE decimal(20,2)")
        self.assertEqual(self.publish("repaired")["published"], sorted([self.name, self.sibling]))
        self.assertEqual(self.rows(self.name), [])
        self.assertNotEqual((self.sequence(self.name), self.sequence(self.sibling)), before)

    def test_schema_change_requires_a_coordinated_migration(self):
        self.publish()
        self.manifest["nodes"]["model.vintage_data." + self.name]["columns"]["extra"] = {
            "name": "extra", "data_type": "varchar"}
        with duckdb.connect(str(self.db)) as con:
            con.execute(f"ALTER TABLE transform_marts.{self.name} ADD COLUMN extra varchar")
        with self.assertRaisesRegex(ValueError, "schema changed"):
            self.publish("schema")

    def test_reviewed_content_is_frozen_and_a_replay_keeps_its_sequence(self):
        content = self.root / "reviewed"
        for kind in ("charts", "dashboards"):
            (content / kind).mkdir(parents=True)
            (content / kind / "fixture.yml").write_text("slug: fixture\n")
        result = self.publish(content_root=content)
        batch = pathlib.Path(result["path"])
        info = json.loads((batch / "batch.json").read_text())
        self.assertEqual(info["content_sha256"], publisher.content_digest(batch / "content"))
        counter = (self.state / "sequence").read_text()
        replay = self.publish(content_root=content)
        self.assertEqual(replay["skipped"], sorted([self.name, self.sibling]))
        self.assertEqual((self.state / "sequence").read_text(), counter)
        self.assertEqual(json.loads((batch / "batch.json").read_text())["sequence"], info["sequence"])
        (content / "charts" / "fixture.yml").write_text("slug: changed\n")
        self.assertEqual((batch / "content" / "charts" / "fixture.yml").read_text(), "slug: fixture\n")

    def test_legacy_metadata_publishes_without_its_removed_payloads(self):
        result = self.publish()
        batch = pathlib.Path(result["path"])
        info = json.loads((batch / "batch.json").read_text())
        legacy = {**info, "schema_version": 1,
                  "tables": [{**row, "file": f"{row['table']}.csv", "sha256": "0" * 64} for row in info["tables"]]}
        (batch / "batch.json").write_text(json.dumps(legacy, indent=2) + "\n")
        self.con.execute("DELETE FROM _publish.models WHERE model=%s", (self.name,))
        outcome = self.publish()
        self.assertEqual(outcome["published"], [self.name])
        self.assertEqual(outcome["skipped"], [self.sibling])
        self.assertEqual(json.loads((batch / "batch.json").read_text())["schema_version"], 1)

    def test_republishing_does_not_grow_the_table_and_readers_keep_access(self):
        import psycopg

        with duckdb.connect(str(self.db)) as con:
            con.execute(f"INSERT INTO transform_marts.{self.name}(id, text_value) "
                        "SELECT 'bulk-' || i, repeat('x', 200) FROM range(5000) t(i)")
        self.publish()
        size = lambda: self.con.execute(
            "SELECT pg_total_relation_size(%s)", (f"transform_marts.{self.name}",)).fetchone()[0]
        first = size()
        for invocation in ("two", "three", "four"):
            self.publish(invocation)
        # Replacing rows in place would leave each prior copy behind as dead tuples.
        self.assertLess(size(), first * 1.5)
        values = dict(
            line.split("=", 1) for line in
            pathlib.Path(os.environ["VINTAGE_TEST_ENV_FILE"]).read_text().splitlines()
            if line and not line.startswith("#"))
        with psycopg.connect(host="127.0.0.1", port=int(values["LIGHTDASH_PG_PORT"]),
                             dbname=values.get("LIGHTDASH_SERVING_DB", "vintage_serving"), user="mart_reader",
                             password=values["LIGHTDASH_READER_PASSWORD"]) as reader:
            self.assertEqual(reader.execute(f"SELECT count(*) FROM transform_marts.{self.name}").fetchone()[0], 5004)


@unittest.skipUnless(os.environ.get("VINTAGE_TEST_ENV_FILE"), "set VINTAGE_TEST_ENV_FILE to an isolated test stack")
class DiscardExportDataTest(unittest.TestCase):
    """The retired payloads go; every live catalog record stays byte for byte."""

    def setUp(self):
        import psycopg

        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = pathlib.Path(self.tmp.name) / "state"
        (self.root / "batches").mkdir(parents=True)
        (self.root / "failed-batches").mkdir()
        values = dict(
            line.split("=", 1) for line in
            pathlib.Path(os.environ["VINTAGE_TEST_ENV_FILE"]).read_text().splitlines()
            if line and not line.startswith("#"))
        self.con = psycopg.connect(
            host="127.0.0.1", port=int(values["LIGHTDASH_PG_PORT"]),
            dbname=values.get("LIGHTDASH_SERVING_DB", "vintage_serving"), user="mart_publisher",
            password=values["LIGHTDASH_PUBLISHER_PASSWORD"], autocommit=True)
        self.addCleanup(self.con.close)
        self.con.execute("CREATE SCHEMA IF NOT EXISTS _publish")
        self.con.execute("""CREATE TABLE IF NOT EXISTS _publish.models (
            model text PRIMARY KEY, sequence bigint NOT NULL, batch_id text NOT NULL,
            schema_sha256 text NOT NULL, row_count bigint NOT NULL,
            captured_at timestamptz NOT NULL, published_at timestamptz NOT NULL DEFAULT now())""")
        # The stack is disposable; this class owns the whole ledger.
        self.con.execute("DELETE FROM _publish.models")
        self.addCleanup(self.con.execute, "DELETE FROM _publish.models")
        self.environment = mock.patch.dict(os.environ, {
            "LIGHTDASH_STATE_ROOT": str(self.root),
            "LIGHTDASH_PG_HOST": "127.0.0.1",
            "LIGHTDASH_PG_PORT": values["LIGHTDASH_PG_PORT"],
            "LIGHTDASH_SERVING_DB": values.get("LIGHTDASH_SERVING_DB", "vintage_serving"),
            "LIGHTDASH_PUBLISHER_PASSWORD": values["LIGHTDASH_PUBLISHER_PASSWORD"],
        })
        self.environment.start()
        self.addCleanup(self.environment.stop)
        self.current = self.legacy("1" * 64, ["fct_current"])
        self.pending = self.legacy("2" * 64, ["fct_pending"])
        self.superseded = self.legacy("3" * 64, ["fct_superseded"])
        self.partial = self.legacy("4" * 64, ["fct_partial"], write=False)
        self.modern = self.legacy("5" * 64, ["fct_modern"], version=2)
        temporary = self.root / "batches" / ".capture-leftover"
        temporary.mkdir()
        (temporary / "fct_interrupted.csv").write_text("x" * 4096)
        failed = self.root / "failed-batches" / ("6" * 64)
        failed.mkdir()
        (failed / "fct_failed.csv").write_text("x" * 4096)
        self.con.execute(
            "INSERT INTO _publish.models(model, sequence, batch_id, schema_sha256, row_count, captured_at)"
            " VALUES (%s,%s,%s,%s,%s,now())", ("fct_current", 1, "1" * 64, "abc", 1))

    def legacy(self, identity, tables, *, write=True, version=1):
        batch = self.root / "batches" / identity
        (batch / "content" / "charts").mkdir(parents=True)
        (batch / "content" / "charts" / "chart.yml").write_text("slug: fixture\n")
        (batch / "manifest.json").write_text(json.dumps({"nodes": {}}))
        (batch / "run_results.json").write_text(json.dumps({"results": []}))
        rows = []
        for table in tables:
            row = {"unique_id": "model.vintage_data." + table, "table": table,
                   "columns": [{"name": "id", "type": "text"}], "schema_sha256": "abc", "rows": 1}
            if version == 1:
                row.update(file=f"{table}.csv", sha256="0" * 64)
                if write:
                    (batch / f"{table}.csv").write_text("x" * 8192)
            rows.append(row)
        (batch / "batch.json").write_text(json.dumps(
            {"schema_version": version, "batch_id": identity, "sequence": 1,
             "captured_at": "2026-09-07T00:00:00+00:00", "tables": rows}, indent=2) + "\n")
        return batch

    def inventory(self):
        return {str(path.relative_to(self.root)): path.lstat().st_size
                for path in sorted(self.root.rglob("*")) if path.is_file() and not path.is_symlink()}

    def test_dry_run_reports_candidates_without_touching_anything(self):
        before = self.inventory()
        report = publisher.discard_export_data(self.root)
        self.assertTrue(report["ok"])
        self.assertFalse(report["apply"])
        self.assertEqual(report["candidate_files"], 5)
        self.assertEqual(report["removed_files"], 0)
        self.assertGreaterEqual(report["candidate_bytes"], 3 * 8192 + 2 * 4096)
        self.assertEqual(self.inventory(), before)

    def test_apply_removes_only_retired_payloads_and_is_repeatable(self):
        report = publisher.discard_export_data(self.root, apply=True)
        self.assertTrue(report["ok"])
        self.assertEqual(report["removed_files"], 5)
        self.assertGreaterEqual(report["removed_bytes"], 3 * 8192 + 2 * 4096)
        self.assertEqual([], list(self.root.rglob("*.csv")))
        self.assertFalse((self.root / "batches" / ".capture-leftover").exists())
        self.assertEqual([], list((self.root / "failed-batches").iterdir()))
        for batch in (self.current, self.pending, self.superseded, self.partial, self.modern):
            self.assertEqual(
                sorted(path.name for path in batch.iterdir()),
                ["batch.json", "content", "manifest.json", "run_results.json"])
            self.assertEqual(json.loads((batch / "batch.json").read_text())["batch_id"], batch.name)
        again = publisher.discard_export_data(self.root, apply=True)
        self.assertEqual((again["removed_files"], again["removed_bytes"]), (0, 0))
        self.assertTrue(again["ok"])

    def test_missing_live_metadata_stops_before_any_deletion(self):
        self.con.execute(
            "INSERT INTO _publish.models(model, sequence, batch_id, schema_sha256, row_count, captured_at)"
            " VALUES (%s,%s,%s,%s,%s,now())", ("fct_absent", 1, "9" * 64, "abc", 1))
        before = self.inventory()
        with self.assertRaisesRegex(ValueError, "9" * 64):
            publisher.discard_export_data(self.root, apply=True)
        self.assertEqual(self.inventory(), before)

    def test_traversal_and_malformed_metadata_fail_closed(self):
        payloads = sorted(self.root.rglob("*.csv"))
        info = json.loads((self.pending / "batch.json").read_text())
        info["tables"][0]["file"] = "../escape.csv"
        (self.pending / "batch.json").write_text(json.dumps(info))
        with self.assertRaisesRegex(ValueError, "unsafe legacy payload"):
            publisher.discard_export_data(self.root, apply=True)
        self.assertEqual(sorted(self.root.rglob("*.csv")), payloads)
        (self.pending / "batch.json").write_text("{not json")
        with self.assertRaisesRegex(ValueError, "refusing to guess"):
            publisher.discard_export_data(self.root, apply=True)
        self.assertEqual(sorted(self.root.rglob("*.csv")), payloads)

    def test_unknown_and_symlinked_paths_are_reported_untouched(self):
        stray = self.superseded / "unexpected.tsv"
        stray.write_text("x")
        (self.superseded / "linked.csv").symlink_to(self.current / "fct_current.csv")
        report = publisher.discard_export_data(self.root, apply=True)
        self.assertIn(str(stray), report["unknown"])
        self.assertIn(str(self.superseded / "linked.csv"), report["unknown"])
        self.assertTrue(stray.is_file())
        self.assertTrue((self.superseded / "linked.csv").is_symlink())
        self.assertFalse((self.current / "fct_current.csv").exists())

if __name__ == "__main__":
    unittest.main()
