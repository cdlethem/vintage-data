"""Round-trip tests use an explicitly selected disposable Postgres deployment."""
import copy
import json
import os
import pathlib
import tempfile
import unittest
import uuid
from decimal import Decimal

import duckdb

from visualization import project, publisher


def fixture(name="fct_fixture"):
    uid = "model.vintage_data." + name
    node = {"unique_id": uid, "resource_type": "model", "package_name": "vintage_data", "name": name,
            "alias": name, "original_file_path": "models/marts/fixture/" + name + ".sql", "schema": "transform_marts",
            "config": {"materialized": "table"}, "columns": {k:{"name": k,"data_type": t} for k,t in
                [("id","varchar"),("amount","decimal(20,2)"),("text_value","varchar"),("observed_at","timestamp with time zone"),("payload","json"),("flag","boolean") ]}}
    manifest = {"metadata": {"invocation_id": "one", "adapter_type": "duckdb"}, "nodes": {uid: node}, "sources": {}}
    results = {"metadata": {"invocation_id": "one"}, "results": [{"unique_id": uid, "status": "success"}]}
    return manifest, results


class SnapshotTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.tmp.name)
        self.db = self.root / "warehouse.duckdb"
        self.name = "fct_fixture_" + uuid.uuid4().hex[:12]
        self.manifest, self.results = fixture(self.name)
        with duckdb.connect(str(self.db)) as con:
            con.execute("CREATE SCHEMA transform_marts")
            con.execute(f"CREATE TABLE transform_marts.{self.name}(id varchar, amount decimal(20,2), text_value varchar, observed_at timestamptz, payload json, flag boolean)")
            con.execute(f"INSERT INTO transform_marts.{self.name} VALUES (?, ?, ?, ?, ?, ?)",
                        ['one',Decimal('123456789012345678.12'),'\\N,quote"\nUnicode café','2026-09-07 01:02:03.123456+02','{"a": [1, null]}',True])
            con.execute(f"INSERT INTO transform_marts.{self.name}(id) VALUES ('two')")

    def tearDown(self):
        self.tmp.cleanup()

    def capture(self, invocation="one"):
        self.manifest["metadata"]["invocation_id"] = invocation
        self.results["metadata"]["invocation_id"] = invocation
        return pathlib.Path(publisher.capture(self.manifest, self.results, self.db, self.root / "state")["path"])

    def test_snapshot_replay_and_integrity(self):
        path = self.capture()
        self.assertEqual(path, self.capture())
        data = json.loads((path / "batch.json").read_text())
        self.assertEqual(data["tables"][0]["rows"], 2)
        csv = (path / data["tables"][0]["file"]).read_text()
        self.assertIn('123456789012345678.12', csv)
        self.assertIn('"\\N,quote""\nUnicode café"', csv)
        self.assertIn('\\N', csv)
        self.assertEqual(data["tables"][0]["sha256"], publisher.file_digest(path / data["tables"][0]["file"]))

    def test_failed_or_mismatched_build_cannot_publish(self):
        self.results["results"][0]["status"] = "error"
        with self.assertRaisesRegex(ValueError, "failed"):
            self.capture()
        self.results["results"][0]["status"] = "success"
        self.results["metadata"]["invocation_id"] = "different"
        with self.assertRaisesRegex(ValueError, "same dbt invocation"):
            publisher.capture(self.manifest, self.results, self.db, self.root / "state")

    def test_unsupported_type_and_long_identifiers_fail(self):
        with self.assertRaises(ValueError):
            project.pg_type("struct(a integer)")
        with self.assertRaises(ValueError):
            project.identifier("a" * 64)

    def test_snapshot_releases_duckdb(self):
        self.capture()
        with duckdb.connect(str(self.db)) as con:
            con.execute(f"DELETE FROM transform_marts.{self.name}")
        data = json.loads((self.capture('empty') / "batch.json").read_text())
        self.assertEqual(data['tables'][0]['rows'], 0)


@unittest.skipUnless(os.environ.get("VINTAGE_TEST_ENV_FILE"), "set VINTAGE_TEST_ENV_FILE to an isolated test stack")
class PostgresRoundTripTest(SnapshotTest):
    def setUp(self):
        super().setUp()
        import psycopg
        values = dict(line.split('=',1) for line in pathlib.Path(os.environ['VINTAGE_TEST_ENV_FILE']).read_text().splitlines() if line and not line.startswith('#'))
        self.con = psycopg.connect(host='127.0.0.1',port=int(values['LIGHTDASH_PG_PORT']), dbname='vintage_serving',user='mart_publisher',password=values['LIGHTDASH_PUBLISHER_PASSWORD'], autocommit=True)

    def tearDown(self):
        from psycopg import sql
        self.con.execute(sql.SQL('DROP TABLE IF EXISTS {}').format(sql.Identifier('transform_marts',self.name)))
        if self.con.execute("SELECT to_regclass('_publish.models')").fetchone()[0]:
            self.con.execute('DELETE FROM _publish.models WHERE model=%s',(self.name,))
        self.con.close()
        super().tearDown()

    def test_exact_roundtrip_idempotence_and_ordering(self):
        path = self.capture()
        result = publisher.publish(path, connection=self.con)
        self.assertEqual(result['published'],[self.name])
        rows = self.con.execute(f'SELECT id,amount,text_value,observed_at,payload,flag FROM transform_marts.{self.name} ORDER BY id').fetchall()
        self.assertEqual(rows[0][1],Decimal('123456789012345678.12'))
        self.assertEqual(rows[0][2],'\\N,quote"\nUnicode café')
        self.assertEqual(rows[0][3].isoformat(),'2026-09-06T23:02:03.123456+00:00')
        self.assertEqual(json.loads(rows[0][4]),{'a':[1,None]})
        self.assertEqual(rows[1][1:],(None,)*5)
        self.assertEqual(publisher.publish(path,connection=self.con)['skipped'],[self.name])
        with duckdb.connect(str(self.db)) as con:
            con.execute(f"DELETE FROM transform_marts.{self.name} WHERE id='two'")
            con.execute(f"UPDATE transform_marts.{self.name} SET amount=42.01")
        newer=self.capture('newer')
        publisher.publish(newer,connection=self.con)
        publisher.publish(path,connection=self.con)
        self.assertEqual(self.con.execute(f'SELECT amount FROM transform_marts.{self.name}').fetchall(),[(Decimal('42.01'),)])
        with duckdb.connect(str(self.db)) as con:
            con.execute(f'DELETE FROM transform_marts.{self.name}')
        publisher.publish(self.capture('empty'),connection=self.con)
        self.assertEqual(self.con.execute(f'SELECT count(*) FROM transform_marts.{self.name}').fetchone()[0],0)

    def test_bad_copy_rolls_back_previous_data(self):
        path=self.capture();publisher.publish(path,connection=self.con)
        newer=self.capture('corrupted')
        info=json.loads((newer/'batch.json').read_text());row=info['tables'][0]
        data=newer/row['file'];data.write_text('"broken","not-a-decimal",\\N,\\N,\\N,\\N\n')
        row['sha256']=publisher.file_digest(data);(newer/'batch.json').write_text(json.dumps(info))
        with self.assertRaises(Exception):
            publisher.publish(newer,connection=self.con)
        self.assertEqual(self.con.execute(f'SELECT count(*) FROM transform_marts.{self.name}').fetchone()[0],2)

    def test_schema_change_requires_migration(self):
        publisher.publish(self.capture(),connection=self.con)
        self.manifest['nodes'][next(iter(self.manifest['nodes']))]['columns']['extra']={'name':'extra','data_type':'varchar'}
        with duckdb.connect(str(self.db)) as con:
            con.execute(f'ALTER TABLE transform_marts.{self.name} ADD COLUMN extra varchar')
        with self.assertRaisesRegex(ValueError,'schema changed'):
            publisher.publish(self.capture('schema'),connection=self.con)

    def test_second_table_failure_rolls_back_first_table_and_ledger(self):
        initial=self.capture();publisher.publish(initial,connection=self.con)
        before=self.con.execute('SELECT sequence FROM _publish.models WHERE model=%s',(self.name,)).fetchone()
        with duckdb.connect(str(self.db)) as con:
            con.execute(f'DELETE FROM transform_marts.{self.name}')
        newer=self.capture('two-table-failure')
        info=json.loads((newer/'batch.json').read_text())
        second=copy.deepcopy(info['tables'][0])
        second.update(table=self.name+'_second',file='second.csv',rows=1)
        data=newer/'second.csv'
        data.write_text('"broken","not-a-decimal",\\N,\\N,\\N,\\N\n')
        second['sha256']=publisher.file_digest(data)
        info['tables'].append(second)
        (newer/'batch.json').write_text(json.dumps(info))
        with self.assertRaises(Exception):
            publisher.publish(newer,connection=self.con)
        self.assertEqual(self.con.execute(f'SELECT count(*) FROM transform_marts.{self.name}').fetchone()[0],2)
        self.assertEqual(self.con.execute('SELECT sequence FROM _publish.models WHERE model=%s',(self.name,)).fetchone(),before)
