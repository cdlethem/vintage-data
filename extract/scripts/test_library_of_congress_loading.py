import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest


SCRIPT = Path(__file__).with_name("fetch_library_of_congress.py")
REPO_ROOT = SCRIPT.parents[2]
SPEC = importlib.util.spec_from_file_location("fetch_library_of_congress_loading", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)

sys.path.insert(0, str(REPO_ROOT / "load"))
try:
    from loader.cadence import CadencePolicy
    from loader.config import DagSettings, LoadConfig, ServiceSettings, SourceSettings
    from loader.queue import Job
    from loader.service import LoaderService
finally:
    sys.path.remove(str(REPO_ROOT / "load"))


class LibraryOfCongressLoadingTests(unittest.TestCase):
    def test_fixture_records_load_through_real_loader_with_payloads_preserved(self):
        fetched_at = "2026-09-17T12:00:00+00:00"
        wire_records = [
            {
                "id": "https://www.loc.gov/item/one/",
                "title": "Robotics handbook",
                "date": "2024",
                "dates": ["2024", "2025"],
                "contributor": ["Ada Example"],
                "subject": ["Robotics"],
                "digitized": True,
                "online_format": ["pdf"],
                "mime_type": ["application/pdf"],
                "url": "https://www.loc.gov/item/one/",
                "resources": [{"url": "https://tile.loc.gov/one.pdf"}],
                "future_field": {"preserve": "verbatim"},
            },
            {
                "id": "https://www.loc.gov/item/two/",
                "title": "Robot design",
                "url": "https://www.loc.gov/item/two/",
            },
        ]
        records = [MODULE.normalize_item(record, fetched_at) for record in wire_records]

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source_root = root / "raw"
            data_dir = source_root / f"source={MODULE.SOURCE}" / "dt=2026-09-17"
            data_dir.mkdir(parents=True)
            fixture = data_dir / "library_of_congress_fixture.ndjson"
            fixture.write_text(
                "".join(json.dumps(record, separators=(",", ":")) + "\n" for record in records),
                encoding="utf-8",
            )
            manifest = {
                "records": len(records),
                "started_at": fetched_at,
                "script": SCRIPT.name,
                "schema_version": 2,
                "status": "success",
                "health": "healthy",
                "completeness": "complete",
            }
            fixture.with_name(fixture.name + ".meta.json").write_text(
                json.dumps(manifest), encoding="utf-8"
            )

            config = LoadConfig(
                destination={
                    "name": "fixture_duckdb",
                    "type": "duckdb",
                    "database": str(root / "warehouse.duckdb"),
                    "lock_timeout_s": 1,
                },
                source_root=source_root,
                queue_dir=root / "queue",
                raw_schema="raw",
                meta_schema="_load",
                service=ServiceSettings(),
                dag=DagSettings(),
                defaults=SourceSettings(
                    min_age_s=0,
                    column_types={
                        "source": "string",
                        "id": "string",
                        "fetched_at": "timestamp",
                    },
                ),
                overrides={},
                cadence=CadencePolicy(enabled=False, sources_dir=root / "sources"),
                path=root / "load.yml",
            )
            service = LoaderService(config)
            job_path = root / "job.json"
            job_path.write_text("{}", encoding="utf-8")
            result = service.run_job(
                Job(
                    "fixture-job",
                    {
                        "kind": "files",
                        "paths": [str(fixture)],
                        "load_id": "fixture-load",
                        "submitted_at": fetched_at,
                    },
                    job_path,
                )
            )

            self.assertEqual(result["status"], "ok", result["errors"])
            self.assertEqual(result["files_loaded"], 1)
            self.assertEqual(result["rows_loaded"], 2)
            rows = service.destination.con.execute(
                'select id, title, resources, _payload from raw."library_of_congress" order by id'
            ).fetchall()
            service.destination.close()

        self.assertEqual(len(rows), 2)
        self.assertEqual([row[0] for row in rows], [wire_records[0]["id"], wire_records[1]["id"]])
        self.assertEqual([row[1] for row in rows], ["Robotics handbook", "Robot design"])
        first_resources = json.loads(rows[0][2]) if isinstance(rows[0][2], str) else rows[0][2]
        first_payload = json.loads(rows[0][3]) if isinstance(rows[0][3], str) else rows[0][3]
        second_payload = json.loads(rows[1][3]) if isinstance(rows[1][3], str) else rows[1][3]
        self.assertEqual(first_resources, records[0]["resources"])
        self.assertEqual(first_payload, records[0])
        self.assertEqual(second_payload, records[1])
        self.assertEqual(first_payload["raw"]["future_field"], {"preserve": "verbatim"})


if __name__ == "__main__":
    unittest.main()
