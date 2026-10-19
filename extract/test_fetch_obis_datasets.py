"""Behavioral tests for fetch_obis_datasets.py (offline, scripted transport)."""

import contextlib
import importlib.util
import io
import json
import re
import unittest
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

import yaml

SCRIPT = Path(__file__).parent / "scripts" / "fetch_obis_datasets.py"
SOURCE_CONFIG = Path(__file__).parent / "sources" / "obis_datasets.yml"
SPEC = importlib.util.spec_from_file_location("fetch_obis_datasets", SCRIPT)
obis = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(obis)

NOW = datetime(2026, 10, 19, 8, 52, tzinfo=timezone.utc)
FETCHED_AT = "2026-10-19T08:52:00Z"


def dataset(id_, title, created="2026-01-01T00:00:00.000Z", records=100, rights="CC0 1.0"):
    return {
        "id": id_,
        "title": title,
        "core": "occurrence",
        "created": created,
        "updated": None,
        "published": None,
        "url": f"https://example.org/{id_}",
        "archive": None,
        "records": records,
        "statistics": {"Occurrence": records, "dropped": 0},
        "intellectualrights": rights,
        "extent": "POLYGON((0 0,1 0,1 1,0 0))",
        "institutes": [{"name": "OBIS Australia", "oceanexpert_id": 1}],
        "nodes": [{"id": "n-1", "name": "OBIS Australia"}],
        "keywords": [{"keyword": "metabarcoding", "thesaurus": "N/A"}],
        "tags": ["env-dna"],
        "feed": {"id": "f-1", "url": "https://example.org/rss", "active": True},
        "citation": None,
        "downloads": [{"year": 2026, "downloads": 81, "records": 5000}],
        "abstract": "<p>html</p>",
    }


def snapshot(records, total=None):
    return {"total": total if total is not None else len(records), "results": records}


class ScriptedTransport:
    def __init__(self, documents):
        self.documents = list(documents)
        self.urls = []

    def __call__(self, url, timeout):
        self.urls.append(url)
        if not self.documents:
            raise AssertionError("more requests than scripted documents")
        return self.documents.pop(0)


def run_fetcher(documents, state_file, output):
    stderr = io.StringIO()
    with contextlib.redirect_stderr(stderr):
        count = obis.run(
            path=state_file,
            output=output,
            transport=ScriptedTransport(documents),
            now=lambda: NOW,
        )
    return count, stderr.getvalue()


class ObisDatasetsTests(unittest.TestCase):
    def setUp(self):
        self._td = TemporaryDirectory(prefix="obis-test-")
        self.state = Path(self._td.name) / "obis_datasets.json"
        self.out = io.StringIO()

    def tearDown(self):
        self._td.cleanup()

    def lines(self):
        return [json.loads(line) for line in self.out.getvalue().splitlines() if line]

    def summary(self, stderr_text):
        for line in stderr_text.splitlines():
            if line.startswith("VINTAGE_RUN_SUMMARY\t"):
                return json.loads(line[len("VINTAGE_RUN_SUMMARY\t"):])
        self.fail("no VINTAGE_RUN_SUMMARY line on stderr")

    def test_baseline_run_emits_full_snapshot(self):
        docs = [snapshot([dataset("a", "Alpha"), dataset("b", "Beta"), dataset("c", "Gamma")])]
        count, stderr = run_fetcher(docs, self.state, self.out)
        self.assertEqual(count, 3)
        rows = self.lines()
        self.assertEqual([row["id"] for row in rows], ["a", "b", "c"])
        for row in rows:
            self.assertEqual(row["source"], "obis_datasets")
            self.assertEqual(row["fetched_at"], FETCHED_AT)
        summary = self.summary(stderr)
        self.assertEqual(summary["health"], "healthy")
        self.assertTrue(summary["state_change"]["baseline"])
        self.assertEqual(summary["metrics"]["records_in_snapshot"], 3)
        saved = json.loads(self.state.read_text())
        self.assertEqual(set(saved["datasets"]), {"a", "b", "c"})

    def test_quiet_second_run_emits_nothing(self):
        docs = snapshot([dataset("a", "Alpha"), dataset("b", "Beta")])
        count1, _ = run_fetcher([docs], self.state, self.out)
        self.out.truncate(0)
        self.out.seek(0)
        count2, _ = run_fetcher([docs], self.state, self.out)
        self.assertEqual(count1, 2)
        self.assertEqual(count2, 0)
        self.assertEqual(self.lines(), [])

    def test_changed_statistics_emits_only_changed(self):
        original = [dataset("a", "Alpha"), dataset("b", "Beta")]
        changed = [dataset("a", "Alpha"), dataset("b", "Beta", records=999)]
        run_fetcher([snapshot(original)], self.state, self.out)
        self.out.truncate(0)
        self.out.seek(0)
        count, _ = run_fetcher([snapshot(changed)], self.state, self.out)
        self.assertEqual(count, 1)
        rows = self.lines()
        self.assertEqual(rows[0]["id"], "b")
        self.assertEqual(rows[0]["records"], 999)
        self.assertEqual(rows[0]["statistics"]["Occurrence"], 999)

    def test_new_dataset_emitted_once_removed_dataset_emits_nothing(self):
        original = [dataset("a", "Alpha")]
        run_fetcher([snapshot(original)], self.state, self.out)
        # A brand-new dataset appears in the catalog.
        self.out.truncate(0)
        self.out.seek(0)
        count, _ = run_fetcher([snapshot(original + [dataset("b", "Beta")])], self.state, self.out)
        self.assertEqual(count, 1)
        self.assertEqual(self.lines()[0]["id"], "b")
        # Then it disappears from the catalog: no delete events, state drops it.
        self.out.truncate(0)
        self.out.seek(0)
        count, _ = run_fetcher([snapshot([dataset("a", "Alpha")])], self.state, self.out)
        self.assertEqual(count, 0)
        saved = json.loads(self.state.read_text())
        self.assertEqual(set(saved["datasets"]), {"a"})

    def test_no_state_always_emits_full_snapshot(self):
        docs = snapshot([dataset("a", "Alpha"), dataset("b", "Beta")])
        count, _ = run_fetcher([docs], None, self.out)
        self.assertEqual(count, 2)
        self.assertEqual(len(self.lines()), 2)
        self.assertFalse(self.state.exists())

    def test_corrupt_state_fails_and_is_not_overwritten(self):
        self.state.write_text("{not json", encoding="utf-8")
        with self.assertRaises(ValueError):
            run_fetcher([snapshot([dataset("a", "Alpha")])], self.state, self.out)
        self.assertEqual(self.state.read_text(), "{not json")
        self.assertEqual(self.lines(), [])

    def test_null_collection_fields_project_safely(self):
        record = dataset("a", "Alpha")
        record["keywords"] = None
        record["institutes"] = None
        record["nodes"] = None
        record["tags"] = None
        run_fetcher([snapshot([record])], self.state, self.out)
        row = self.lines()[0]
        self.assertEqual(row["keywords"], [])
        self.assertEqual(row["institutes"], [])
        self.assertEqual(row["nodes"], [])
        self.assertEqual(row["tags"], [])

    def test_wrong_version_state_fails(self):
        self.state.write_text(
            json.dumps({"version": 99, "source": "obis_datasets", "datasets": {}}), encoding="utf-8"
        )
        with self.assertRaises(ValueError):
            run_fetcher([snapshot([dataset("a", "Alpha")])], self.state, self.out)

    def test_empty_results_fail_and_preserve_state(self):
        run_fetcher([snapshot([dataset("a", "Alpha")])], self.state, self.out)
        before = self.state.read_text()
        with self.assertRaises(ValueError):
            run_fetcher([snapshot([])], self.state, self.out)
        self.assertEqual(self.state.read_text(), before)

    def test_duplicate_ids_fail(self):
        with self.assertRaises(ValueError):
            run_fetcher(
                [snapshot([dataset("a", "Alpha"), dataset("a", "Alpha again")])], self.state, self.out
            )

    def test_total_mismatch_warns_but_succeeds(self):
        count, stderr = run_fetcher([snapshot([dataset("a", "Alpha")], total=5)], self.state, self.out)
        self.assertEqual(count, 1)
        self.assertIn("total=5", stderr)
        self.assertTrue(self.summary(stderr)["metrics"]["total_field_mismatch"])

    def test_projection_excludes_usage_and_html_fields(self):
        run_fetcher([snapshot([dataset("a", "Alpha")])], self.state, self.out)
        row = self.lines()[0]
        for excluded in ("downloads", "abstract", "archive", "feed"):
            self.assertNotIn(excluded, row)
        self.assertEqual(row["feed_url"], "https://example.org/rss")
        self.assertEqual(row["institutes"], ["OBIS Australia"])
        self.assertEqual(row["keywords"], ["metabarcoding"])

    def test_state_not_written_when_output_fails(self):
        class FailingOutput(io.StringIO):
            def write(self, text):
                raise OSError("boom mid-stream")

        with self.assertRaises(OSError):
            run_fetcher([snapshot([dataset("a", "Alpha")])], self.state, FailingOutput())
        self.assertFalse(self.state.exists())

    def test_request_json_rejects_foreign_urls(self):
        self.assertEqual(obis.build_url(), "https://api.obis.org/v3/dataset?size=10000")
        with self.assertRaises(ValueError):
            obis.request_json("https://evil.example/v3/dataset?size=10000", timeout=5)
        with self.assertRaises(ValueError):
            obis.request_json("http://api.obis.org/v3/dataset?size=10000", timeout=5)


class ObisSourceConfigTests(unittest.TestCase):
    def test_source_config(self):
        config = yaml.safe_load(SOURCE_CONFIG.read_text())
        self.assertEqual(config["name"], "obis_datasets")
        self.assertEqual(config["script"], "fetch_obis_datasets.py")
        self.assertTrue((Path(__file__).parent / "scripts" / config["script"]).is_file())
        self.assertTrue(re.fullmatch(r"\d+ \d+ \* \* \*", config["schedule"]))
        self.assertTrue(config["enabled"])
        for key in ("cadence_note", "rate_limit", "licence"):
            self.assertTrue(config[key])
        self.assertTrue(config["attribution_required"])


if __name__ == "__main__":
    unittest.main()
