"""Offline tests for fetch_ghsa_advisories.py.

Fixtures mock urllib and pin the clock; nothing here calls api.github.com.
The replay test pins the behaviour a production state file depends on: a run
whose watermark already covers part of a day bucket emits only the newer
records, never a duplicate.
"""
from __future__ import annotations

import importlib.util
import io
import json
import pathlib
import tempfile
import unittest
import urllib.error
import urllib.parse
from datetime import datetime, timezone
from unittest import mock


REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
SCRIPT = pathlib.Path(__file__).parent / "scripts" / "fetch_ghsa_advisories.py"
RUN_METADATA_PATH = REPO_ROOT / "orchestration" / "include" / "run_metadata.py"

RUN_METADATA_SPEC = importlib.util.spec_from_file_location("ghsa_test_run_metadata", RUN_METADATA_PATH)
RUN_METADATA = importlib.util.module_from_spec(RUN_METADATA_SPEC)
assert RUN_METADATA_SPEC.loader is not None
RUN_METADATA_SPEC.loader.exec_module(RUN_METADATA)
SPEC = importlib.util.spec_from_file_location("fetch_ghsa_advisories", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)

NOW = datetime(2026, 11, 25, 9, 23, 0, tzinfo=timezone.utc)
DAY_24 = "2026-11-24"
DAY_25 = "2026-11-25"


class FixedDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        return cls(2026, 11, 25, 9, 23, 0, tzinfo=tz or timezone.utc)


class JsonResponse(io.BytesIO):
    def __init__(self, document, *, status=200, headers=None, raw=None):
        if raw is not None:
            super().__init__(raw)
        else:
            super().__init__(json.dumps(document).encode("utf-8"))
        self.status = status
        self.headers = headers if headers is not None else {}

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()


def http_error(code, retry_after=None):
    headers = {} if retry_after is None else {"Retry-After": retry_after}
    return urllib.error.HTTPError("https://api.github.com/advisories", code, "error", headers, io.BytesIO(b"failure"))


def advisory(ghsa_id, *, published_at, updated_at, cve_id="CVE-2026-0001", severity="high"):
    return {
        "ghsa_id": ghsa_id,
        "cve_id": cve_id,
        "url": f"https://api.github.com/advisories/{ghsa_id}",
        "html_url": f"https://github.com/advisories/{ghsa_id}",
        "summary": f"summary {ghsa_id}",
        "description": "detailed description",
        "type": "reviewed",
        "severity": severity,
        "published_at": published_at,
        "updated_at": updated_at,
        "withdrawn_at": None,
        "cvss": {"score": "9.8", "vector_string": "CVSS:3.1/AV:N/AC:L/PR:N/UI:N/S:U/C:H/I:H/A:H"},
        "cwes": [{"cwe_id": "CWE-79", "name": "XSS"}],
        "vulnerabilities": [
            {
                "package": {"ecosystem": "PyPI", "name": "example"},
                "vulnerable_version_range": "< 1.2.3",
                "first_patched_version": "1.2.3",
            }
        ],
        "references": [f"https://github.com/advisories/{ghsa_id}"],
    }


def page(records, *, next_link=None):
    headers = {}
    if next_link is not None:
        headers["Link"] = f'<{next_link}>; rel="next"'
    return JsonResponse(records, headers=headers)


def new_metrics():
    return {
        "pages": 0,
        "records_received": 0,
        "records_emitted": 0,
        "records_skipped_before_watermark": 0,
        "cap_reason": None,
        "next_watermark": None,
        "requests": {"attempted": 0, "succeeded": 0, "failed": 0, "payload_bytes": 0},
        "_retry_budget": [0.0],
        "_fetched_at": "2026-11-25T09:23:00Z",
    }


def collect(window, watermark, documents, **overrides):
    arguments = {
        "max_pages": 5,
        "max_records": 50,
        "timeout": 17,
        "request_delay": 0.5,
    }
    arguments.update(overrides)
    metrics = new_metrics()
    responses = [document if isinstance(document, (JsonResponse, Exception)) else JsonResponse(document) for document in documents]
    with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=responses) as urlopen:
        with mock.patch.object(MODULE.time, "sleep") as sleep:
            result = MODULE.collect_advisories(window, watermark, metrics=metrics, **arguments)
    return result, metrics, urlopen, sleep


def main_out_err(arguments, responses, state_file):
    out, err = io.StringIO(), io.StringIO()
    args = ["--timeout", "17", "--request-delay", "0.5"]
    args.extend(arguments)
    if state_file is None:
        args.append("--no-state")
    else:
        args += ["--state-file", str(state_file)]
    effect = [document if isinstance(document, (JsonResponse, Exception)) else JsonResponse(document) for document in responses]
    with mock.patch.object(MODULE, "datetime", FixedDateTime):
        with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=effect):
            with mock.patch.object(MODULE.time, "sleep"):
                code = MODULE.main(args, out=out, err=err)
    return code, out.getvalue(), err.getvalue()


def emitted_summary(stderr_text):
    payload, _diagnostics = RUN_METADATA.extract_summary_line(stderr_text)
    assert payload is not None, f"no summary line in stderr: {stderr_text!r}"
    return RUN_METADATA.validate_summary(json.loads(payload))


def day24_pages():
    next_link = f"https://api.github.com/advisories?per_page=100&sort=updated&direction=asc&updated={DAY_24}&cursor=24b"
    return [
        page(
            [
                advisory("GHSA-a1a1-a1a1-a1a1", published_at="2026-11-20T10:00:00Z", updated_at=f"{DAY_24}T08:00:00Z"),
                advisory("GHSA-a2a2-a2a2-a2a2", published_at="2026-11-21T10:00:00Z", updated_at=f"{DAY_24}T12:00:00Z"),
            ],
            next_link=next_link,
        ),
        page(
            [
                advisory("GHSA-a3a3-a3a3-a3a3", published_at="2026-11-22T10:00:00Z", updated_at=f"{DAY_24}T16:00:00Z"),
            ]
        ),
        page(
            [
                advisory("GHSA-b1b1-b1b1-b1b1", published_at="2026-11-23T10:00:00Z", updated_at=f"{DAY_25}T02:30:00Z"),
            ]
        ),
    ]


class NormalizeAdvisoryTests(unittest.TestCase):
    def test_lifts_stable_fields_and_keeps_raw(self):
        original = advisory(
            "GHSA-c1c1-c1c1-c1c1",
            published_at="2026-11-20T10:00:00-05:00",
            updated_at=f"{DAY_24}T08:00:00Z",
            severity="critical",
        )
        record = MODULE.normalize_advisory(original, "2026-11-25T09:23:00Z")
        self.assertEqual(record["source"], "ghsa_advisories")
        self.assertEqual(record["id"], "GHSA-c1c1-c1c1-c1c1")
        self.assertEqual(record["fetched_at"], "2026-11-25T09:23:00Z")
        self.assertEqual(record["cve_id"], "CVE-2026-0001")
        self.assertEqual(record["severity"], "critical")
        self.assertEqual(record["published_at"], "2026-11-20T15:00:00Z")
        self.assertEqual(record["updated_at"], f"{DAY_24}T08:00:00Z")
        self.assertEqual(record["cvss"]["score"], "9.8")
        self.assertEqual(record["vulnerabilities"][0]["package"]["ecosystem"], "PyPI")
        self.assertIs(record["raw"], original)

    def test_rejects_untrusted_shapes(self):
        good = advisory("GHSA-d1d1-d1d1-d1d1", published_at=f"{DAY_24}T01:00:00Z", updated_at=f"{DAY_24}T02:00:00Z")
        cases = (
            ("not an object", [], "advisory is not a JSON object"),
            ("bad id", {**good, "ghsa_id": "GHSA-XXXX"}, "invalid ghsa_id"),
            ("id too long", {**good, "ghsa_id": "GHSA-abcdef-ghijkl-mnopqr"}, "invalid ghsa_id"),
            ("missing published", {**good, "published_at": None}, "published_at"),
            ("naive updated", {**good, "updated_at": "2026-11-24T02:00:00"}, "ISO-8601"),
            ("updated before published", {**good, "updated_at": "2026-11-23T00:00:00Z"}, "before published_at"),
            ("vulnerabilities not a list", {**good, "vulnerabilities": {}}, "non-list vulnerabilities"),
        )
        for label, broken, message in cases:
            with self.subTest(label):
                with self.assertRaisesRegex(MODULE.GhadvisoryError, message):
                    MODULE.normalize_advisory(broken, "stamp")


class ComputeWindowTests(unittest.TestCase):
    def test_first_run_uses_lookback_including_today(self):
        days, dropped = MODULE.compute_window(NOW, None, lookback_days=5, max_days=14)
        self.assertEqual(days, ["2026-11-21", "2026-11-22", "2026-11-23", "2026-11-24", "2026-11-25"])
        self.assertEqual(dropped, 0)

    def test_watermark_day_re_fetched_then_days_to_today(self):
        watermark = datetime(2026, 11, 24, 12, 0, 0, tzinfo=timezone.utc)
        days, dropped = MODULE.compute_window(NOW, watermark, lookback_days=5, max_days=14)
        self.assertEqual(days, [DAY_24, DAY_25])
        self.assertEqual(dropped, 0)

    def test_same_day_watermark_is_a_single_day_window(self):
        watermark = datetime(2026, 11, 25, 2, 30, 0, tzinfo=timezone.utc)
        days, dropped = MODULE.compute_window(NOW, watermark, lookback_days=5, max_days=14)
        self.assertEqual(days, [DAY_25])
        self.assertEqual(dropped, 0)

    def test_long_gap_drops_oldest_days_and_reports_them(self):
        watermark = datetime(2026, 11, 5, 1, 0, 0, tzinfo=timezone.utc)
        days, dropped = MODULE.compute_window(NOW, watermark, lookback_days=5, max_days=14)
        self.assertEqual(dropped, 7)
        self.assertEqual(len(days), 14)
        self.assertEqual(days[0], "2026-11-12")
        self.assertEqual(days[-1], DAY_25)


class CollectAdvisoriesTests(unittest.TestCase):
    def test_follows_link_cursor_across_days_and_paces_pages(self):
        watermark = None
        (records, new_watermark), metrics, urlopen, sleep = collect([DAY_24, DAY_25], watermark, day24_pages())
        self.assertEqual([record["id"] for record in records],
                         ["GHSA-a1a1-a1a1-a1a1", "GHSA-a2a2-a2a2-a2a2", "GHSA-a3a3-a3a3-a3a3", "GHSA-b1b1-b1b1-b1b1"])
        self.assertEqual(new_watermark, datetime(2026, 11, 25, 2, 30, 0, tzinfo=timezone.utc))
        self.assertEqual(len(urlopen.call_args_list), 3)
        self.assertEqual(metrics["pages"], 3)
        self.assertEqual(metrics["records_received"], 4)
        self.assertEqual(metrics["records_skipped_before_watermark"], 0)
        self.assertEqual(metrics["requests"]["attempted"], 3)
        self.assertEqual(metrics["requests"]["succeeded"], 3)
        self.assertEqual(metrics["requests"]["failed"], 0)
        self.assertGreater(metrics["requests"]["payload_bytes"], 0)
        self.assertIsNone(metrics["cap_reason"])
        # Two paces: after day 24 page 1 and after day 24 page 2, not after the final page.
        self.assertEqual(sleep.call_count, 2)
        first_query = urllib.parse.urlsplit(urlopen.call_args_list[0].args[0].full_url).query
        self.assertIn("updated=2026-11-24", first_query)

    def test_watermark_filters_the_boundary_day_without_duplicates(self):
        watermark = datetime(2026, 11, 24, 12, 0, 0, tzinfo=timezone.utc)
        (records, new_watermark), metrics, _, _ = collect([DAY_24, DAY_25], watermark, day24_pages())
        self.assertEqual([record["id"] for record in records], ["GHSA-a3a3-a3a3-a3a3", "GHSA-b1b1-b1b1-b1b1"])
        self.assertEqual(metrics["records_skipped_before_watermark"], 2)
        self.assertEqual(metrics["records_received"], 4)
        self.assertEqual(new_watermark, datetime(2026, 11, 25, 2, 30, 0, tzinfo=timezone.utc))

    def test_empty_bucket_is_successful_and_leaves_watermark_unset(self):
        (records, new_watermark), metrics, _, _ = collect([DAY_25], None, [page([])])
        self.assertEqual(records, [])
        self.assertIsNone(new_watermark)
        self.assertIsNone(metrics["cap_reason"])
        self.assertEqual(metrics["pages"], 1)

    def test_non_array_page_fails_the_run(self):
        with mock.patch.object(MODULE.urllib.request, "urlopen",
                               side_effect=[JsonResponse({"unexpected": "shape"})]):
            with self.assertRaisesRegex(MODULE.GhadvisoryError, "not a JSON array"):
                MODULE.collect_advisories(
                    [DAY_25], None,
                    max_pages=5, max_records=10, timeout=17, request_delay=0,
                    metrics=new_metrics(),
                )

    def test_next_link_must_stay_on_the_advisories_endpoint(self):
        bad = page([advisory("GHSA-e1e1-e1e1-e1e1", published_at="2026-11-24T01:00:00Z", updated_at=f"{DAY_24}T02:00:00Z")],
                   next_link="https://api.github.com/repos?cursor=next")
        with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=[bad]):
            with self.assertRaisesRegex(MODULE.GhadvisoryError, "leaves the advisories endpoint"):
                MODULE.collect_advisories(
                    [DAY_24], None,
                    max_pages=5, max_records=10, timeout=17, request_delay=0,
                    metrics=new_metrics(),
                )

    def test_day_page_cap_reports_partial_completeness(self):
        link = f"https://api.github.com/advisories?per_page=100&sort=updated&direction=asc&updated={DAY_24}&cursor=x"
        documents = [page([advisory("GHSA-f1f1-f1f1-f1f1", published_at="2026-11-24T01:00:00Z", updated_at=f"{DAY_24}T02:00:00Z")], next_link=link)]
        (records, new_watermark), metrics, _, _ = collect([DAY_24], None, documents, max_pages=1)
        self.assertEqual(len(records), 1)
        self.assertEqual(metrics["cap_reason"], "max_pages=1 exceeded on 2026-11-24")
        self.assertEqual(new_watermark, datetime(2026, 11, 24, 2, 0, 0, tzinfo=timezone.utc))

    def test_record_cap_stops_with_the_watermark_at_the_last_emitted_record(self):
        documents = [
            page([
                advisory("GHSA-g1g1-g1g1-g1g1", published_at="2026-11-24T01:00:00Z", updated_at=f"{DAY_24}T02:00:00Z"),
                advisory("GHSA-g2g2-g2g2-g2g2", published_at="2026-11-24T01:30:00Z", updated_at=f"{DAY_24}T03:00:00Z"),
            ])
        ]
        (records, new_watermark), metrics, _, _ = collect([DAY_24], None, documents, max_records=1)
        self.assertEqual([record["id"] for record in records], ["GHSA-g1g1-g1g1-g1g1"])
        self.assertEqual(metrics["cap_reason"], "max_records=1 exceeded on 2026-11-24")
        self.assertEqual(new_watermark, datetime(2026, 11, 24, 2, 0, 0, tzinfo=timezone.utc))


class RequestPageTests(unittest.TestCase):
    def _request(self, responses, timeout=17):
        metrics = new_metrics()
        with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=responses) as urlopen:
            with mock.patch.object(MODULE.time, "sleep") as sleep:
                document, headers = MODULE.request_page("https://api.github.com/advisories?updated=2026-11-25", timeout, metrics, metrics["_retry_budget"])
        return document, metrics, sleep, urlopen

    def test_recovers_from_502_then_200(self):
        document, metrics, sleep, _ = self._request([http_error(502), page([advisory("GHSA-h1h1-h1h1-h1h1", published_at="2026-11-24T01:00:00Z", updated_at=f"{DAY_24}T02:00:00Z")])])
        self.assertEqual(len(document), 1)
        self.assertEqual(metrics["requests"]["attempted"], 2)
        self.assertEqual(metrics["requests"]["succeeded"], 1)
        self.assertEqual(metrics["requests"]["failed"], 1)
        sleep.assert_called_once()

    def test_403_with_retry_after_is_a_secondary_rate_limit(self):
        document, metrics, sleep, _ = self._request([http_error(403, retry_after="30"), page([])])
        self.assertEqual(document, [])
        sleep.assert_called_once_with(30)
        self.assertEqual(metrics["requests"]["failed"], 1)

    def test_403_without_retry_after_fails_immediately(self):
        with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=[http_error(403)]):
            with self.assertRaisesRegex(MODULE.GhadvisoryError, "HTTP 403"):
                MODULE.request_page("https://api.github.com/advisories?updated=2026-11-25", 17, new_metrics(), [0.0])

    def test_exhausted_retry_budget_fails(self):
        with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=[http_error(500), http_error(500), http_error(500), http_error(500)]):
            with self.assertRaisesRegex(MODULE.GhadvisoryError, "HTTP 500"):
                MODULE.request_page("https://api.github.com/advisories?updated=2026-11-25", 17, new_metrics(), [0.0])

    def test_oversized_response_fails(self):
        body = b"x" * (MODULE.MAX_RESPONSE_BYTES + 1)
        with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=[JsonResponse(None, raw=body)]):
            with self.assertRaisesRegex(MODULE.GhadvisoryError, "exceeds"):
                MODULE.request_page("https://api.github.com/advisories?updated=2026-11-25", 17, new_metrics(), [0.0])

    def test_non_json_response_fails(self):
        with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=[JsonResponse(None, raw=b"<html>nope</html>")]):
            with self.assertRaisesRegex(MODULE.GhadvisoryError, "not valid UTF-8 JSON"):
                MODULE.request_page("https://api.github.com/advisories?updated=2026-11-25", 17, new_metrics(), [0.0])


class MainEndToEndTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.state_file = pathlib.Path(self._tmp.name) / "ghsa_advisories.json"

    def tearDown(self):
        self._tmp.cleanup()

    def test_first_run_emits_records_summary_and_state(self):
        code, out, err = main_out_err(["--lookback-days", "2"], day24_pages(), self.state_file)
        self.assertEqual(code, 0)
        records = [json.loads(line) for line in out.splitlines()]
        self.assertEqual(len(records), 4)
        self.assertTrue(all(record["fetched_at"] == "2026-11-25T09:23:00Z" for record in records))
        summary = emitted_summary(err)
        self.assertEqual(summary["health"], "healthy")
        self.assertEqual(summary["completeness"], "complete")
        self.assertEqual(summary["requests"]["attempted"], 3)
        self.assertEqual(summary["metrics"]["records_emitted"], 4)
        self.assertEqual(summary["state_change"]["watermark"], f"{DAY_25}T02:30:00Z")
        state = json.loads(self.state_file.read_text(encoding="utf-8"))
        self.assertEqual(state["source"], "ghsa_advisories")
        self.assertEqual(state["watermark"], f"{DAY_25}T02:30:00Z")

    def test_replay_after_watermark_emits_only_newer_records(self):
        # First run lands the watermark at 2026-11-25T02:30:00Z.
        code, _, _ = main_out_err(["--lookback-days", "2"], day24_pages(), self.state_file)
        self.assertEqual(code, 0)
        # Replay: the day-25 bucket now also carries an older duplicate of
        # GHSA-b1b1 (updated at the watermark) and one newer record.
        replay = [
            page([
                advisory("GHSA-b1b1-b1b1-b1b1", published_at="2026-11-23T10:00:00Z", updated_at=f"{DAY_25}T02:30:00Z"),
                advisory("GHSA-c9c9-c9c9-c9c9", published_at="2026-11-24T10:00:00Z", updated_at=f"{DAY_25}T07:15:00Z"),
            ])
        ]
        code, out, err = main_out_err([], replay, self.state_file)
        self.assertEqual(code, 0)
        records = [json.loads(line) for line in out.splitlines()]
        self.assertEqual([record["id"] for record in records], ["GHSA-c9c9-c9c9-c9c9"])
        summary = emitted_summary(err)
        self.assertEqual(summary["completeness"], "complete")
        self.assertEqual(summary["coverage"]["records_skipped_before_watermark"], 1)
        self.assertEqual(summary["state_change"]["watermark"], f"{DAY_25}T07:15:00Z")

    def test_persistent_500_fails_and_leaves_state_untouched(self):
        code, out, err = main_out_err([], [http_error(500), http_error(500), http_error(500), http_error(500)], self.state_file)
        self.assertEqual(code, 1)
        self.assertEqual(out, "")
        summary = emitted_summary(err)
        self.assertEqual(summary["health"], "failed")
        self.assertEqual(summary["completeness"], "failed")
        self.assertIn("error", summary["metrics"])
        self.assertFalse(self.state_file.exists())

    def test_no_state_never_writes_a_state_file(self):
        code, out, err = main_out_err(["--lookback-days", "2"], day24_pages(), None)
        self.assertEqual(code, 0)
        self.assertEqual(len(out.splitlines()), 4)
        self.assertEqual(emitted_summary(err)["completeness"], "complete")

    def test_state_path_inside_the_checkout_is_rejected(self):
        code, out, err = main_out_err(
            [], day24_pages(), pathlib.Path(__file__).parent / "forbidden_state.json"
        )
        self.assertEqual(code, 1)
        self.assertIn("outside the checkout", err)
        self.assertFalse((pathlib.Path(__file__).parent / "forbidden_state.json").exists())


class YamlConfigTests(unittest.TestCase):
    def test_source_yaml_matches_the_script_contract(self):
        import yaml

        config_path = pathlib.Path(__file__).parent / "sources" / "ghsa_advisories.yml"
        cfg = yaml.safe_load(config_path.read_text(encoding="utf-8"))
        self.assertEqual(cfg["name"], "ghsa_advisories")
        self.assertEqual(cfg["script"], "fetch_ghsa_advisories.py")
        self.assertTrue((pathlib.Path(__file__).parent / "scripts" / cfg["script"]).is_file())
        # args must be literal CLI tokens the parser accepts.
        parser = MODULE._parser()
        parsed = parser.parse_args(cfg["args"])
        self.assertEqual(parsed.lookback_days, 5)
        self.assertEqual(parsed.max_days, 14)
        self.assertEqual(parsed.max_pages, 200)
        self.assertEqual(parsed.max_records, 20000)
        for field in ("schedule", "cadence_note", "rate_limit", "licence", "attribution"):
            self.assertTrue(cfg.get(field), f"missing {field}")
        parts = cfg["schedule"].split()
        self.assertEqual(len(parts), 5)


if __name__ == "__main__":
    unittest.main()
