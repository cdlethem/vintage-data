"""Behavioral tests for fetch_github_advisories.py (offline, scripted transport)."""

import contextlib
import importlib.util
import io
import json
import tempfile
import unittest
import urllib.error
import urllib.parse
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

import yaml

SCRIPT = Path(__file__).parent / "scripts" / "fetch_github_advisories.py"
SOURCE_CONFIG = Path(__file__).parent / "sources" / "github_advisories.yml"
SPEC = importlib.util.spec_from_file_location("fetch_github_advisories", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)

NOW = datetime(2026, 10, 8, 5, 30, 0, tzinfo=timezone.utc)


def advisory(ghsa_id, published_at, *, severity="high", cve_id=None, ecosystem="pip", package="some-pkg"):
    return {
        "ghsa_id": ghsa_id,
        "cve_id": cve_id,
        "summary": f"Summary for {ghsa_id}",
        "description": f"Details for {ghsa_id}",
        "severity": severity,
        "cvss": {"score": 7.5, "vector_string": "CVSS:3.1/AV:N/AC:L/PR:N/UI:N/S:U/C:H/I:N/A:N"},
        "cvss_severities": {"cvss_v3": {"score": 7.5, "vector_string": "CVSS:3.1/AV:N/AC:L/PR:N/UI:N/S:U/C:H/I:N/A:N"}},
        "cwes": [{"cwe_id": "CWE-116", "name": "Improper Encoding or Escaping of Output"}],
        "vulnerabilities": [
            {
                "package": {"ecosystem": ecosystem, "name": package},
                "vulnerable_version_range": "<= 1.0",
                "first_patched_version": "1.1",
                "vulnerable_functions": [],
            }
        ],
        "epss": {"percentage": 0.01, "percentile": 0.5},
        "comments": 0,
        "credits": [{"user": {"login": "alice", "name": "Alice", "avatar_url": "https://avatars.example/a.png"}}],
        "github_reviewed_at": published_at,
        "nvd_published_at": None,
        "published_at": published_at,
        "updated_at": published_at,
        "withdrawn_at": None,
        "references": [{"type": "WEB", "url": f"https://github.com/advisories/{ghsa_id}"}],
        "identifiers": [{"value": ghsa_id, "type": "GHSA"}],
        "type": "reviewed",
        "source_code_location": None,
        "url": f"https://github.com/advisories/{ghsa_id}",
        "html_url": f"https://github.com/advisories/{ghsa_id}",
    }


def _parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


class ApiTransport:
    """Simulate api.github.com/advisories over a fixed universe of advisories.

    For a ``published=START..END`` query it returns the in-range records in
    (published_at, ghsa_id) order, truncated to the first PAGE_SIZE — exactly
    how the real endpoint behaves once ``page`` stops working.
    """

    def __init__(self, universe):
        self.universe = sorted(universe, key=lambda a: (a["published_at"], a["ghsa_id"]))
        self.requests = []  # list of (start, end) windows as queried

    def __call__(self, url, timeout, counters=None):
        query = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(url).query))
        start_s, end_s = query["published"].split("..")
        start, end = _parse_ts(start_s), _parse_ts(end_s)
        self.requests.append((start, end))
        in_range = [a for a in self.universe if start <= _parse_ts(a["published_at"]) <= end]
        return in_range[: MODULE.PAGE_SIZE]


class FakeClock:
    def __init__(self):
        self.value = 0.0
        self.sleeps = []

    def monotonic(self):
        return self.value

    def sleep(self, delay):
        self.value += delay
        self.sleeps.append(delay)


class GitHubAdvisoriesTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.state_file = Path(self._tmp.name) / "state" / "github_advisories.json"

    def run_fetch(self, transport, *, now=NOW, path=None, lookback=24):
        clock = FakeClock()
        output, errors = io.StringIO(), io.StringIO()
        with contextlib.redirect_stderr(errors):
            count = MODULE.run(
                path=path,
                output=output,
                lookback_hours=lookback,
                transport=transport,
                now=lambda: now,
                monotonic=clock.monotonic,
                sleep=clock.sleep,
            )
        return count, [json.loads(line) for line in output.getvalue().splitlines()], errors.getvalue(), clock

    # -- normalization ------------------------------------------------------

    def test_normalizes_advisory_and_strips_personal_data(self):
        transport = ApiTransport(
            [
                advisory("GHSA-1111-aaaa-bbbb", "2026-10-08T04:00:00Z", cve_id="CVE-2026-12345"),
                advisory(
                    "GHSA-2222-cccc-dddd", "2026-10-08T04:05:00Z",
                    severity="critical", ecosystem="npm", package="some-lib",
                ),
            ]
        )
        count, records, summary, _ = self.run_fetch(transport)
        self.assertEqual(count, 2)
        self.assertEqual(len(records), 2)
        first = records[0]
        self.assertEqual(first["source"], "github_advisories")
        self.assertEqual(first["fetched_at"], "2026-10-08T05:30:00Z")
        self.assertEqual(first["id"], "GHSA-1111-aaaa-bbbb")
        self.assertEqual(first["cve_id"], "CVE-2026-12345")
        self.assertEqual(first["severity"], "high")
        self.assertEqual(first["cvss"]["score"], 7.5)
        self.assertEqual(first["cwes"], ["CWE-116"])
        self.assertEqual(first["vulnerabilities"][0]["package"], "some-pkg")
        self.assertEqual(first["vulnerabilities"][0]["ecosystem"], "pip")
        self.assertEqual(first["vulnerabilities"][0]["first_patched_version"], "1.1")
        self.assertEqual(first["epss"]["percentage"], 0.01)
        self.assertEqual(first["published_at"], "2026-10-08T04:00:00Z")
        self.assertEqual(first["references"], ["https://github.com/advisories/GHSA-1111-aaaa-bbbb"])
        self.assertEqual(records[1]["severity"], "critical")
        # one run, one fetch timestamp across all records
        self.assertEqual(records[0]["fetched_at"], records[1]["fetched_at"])
        payload = json.dumps(records)
        self.assertNotIn("credits", payload)
        self.assertNotIn("alice", payload)
        self.assertIn('"health":"healthy"', summary)

    # -- windows and watermark ----------------------------------------------

    def test_initial_run_queries_the_lookback_window(self):
        transport = ApiTransport([])
        self.run_fetch(transport)
        self.assertEqual(
            transport.requests,
            [(NOW - timedelta(hours=24), NOW)],
        )

    def test_watermark_keeps_only_strictly_newer_advisories(self):
        watermark_at, watermark_id = "2026-10-08T05:00:00Z", "GHSA-b"
        self.state_file.parent.mkdir(parents=True)
        self.state_file.write_text(
            json.dumps(
                {
                    "version": 1,
                    "source": "github_advisories",
                    "last_published_at": watermark_at,
                    "last_id": watermark_id,
                }
            )
            + "\n",
            encoding="utf-8",
        )
        transport = ApiTransport(
            [
                advisory("GHSA-a", "2026-10-08T04:59:59Z"),  # older: skip
                advisory("GHSA-a", "2026-10-08T05:00:00Z"),  # same second, id < watermark: skip
                advisory("GHSA-b", "2026-10-08T05:00:00Z"),  # exactly the watermark: skip
                advisory("GHSA-c", "2026-10-08T05:00:00Z"),  # same second, id > watermark: keep
                advisory("GHSA-a", "2026-10-08T05:00:01Z"),  # newer second: keep
            ]
        )
        count, records, _, _ = self.run_fetch(transport, path=self.state_file)
        self.assertEqual(count, 2)
        self.assertEqual([r["id"] for r in records], ["GHSA-c", "GHSA-a"])
        state = json.loads(self.state_file.read_text(encoding="utf-8"))
        self.assertEqual(state["last_published_at"], "2026-10-08T05:00:01Z")
        self.assertEqual(state["last_id"], "GHSA-a")

    def test_second_run_replays_nothing_and_advances_to_window_end(self):
        first = ApiTransport([advisory("GHSA-1", "2026-10-08T04:00:00Z")])
        count, records, _, _ = self.run_fetch(first, path=self.state_file)
        self.assertEqual(count, 1)
        self.assertTrue(self.state_file.exists())

        second = ApiTransport([advisory("GHSA-1", "2026-10-08T04:00:00Z")])
        count2, records2, summary2, _ = self.run_fetch(second, path=self.state_file)
        self.assertEqual(count2, 0)
        self.assertEqual(records2, [])
        state = json.loads(self.state_file.read_text(encoding="utf-8"))
        # no new records: the watermark moves to the window end, not the last record
        self.assertEqual(state["last_published_at"], "2026-10-08T05:30:00Z")
        self.assertIn('"health":"healthy"', summary2)

    def test_window_clamped_to_max_lookback_is_flagged_truncated(self):
        old_watermark = (NOW - timedelta(hours=200)).isoformat().replace("+00:00", "Z")
        self.state_file.parent.mkdir(parents=True)
        self.state_file.write_text(
            json.dumps(
                {
                    "version": 1,
                    "source": "github_advisories",
                    "last_published_at": old_watermark,
                    "last_id": "GHSA-old",
                }
            )
            + "\n",
            encoding="utf-8",
        )
        transport = ApiTransport([])
        self.run_fetch(transport, path=self.state_file)
        self.assertEqual(
            transport.requests[0],
            (NOW - timedelta(hours=MODULE.MAX_LOOKBACK_HOURS), NOW),
        )
        # clamped start: the 200h-old watermark is replaced by the 168h floor
        state = json.loads(self.state_file.read_text(encoding="utf-8"))
        self.assertEqual(state["last_published_at"], "2026-10-08T05:30:00Z")

    def test_empty_window_makes_no_requests_and_changes_no_state(self):
        self.state_file.parent.mkdir(parents=True)
        self.state_file.write_text(
            json.dumps(
                {
                    "version": 1,
                    "source": "github_advisories",
                    "last_published_at": "2026-10-08T05:30:00Z",
                    "last_id": "GHSA-x",
                }
            )
            + "\n",
            encoding="utf-8",
        )
        transport = ApiTransport([])
        count, records, summary, _ = self.run_fetch(transport, path=self.state_file)
        self.assertEqual(count, 0)
        self.assertEqual(records, [])
        self.assertEqual(transport.requests, [])
        state = json.loads(self.state_file.read_text(encoding="utf-8"))
        self.assertEqual(state["last_published_at"], "2026-10-08T05:30:00Z")
        self.assertEqual(state["last_id"], "GHSA-x")
        self.assertIn('"health":"healthy"', summary)

    # -- bisection (the endpoint ignores `page`) -----------------------------

    def test_bisects_capped_slices_without_dropping_or_duplicating(self):
        # 250 advisories across three hours; one sits exactly on the first bisection midpoint.
        universe = [
            advisory(f"GHSA-{i:04d}-{i:04x}-{i:04x}",
                     (NOW - timedelta(hours=3) + timedelta(seconds=42 * i)).isoformat().replace("+00:00", "Z"))
            for i in range(250)
        ]
        universe[125]["published_at"] = (NOW - timedelta(hours=3) + timedelta(hours=1, minutes=30)).isoformat().replace("+00:00", "Z")
        transport = ApiTransport(universe)
        count, records, summary, clock = self.run_fetch(transport)
        self.assertEqual(count, 250)
        ids = [r["id"] for r in records]
        self.assertEqual(len(set(ids)), 250)
        self.assertEqual(
            sorted(r["published_at"] for r in records),
            sorted(a["published_at"] for a in universe),
        )
        # more than one request proves the capped window was split, not dropped
        self.assertGreater(len(transport.requests), 1)
        # 1 s self-imposed spacing between successive requests
        self.assertEqual(len(clock.sleeps), len(transport.requests) - 1)
        self.assertTrue(all(s >= 1.0 for s in clock.sleeps))
        self.assertIn('"health":"healthy"', summary)

    # -- bisection floor -------------------------------------------------------

    def test_capped_slice_at_the_floor_fails_closed(self):
        # 150 advisories inside a 4-minute window: the window is below the 5-minute
        # floor, so bisection cannot split it and the run must raise, not truncate.
        base = NOW - timedelta(minutes=4)
        universe = [
            advisory(
                f"GHSA-{i:04d}-{i:04x}-{i:04x}",
                (base + timedelta(seconds=1.5 * i)).isoformat().replace("+00:00", "Z"),
            )
            for i in range(150)
        ]
        transport = ApiTransport(universe)
        with self.assertRaises(ValueError) as caught:
            self.run_fetch(transport)
        self.assertIn("page cap", str(caught.exception))
        self.assertFalse(self.state_file.exists())

    def test_unreviewed_advisory_with_no_package_mapping_is_emitted(self):
        bare = advisory("GHSA-bare-0000-0000", "2026-10-08T04:00:00Z")
        bare["vulnerabilities"] = []
        bare["type"] = "unreviewed"
        bare["cve_id"] = None
        bare["cvss"] = None
        bare["cwes"] = None
        bare["epss"] = None
        transport = ApiTransport([bare])
        count, records, _, _ = self.run_fetch(transport)
        self.assertEqual(count, 1)
        self.assertEqual(records[0]["vulnerabilities"], [])
        self.assertEqual(records[0]["severity"], "high")
        self.assertEqual(records[0]["published_at"], "2026-10-08T04:00:00Z")

    # -- rate limiting and transport errors ----------------------------------

    def test_rate_limit_sleeps_until_reset_and_retries_once(self):
        url = MODULE.build_url(NOW - timedelta(hours=1), NOW)
        reset_epoch = 1_000_030
        rate_limited = urllib.error.HTTPError(
            url, 403, "API rate limit exceeded",
            {"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": str(reset_epoch)},
            None,
        )

        class OkResponse:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def read(self):
                return b"[]"

        sleeps = []
        counters = {"attempted": 0, "succeeded": 0, "rate_limit_waits": 0}
        with mock.patch.object(
            MODULE.urllib.request, "urlopen", side_effect=[rate_limited, OkResponse()]
        ) as urlopen, mock.patch.object(MODULE.time, "time", return_value=1_000_000.0):
            rows = MODULE.request_json(
                url, 30, counters=counters,
                epoch_now=lambda: 1_000_000.0,
                sleep=sleeps.append,
            )
        self.assertEqual(rows, [])
        self.assertEqual(urlopen.call_count, 2)
        self.assertEqual(sleeps, [35])  # reset + 5 s margin
        self.assertEqual(counters, {"attempted": 2, "succeeded": 1, "rate_limit_waits": 1})

    def test_rate_limit_wait_is_capped(self):
        url = MODULE.build_url(NOW - timedelta(hours=1), NOW)
        rate_limited = urllib.error.HTTPError(
            url, 429, "abuse detected",
            {"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": str(1_000_000 + 3600 * 24)},
            None,
        )

        class OkResponse:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def read(self):
                return b"[]"

        sleeps = []
        with mock.patch.object(
            MODULE.urllib.request, "urlopen", side_effect=[rate_limited, OkResponse()]
        ), mock.patch.object(MODULE.time, "time", return_value=1_000_000.0):
            MODULE.request_json(
                url, 30,
                counters={"attempted": 0, "succeeded": 0, "rate_limit_waits": 0},
                epoch_now=lambda: 1_000_000.0,
                sleep=sleeps.append,
            )
        self.assertEqual(sleeps, [MODULE.MAX_RATE_LIMIT_WAIT])

    def test_non_rate_limit_http_error_propagates_without_retry(self):
        url = MODULE.build_url(NOW - timedelta(hours=1), NOW)
        server_error = urllib.error.HTTPError(url, 500, "internal error", {}, None)
        with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=server_error) as urlopen:
            with self.assertRaises(urllib.error.HTTPError):
                MODULE.request_json(
                    url, 30,
                    counters={"attempted": 0, "succeeded": 0, "rate_limit_waits": 0},
                )
        self.assertEqual(urlopen.call_count, 1)

    def test_non_list_response_fails_without_writing_state(self):
        def bad_transport(url, timeout, counters=None):
            return {"message": "Something went wrong"}

        with self.assertRaises(ValueError) as caught:
            self.run_fetch(bad_transport, path=self.state_file)
        self.assertIn("not a JSON list", str(caught.exception))
        self.assertFalse(self.state_file.exists())

    def test_record_without_ghsa_id_fails_without_writing_state(self):
        def bad_transport(url, timeout, counters=None):
            return [{"summary": "no id here"}]

        with self.assertRaises(ValueError) as caught:
            self.run_fetch(bad_transport, path=self.state_file)
        self.assertIn("ghsa_id", str(caught.exception))
        self.assertFalse(self.state_file.exists())

    # -- state durability -----------------------------------------------------

    def test_failed_run_does_not_advance_state(self):
        first = ApiTransport([advisory("GHSA-1", "2026-10-08T04:00:00Z")])
        self.run_fetch(first, path=self.state_file)
        before = self.state_file.read_text(encoding="utf-8")

        def boom(url, timeout, counters=None):
            raise RuntimeError("connection reset")

        with self.assertRaises(RuntimeError):
            self.run_fetch(boom, path=self.state_file)
        self.assertEqual(self.state_file.read_text(encoding="utf-8"), before)

    def test_no_state_path_is_never_written(self):
        transport = ApiTransport([advisory("GHSA-1", "2026-10-08T04:00:00Z")])
        count, _, _, _ = self.run_fetch(transport, path=None)
        self.assertEqual(count, 1)
        self.assertFalse(self.state_file.exists())
        self.assertFalse(list(Path(self._tmp.name).rglob("*")), "no file may appear under the data root")

    def test_malformed_state_is_rejected(self):
        self.state_file.parent.mkdir(parents=True)
        self.state_file.write_text(
            json.dumps({"version": 99, "source": "github_advisories"}), encoding="utf-8"
        )
        with self.assertRaises(ValueError) as caught:
            self.run_fetch(ApiTransport([]), path=self.state_file)
        self.assertIn("malformed state", str(caught.exception))

    # -- source YAML -----------------------------------------------------------

    def test_source_yaml_matches_fetcher_contract(self):
        config = yaml.safe_load(SOURCE_CONFIG.read_text(encoding="utf-8"))
        self.assertEqual(config["name"], MODULE.SOURCE)
        self.assertEqual(config["script"], SCRIPT.name)
        self.assertEqual(config["args"], [])
        self.assertEqual(len(config["schedule"].split()), 5)
        self.assertTrue(config["enabled"])
        self.assertIn("rate_limit", config)
        self.assertIn("licence", config)
        self.assertIn("attribution_required", config)


if __name__ == "__main__":
    unittest.main()
