from datetime import datetime, timezone
import importlib.util
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock
import urllib.parse


SCRIPT = Path(__file__).parent / "scripts" / "fetch_usgs_water_data_ogc.py"
CONFIG = Path(__file__).parent / "sources" / "usgs_water_data_ogc.yml"
SPEC = importlib.util.spec_from_file_location("fetch_usgs_water_data_ogc", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)
NOW = datetime(2026, 9, 17, 12, 0, tzinfo=timezone.utc)
BASE = MODULE.BASE_URL


class JsonResponse(io.BytesIO):
    def __init__(self, document):
        super().__init__(json.dumps(document).encode("utf-8"))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


class BrokenOutput:
    def write(self, value):
        raise OSError("downstream closed")

    def flush(self):
        raise AssertionError("flush must not follow a failed write")


def feature(
    name,
    station="USGS-01646500",
    observed_at="2026-09-17T11:00:00Z",
    parameter="00060",
    series="series-1",
    value="12.5",
    **properties,
):
    values = {
        "monitoring_location_id": station,
        "time": observed_at,
        "parameter_code": parameter,
        "time_series_id": series,
        "statistic_id": "00003",
        "method_id": "method-1",
        "unit_of_measure": "ft3/s",
        "value": value,
        "approval_status": "approved",
        **properties,
    }
    return {
        "type": "Feature",
        "id": name,
        "geometry": {"type": "Point", "coordinates": [-77.1, 38.9]},
        "properties": values,
    }


def page(features=(), *, collection="continuous", next_url=None):
    links = [] if next_url is None else [{"rel": "next", "href": next_url, "type": "application/geo+json"}]
    return {
        "type": "FeatureCollection",
        "collection": collection,
        "numberReturned": len(features),
        "features": list(features),
        "links": links,
    }


def empty_state(next_target=None):
    return {"version": MODULE.STATE_VERSION, "next_target": next_target, "targets": {}}


def collect(targets, state, documents, **overrides):
    arguments = {
        "page_size": 2,
        "max_pages": 10,
        "max_records": 20,
        "lookback_hours": 48,
        "timeout": 17,
        "request_delay": 0,
        "now": NOW,
    }
    arguments.update(overrides)
    responses = [JsonResponse(document) for document in documents]
    with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=responses) as urlopen:
        with mock.patch.object(MODULE.time, "sleep") as sleep:
            result = MODULE.collect_observations(targets, state, **arguments)
    return result, urlopen, sleep


class FetchUSGSWaterDataOGCTests(unittest.TestCase):
    def setUp(self):
        self.targets = MODULE.build_targets(["01646500"], ["continuous"])
        self.target = self.targets[0]

    def test_normalizes_raw_observation_and_stable_measurement_aware_identity(self):
        original = feature("provider-1", qualifier="ice")
        record = MODULE.normalize_feature(original, self.target, "2026-09-17T12:00:00Z")
        later = MODULE.normalize_feature(original, self.target, "2026-09-18T12:00:00Z")
        other_measurement = MODULE.normalize_feature(
            feature("provider-1", parameter="00065", series="series-2"),
            self.target,
            "2026-09-17T12:00:00Z",
        )

        self.assertEqual(record["source"], "usgs_water_data_ogc")
        self.assertEqual(record["station_id"], "USGS-01646500")
        self.assertEqual(record["collection_id"], "continuous")
        self.assertEqual(record["observed_at"], "2026-09-17T11:00:00Z")
        self.assertEqual(record["value"], 12.5)
        self.assertEqual(record["provider_feature_id"], "provider-1")
        self.assertEqual(record["measurement"]["parameter_code"], "00060")
        self.assertEqual(record["raw_observation"], original["properties"])
        self.assertEqual(record["raw_geometry"], original["geometry"])
        self.assertEqual(record["id"], later["id"])
        self.assertNotEqual(record["fetched_at"], later["fetched_at"])
        self.assertNotEqual(record["id"], other_measurement["id"])

    def test_normalizes_timezone_and_missing_optional_value(self):
        item = feature(
            "provider-2",
            observed_at="2026-09-17T07:00:00-04:00",
            value=None,
            series=None,
        )
        record = MODULE.normalize_feature(item, self.target, "stamp")
        self.assertEqual(record["observed_at"], "2026-09-17T11:00:00Z")
        self.assertIsNone(record["value"])
        self.assertIsNone(record["measurement"]["series_id"])

    def test_rejects_observations_without_trustworthy_identity(self):
        cases = (
            ({"type": "Feature", "properties": []}, "properties"),
            (feature("x", station="USGS-99999999"), "does not match"),
            (feature("x", observed_at="2026-09-17 11:00:00"), "timezone"),
            (feature("x", parameter=None, series=None), "measurement identity"),
            (feature("x", value="nan"), "not finite"),
        )
        for item, message in cases:
            with self.subTest(message=message):
                with self.assertRaisesRegex(MODULE.OGCResponseError, message):
                    MODULE.normalize_feature(item, self.target, "stamp")

    def test_build_targets_requires_explicit_allowed_station_and_collection(self):
        self.assertEqual(self.target["station"], "USGS-01646500")
        with self.assertRaisesRegex(ValueError, "explicitly allowed"):
            MODULE.build_targets(["01646500"], ["invented-live-collection"])
        with self.assertRaisesRegex(ValueError, "station identifier"):
            MODULE.build_targets(["not-a-station"], ["continuous"])
        with self.assertRaisesRegex(ValueError, "at least one"):
            MODULE.build_targets([], ["continuous"])

    def test_initial_request_is_bounded_identified_and_timed_out(self):
        (records, state, metrics), urlopen, sleep = collect(
            self.targets,
            empty_state(),
            [page([feature("one")])],
        )
        self.assertEqual([record["provider_feature_id"] for record in records], ["one"])
        request = urlopen.call_args.args[0]
        parsed = urllib.parse.urlsplit(request.full_url)
        query = urllib.parse.parse_qs(parsed.query)
        self.assertEqual(parsed.path, "/ogcapi/v0/collections/continuous/items")
        self.assertEqual(query["monitoring_location_id"], ["USGS-01646500"])
        self.assertEqual(query["limit"], ["2"])
        self.assertEqual(
            query["datetime"],
            ["2026-09-15T12:00:00Z/2026-09-17T12:00:00Z"],
        )
        self.assertEqual(request.headers["User-agent"], MODULE.USER_AGENT)
        self.assertEqual(urlopen.call_args.kwargs, {"timeout": 17})
        sleep.assert_not_called()
        self.assertNotIn("api_key", query)
        self.assertNotIn("Authorization", request.headers)
        self.assertEqual(state["targets"], {})
        self.assertEqual(metrics["pages"], 1)
        self.assertEqual(metrics["records_received"], 1)
        self.assertEqual(metrics["targets_completed"], 1)
        self.assertGreater(metrics["requests"]["response_bytes"], 0)

    def test_empty_feature_collection_is_successful(self):
        (records, state, metrics), _, _ = collect(self.targets, empty_state(), [page()])
        self.assertEqual(records, [])
        self.assertEqual(state["targets"], {})
        self.assertEqual(metrics["targets_completed"], 1)
        self.assertIsNone(metrics["cap_reason"])

    def test_uses_only_returned_next_link_and_paces_pages(self):
        next_url = f"{BASE}/collections/continuous/items?cursor=opaque-2"
        documents = [page([feature("one")], next_url=next_url), page([feature("two")])]
        (records, state, metrics), urlopen, sleep = collect(
            self.targets,
            empty_state(),
            documents,
            request_delay=0.75,
        )
        self.assertEqual([record["provider_feature_id"] for record in records], ["one", "two"])
        self.assertEqual(urlopen.call_args_list[1].args[0].full_url, next_url)
        sleep.assert_called_once_with(0.75)
        self.assertEqual(state["targets"], {})
        self.assertEqual(metrics["pages"], 2)
        self.assertEqual(metrics["requests"]["requests"], 2)

    def test_rejects_malformed_catalog_and_pagination_documents(self):
        current = f"{BASE}/collections/continuous/items?cursor=one"
        malformed = (
            ([], "FeatureCollection"),
            ({"type": "FeatureCollection", "features": {}}, "features"),
            (page([], collection="other"), "does not match"),
            ({**page([feature("x")]), "numberReturned": 0}, "numberReturned"),
            ({**page(), "links": ["bad"]}, "malformed link"),
            (page([], next_url="https://example.com/items?cursor=2"), "origin"),
            (page([], next_url=f"{BASE}/collections/other/items?cursor=2"), "collection"),
            (
                {**page(), "links": [{"rel": "next", "href": current}, {"rel": "next", "href": current + "2"}]},
                "multiple next",
            ),
            (page([], next_url=current), "repeats"),
        )
        for document, message in malformed:
            with self.subTest(message=message):
                with self.assertRaisesRegex(MODULE.OGCResponseError, message):
                    MODULE.parse_page(document, current, self.target, BASE)

    def test_rejects_non_json_and_oversized_responses(self):
        class RawResponse(io.BytesIO):
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc_value, traceback):
                self.close()

        metrics = {"requests": 0, "response_bytes": 0}
        with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=RawResponse(b"not json")):
            with self.assertRaisesRegex(MODULE.OGCResponseError, "valid UTF-8 JSON"):
                MODULE._request_document("https://example.test", 2, metrics)
        with mock.patch.object(MODULE, "MAX_RESPONSE_BYTES", 3):
            with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=RawResponse(b"1234")):
                with self.assertRaisesRegex(MODULE.OGCResponseError, "exceeds"):
                    MODULE._request_document("https://example.test", 2, metrics)
        self.assertEqual(metrics["requests"], 2)
        self.assertEqual(metrics["response_bytes"], 12)

    def test_page_limit_rotates_targets_and_resumes_returned_page(self):
        targets = MODULE.build_targets(["01646500", "09380000"], ["continuous"])
        a_next = f"{BASE}/collections/continuous/items?cursor=a2"
        b_next = f"{BASE}/collections/continuous/items?cursor=b2"
        state = empty_state()

        (first, state, first_metrics), _, _ = collect(
            targets,
            state,
            [page([feature("a1")], next_url=a_next)],
            max_pages=1,
        )
        (second, state, second_metrics), second_http, _ = collect(
            targets,
            state,
            [page([feature("b1", station="USGS-09380000")], next_url=b_next)],
            max_pages=1,
        )
        (third, state, third_metrics), third_http, _ = collect(
            targets,
            state,
            [page([feature("a2")])],
            max_pages=1,
        )

        self.assertEqual([row["provider_feature_id"] for row in first + second + third], ["a1", "b1", "a2"])
        self.assertEqual(second_http.call_args.args[0].full_url.split("?")[0], f"{BASE}/collections/continuous/items")
        self.assertIn("USGS-09380000", second_http.call_args.args[0].full_url)
        self.assertEqual(third_http.call_args.args[0].full_url, a_next)
        self.assertEqual(first_metrics["cap_reason"], "pages")
        self.assertEqual(second_metrics["cap_reason"], "pages")
        self.assertEqual(third_metrics["cap_reason"], "pages")
        self.assertNotIn(targets[0]["key"], state["targets"])
        self.assertIn(targets[1]["key"], state["targets"])

    def test_record_limit_rotates_targets_and_resumes_inside_pages_without_skips(self):
        targets = MODULE.build_targets(["01646500", "09380000"], ["continuous"])
        state = empty_state()
        pages = {
            "a": page([feature("a0"), feature("a1"), feature("a2")]),
            "b": page(
                [
                    feature("b0", station="USGS-09380000"),
                    feature("b1", station="USGS-09380000"),
                ]
            ),
        }
        observed = []
        requested = []
        expected = (("a", "a0"), ("b", "b0"), ("a", "a1"), ("b", "b1"), ("a", "a2"))
        for target_name, expected_id in expected:
            (rows, state, metrics), urlopen, _ = collect(
                targets,
                state,
                [pages[target_name]],
                max_records=1,
            )
            observed.extend(row["provider_feature_id"] for row in rows)
            requested.append(urlopen.call_args.args[0].full_url)
            self.assertEqual(metrics["cap_reason"], "records")
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["provider_feature_id"], expected_id)

        self.assertEqual(observed, [item[1] for item in expected])
        self.assertIn("USGS-01646500", requested[0])
        self.assertIn("USGS-09380000", requested[1])
        self.assertEqual(requested[0], requested[2])
        self.assertEqual(requested[1], requested[3])
        self.assertNotIn(targets[0]["key"], state["targets"])
        self.assertNotIn(targets[1]["key"], state["targets"])

    def test_rejects_invalid_global_bounds_before_http(self):
        cases = (
            ({"page_size": 0}, "page_size"),
            ({"max_pages": 0}, "max_pages"),
            ({"max_records": 0}, "max_records"),
            ({"lookback_hours": 745}, "lookback_hours"),
            ({"timeout": 0}, "timeout"),
            ({"request_delay": -1}, "request_delay"),
        )
        defaults = {
            "page_size": 2,
            "max_pages": 2,
            "max_records": 2,
            "lookback_hours": 48,
            "timeout": 2,
            "request_delay": 0,
            "now": NOW,
        }
        for override, message in cases:
            with self.subTest(message=message):
                arguments = {**defaults, **override}
                with mock.patch.object(MODULE.urllib.request, "urlopen") as urlopen:
                    with self.assertRaisesRegex(ValueError, message):
                        MODULE.collect_observations(self.targets, empty_state(), **arguments)
                urlopen.assert_not_called()

    def test_state_path_rejects_checkout_from_script_cwd_and_resolved_symlink(self):
        scripts = SCRIPT.parent
        previous = Path.cwd()
        try:
            os.chdir(scripts)
            with self.assertRaisesRegex(ValueError, "outside the repository"):
                MODULE.validate_state_path("../state-usgs-water.json")
        finally:
            os.chdir(previous)

        with tempfile.TemporaryDirectory() as directory:
            link = Path(directory) / "checkout-link"
            try:
                link.symlink_to(MODULE.REPOSITORY_ROOT, target_is_directory=True)
            except OSError as exc:
                self.skipTest(f"symlinks unavailable: {exc}")
            with self.assertRaisesRegex(ValueError, "outside the repository"):
                MODULE.validate_state_path(link / "state.json")

    def test_external_state_round_trip_is_atomic(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "nested" / "state.json"
            validated = MODULE.validate_state_path(path)
            state = {
                "version": MODULE.STATE_VERSION,
                "next_target": self.target["key"],
                "targets": {self.target["key"]: {"url": f"{BASE}/collections/continuous/items?cursor=x", "offset": 2}},
            }
            MODULE.save_state(validated, state)
            self.assertEqual(MODULE.load_state(validated), state)
            self.assertEqual(list(validated.parent.glob(f".{validated.name}.*.tmp")), [])

    def test_main_persists_state_only_after_successful_emission(self):
        next_state = {
            "version": MODULE.STATE_VERSION,
            "next_target": self.target["key"],
            "targets": {self.target["key"]: {"url": f"{BASE}/collections/continuous/items?cursor=x", "offset": 0}},
        }
        metrics = {
            "pages": 1,
            "records_received": 1,
            "targets_attempted": 1,
            "targets_completed": 0,
            "cap_reason": "pages",
            "requests": {"requests": 1, "response_bytes": 123},
        }
        record = MODULE.normalize_feature(feature("one"), self.target, "stamp")
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.json"
            out = io.StringIO()
            err = io.StringIO()
            with mock.patch.object(MODULE, "collect_observations", return_value=([record], next_state, metrics)):
                status = MODULE.main(["--state-path", str(path), "--station", "01646500"], out=out, err=err)
            self.assertEqual(status, 0)
            self.assertEqual(MODULE.load_state(path), next_state)
            self.assertEqual(json.loads(out.getvalue())["provider_feature_id"], "one")
            summary = json.loads(err.getvalue().split("\t", 1)[1])
            self.assertEqual(summary["records"], 1)
            self.assertEqual(summary["requests"], {"attempted": 1, "bytes": 123})
            self.assertEqual(summary["coverage"]["cap_reason"], "pages")
            self.assertEqual(summary["completeness"], "partial")

    def test_complete_summary_reports_all_observed_metrics(self):
        metrics = {
            "pages": 2,
            "records_received": 3,
            "targets_attempted": 2,
            "targets_completed": 2,
            "cap_reason": None,
            "requests": {"requests": 2, "response_bytes": 456},
        }
        summary = MODULE.run_summary(
            metrics,
            records=3,
            targets=2,
            continuations=0,
            max_pages=4,
            max_records=400,
        )
        self.assertEqual(summary["health"], "healthy")
        self.assertEqual(summary["completeness"], "complete")
        self.assertEqual(summary["records"], 3)
        self.assertEqual(summary["requests"], {"attempted": 2, "bytes": 456})
        self.assertEqual(
            summary["partitions"],
            {"configured": 2, "attempted": 2, "completed": 2, "remaining": 0, "continuations": 0},
        )
        self.assertEqual(summary["coverage"]["records_received"], 3)

    def test_failed_emission_leaves_previous_state_untouched_and_reports_failure(self):
        metrics = {
            "pages": 1,
            "records_received": 1,
            "targets_attempted": 1,
            "targets_completed": 0,
            "cap_reason": "records",
            "requests": {"requests": 1, "response_bytes": 10},
        }
        record = MODULE.normalize_feature(feature("one"), self.target, "stamp")
        replacement = {"version": MODULE.STATE_VERSION, "next_target": None, "targets": {}}
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.json"
            original = {"version": MODULE.STATE_VERSION, "next_target": None, "targets": {"old": {}}}
            path.write_text(json.dumps(original), encoding="utf-8")
            err = io.StringIO()
            with mock.patch.object(MODULE, "load_state", return_value=empty_state()):
                with mock.patch.object(MODULE, "collect_observations", return_value=([record], replacement, metrics)):
                    status = MODULE.main(["--state-path", str(path), "--station", "01646500"], out=BrokenOutput(), err=err)
            self.assertEqual(status, 1)
            self.assertEqual(json.loads(path.read_text(encoding="utf-8")), original)
            summary = json.loads(err.getvalue().split("\t", 1)[1])
            self.assertEqual(summary["health"], "failed")
            self.assertEqual(summary["completeness"], "failed")
            self.assertEqual(summary["records"], 0)
            self.assertIn("downstream closed", summary["error"])

    def test_configuration_is_disabled_and_requires_run_summary(self):
        text = CONFIG.read_text(encoding="utf-8")
        self.assertRegex(text, r"(?m)^enabled:\s*false\s*$")
        self.assertRegex(text, r"(?ms)^run_summary:\s*\n\s+required:\s*true\s*$")
        self.assertRegex(text, r"actual provider catalog collection identifiers")
        self.assertRegex(text, r"representative live observation schema")
        self.assertRegex(text, r"bounded live-source smoke")
        self.assertRegex(text, r"Airflow DAG import check")
        self.assertRegex(text, r"(?s)Mocked fixtures.*do not verify")


if __name__ == "__main__":
    unittest.main()
