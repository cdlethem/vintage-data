import contextlib
import copy
import importlib.util
import io
import json
from datetime import datetime, timezone
from pathlib import Path
import re
import unittest
from unittest import mock
import urllib.parse


SCRIPT = Path(__file__).parent / "scripts" / "fetch_nifc_wildfires.py"
SOURCE_CONFIG = Path(__file__).parent / "sources" / "nifc_wildfires.yml"
SPEC = importlib.util.spec_from_file_location("fetch_nifc_wildfires", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


def make_feature(object_id, irwin_id, **attribute_overrides):
    attributes = {
        "OBJECTID": object_id,
        "IrwinID": irwin_id,
        "IncidentName": "Lynne",
        "UniqueFireIdentifier": "2026-FLFNF-000419",
        "IncidentTypeCategory": "WF",
        "IncidentTypeKind": "FI",
        "IncidentComplexityLevel": "Type 5 Incident",
        "FireCause": "Undetermined",
        "POOState": "US-FL",
        "POOCounty": "Marion",
        "POOCity": None,
        "POOProtectingAgency": "USFS",
        "DispatchCenterID": "FLFIC",
        "GACC": "SACC",
        "IncidentSize": 1.92,
        "DiscoveryAcres": 1.5,
        "PercentContained": None,
        "InitialLatitude": 29.217294,
        "InitialLongitude": -81.958818,
        "FireDiscoveryDateTime": 1770723000000,
        "ModifiedOnDateTime_dt": 1791909302213,
    }
    attributes.update(attribute_overrides)
    feature = {
        "attributes": attributes,
        "geometry": {"x": -81.957417, "y": 29.216917},
    }
    return feature


def chunked(features, size):
    return [features[i:i + size] for i in range(0, len(features), size)]


class JsonResponse(io.BytesIO):
    def __init__(self, document):
        super().__init__(json.dumps(document).encode("utf-8"))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


class ArcGisServer:
    """Route requests to the WFIGS query endpoint by their parsed query string."""

    def __init__(self, count, features):
        self.count = count
        self.features = features
        self.pages = chunked(features, MODULE.PAGE_SIZE)
        self.calls = []

    def __call__(self, request, *args, **kwargs):
        parsed = urllib.parse.urlparse(request.full_url)
        query = urllib.parse.parse_qs(parsed.query)
        self.calls.append(query)
        if query.get("returnCountOnly"):
            document = {"count": self.count}
        else:
            offset = int(query["resultOffset"][0])
            limit = int(query["resultRecordCount"][0])
            assert limit == MODULE.PAGE_SIZE
            start = offset // MODULE.PAGE_SIZE
            document = {"features": self.pages[start][:limit]}
        return JsonResponse(document)


class FetchNifcWildfiresTests(unittest.TestCase):
    def fetch(self, count, features, **kwargs):
        server = ArcGisServer(count, features)
        with mock.patch.object(
            MODULE.urllib.request, "urlopen", side_effect=server
        ):
            records, summary = MODULE.fetch_wildfires(**kwargs)
            records = list(records)
        return records, summary, server

    def test_envelope_and_normalization(self):
        records, summary, _ = self.fetch(1, [make_feature(1, "{ABC}")])

        self.assertEqual(len(records), 1)
        record = records[0]
        for key in ("source", "fetched_at", "id"):
            self.assertIn(key, record)
        self.assertEqual(record["source"], "nifc_wildfires")
        self.assertEqual(record["id"], "{ABC}")
        self.assertRegex(record["fetched_at"], r"\+00:00$")

        self.assertEqual(record["incident_name"], "Lynne")
        self.assertEqual(record["unique_fire_identifier"], "2026-FLFNF-000419")
        self.assertEqual(record["incident_type_category"], "WF")
        self.assertEqual(record["poo_state"], "US-FL")
        self.assertIsNone(record["poo_city"])
        self.assertEqual(record["incident_size_acres"], 1.92)
        self.assertEqual(record["discovery_acres"], 1.5)
        self.assertIsNone(record["percent_contained"])
        self.assertEqual(record["lon"], -81.957417)
        self.assertEqual(record["lat"], 29.216917)
        expected_discovery = datetime.fromtimestamp(
            1770723000, tz=timezone.utc).isoformat(timespec="seconds")
        self.assertEqual(record["fire_discovery_date_time"], expected_discovery)

    def test_single_utc_timestamp_across_records(self):
        records, _, _ = self.fetch(
            2, [make_feature(1, "{A}"), make_feature(2, "{B}")]
        )
        self.assertEqual(records[0]["fetched_at"], records[1]["fetched_at"])

    def test_objectid_fallback_id_when_irwin_id_missing(self):
        records, _, _ = self.fetch(
            1, [make_feature(42, None, IncidentName="Fallback")]
        )
        self.assertEqual(records[0]["id"], "oid-42")

    def test_pages_beyond_first_page(self):
        features = [
            make_feature(i, f"{{I{i:05d}}}") for i in range(MODULE.PAGE_SIZE + 100)
        ]
        records, summary, server = self.fetch(len(features), features)

        self.assertEqual(len(records), MODULE.PAGE_SIZE + 100)
        data_calls = [c for c in server.calls if not c.get("returnCountOnly")]
        self.assertEqual(len(data_calls), 2)
        self.assertEqual(data_calls[0]["resultOffset"], ["0"])
        self.assertEqual(data_calls[1]["resultOffset"], [str(MODULE.PAGE_SIZE)])
        self.assertEqual(summary["completeness"], "complete")

    def test_requests_are_ordered_and_keyed(self):
        server = ArcGisServer(0, [])
        with mock.patch.object(
            MODULE.urllib.request, "urlopen", side_effect=server
        ):
            records, summary = MODULE.fetch_wildfires()
            self.assertEqual(list(records), [])
        self.assertEqual(len(server.calls), 1)
        self.assertEqual(server.calls[0]["where"], ["1=1"])
        self.assertEqual(server.calls[0]["orderByFields"], ["OBJECTID"])
        self.assertEqual(server.calls[0]["outFields"], ["*"])
        self.assertEqual(server.calls[0]["returnGeometry"], ["true"])
        self.assertEqual(summary["requests"]["attempted"], 1)
        self.assertEqual(summary["requests"]["succeeded"], 1)

    def test_deduplicates_on_incident_id(self):
        records, summary, _ = self.fetch(
            3,
            [
                make_feature(1, "{SAME}"),
                make_feature(2, "{SAME}"),
                make_feature(3, "{OTHER}"),
            ],
        )
        self.assertEqual([r["id"] for r in records], ["{SAME}", "{OTHER}"])
        self.assertEqual(summary["metrics"]["duplicates_skipped"], 1)
        self.assertEqual(summary["metrics"]["incidents"], 2)

    def test_truncates_at_max_features_and_reports_partial(self):
        features = [make_feature(i, f"{{I{i:05d}}}") for i in range(MODULE.MAX_FEATURES + 500)]
        records, summary, server = self.fetch(len(features), features)

        self.assertEqual(len(records), MODULE.MAX_FEATURES)
        self.assertEqual(summary["completeness"], "partial")
        self.assertEqual(summary["health"], "degraded")
        self.assertEqual(summary["metrics"]["layer_total"], MODULE.MAX_FEATURES + 500)
        data_calls = [c for c in server.calls if not c.get("returnCountOnly")]
        self.assertEqual(len(data_calls), MODULE.MAX_FEATURES // MODULE.PAGE_SIZE)

    def test_empty_layer_is_complete_and_healthy(self):
        records, summary, _ = self.fetch(0, [])
        self.assertEqual(records, [])
        self.assertEqual(summary["completeness"], "complete")
        self.assertEqual(summary["health"], "healthy")

    def test_rejects_invalid_layer_counts(self):
        for count in (-1, True, "430"):
            with self.subTest(count=count):
                with mock.patch.object(
                    MODULE.urllib.request,
                    "urlopen",
                    return_value=JsonResponse({"count": count}),
                ) as urlopen:
                    with self.assertRaisesRegex(ValueError, "invalid count"):
                        MODULE.fetch_wildfires()
                    # The bad count must stop the run before any data page.
                    self.assertEqual(urlopen.call_count, 1)


    def test_rejects_missing_features_page(self):
        with mock.patch.object(
            MODULE.urllib.request,
            "urlopen",
            side_effect=[JsonResponse({"count": 1}), JsonResponse({})],
        ):
            with self.assertRaisesRegex(ValueError, "missing features"):
                list(MODULE.fetch_wildfires())

    def test_rejects_features_without_attributes_or_geometry(self):
        for feature, message in (
            ({"attributes": None, "geometry": {"x": 1, "y": 2}}, "missing attributes"),
            ({"attributes": {"OBJECTID": 1}, "geometry": None}, "point geometry"),
        ):
            with self.subTest(feature=feature):
                with mock.patch.object(
                    MODULE.urllib.request,
                    "urlopen",
                    side_effect=[
                        JsonResponse({"count": 1}),
                        JsonResponse({"features": [feature]}),
                    ],
                ):
                    with self.assertRaisesRegex(ValueError, message):
                        list(MODULE.fetch_wildfires())

    def test_rejects_missing_stable_identity(self):
        feature = make_feature(None, None)
        with mock.patch.object(
            MODULE.urllib.request,
            "urlopen",
            side_effect=[
                JsonResponse({"count": 1}),
                JsonResponse({"features": [feature]}),
            ],
        ):
            with self.assertRaisesRegex(ValueError, "neither IrwinID nor a numeric OBJECTID"):
                list(MODULE.fetch_wildfires())

    def test_rejects_non_finite_geometry_ordinates(self):
        for value in ("west", True, float("nan"), float("inf")):
            feature = make_feature(1, "{G}")
            feature["geometry"] = {"x": value, "y": 29.2}
            with self.subTest(value=value):
                with mock.patch.object(
                    MODULE.urllib.request,
                    "urlopen",
                    side_effect=[
                        JsonResponse({"count": 1}),
                        JsonResponse({"features": [feature]}),
                    ],
                ):
                    with self.assertRaisesRegex(ValueError, "not finite"):
                        list(MODULE.fetch_wildfires())

    def test_rejects_non_numeric_measurements_and_timestamps(self):
        for attr, value in (
            ("IncidentSize", "big"),
            ("IncidentSize", True),
            ("IncidentName", 123),
            ("FireDiscoveryDateTime", "yesterday"),
            ("FireDiscoveryDateTime", -1),
        ):
            with self.subTest(attr=attr):
                with mock.patch.object(
                    MODULE.urllib.request,
                    "urlopen",
                    side_effect=[
                        JsonResponse({"count": 1}),
                        JsonResponse({"features": [make_feature(1, "{M}", **{attr: value})]}),
                    ],
                ):
                    with self.assertRaises(ValueError):
                        list(MODULE.fetch_wildfires())

    def test_main_emits_ndjson_and_run_summary(self):
        stdout, stderr = io.StringIO(), io.StringIO()
        with mock.patch.object(
            MODULE.urllib.request,
            "urlopen",
            side_effect=ArcGisServer(2, [make_feature(1, "{A}"), make_feature(2, "{B}")]),
        ):
            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                MODULE.main(["--timeout", "30"])

        lines = [line for line in stdout.getvalue().splitlines() if line.strip()]
        self.assertEqual(len(lines), 2)
        first = json.loads(lines[0])
        self.assertEqual(first["id"], "{A}")

        summary_lines = [
            line for line in stderr.getvalue().splitlines()
            if line.startswith(MODULE.SUMMARY_PREFIX)
        ]
        self.assertEqual(len(summary_lines), 1)
        summary = json.loads(summary_lines[0][len(MODULE.SUMMARY_PREFIX):])
        self.assertEqual(summary["health"], "healthy")
        self.assertEqual(summary["completeness"], "complete")
        self.assertEqual(summary["requests"]["attempted"], 2)
        self.assertEqual(summary["metrics"]["incidents"], 2)

    def test_main_rejects_out_of_range_timeout_before_http(self):
        for timeout in (0, -5, MODULE.MAX_TIMEOUT + 1):
            with self.subTest(timeout=timeout):
                with mock.patch.object(MODULE.urllib.request, "urlopen") as urlopen:
                    with self.assertRaisesRegex(ValueError, "timeout"):
                        MODULE.main(["--timeout", str(timeout)])
                    urlopen.assert_not_called()

    def test_source_configuration(self):
        text = SOURCE_CONFIG.read_text(encoding="utf-8")

        self.assertRegex(text, r'(?m)^schedule: "41 6 \* \* \*"')
        self.assertRegex(text, r"(?m)^enabled: true$")
        self.assertIn('args: ["--timeout", "60"]', text)
        self.assertIn("retries: 1", text)
        self.assertIn("timeout_minutes: 10", text)
        self.assertIn("run_summary:", text)
        self.assertIn("required: true", text)
        self.assertIn("public domain", text)
        self.assertIn("attribution_required: true", text)
        self.assertIn("live check 2026-10-15: 430 incidents", text)


if __name__ == "__main__":
    unittest.main()
