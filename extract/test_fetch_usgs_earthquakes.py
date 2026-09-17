import importlib.util
import io
import json
import os
import unittest
from datetime import datetime
from pathlib import Path
from unittest import mock


SCRIPT = Path(__file__).parent / "scripts" / "fetch_usgs_earthquakes.py"
SPEC = importlib.util.spec_from_file_location("fetch_usgs_earthquakes", SCRIPT)
usgs = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(usgs)


def response(document):
    return io.BytesIO(json.dumps(document).encode("utf-8"))


def feature(event_id="us7000test", *, magnitude=4.2):
    return {
        "type": "Feature",
        "properties": {
            "mag": magnitude,
            "place": "10 km NW of Testville",
            "updated": 1_789_000_000_000,
        },
        "geometry": {"type": "Point", "coordinates": [-122.5, 37.5, 8.1]},
        "id": event_id,
    }


class FetchEarthquakesTest(unittest.TestCase):
    def fetch(self, document, timeout=30):
        with mock.patch.object(
            usgs.urllib.request, "urlopen", return_value=response(document)
        ) as urlopen:
            records = list(usgs.fetch_earthquakes(timeout=timeout))
        return records, urlopen

    def test_normalizes_records_and_preserves_source_feature_data(self):
        first = feature("us7000alpha", magnitude=2.5)
        second = feature("ci12345678", magnitude=3.1)

        records, urlopen = self.fetch(
            {"type": "FeatureCollection", "features": [first, second]}, timeout=17
        )

        self.assertEqual([row["id"] for row in records], ["us7000alpha", "ci12345678"])
        self.assertEqual([row["source"] for row in records], [usgs.SOURCE, usgs.SOURCE])
        self.assertEqual(records[0]["properties"], first["properties"])
        self.assertEqual(records[0]["geometry"], first["geometry"])
        self.assertEqual(records[1]["properties"], second["properties"])
        self.assertEqual(records[1]["geometry"], second["geometry"])
        self.assertEqual(records[0]["fetched_at"], records[1]["fetched_at"])
        fetched_at = datetime.fromisoformat(records[0]["fetched_at"])
        self.assertIsNotNone(fetched_at.utcoffset())
        self.assertEqual(fetched_at.utcoffset().total_seconds(), 0)

        urlopen.assert_called_once()
        request = urlopen.call_args.args[0]
        self.assertEqual(request.full_url, usgs.URL)
        self.assertEqual(urlopen.call_args.kwargs, {"timeout": 17})
        self.assertEqual(request.get_header("User-agent"), usgs.USER_AGENT)
        self.assertEqual(
            request.get_header("Accept"), "application/geo+json, application/json"
        )

    def test_stable_id_is_unchanged_when_properties_are_revised(self):
        original, _ = self.fetch(
            {"type": "FeatureCollection", "features": [feature(magnitude=4.2)]}
        )
        revised, _ = self.fetch(
            {"type": "FeatureCollection", "features": [feature(magnitude=4.6)]}
        )

        self.assertEqual(original[0]["id"], "us7000test")
        self.assertEqual(revised[0]["id"], "us7000test")
        self.assertNotEqual(original[0]["properties"], revised[0]["properties"])

    def test_empty_feature_collection_is_valid(self):
        records, urlopen = self.fetch(
            {"type": "FeatureCollection", "features": []}
        )

        self.assertEqual(records, [])
        urlopen.assert_called_once()

    def test_rejects_malformed_responses(self):
        cases = (
            [],
            {},
            {"type": "FeatureCollection", "features": {}},
            {"type": "NotAFeatureCollection", "features": []},
        )
        for document in cases:
            with self.subTest(document=document):
                with self.assertRaisesRegex(ValueError, "USGS response"):
                    self.fetch(document)

    def test_rejects_malformed_features_clearly(self):
        cases = (
            ("not an object", "not an object"),
            ({"type": "Other", "id": "x"}, "not a GeoJSON Feature"),
            (
                {
                    "type": "Feature",
                    "id": "x",
                    "properties": [],
                    "geometry": {"type": "Point", "coordinates": [1, 2]},
                },
                "invalid properties",
            ),
            (
                {
                    "type": "Feature",
                    "id": "x",
                    "properties": {},
                    "geometry": {"type": "Polygon", "coordinates": []},
                },
                "Point geometry",
            ),
            (
                {
                    "type": "Feature",
                    "id": "x",
                    "properties": {},
                    "geometry": {"type": "Point", "coordinates": [1]},
                },
                "invalid Point coordinates",
            ),
        )
        for malformed, message in cases:
            with self.subTest(message=message):
                document = {"type": "FeatureCollection", "features": [malformed]}
                with self.assertRaisesRegex(ValueError, message):
                    self.fetch(document)

    def test_rejects_missing_event_identifiers(self):
        for event_id in (None, ""):
            with self.subTest(event_id=event_id):
                malformed = feature(event_id)
                with self.assertRaisesRegex(ValueError, "missing the USGS event id"):
                    self.fetch(
                        {"type": "FeatureCollection", "features": [malformed]}
                    )

    def test_rejects_unbounded_request_timeouts_before_http(self):
        with mock.patch.object(usgs.urllib.request, "urlopen") as urlopen:
            for timeout in (0, -1, float("nan"), float("inf"), usgs.MAX_TIMEOUT + 1):
                with self.subTest(timeout=timeout):
                    with self.assertRaisesRegex(ValueError, "finite|at most"):
                        list(usgs.fetch_earthquakes(timeout=timeout))
            urlopen.assert_not_called()

    def test_uses_configured_extraction_user_agent(self):
        configured = "offline-test-extractor/1.0"
        with mock.patch.dict(os.environ, {"EXTRACT_USER_AGENT": configured}):
            spec = importlib.util.spec_from_file_location("configured_usgs", SCRIPT)
            configured_usgs = importlib.util.module_from_spec(spec)
            assert spec.loader is not None
            spec.loader.exec_module(configured_usgs)

        with mock.patch.object(
            configured_usgs.urllib.request,
            "urlopen",
            return_value=response({"type": "FeatureCollection", "features": []}),
        ) as urlopen:
            self.assertEqual(list(configured_usgs.fetch_earthquakes()), [])

        request = urlopen.call_args.args[0]
        self.assertEqual(request.get_header("User-agent"), configured)


if __name__ == "__main__":
    unittest.main()
