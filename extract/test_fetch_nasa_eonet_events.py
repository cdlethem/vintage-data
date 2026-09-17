import contextlib
import importlib.util
import io
import json
from pathlib import Path
import re
import unittest
from unittest import mock


SCRIPT = Path(__file__).parent / "scripts" / "fetch_nasa_eonet_events.py"
SOURCE_CONFIG = Path(__file__).parent / "sources" / "nasa_eonet_events.yml"
SPEC = importlib.util.spec_from_file_location("fetch_nasa_eonet_events", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


# Field layout follows the NASA EONET v3 Events API GeoJSON representation documented
# at https://eonet.gsfc.nasa.gov/docs/v3#eventsAPI. Values are fixed local test data.
GEOJSON_FEATURE = {
    "type": "Feature",
    "properties": {
        "id": "EONET_6783",
        "title": "Example wildfire",
        "description": None,
        "link": "https://eonet.gsfc.nasa.gov/api/v3/events/EONET_6783",
        "closed": None,
        "categories": [{"id": "wildfires", "title": "Wildfires"}],
        "sources": [
            {
                "id": "InciWeb",
                "url": "https://inciweb.wildfire.gov/incident-information/example",
            }
        ],
    },
    "geometry": {
        "type": "Point",
        "coordinates": [-120.75, 38.25],
        "date": "2026-09-16T18:00:00Z",
        "magnitudeValue": 1240.5,
        "magnitudeUnit": "acres",
    },
}


class JsonResponse(io.BytesIO):
    def __init__(self, document):
        super().__init__(json.dumps(document).encode("utf-8"))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def collection(*features):
    return {"type": "FeatureCollection", "features": list(features)}


class FetchNasaEonetEventsTests(unittest.TestCase):
    def fetch(self, document, **kwargs):
        with mock.patch.object(
            MODULE.urllib.request,
            "urlopen",
            return_value=JsonResponse(document),
        ) as urlopen:
            records = list(MODULE.fetch_events(**kwargs))
        return records, urlopen

    def test_normalizes_schema_faithful_geojson_observation(self):
        self.assertNotIn("id", GEOJSON_FEATURE)
        self.assertNotIn("geometry", GEOJSON_FEATURE["properties"])
        self.assertIn("date", GEOJSON_FEATURE["geometry"])
        self.assertIn("magnitudeValue", GEOJSON_FEATURE["geometry"])

        records, _ = self.fetch(collection(GEOJSON_FEATURE))

        self.assertEqual(len(records), 1)
        record = records[0]
        self.assertEqual(record["source"], "nasa_eonet_events")
        self.assertEqual(record["id"], "EONET_6783")
        self.assertEqual(record["title"], "Example wildfire")
        self.assertEqual(
            record["categories"],
            [{"id": "wildfires", "title": "Wildfires"}],
        )
        self.assertEqual(
            record["source_links"],
            ["https://inciweb.wildfire.gov/incident-information/example"],
        )
        self.assertEqual(record["date"], "2026-09-16T18:00:00Z")
        self.assertEqual(record["magnitude_value"], 1240.5)
        self.assertEqual(record["magnitude_unit"], "acres")
        self.assertEqual(record["coordinates"], [-120.75, 38.25])
        self.assertEqual(record["geometry"], GEOJSON_FEATURE["geometry"])
        self.assertEqual(record["raw"], GEOJSON_FEATURE)
        self.assertEqual(
            record["attribution"],
            "NASA Earth Observatory Natural Event Tracker (EONET)",
        )
        self.assertIn("not authoritative", record["precision_caveat"])
        self.assertRegex(record["fetched_at"], r"\+00:00$")

    def test_uses_one_run_level_fetch_timestamp(self):
        second_feature = json.loads(json.dumps(GEOJSON_FEATURE))
        second_feature["properties"]["id"] = "EONET_6784"

        records, _ = self.fetch(collection(GEOJSON_FEATURE, second_feature))

        self.assertEqual(records[0]["fetched_at"], records[1]["fetched_at"])

    def test_empty_feature_collection_emits_no_records(self):
        records, _ = self.fetch(collection())
        self.assertEqual(records, [])

    def test_request_is_open_bounded_and_not_paginated(self):
        document = collection()
        document["pagination"] = {"next": "https://example.invalid/not-supported"}

        records, urlopen = self.fetch(document, limit=73, timeout=17)

        self.assertEqual(records, [])
        urlopen.assert_called_once()
        request = urlopen.call_args.args[0]
        self.assertEqual(
            request.full_url,
            f"{MODULE.URL}?status=open&limit=73",
        )
        self.assertEqual(request.get_header("Accept"), "application/geo+json")
        self.assertTrue(request.get_header("User-agent"))
        self.assertEqual(urlopen.call_args.kwargs, {"timeout": 17})

    def test_invalid_bounds_fail_before_http(self):
        for kwargs in (
            {"limit": 0},
            {"limit": MODULE.MAX_LIMIT + 1},
            {"timeout": 0},
        ):
            with self.subTest(kwargs=kwargs):
                with mock.patch.object(MODULE.urllib.request, "urlopen") as urlopen:
                    with self.assertRaises(ValueError):
                        list(MODULE.fetch_events(**kwargs))
                urlopen.assert_not_called()

    def test_rejects_malformed_collections(self):
        malformed = (
            ([], "not an object"),
            ({}, "not a GeoJSON FeatureCollection"),
            ({"type": "FeatureCollection"}, "missing features"),
            ({"type": "FeatureCollection", "features": {}}, "missing features"),
        )
        for document, message in malformed:
            with self.subTest(document=document):
                with self.assertRaisesRegex(ValueError, message):
                    self.fetch(document)

    def test_rejects_missing_or_malformed_property_id(self):
        for value in (None, "", 6783, "EVENT_6783", "EONET_"):
            feature = json.loads(json.dumps(GEOJSON_FEATURE))
            if value is None:
                del feature["properties"]["id"]
            else:
                feature["properties"]["id"] = value
            with self.subTest(value=value):
                with self.assertRaisesRegex(ValueError, "properties.id"):
                    self.fetch(collection(feature))

    def test_rejects_feature_with_misplaced_observation_data(self):
        feature = json.loads(json.dumps(GEOJSON_FEATURE))
        feature["properties"]["geometry"] = feature.pop("geometry")

        with self.assertRaisesRegex(ValueError, "invalid geometry"):
            self.fetch(collection(feature))

    def test_main_emits_one_ndjson_record_per_feature(self):
        second_feature = json.loads(json.dumps(GEOJSON_FEATURE))
        second_feature["properties"]["id"] = "EONET_6784"
        stdout = io.StringIO()

        with mock.patch.object(
            MODULE.urllib.request,
            "urlopen",
            return_value=JsonResponse(collection(GEOJSON_FEATURE, second_feature)),
        ):
            with contextlib.redirect_stdout(stdout):
                MODULE.main(["--limit", "2"])

        lines = stdout.getvalue().splitlines()
        self.assertEqual(len(lines), 2)
        self.assertEqual(
            [json.loads(line)["id"] for line in lines],
            ["EONET_6783", "EONET_6784"],
        )

    def test_source_configuration_is_hourly_and_disabled(self):
        text = SOURCE_CONFIG.read_text(encoding="utf-8")

        self.assertRegex(text, r'(?m)^schedule: "19 \* \* \* \*"')
        self.assertRegex(text, r"(?m)^enabled: false$")
        self.assertRegex(text, r'(?m)^args: \["--limit", "100"\]$')
        self.assertIsNone(re.search(r"(?m)^enabled: true$", text))


if __name__ == "__main__":
    unittest.main()
