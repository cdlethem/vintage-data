import importlib.util
import io
import json
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch


MODULE_PATH = Path(__file__).parent / "scripts" / "fetch_usgs_earthquakes.py"
SPEC = importlib.util.spec_from_file_location("fetch_usgs_earthquakes", MODULE_PATH)
usgs = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = usgs
SPEC.loader.exec_module(usgs)


VALID_FEATURE = {
    "type": "Feature",
    "id": "us7000example",
    "properties": {"mag": 4.2, "place": "10 km NW of Exampleville"},
    "geometry": {"type": "Point", "coordinates": [-122.5, 47.6, 12.3]},
}


class FakeResponse(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


class FixedDateTime:
    @classmethod
    def now(cls, tz):
        assert tz is timezone.utc
        return datetime(2026, 9, 13, 9, 0, tzinfo=timezone.utc)


class FetchUsgsEarthquakesTests(unittest.TestCase):
    def test_normalizes_features_with_stable_ids_and_preserves_geojson(self):
        feature = json.loads(json.dumps(VALID_FEATURE))
        records = list(usgs.normalize_features(
            {"type": "FeatureCollection", "features": [feature]},
            "2026-09-13T09:00:00+00:00",
        ))

        self.assertEqual(records, [{
            "type": "Feature",
            "id": "us7000example",
            "properties": feature["properties"],
            "geometry": feature["geometry"],
            "source": "usgs_earthquakes",
            "fetched_at": "2026-09-13T09:00:00+00:00",
        }])

    def test_accepts_empty_feature_collection(self):
        self.assertEqual(
            list(usgs.normalize_features(
                {"type": "FeatureCollection", "features": []}, "fetched-at"
            )),
            [],
        )

    def test_rejects_malformed_responses(self):
        cases = [
            {},
            {"type": "FeatureCollection", "features": {}},
            {"type": "FeatureCollection", "features": [{"id": "event"}]},
            {"type": "FeatureCollection", "features": [{
                "type": "Feature",
                "id": "event",
                "properties": {},
                "geometry": {"type": "LineString", "coordinates": []},
            }]},
        ]

        for document in cases:
            with self.subTest(document=document):
                with self.assertRaises(ValueError):
                    list(usgs.normalize_features(document, "fetched-at"))

    def test_rejects_missing_event_identifier(self):
        feature = json.loads(json.dumps(VALID_FEATURE))
        del feature["id"]

        with self.assertRaisesRegex(ValueError, "missing its event id"):
            list(usgs.normalize_features(
                {"type": "FeatureCollection", "features": [feature]}, "fetched-at"
            ))

    def test_fetches_one_feed_with_configured_user_agent_and_bounded_timeout(self):
        response = FakeResponse(json.dumps({
            "type": "FeatureCollection", "features": [VALID_FEATURE]
        }).encode())
        with patch.object(usgs, "datetime", FixedDateTime), patch.object(
            usgs.urllib.request, "urlopen", return_value=response
        ) as urlopen:
            records = list(usgs.fetch_features(timeout=30))

        self.assertEqual(records[0]["fetched_at"], "2026-09-13T09:00:00+00:00")
        self.assertEqual(urlopen.call_count, 1)
        request = urlopen.call_args.args[0]
        self.assertEqual(request.full_url, usgs.URL)
        self.assertEqual(request.get_header("User-agent"), usgs.USER_AGENT)
        self.assertEqual(urlopen.call_args.kwargs["timeout"], 30)

    def test_rejects_unbounded_timeouts(self):
        for timeout in (0, usgs.MAX_TIMEOUT_SECONDS + 1):
            with self.subTest(timeout=timeout):
                with self.assertRaisesRegex(ValueError, "timeout must be between"):
                    list(usgs.fetch_features(timeout))


if __name__ == "__main__":
    unittest.main()
