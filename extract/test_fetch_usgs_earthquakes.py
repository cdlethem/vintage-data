import contextlib
import importlib.util
import io
import json
import math
from pathlib import Path
import re
import unittest
from unittest import mock


SCRIPT = Path(__file__).parent / "scripts" / "fetch_usgs_earthquakes.py"
SOURCE_CONFIG = Path(__file__).parent / "sources" / "usgs_earthquakes.yml"
SPEC = importlib.util.spec_from_file_location("fetch_usgs_earthquakes", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


PROPERTIES = {
    "mag": 2.4,
    "place": "12 km SE of Example, California",
    "time": 1789632000123,
    "updated": 1789632300456,
    "url": "https://earthquake.usgs.gov/earthquakes/eventpage/example1",
    "detail": "https://earthquake.usgs.gov/fdsnws/event/1/query?eventid=example1&format=geojson",
    "status": "reviewed",
    "tsunami": 0,
    "sig": 89,
    "net": "ci",
    "code": "example1",
    "ids": ",example1,",
    "sources": ",ci,",
    "types": ",origin,phase-data,",
    "nst": 42,
    "dmin": 0.04,
    "rms": 0.17,
    "gap": 31,
    "magType": "ml",
    "type": "earthquake",
    "title": "M 2.4 - 12 km SE of Example, California",
}


DEFAULT_COORDINATES = object()


def feature(event_id="ci-example1", coordinates=DEFAULT_COORDINATES):
    if coordinates is DEFAULT_COORDINATES:
        coordinates = [-117.125, 34.25, 8.75]
    return {
        "type": "Feature",
        "properties": dict(PROPERTIES),
        "geometry": {"type": "Point", "coordinates": coordinates},
        "id": event_id,
    }


def collection(*features):
    return {
        "type": "FeatureCollection",
        "metadata": {"generated": 1789632400000, "count": len(features)},
        "features": list(features),
        "bbox": [-180, -90, -1, 180, 90, 700],
    }


class JsonResponse(io.BytesIO):
    def __init__(self, document):
        super().__init__(json.dumps(document).encode("utf-8"))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


class FetchUsgsEarthquakesTests(unittest.TestCase):
    def fetch(self, document, **kwargs):
        with mock.patch.object(
            MODULE.urllib.request,
            "urlopen",
            return_value=JsonResponse(document),
        ) as urlopen:
            records = list(MODULE.fetch_earthquakes(**kwargs))
        return records, urlopen

    def test_preserves_valid_feature_properties_and_point_geometry(self):
        integer_geometry = {"type": "Point", "coordinates": [-117, 34, 8]}
        floating_geometry = {
            "type": "Point",
            "coordinates": [-122.375, 47.625, 12.25],
        }
        integer_feature = feature("ci-integer", integer_geometry["coordinates"])
        floating_feature = feature("uw-floating", floating_geometry["coordinates"])

        records, _ = self.fetch(collection(integer_feature, floating_feature))

        self.assertEqual([record["id"] for record in records], ["ci-integer", "uw-floating"])
        self.assertEqual([record["source"] for record in records], [MODULE.SOURCE] * 2)
        self.assertEqual(records[0]["properties"], integer_feature["properties"])
        self.assertEqual(records[1]["properties"], floating_feature["properties"])
        self.assertEqual(records[0]["geometry"], integer_geometry)
        self.assertEqual(records[1]["geometry"], floating_geometry)
        self.assertEqual(records[0]["type"], "Feature")
        self.assertRegex(records[0]["fetched_at"], r"\+00:00$")
        self.assertEqual(records[0]["fetched_at"], records[1]["fetched_at"])

    def test_empty_feature_collection_emits_no_records(self):
        records, _ = self.fetch(collection())
        self.assertEqual(records, [])

    def test_request_is_single_bounded_official_feed_request(self):
        records, urlopen = self.fetch(collection(), timeout=17)

        self.assertEqual(records, [])
        urlopen.assert_called_once()
        request = urlopen.call_args.args[0]
        self.assertEqual(request.full_url, MODULE.URL)
        self.assertEqual(
            request.full_url,
            "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson",
        )
        self.assertEqual(request.get_header("Accept"), "application/geo+json")
        self.assertEqual(request.get_header("User-agent"), MODULE.USER_AGENT)
        self.assertTrue(request.get_header("User-agent"))
        self.assertEqual(urlopen.call_args.kwargs, {"timeout": 17})

    def test_invalid_timeout_fails_before_http(self):
        for timeout in (0, -1, True, 1.5, "17"):
            with self.subTest(timeout=timeout):
                with mock.patch.object(MODULE.urllib.request, "urlopen") as urlopen:
                    with self.assertRaisesRegex(ValueError, "positive integer"):
                        list(MODULE.fetch_earthquakes(timeout=timeout))
                urlopen.assert_not_called()

    def test_rejects_malformed_responses(self):
        malformed = (
            ([], "not an object"),
            ({}, "not a GeoJSON FeatureCollection"),
            ({"type": "FeatureCollection"}, "features list"),
            ({"type": "FeatureCollection", "features": {}}, "features list"),
        )
        for document, message in malformed:
            with self.subTest(document=document):
                with self.assertRaisesRegex(ValueError, message):
                    self.fetch(document)

    def test_rejects_missing_or_invalid_event_id(self):
        for event_id in (None, "", 123, True):
            earthquake = feature()
            if event_id is None:
                del earthquake["id"]
            else:
                earthquake["id"] = event_id
            with self.subTest(event_id=event_id):
                with self.assertRaisesRegex(ValueError, "valid id"):
                    self.fetch(collection(earthquake))

    def test_rejects_non_feature_invalid_properties_and_non_point_geometry(self):
        malformed = []

        not_a_feature = feature()
        not_a_feature["type"] = "Point"
        malformed.append((not_a_feature, "invalid GeoJSON feature"))

        invalid_properties = feature()
        invalid_properties["properties"] = None
        malformed.append((invalid_properties, "invalid properties"))

        for geometry in (None, {}, {"type": "LineString", "coordinates": [1, 2]}):
            invalid_geometry = feature()
            invalid_geometry["geometry"] = geometry
            malformed.append((invalid_geometry, "invalid Point geometry"))

        for earthquake, message in malformed:
            with self.subTest(message=message, feature=earthquake):
                with self.assertRaisesRegex(ValueError, message):
                    self.fetch(collection(earthquake))

    def test_rejects_coordinates_that_are_not_lists_of_two_or_more_ordinates(self):
        for coordinates in (None, "1,2", {}, [], [1]):
            with self.subTest(coordinates=coordinates):
                with self.assertRaisesRegex(ValueError, "list with at least two"):
                    self.fetch(collection(feature(coordinates=coordinates)))

        with self.assertRaisesRegex(ValueError, "list with at least two"):
            MODULE.normalize_feature(feature(coordinates=(1, 2)), "stamp")

    def test_rejects_every_non_numeric_or_non_finite_ordinate(self):
        invalid_values = ("1.25", None, True, False, math.nan, math.inf, -math.inf)
        positions = (0, 1, 2)
        for position in positions:
            for invalid in invalid_values:
                coordinates = [-117.125, 34.25, 8.75]
                coordinates[position] = invalid
                with self.subTest(position=position, invalid=invalid):
                    with self.assertRaisesRegex(
                        ValueError,
                        rf"ordinate {position} must be a finite, non-boolean number",
                    ):
                        self.fetch(collection(feature(coordinates=coordinates)))

    def test_rejects_invalid_additional_ordinates_beyond_depth(self):
        for invalid in ("fourth", None, True, math.nan, math.inf, -math.inf):
            with self.subTest(invalid=invalid):
                with self.assertRaisesRegex(ValueError, "ordinate 3"):
                    self.fetch(
                        collection(feature(coordinates=[-117.125, 34.25, 8.75, invalid]))
                    )

    def test_main_emits_one_ndjson_record_per_feature(self):
        stdout = io.StringIO()
        document = collection(feature("ci-example1"), feature("us-example2"))

        with mock.patch.object(
            MODULE.urllib.request,
            "urlopen",
            return_value=JsonResponse(document),
        ):
            with contextlib.redirect_stdout(stdout):
                MODULE.main(["--timeout", "19"])

        records = [json.loads(line) for line in stdout.getvalue().splitlines()]
        self.assertEqual([record["id"] for record in records], ["ci-example1", "us-example2"])

    def test_source_configuration_is_quarter_hourly_and_disabled(self):
        text = SOURCE_CONFIG.read_text(encoding="utf-8")

        self.assertRegex(text, r'(?m)^schedule: "7,22,37,52 \* \* \* \*"')
        self.assertRegex(text, r"(?m)^enabled: false$")
        self.assertRegex(text, r"(?m)^retries: 2$")
        self.assertRegex(text, r"(?m)^timeout_minutes: 10$")
        self.assertRegex(text, r"(?m)^licence: .*public domain")
        self.assertRegex(text, r"official feed updates every minute")
        self.assertIsNone(re.search(r"(?m)^enabled: true$", text))


if __name__ == "__main__":
    unittest.main()
