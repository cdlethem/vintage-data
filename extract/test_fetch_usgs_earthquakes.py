import contextlib
import copy
import importlib.util
import io
import json
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


USGS_FEATURE = {
    "type": "Feature",
    "properties": {
        "mag": 2.5,
        "place": "10 km NW of Example, California",
        "time": 1789603200123,
        "updated": 1789603260456,
        "tz": None,
        "url": "https://earthquake.usgs.gov/earthquakes/eventpage/us7000example",
        "detail": "https://earthquake.usgs.gov/fdsnws/event/1/query?eventid=us7000example&format=geojson",
        "felt": 3,
        "cdi": 2.1,
        "mmi": None,
        "alert": None,
        "status": "reviewed",
        "tsunami": 0,
        "sig": 96,
        "net": "us",
        "code": "7000example",
        "ids": ",us7000example,",
        "sources": ",us,",
        "types": ",origin,phase-data,",
        "nst": 32,
        "dmin": 0.12,
        "rms": 0.41,
        "gap": 47,
        "magType": "ml",
        "type": "earthquake",
        "title": "M 2.5 - 10 km NW of Example, California",
    },
    "geometry": {"type": "Point", "coordinates": [-121.5, 37.25, 8.4]},
    "id": "us7000example",
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


def feature_with_id(earthquake_id):
    feature = copy.deepcopy(USGS_FEATURE)
    feature["id"] = earthquake_id
    return feature


class FetchUsgsEarthquakesTests(unittest.TestCase):
    def fetch(self, document, **kwargs):
        with mock.patch.object(
            MODULE.urllib.request,
            "urlopen",
            return_value=JsonResponse(document),
        ) as urlopen:
            records = list(MODULE.fetch_earthquakes(**kwargs))
        return records, urlopen

    def test_preserves_properties_point_geometry_and_stable_id(self):
        records, _ = self.fetch(collection(USGS_FEATURE))

        self.assertEqual(len(records), 1)
        record = records[0]
        self.assertEqual(record["source"], "usgs_earthquakes")
        self.assertEqual(record["id"], "us7000example")
        self.assertEqual(record["properties"], USGS_FEATURE["properties"])
        self.assertEqual(record["geometry"], USGS_FEATURE["geometry"])
        self.assertRegex(record["fetched_at"], r"\+00:00$")

    def test_uses_one_utc_fetch_timestamp_per_run(self):
        records, _ = self.fetch(
            collection(USGS_FEATURE, feature_with_id("us7000second"))
        )

        self.assertEqual(records[0]["fetched_at"], records[1]["fetched_at"])
        self.assertRegex(records[0]["fetched_at"], r"\+00:00$")

    def test_empty_feature_collection_emits_no_records(self):
        records, _ = self.fetch(collection())
        self.assertEqual(records, [])

    def test_makes_one_bounded_request_with_configured_user_agent(self):
        records, urlopen = self.fetch(collection(), timeout=17)

        self.assertEqual(records, [])
        urlopen.assert_called_once()
        request = urlopen.call_args.args[0]
        self.assertEqual(request.full_url, MODULE.URL)
        self.assertEqual(request.get_header("Accept"), "application/geo+json")
        self.assertEqual(request.get_header("User-agent"), MODULE.USER_AGENT)
        self.assertEqual(urlopen.call_args.kwargs, {"timeout": 17})

    def test_invalid_timeout_fails_before_http(self):
        for timeout in (True, 0, -1, 1.5, MODULE.MAX_TIMEOUT + 1):
            with self.subTest(timeout=timeout):
                with mock.patch.object(MODULE.urllib.request, "urlopen") as urlopen:
                    with self.assertRaisesRegex(ValueError, "timeout"):
                        list(MODULE.fetch_earthquakes(timeout=timeout))
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

    def test_rejects_missing_or_invalid_feature_ids(self):
        for earthquake_id in (None, "", 123, False):
            feature = copy.deepcopy(USGS_FEATURE)
            if earthquake_id is None:
                del feature["id"]
            else:
                feature["id"] = earthquake_id
            with self.subTest(earthquake_id=earthquake_id):
                with self.assertRaisesRegex(ValueError, "stable source id"):
                    self.fetch(collection(feature))

    def test_rejects_invalid_features_properties_and_geometry(self):
        invalid_features = (
            (None, "invalid GeoJSON feature"),
            ({}, "invalid GeoJSON feature"),
            ({**USGS_FEATURE, "type": "NotFeature"}, "invalid GeoJSON feature"),
            ({**USGS_FEATURE, "properties": None}, "invalid properties"),
            ({**USGS_FEATURE, "geometry": None}, "GeoJSON Point"),
            (
                {**USGS_FEATURE, "geometry": {"type": "LineString", "coordinates": []}},
                "GeoJSON Point",
            ),
        )
        for feature, message in invalid_features:
            with self.subTest(feature=feature):
                with self.assertRaisesRegex(ValueError, message):
                    self.fetch(collection(feature))

    def test_accepts_ordinary_integer_and_float_coordinates(self):
        feature = copy.deepcopy(USGS_FEATURE)
        feature["geometry"]["coordinates"] = [-121, 37.25, 8, 1.5]

        records, _ = self.fetch(collection(feature))

        self.assertEqual(records[0]["geometry"], feature["geometry"])

    def test_rejects_short_or_non_list_coordinates(self):
        for coordinates in (None, "-121,37", {}, [], [1]):
            feature = copy.deepcopy(USGS_FEATURE)
            feature["geometry"]["coordinates"] = coordinates
            with self.subTest(coordinates=coordinates):
                with self.assertRaisesRegex(ValueError, "list with at least two"):
                    self.fetch(collection(feature))

    def test_rejects_non_numeric_boolean_and_non_finite_ordinates(self):
        invalid_ordinates = (
            "-121.5",
            None,
            True,
            False,
            float("nan"),
            float("inf"),
            float("-inf"),
        )
        for ordinate in invalid_ordinates:
            feature = copy.deepcopy(USGS_FEATURE)
            feature["geometry"]["coordinates"][1] = ordinate
            with self.subTest(ordinate=ordinate):
                with self.assertRaisesRegex(
                    ValueError, "finite, non-boolean numeric ordinates"
                ):
                    self.fetch(collection(feature))

    def test_preserves_oversized_integers_at_every_point_ordinate(self):
        oversized = 10**400
        positions = (0, 1, 2, 3)
        for position in positions:
            for value in (oversized, -oversized):
                feature = copy.deepcopy(USGS_FEATURE)
                feature["geometry"]["coordinates"].append(7)
                feature["geometry"]["coordinates"][position] = value
                with self.subTest(position=position, negative=value < 0):
                    records, _ = self.fetch(collection(feature))
                    geometry = records[0]["geometry"]
                    self.assertEqual(geometry, feature["geometry"])
                    self.assertEqual(geometry["coordinates"][position], value)

                    encoded = json.dumps(records[0], ensure_ascii=False)
                    decoded = json.loads(encoded)
                    self.assertEqual(decoded["geometry"], feature["geometry"])

    def test_main_emits_valid_ndjson_with_oversized_coordinates(self):
        feature = copy.deepcopy(USGS_FEATURE)
        feature["geometry"]["coordinates"] = [10**400, -(10**400), 10**400, -(10**400)]
        stdout = io.StringIO()

        with mock.patch.object(
            MODULE.urllib.request,
            "urlopen",
            return_value=JsonResponse(collection(feature)),
        ):
            with contextlib.redirect_stdout(stdout):
                MODULE.main(["--timeout", "23"])

        lines = stdout.getvalue().splitlines()
        self.assertEqual(len(lines), 1)
        output = json.loads(lines[0])
        self.assertEqual(output["geometry"], feature["geometry"])

    def test_source_configuration_is_quarter_hourly_and_disabled(self):
        text = SOURCE_CONFIG.read_text(encoding="utf-8")

        self.assertRegex(text, r'(?m)^schedule: "7,22,37,52 \* \* \* \*"')
        self.assertRegex(text, r"(?m)^enabled: false$")
        self.assertIsNone(re.search(r"(?m)^enabled:\s*true$", text))
        self.assertIn('args: ["--timeout", "30"]', text)
        self.assertIn("retries: 2", text)
        self.assertIn("timeout_minutes: 5", text)
        self.assertIn("updates every minute", text)
        self.assertIn("polled every 15 minutes", text)
        self.assertIn("public domain", text)
        self.assertIn("attribution_required: true", text)
        self.assertIn("one bounded request per run", text)
        self.assertIn("live USGS source smoke", text)
        self.assertIn("read-only Airflow import checks", text)


if __name__ == "__main__":
    unittest.main()
