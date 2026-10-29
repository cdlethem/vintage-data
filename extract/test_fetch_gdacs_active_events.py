import contextlib
import copy
import importlib.util
import io
import json
from pathlib import Path
import re
import unittest
from unittest import mock


SCRIPT = Path(__file__).parent / "scripts" / "fetch_gdacs_active_events.py"
SOURCE_CONFIG = Path(__file__).parent / "sources" / "gdacs_active_events.yml"
SPEC = importlib.util.spec_from_file_location("fetch_gdacs_active_events", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


GDACS_FEATURE = {
    "type": "Feature",
    "properties": {
        "eventtype": "FL",
        "eventid": 1103888,
        "episodeid": 111,
        "eventname": "",
        "name": "Flood in Example Region",
        "description": "Flood in Example Region",
        "htmldescription": "Green Flood in Example Region at: 07 Oct 2026 01:00:00.",
        "url": {
            "geometry": (
                "https://www.gdacs.org/gdacsapi/api/polygons/getgeometry"
                "?eventtype=FL&eventid=1103888&episodeid=111"
            ),
            "report": (
                "https://www.gdacs.org/report.aspx?eventid=1103888&episodeid=111&eventtype=FL"
            ),
            "details": (
                "https://www.gdacs.org/gdacsapi/api/events/geteventdata"
                "?eventtype=FL&eventid=1103888"
            ),
        },
        "alertlevel": "Green",
        "alertscore": 0,
        "episodealertlevel": "Green",
        "episodealertscore": 0.0,
        "istemporary": "false",
        "iscurrent": "true",
        "country": "Example Region",
        "fromdate": "2026-10-07T01:00:00",
        "todate": "2026-10-07T01:00:00",
        "datemodified": "2026-10-07T01:30:00",
        "iso3": "EXR",
        "source": "EM-IFC",
        "sourceid": "",
        "polygonlabel": "Centroid",
        "Class": "Point_Centroid",
        "affectedcountries": [],
        "severitydata": {
            "severity": 0,
            "severitytext": "",
            "severityunit": "",
        },
    },
    "geometry": {
        "type": "Point",
        "coordinates": [34.75, 12.5],
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


def feature_with_episode(episodeid):
    feature = copy.deepcopy(GDACS_FEATURE)
    feature["properties"]["episodeid"] = episodeid
    return feature


class FetchGdacsActiveEventsTests(unittest.TestCase):
    def fetch(self, document, **kwargs):
        with mock.patch.object(
            MODULE.urllib.request,
            "urlopen",
            return_value=JsonResponse(document),
        ) as urlopen:
            records = list(MODULE.fetch_events(**kwargs))
        return records, urlopen

    def test_preserves_properties_geometry_and_composed_id(self):
        records, _ = self.fetch(collection(GDACS_FEATURE))

        self.assertEqual(len(records), 1)
        record = records[0]
        self.assertEqual(record["source"], "gdacs_active_events")
        self.assertEqual(record["id"], "FL1103888111Centroid")
        self.assertEqual(record["properties"], GDACS_FEATURE["properties"])
        self.assertEqual(record["geometry"], GDACS_FEATURE["geometry"])
        self.assertRegex(record["fetched_at"], r"\+00:00$")

    def test_uses_one_utc_fetch_timestamp_per_run(self):
        records, _ = self.fetch(collection(GDACS_FEATURE, feature_with_episode(112)))

        self.assertEqual(records[0]["fetched_at"], records[1]["fetched_at"])
        self.assertRegex(records[0]["fetched_at"], r"\+00:00$")
        self.assertEqual(records[1]["id"], "FL1103888112Centroid")

    def test_empty_feature_collection_emits_no_records(self):
        records, _ = self.fetch(collection())
        self.assertEqual(records, [])

    def test_makes_one_bounded_request_with_configured_user_agent(self):
        records, urlopen = self.fetch(collection(), timeout=17)

        self.assertEqual(records, [])
        urlopen.assert_called_once()
        request = urlopen.call_args.args[0]
        self.assertEqual(request.full_url, MODULE.URL)
        self.assertEqual(request.get_header("Accept"), "application/json")
        self.assertEqual(request.get_header("User-agent"), MODULE.USER_AGENT)
        self.assertEqual(urlopen.call_args.kwargs, {"timeout": 17})

    def test_invalid_timeout_fails_before_http(self):
        for timeout in (True, 0, -1, 1.5, MODULE.MAX_TIMEOUT + 1):
            with self.subTest(timeout=timeout):
                with mock.patch.object(MODULE.urllib.request, "urlopen") as urlopen:
                    with self.assertRaisesRegex(ValueError, "timeout"):
                        list(MODULE.fetch_events(timeout=timeout))
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

    def test_rejects_missing_or_invalid_id_fields(self):
        cases = (
            ("eventtype", None),
            ("eventtype", ""),
            ("eventtype", 7),
            ("eventid", None),
            ("eventid", "1103888"),
            ("eventid", 1103888.0),
            ("eventid", True),
            ("episodeid", None),
            ("episodeid", "111"),
            ("episodeid", False),
            ("polygonlabel", None),
            ("polygonlabel", ""),
        )
        for field, value in cases:
            feature = copy.deepcopy(GDACS_FEATURE)
            if value is None:
                del feature["properties"][field]
            else:
                feature["properties"][field] = value
            with self.subTest(field=field, value=value):
                with self.assertRaisesRegex(ValueError, "stable source id"):
                    self.fetch(collection(feature))

    def test_rejects_invalid_features_and_properties(self):
        invalid_features = (
            (None, "invalid GeoJSON feature"),
            ({}, "invalid GeoJSON feature"),
            ({**GDACS_FEATURE, "type": "NotFeature"}, "invalid GeoJSON feature"),
            ({**GDACS_FEATURE, "properties": None}, "invalid properties"),
            ({**GDACS_FEATURE, "properties": "wet"}, "invalid properties"),
        )
        for feature, message in invalid_features:
            with self.subTest(feature=feature):
                with self.assertRaisesRegex(ValueError, message):
                    self.fetch(collection(feature))

    def test_rejects_invalid_geometries(self):
        invalid_geometries = (
            (None, "GeoJSON geometry"),
            ("Point", "GeoJSON geometry"),
            ({"type": "Square"}, "not a GeoJSON geometry"),
            ({"type": "Point"}, "non-empty"),
            ({**GDACS_FEATURE["geometry"], "coordinates": None}, "non-empty"),
            (
                {**GDACS_FEATURE["geometry"], "coordinates": []},
                "non-empty",
            ),
        )
        for geometry, message in invalid_geometries:
            feature = copy.deepcopy(GDACS_FEATURE)
            feature["geometry"] = geometry
            with self.subTest(geometry=geometry):
                with self.assertRaisesRegex(ValueError, message):
                    self.fetch(collection(feature))

    def test_rejects_non_numeric_boolean_and_non_finite_ordinates(self):
        invalid_ordinates = (
            "12.5",
            None,
            True,
            False,
            float("nan"),
            float("inf"),
            float("-inf"),
        )
        for ordinate in invalid_ordinates:
            feature = copy.deepcopy(GDACS_FEATURE)
            feature["geometry"]["coordinates"][1] = ordinate
            with self.subTest(ordinate=ordinate):
                with self.assertRaisesRegex(
                    ValueError, "finite, non-boolean numeric ordinates"
                ):
                    self.fetch(collection(feature))

    def test_accepts_the_geometry_types_the_api_publishes(self):
        geometries = (
            {"type": "Point", "coordinates": [12.5, 34.7]},
            {"type": "MultiPoint", "coordinates": [[12.5, 34.7], [12.6, 34.8]]},
            {"type": "LineString", "coordinates": [[12.5, 34.7], [12.6, 34.8]]},
            {
                "type": "MultiLineString",
                "coordinates": [[[12.5, 34.7], [12.6, 34.8]]],
            },
            {
                "type": "Polygon",
                "coordinates": [[[34.7, 12.5], [34.8, 12.5], [34.7, 12.5]]],
            },
            {
                "type": "MultiPolygon",
                "coordinates": [
                    [[[34.7, 12.5], [34.8, 12.5], [34.7, 12.5]]],
                ],
            },
            {
                "type": "GeometryCollection",
                "geometries": [
                    {"type": "Point", "coordinates": [12.5, 34.7]},
                    {"type": "LineString", "coordinates": [[12.5, 34.7], [12.6, 34.8]]},
                ],
            },
        )
        for geometry in geometries:
            feature = copy.deepcopy(GDACS_FEATURE)
            feature["geometry"] = geometry
            with self.subTest(geometry_type=geometry["type"]):
                records, _ = self.fetch(collection(feature))
                self.assertEqual(records[0]["geometry"], geometry)

    def test_rejects_geometry_collection_with_invalid_member(self):
        feature = copy.deepcopy(GDACS_FEATURE)
        feature["geometry"] = {
            "type": "GeometryCollection",
            "geometries": [{"type": "Point", "coordinates": [12.5, True]}],
        }
        with self.assertRaisesRegex(ValueError, "finite, non-boolean numeric ordinates"):
            self.fetch(collection(feature))

    def test_preserves_oversized_integers_in_coordinates(self):
        feature = copy.deepcopy(GDACS_FEATURE)
        feature["geometry"] = {
            "type": "Point",
            "coordinates": [10**400, -(10**400)],
        }

        records, _ = self.fetch(collection(feature))

        geometry = records[0]["geometry"]
        self.assertEqual(geometry["coordinates"], [10**400, -(10**400)])
        decoded = json.loads(json.dumps(records[0], ensure_ascii=False))
        self.assertEqual(decoded["geometry"], geometry)

    def test_main_emits_valid_ndjson(self):
        stdout = io.StringIO()

        with mock.patch.object(
            MODULE.urllib.request,
            "urlopen",
            return_value=JsonResponse(collection(GDACS_FEATURE)),
        ):
            with contextlib.redirect_stdout(stdout):
                MODULE.main(["--timeout", "23"])

        lines = stdout.getvalue().splitlines()
        self.assertEqual(len(lines), 1)
        output = json.loads(lines[0])
        self.assertEqual(output["source"], "gdacs_active_events")
        self.assertEqual(output["id"], "FL1103888111Centroid")
        self.assertEqual(output["geometry"], GDACS_FEATURE["geometry"])

    def test_source_configuration(self):
        text = SOURCE_CONFIG.read_text(encoding="utf-8")

        self.assertRegex(text, r'(?m)^schedule: "1-46/15 \* \* \* \*"')
        self.assertRegex(text, r"(?m)^enabled: true$")
        self.assertIn('args: ["--timeout", "60"]', text)
        self.assertIn("retries: 1", text)
        self.assertIn("timeout_minutes: 10", text)
        self.assertIn("free to use", text)
        self.assertIn("attribution_required: true", text)
        self.assertIn("one bounded request per run", text)


if __name__ == "__main__":
    unittest.main()
