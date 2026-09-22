import contextlib
from datetime import datetime, timezone
from decimal import Decimal
from fractions import Fraction
import importlib.util
import io
import json
from pathlib import Path
import socket
import unittest
from unittest import mock
import urllib.error
import urllib.parse


SCRIPT = Path(__file__).with_name("fetch_osm_overpass.py")
SPEC = importlib.util.spec_from_file_location("fetch_osm_overpass", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)
FETCHED_AT = "2026-09-17T12:34:56+00:00"


class Response(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def response(document):
    payload = document if isinstance(document, bytes) else json.dumps(document).encode("utf-8")
    return Response(payload)


def fixture_document():
    return {
        "version": 0.6,
        "generator": "Overpass API",
        "elements": [
            {
                "type": "node",
                "id": 7,
                "lat": 51.505,
                "lon": -0.125,
                "tags": {"amenity": "drinking_water", "name": "Tap"},
            },
            {
                "type": "way",
                "id": 7,
                "bounds": {
                    "minlat": 51.504,
                    "minlon": -0.126,
                    "maxlat": 51.506,
                    "maxlon": -0.124,
                },
                "nodes": [7, 8],
                "geometry": [
                    {"lat": 51.504, "lon": -0.126},
                    {"lat": 51.506, "lon": -0.124},
                ],
                "tags": {"amenity": "drinking_water"},
            },
            {
                "type": "relation",
                "id": 7,
                "bounds": {
                    "minlat": 51.503,
                    "minlon": -0.127,
                    "maxlat": 51.507,
                    "maxlon": -0.123,
                },
                "center": {"lat": 51.505, "lon": -0.125},
                "members": [
                    {
                        "type": "way",
                        "ref": 7,
                        "role": "outer",
                        "geometry": [
                            {"lat": 51.504, "lon": -0.126},
                            {"lat": 51.506, "lon": -0.124},
                        ],
                    },
                    {"type": "node", "ref": 7, "role": "label", "lat": 51.505, "lon": -0.125},
                ],
                "tags": {"amenity": "drinking_water", "type": "multipolygon"},
            },
        ],
    }


class FetchOsmOverpassTests(unittest.TestCase):
    def fetch(self, document=None, **changes):
        arguments = {
            "bbox": "51.50,-0.13,51.51,-0.12",
            "tags": ("amenity=drinking_water",),
            "http_timeout": 17,
            "query_timeout": 19,
            "retries": 0,
            "retry_delay": 0,
            "fetched_at": FETCHED_AT,
        }
        arguments.update(changes)
        with mock.patch.object(
            MODULE.urllib.request,
            "urlopen",
            return_value=response(fixture_document() if document is None else document),
        ) as urlopen:
            records = MODULE.fetch_overpass(**arguments)
        return records, urlopen

    def assert_query_preserves_bbox(self, query, expected):
        query_line = query.splitlines()[1]
        encoded_bounds = query_line.rsplit("(", 1)[1].removesuffix(");")
        actual = tuple(Decimal(part) for part in encoded_bounds.split(","))

        self.assertEqual(actual, expected)
        south, west, north, east = actual
        self.assertLess(south, north)
        self.assertLess(west, east)
        latitude_span = Fraction(north) - Fraction(south)
        longitude_span = Fraction(east) - Fraction(west)
        self.assertLessEqual(latitude_span, Fraction(str(MODULE.MAX_LATITUDE_SPAN)))
        self.assertLessEqual(longitude_span, Fraction(str(MODULE.MAX_LONGITUDE_SPAN)))
        self.assertLessEqual(
            latitude_span * longitude_span, Fraction(str(MODULE.MAX_BBOX_AREA))
        )

    def test_preserves_exact_decimal_bounds_in_queries_and_post_bodies(self):
        bboxes = (
            "77.82352159599785,0,77.92352159599785,0.01",
            "0,77.82352159599785,0.01,77.92352159599785",
            "-77.92352159599785,-0.01,-77.82352159599785,0",
            "-0.01,-77.92352159599785,0,-77.82352159599785",
            "77.82352159599785,0,77.82352159599786,0.00000000000001",
            "-77.82352159599786,-0.00000000000001,-77.82352159599785,0",
        )
        tags = MODULE.validate_tags(("amenity=water",))

        for bbox in bboxes:
            with self.subTest(bbox=bbox):
                parsed_bbox = MODULE.parse_bbox(bbox)
                query = MODULE.build_query(parsed_bbox, tags, 20)
                self.assert_query_preserves_bbox(query, parsed_bbox)

                _, urlopen = self.fetch(
                    {"elements": []},
                    bbox=bbox,
                    tags=("amenity=water",),
                    query_timeout=20,
                )
                request = urlopen.call_args.args[0]
                posted_query = urllib.parse.parse_qs(
                    request.data.decode("ascii"), strict_parsing=True
                )["data"][0]
                self.assertEqual(posted_query, query)
                self.assert_query_preserves_bbox(posted_query, parsed_bbox)

    def test_posts_encoded_bounded_query_with_headers_and_timeouts(self):
        records, urlopen = self.fetch()

        self.assertEqual(len(records), 3)
        request = urlopen.call_args.args[0]
        self.assertEqual(request.full_url, MODULE.ENDPOINT)
        self.assertEqual(request.get_method(), "POST")
        self.assertEqual(request.get_header("Accept"), "application/json")
        self.assertEqual(request.get_header("User-agent"), MODULE.USER_AGENT)
        self.assertEqual(
            request.get_header("Content-type"),
            "application/x-www-form-urlencoded; charset=utf-8",
        )
        self.assertEqual(urlopen.call_args.kwargs, {"timeout": 17.0})
        query = urllib.parse.parse_qs(request.data.decode("ascii"))["data"][0]
        self.assertIn("[out:json][timeout:19]", query)
        self.assertIn(f"[maxsize:{MODULE.MAX_RESPONSE_BYTES}]", query)
        self.assertIn('nwr["amenity"="drinking_water"](51.50,-0.13,51.51,-0.12);', query)
        self.assertTrue(query.endswith("out body geom;"))

    def test_uses_environment_user_agent(self):
        self.assertEqual(MODULE.USER_AGENT, MODULE.os.environ.get("EXTRACT_USER_AGENT") or MODULE.USER_AGENT)

    def test_escapes_tag_text_as_quoted_ql_strings(self):
        hostile = 'x"];out;node["all"="yes'
        tags = MODULE.validate_tags((f'name={hostile}', 'quote=" slash=\\'))
        query = MODULE.build_query(MODULE.parse_bbox("0,0,0.01,0.01"), tags, 20)

        self.assertIn('["name"="x\\\"];out;node[\\"all\\\"=\\\"yes"]', query)
        self.assertIn('["quote"="\\\" slash=\\\\"]', query)
        self.assertEqual(query.count("nwr"), 1)
        self.assertEqual(query.count("out body geom;"), 1)

    def test_rejects_missing_invalid_and_oversized_bounds_before_http(self):
        invalid = (
            "",
            "1,2,3",
            "x,2,3,4",
            "nan,2,3,4",
            "91,2,92,3",
            "1,181,2,182",
            "2,1,1,2",
            "1,2,1.100001,2.01",
            "1,2,1.1,2.100001",
        )
        for bbox in invalid:
            with self.subTest(bbox=bbox), mock.patch.object(
                MODULE.urllib.request, "urlopen"
            ) as urlopen:
                with self.assertRaises(ValueError):
                    MODULE.fetch_overpass(bbox, ("amenity=water",))
                urlopen.assert_not_called()

    def test_accepts_decimal_bbox_spans_at_and_just_below_boundaries(self):
        valid = (
            "0,0,0.099999999999999999,0.099999999999999999",
            "0,0,0.1,0.1",
            "1,2,1.1,2.1",
            "-1.1,-2.1,-1,-2",
            "-1.1,2,-1,2.099999999999999999",
            "1,-2.1,1.099999999999999999,-2",
        )
        for bbox in valid:
            with self.subTest(bbox=bbox), mock.patch.object(
                MODULE.urllib.request,
                "urlopen",
                return_value=response({"elements": []}),
            ) as urlopen:
                self.assertEqual(
                    MODULE.fetch_overpass(bbox, ("amenity=water",)), []
                )
                urlopen.assert_called_once()

    def test_rejects_decimal_bbox_spans_just_above_each_boundary_before_http(self):
        invalid = (
            "0,0,0.100000000000000001,0.01",
            "0,0,0.01,0.100000000000000001",
            "-1.1,-2.1,-0.999999999999999999,-2.09",
        )
        for bbox in invalid:
            with self.subTest(bbox=bbox), mock.patch.object(
                MODULE.urllib.request, "urlopen"
            ) as urlopen:
                with self.assertRaises(ValueError):
                    MODULE.fetch_overpass(bbox, ("amenity=water",))
                urlopen.assert_not_called()

    def test_rejects_missing_invalid_and_excess_tag_filters_before_http(self):
        invalid = (
            (),
            ("missing-separator",),
            ("=value",),
            ("key=",),
            ("bad\nkey=value",),
            ("a=b", "a=b"),
            tuple(f"key{index}=value" for index in range(MODULE.MAX_TAGS + 1)),
        )
        for tags in invalid:
            with self.subTest(tags=tags), mock.patch.object(
                MODULE.urllib.request, "urlopen"
            ) as urlopen:
                with self.assertRaises(ValueError):
                    MODULE.fetch_overpass("1,2,1.01,2.01", tags)
                urlopen.assert_not_called()

    def test_rejects_unbounded_timeout_and_retry_options_before_http(self):
        invalid = (
            {"http_timeout": 0},
            {"http_timeout": MODULE.MAX_HTTP_TIMEOUT + 1},
            {"query_timeout": 0},
            {"query_timeout": MODULE.MAX_QUERY_TIMEOUT + 1},
            {"retries": -1},
            {"retries": MODULE.MAX_RETRIES + 1},
            {"retry_delay": -1},
            {"retry_delay": MODULE.MAX_RETRY_DELAY + 1},
        )
        for changes in invalid:
            with self.subTest(changes=changes), mock.patch.object(
                MODULE.urllib.request, "urlopen"
            ) as urlopen:
                with self.assertRaises(ValueError):
                    MODULE.fetch_overpass(
                        "1,2,1.01,2.01", ("amenity=water",), **changes
                    )
                urlopen.assert_not_called()

    def test_normalizes_node_way_and_relation_with_qualified_collision_safe_ids(self):
        records, _ = self.fetch()
        node, way, relation = records

        self.assertEqual([record["id"] for record in records], ["node/7", "way/7", "relation/7"])
        self.assertEqual({record["source"] for record in records}, {"osm_overpass"})
        self.assertEqual({record["fetched_at"] for record in records}, {FETCHED_AT})
        self.assertEqual(node["tags"]["name"], "Tap")
        self.assertEqual((node["lat"], node["lon"]), (51.505, -0.125))
        self.assertEqual(way["nodes"], [7, 8])
        self.assertEqual(way["geometry"][1], {"lat": 51.506, "lon": -0.124})
        self.assertEqual(way["bounds"]["minlat"], 51.504)
        self.assertEqual(relation["center"], {"lat": 51.505, "lon": -0.125})
        self.assertEqual(relation["members"][0]["role"], "outer")
        self.assertEqual(relation["members"][0]["geometry"][0]["lat"], 51.504)
        self.assertEqual(relation["members"][1]["lat"], 51.505)

    def test_empty_elements_is_a_successful_empty_snapshot(self):
        records, _ = self.fetch({"elements": []})
        self.assertEqual(records, [])

    def test_retries_only_bounded_transient_failures_with_backoff(self):
        transient = urllib.error.HTTPError(MODULE.ENDPOINT, 503, "busy", {}, None)
        with mock.patch.object(
            MODULE.urllib.request,
            "urlopen",
            side_effect=[transient, urllib.error.URLError("offline"), response({"elements": []})],
        ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            records = MODULE.fetch_overpass(
                "1,2,1.01,2.01",
                ("amenity=water",),
                retries=2,
                retry_delay=3,
                http_timeout=12,
            )

        self.assertEqual(records, [])
        self.assertEqual(urlopen.call_count, 3)
        self.assertEqual([call.args[0] for call in sleep.call_args_list], [3.0, 6.0])
        self.assertTrue(all(call.kwargs == {"timeout": 12.0} for call in urlopen.call_args_list))

    def test_does_not_retry_permanent_http_or_malformed_json(self):
        cases = (
            ({"side_effect": urllib.error.HTTPError(MODULE.ENDPOINT, 400, "bad", {}, None)}, "HTTP 400"),
            ({"return_value": response(b"not-json")}, "malformed JSON"),
        )
        for behavior, message in cases:
            with self.subTest(message=message), mock.patch.object(
                MODULE.urllib.request, "urlopen", **behavior
            ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
                with self.assertRaisesRegex(MODULE.OverpassError, message):
                    MODULE.fetch_overpass(
                        "1,2,1.01,2.01", ("amenity=water",), retries=2, retry_delay=1
                    )
                urlopen.assert_called_once()
                sleep.assert_not_called()

    def test_timeout_exhaustion_is_a_failure(self):
        with mock.patch.object(
            MODULE.urllib.request, "urlopen", side_effect=socket.timeout("slow")
        ) as urlopen, mock.patch.object(MODULE.time, "sleep"):
            with self.assertRaisesRegex(MODULE.OverpassError, "request failed"):
                MODULE.fetch_overpass(
                    "1,2,1.01,2.01", ("amenity=water",), retries=1, retry_delay=0
                )
        self.assertEqual(urlopen.call_count, 2)

    def test_rejects_oversize_malformed_runtime_error_and_bad_elements(self):
        invalid = (
            (b" " * (MODULE.MAX_RESPONSE_BYTES + 1), "exceeded"),
            ([], "must be an object"),
            ({}, "elements must be a list"),
            ({"remark": "runtime error: timed out", "elements": []}, "reported an error"),
            ({"elements": [None]}, "element must be an object"),
            ({"elements": [{"type": "area", "id": 1, "tags": {}}]}, "invalid type"),
            ({"elements": [{"type": "node", "id": 1, "lat": 1, "lon": 2, "tags": []}]}, "invalid tags"),
            ({"elements": [{"type": "way", "id": 1, "nodes": [], "geometry": None, "tags": {}}]}, "geometry must be a list"),
            ({"elements": [fixture_document()["elements"][0]] * 2}, "duplicate qualified"),
        )
        for document, message in invalid:
            with self.subTest(message=message):
                with self.assertRaisesRegex(MODULE.OverpassError, message):
                    self.fetch(document)

    def test_main_emits_compact_ndjson_and_no_partial_output_on_failure(self):
        stdout = io.StringIO()
        with mock.patch.object(
            MODULE.urllib.request, "urlopen", return_value=response(fixture_document())
        ), contextlib.redirect_stdout(stdout):
            MODULE.main(
                [
                    "--bbox", "51.50,-0.13,51.51,-0.12",
                    "--tag", "amenity=drinking_water",
                    "--retries", "0",
                    "--retry-delay", "0",
                ]
            )
        lines = stdout.getvalue().splitlines()
        self.assertEqual(len(lines), 3)
        self.assertEqual(json.loads(lines[0])["id"], "node/7")
        self.assertNotIn(": ", lines[0])
        self.assertTrue(datetime.fromisoformat(json.loads(lines[0])["fetched_at"]).tzinfo)

        stdout = io.StringIO()
        with mock.patch.object(
            MODULE.urllib.request, "urlopen", return_value=response({"elements": [fixture_document()["elements"][0], None]})
        ), contextlib.redirect_stdout(stdout):
            with self.assertRaises(SystemExit) as raised:
                MODULE.main(["--bbox", "1,2,1.01,2.01", "--tag", "a=b", "--retries", "0"])
        self.assertEqual(raised.exception.code, 1)
        self.assertEqual(stdout.getvalue(), "")


if __name__ == "__main__":
    unittest.main()
