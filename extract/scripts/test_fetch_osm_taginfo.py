import contextlib
import importlib.util
import io
import json
from pathlib import Path
import unittest
from unittest import mock
import urllib.error
import urllib.parse


SCRIPT = Path(__file__).with_name("fetch_osm_taginfo.py")
SOURCE_CONFIG = SCRIPT.parents[1] / "sources" / "osm_taginfo.yml"
RAW_SOURCES = SCRIPT.parents[2] / "transform" / "models" / "base" / "_raw_sources.yml"
BASE_MODEL = SCRIPT.parents[2] / "transform" / "models" / "base" / "base_osm_taginfo.sql"
SPEC = importlib.util.spec_from_file_location("fetch_osm_taginfo", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


class JsonResponse(io.BytesIO):
    def __init__(self, document):
        payload = document if isinstance(document, bytes) else json.dumps(document).encode("utf-8")
        super().__init__(payload)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def stats_document(rows=None, **changes):
    document = {
        "url": "https://taginfo.openstreetmap.org/api/4/key/stats?key=highway",
        "data_until": "2026-09-15T00:59:22Z",
        "data": rows
        if rows is not None
        else [
            {
                "type": "all",
                "count": 245000000,
                "count_fraction": 0.037,
                "values": 18347,
            }
        ],
    }
    document.update(changes)
    return document


def values_document(rows=None, *, rp=2, total=18_347, **changes):
    document = {
        "url": "https://taginfo.openstreetmap.org/api/4/key/values?key=highway",
        "data_until": "2026-09-15T00:59:22Z",
        "page": 1,
        "rp": rp,
        "total": total,
        "data": rows
        if rows is not None
        else [
            {"value": "residential", "count": 50000000, "fraction": 0.2},
            {"value": "service", "count": 40000000, "fraction": 0.16},
        ],
    }
    document.update(changes)
    return document


class FetchOsmTaginfoTests(unittest.TestCase):
    def fetch(self, documents, **changes):
        responses = [JsonResponse(document) for document in documents]
        arguments = {"keys": ("highway",), "value_limit": 2, "timeout": 17}
        arguments.update(changes)
        with mock.patch.object(
            MODULE.urllib.request, "urlopen", side_effect=responses
        ) as urlopen:
            records = list(MODULE.fetch_taginfo(**arguments))
        return records, urlopen

    def test_normalizes_stats_and_values_with_stable_ids_and_raw_lineage(self):
        stat_row = {
            "type": "ways",
            "count": 230000000,
            "count_fraction": 0.18,
            "values": 17000,
            "extra": "kept",
        }
        value_row = {
            "value": "residential",
            "count": 50000000,
            "fraction": 0.2,
            "description": "kept too",
        }
        stats = stats_document([stat_row], generator="taginfo")
        values = values_document([value_row], rp=2, total=1, generator="taginfo")

        records, _ = self.fetch([stats, values])

        self.assertEqual(len(records), 2)
        statistic, frequency = records
        self.assertEqual(statistic["source"], "osm_taginfo")
        self.assertEqual(statistic["id"], "key_stats:highway:ways")
        self.assertEqual(statistic["endpoint"], "key/stats")
        self.assertEqual(statistic["key"], "highway")
        self.assertIsNone(statistic["value"])
        self.assertEqual(statistic["count"], 230000000)
        self.assertEqual(statistic["fraction"], 0.18)
        self.assertEqual(statistic["values_count"], 17000)
        self.assertEqual(statistic["data_until"], "2026-09-15T00:59:22Z")
        self.assertEqual(statistic["raw_payload"], stat_row)
        self.assertNotIn("data", statistic["response_metadata"])
        self.assertEqual(statistic["response_metadata"]["generator"], "taginfo")

        self.assertEqual(frequency["id"], "key_values:highway:residential")
        self.assertEqual(frequency["endpoint"], "key/values")
        self.assertEqual(frequency["value"], "residential")
        self.assertEqual(frequency["observation_type"], "value_frequency")
        self.assertEqual(frequency["raw_payload"], value_row)
        self.assertEqual(statistic["fetched_at"], frequency["fetched_at"])

        later = MODULE._normalize_value(
            value_row,
            "highway",
            "later",
            values["data_until"],
            {"page": 1, "rp": 2, "total": 1},
        )
        self.assertEqual(later["id"], frequency["id"])
        self.assertNotEqual(later["fetched_at"], frequency["fetched_at"])

    def test_requests_only_bounded_selected_endpoints_with_headers_and_timeout(self):
        records, urlopen = self.fetch([stats_document(), values_document()])

        self.assertEqual(len(records), 3)
        self.assertEqual(urlopen.call_count, 2)
        stats_request = urlopen.call_args_list[0].args[0]
        values_request = urlopen.call_args_list[1].args[0]
        self.assertEqual(urllib.parse.urlsplit(stats_request.full_url).path, "/api/4/key/stats")
        self.assertEqual(
            urllib.parse.parse_qs(urllib.parse.urlsplit(stats_request.full_url).query),
            {"key": ["highway"]},
        )
        self.assertEqual(urllib.parse.urlsplit(values_request.full_url).path, "/api/4/key/values")
        self.assertEqual(
            urllib.parse.parse_qs(urllib.parse.urlsplit(values_request.full_url).query),
            {
                "key": ["highway"],
                "page": ["1"],
                "rp": ["2"],
                "sortname": ["count"],
                "sortorder": ["desc"],
            },
        )
        for call in urlopen.call_args_list:
            request = call.args[0]
            self.assertEqual(request.get_header("Accept"), "application/json")
            self.assertTrue(request.get_header("User-agent"))
            self.assertEqual(call.kwargs, {"timeout": 17})

    def test_multiple_keys_use_two_requests_each_and_one_timestamp(self):
        documents = [
            stats_document(),
            values_document(),
            stats_document(),
            values_document(),
        ]
        records, urlopen = self.fetch(documents, keys=("highway", "amenity"))

        self.assertEqual(urlopen.call_count, 4)
        self.assertEqual({record["key"] for record in records}, {"highway", "amenity"})
        self.assertEqual(len({record["fetched_at"] for record in records}), 1)
        third_query = urllib.parse.parse_qs(
            urllib.parse.urlsplit(urlopen.call_args_list[2].args[0].full_url).query
        )
        self.assertEqual(third_query["key"], ["amenity"])

    def test_invalid_request_bounds_fail_before_http(self):
        invalid = (
            ({"keys": ()}, "at least one"),
            ({"keys": ("highway", "highway")}, "duplicates"),
            ({"keys": tuple(f"key{number}" for number in range(11))}, "at most"),
            ({"keys": ("bad\nkey",)}, "invalid OSM key"),
            ({"value_limit": 0}, "value_limit"),
            ({"value_limit": 101}, "value_limit"),
            ({"timeout": 0}, "timeout"),
        )
        for changes, message in invalid:
            with self.subTest(changes=changes):
                arguments = {"keys": ("highway",), "value_limit": 2, "timeout": 17}
                arguments.update(changes)
                with mock.patch.object(MODULE.urllib.request, "urlopen") as urlopen:
                    with self.assertRaisesRegex(ValueError, message):
                        list(MODULE.fetch_taginfo(**arguments))
                urlopen.assert_not_called()

    def test_rejects_malformed_envelopes_rows_and_value_page_metadata(self):
        invalid = (
            ([[], values_document()], "must be an object"),
            ([stats_document(data={}), values_document()], "data must be a list"),
            ([stats_document(data_until=None), values_document()], "data_until"),
            ([stats_document([]), values_document()], "no observations"),
            ([stats_document([stats_document()["data"][0]] * 5), values_document()], "4-row bound"),
            ([stats_document([{"type": "all", "count": -1, "count_fraction": 0.1, "values": 2}]), values_document()], "count"),
            ([stats_document(), values_document(page=2)], "expected 1"),
            ([stats_document(), values_document(rp=3)], "expected 2"),
            ([stats_document(), values_document(total=1)], "more rows than"),
            ([stats_document(), values_document([{"value": "x", "count": 1, "fraction": 2.0}], total=1)], "fraction"),
        )
        for documents, message in invalid:
            with self.subTest(message=message):
                with self.assertRaisesRegex(MODULE.TaginfoError, message):
                    self.fetch(documents)

    def test_malformed_or_unavailable_json_has_clear_failure_and_no_output(self):
        cases = (
            ({"return_value": JsonResponse(b"not-json")}, "malformed JSON"),
            ({"side_effect": urllib.error.URLError("offline")}, "unavailable"),
        )
        for mock_behavior, message in cases:
            with self.subTest(message=message):
                stdout = io.StringIO()
                with mock.patch.object(
                    MODULE.urllib.request, "urlopen", **mock_behavior
                ):
                    with contextlib.redirect_stdout(stdout):
                        with self.assertRaisesRegex(MODULE.TaginfoError, message):
                            list(MODULE.fetch_taginfo(keys=("highway",), value_limit=2))
                self.assertEqual(stdout.getvalue(), "")

    def test_rejects_oversize_response_before_json_parsing(self):
        response = JsonResponse(b" " * (MODULE.MAX_RESPONSE_BYTES + 1))
        with mock.patch.object(
            MODULE.urllib.request, "urlopen", return_value=response
        ):
            with self.assertRaisesRegex(MODULE.TaginfoError, "exceeded"):
                list(MODULE.fetch_taginfo(keys=("highway",), value_limit=2))

    def test_later_endpoint_failure_cannot_emit_partial_records(self):
        stdout = io.StringIO()
        responses = [JsonResponse(stats_document()), urllib.error.URLError("offline")]
        with mock.patch.object(
            MODULE.urllib.request, "urlopen", side_effect=responses
        ):
            with contextlib.redirect_stdout(stdout):
                with self.assertRaises(SystemExit) as raised:
                    MODULE.main(["--key", "highway", "--value-limit", "2"])
        self.assertEqual(raised.exception.code, 1)
        self.assertEqual(stdout.getvalue(), "")

    def test_main_emits_compact_reloadable_ndjson(self):
        stdout = io.StringIO()
        responses = [JsonResponse(stats_document()), JsonResponse(values_document())]
        with mock.patch.object(
            MODULE.urllib.request, "urlopen", side_effect=responses
        ):
            with contextlib.redirect_stdout(stdout):
                MODULE.main(["--key", "highway", "--value-limit", "2"])

        lines = stdout.getvalue().splitlines()
        self.assertEqual(len(lines), 3)
        self.assertEqual(json.loads(lines[0])["source"], "osm_taginfo")
        self.assertNotIn(": ", lines[0])

    def test_source_configuration_is_conservative_bounded_and_disabled(self):
        text = SOURCE_CONFIG.read_text(encoding="utf-8")
        self.assertRegex(text, r"(?m)^schedule: \"[^\"]+\".*monthly")
        self.assertRegex(text, r"(?m)^enabled: false$")
        self.assertIn('"--value-limit", "50"', text)
        self.assertIn("two requests per configured key", text)
        self.assertIn("not a bulk", text)
        self.assertIn("https://taginfo.openstreetmap.org/taginfo/apidoc", text)

    def test_raw_source_inventory_and_base_model_recognize_source(self):
        inventory = RAW_SOURCES.read_text(encoding="utf-8")
        model = BASE_MODEL.read_text(encoding="utf-8")
        self.assertIn("  - name: osm_taginfo\n", inventory)
        self.assertIn("- name: base_osm_taginfo\n", inventory)
        self.assertIn("source('raw', 'osm_taginfo')", model)
        for column in ("key", "value", "count", "fraction", "data_until", "raw_payload"):
            self.assertIn(f'    "{column}"', model)


if __name__ == "__main__":
    unittest.main()
