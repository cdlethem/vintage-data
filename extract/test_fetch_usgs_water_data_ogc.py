import contextlib
import importlib.util
import json
import pathlib
import tempfile
import unittest
from datetime import datetime, timezone

SCRIPT = pathlib.Path(__file__).parent / "scripts" / "fetch_usgs_water_data_ogc.py"
SPEC = importlib.util.spec_from_file_location("usgs_water_data_ogc", SCRIPT)
usgs = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(usgs)


class FakeResponse:
    def __init__(self, document):
        self.document = document

    def read(self):
        return json.dumps(self.document).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, *unused):
        return False


class FakeOpener:
    def __init__(self, responses):
        self.responses = list(responses)
        self.requests = []

    def __call__(self, request, timeout):
        self.requests.append((request.full_url, timeout, request.get_header("User-agent")))
        if not self.responses:
            raise AssertionError("unexpected HTTP request")
        return FakeResponse(self.responses.pop(0))


def catalog(*collections):
    return {"collections": [{"id": value} for value in collections]}


def feature(identifier="row-1", station="01646500", observed_at="2026-09-01T00:00:00Z", parameter="00060"):
    return {
        "type": "Feature",
        "id": identifier,
        "properties": {
            "monitoring_location_id": station,
            "time": observed_at,
            "parameter_code": parameter,
            "value": 123.4,
            "unit_of_measure": "ft3/s",
            "statistic_code": "00003",
        },
    }


def page(features, next_href=None):
    links = [] if next_href is None else [{"rel": "next", "href": next_href}]
    return {"type": "FeatureCollection", "features": features, "links": links}


class UsgsWaterDataOgcTests(unittest.TestCase):
    def collect(self, opener, **kwargs):
        state = kwargs.pop("state", {"version": 1, "continuations": {}})
        records = list(usgs.fetch_observations(
            ("01646500",), opener=opener, sleeper=lambda seconds: None,
            pace_seconds=0, now=datetime(2026, 9, 2, tzinfo=timezone.utc), state=state, **kwargs))
        return records, state

    def test_normalizes_observation_with_stable_measurement_id(self):
        raw = feature()
        first = usgs.normalize_observation(raw, "daily", "2026-09-02T00:00:00+00:00")
        second = usgs.normalize_observation(json.loads(json.dumps(raw)), "daily", "later")
        self.assertEqual(first["id"], "usgs_water_data_ogc:daily:01646500:2026-09-01T00:00:00Z:row-1")
        self.assertEqual(first["id"], second["id"])
        self.assertEqual(first["metric"], "00060")
        self.assertEqual(first["units"], "ft3/s")
        self.assertEqual(first["interval"], "00003")
        self.assertEqual(first["observation"], raw["properties"])
        self.assertEqual(first["source_url"], usgs.API_ROOT + "/collections/daily/items/row-1")

    def test_empty_data_is_complete_and_makes_one_items_request(self):
        opener = FakeOpener([catalog("daily"), page([])])
        records, state = self.collect(opener)
        self.assertEqual(records, [])
        self.assertEqual(state["last_summary"]["completeness"], "complete")
        self.assertEqual(state["last_summary"]["records"], 0)
        self.assertEqual(state["last_summary"]["requests"]["attempted"], 2)

    def test_follows_next_link_and_persists_finished_cursor_removal(self):
        opener = FakeOpener([catalog("daily"), page([feature("one")], "?cursor=two"), page([feature("two")])])
        records, state = self.collect(opener, max_pages=3)
        self.assertEqual([record["id"].rsplit(":", 1)[-1] for record in records], ["one", "two"])
        self.assertEqual(opener.requests[2][0], usgs.API_ROOT + "/collections/daily/items?cursor=two")
        self.assertEqual(state["continuations"], {})
        self.assertEqual(state["last_summary"]["pagination"]["pages"], 2)

    def test_record_cap_resumes_inside_page_without_skipping_data(self):
        first_opener = FakeOpener([catalog("daily"), page([feature("one"), feature("two")])])
        records, state = self.collect(first_opener, max_records=1)
        self.assertEqual([record["id"].rsplit(":", 1)[-1] for record in records], ["one"])
        self.assertEqual(state["last_summary"]["completeness"], "partial")
        key = "daily|01646500||14"
        self.assertEqual(state["continuations"][key]["offset"], 1)

        second_opener = FakeOpener([catalog("daily"), page([feature("one"), feature("two")])])
        records, state = self.collect(second_opener, state=state, max_records=2)
        self.assertEqual([record["id"].rsplit(":", 1)[-1] for record in records], ["two"])
        self.assertEqual(state["continuations"], {})

    def test_page_bound_stops_before_following_next_link(self):
        opener = FakeOpener([catalog("daily"), page([feature("one")], "?cursor=two")])
        records, state = self.collect(opener, max_pages=1, max_records=10)
        self.assertEqual(len(records), 1)
        self.assertEqual(len(opener.requests), 2)
        self.assertTrue(state["last_summary"]["pagination"]["capped"])
        self.assertEqual(state["last_summary"]["completeness"], "partial")

    def test_malformed_response_fails_without_continuation(self):
        opener = FakeOpener([catalog("daily"), {"features": {}}])
        state = {"version": 1, "continuations": {}}
        with self.assertRaisesRegex(RuntimeError, "retrieval failed"):
            list(usgs.fetch_observations(("01646500",), opener=opener, sleeper=lambda seconds: None,
                                         pace_seconds=0, state=state))
        self.assertEqual(state["continuations"], {})
        self.assertEqual(state["last_summary"]["health"], "failed")
        self.assertEqual(state["last_summary"]["failures"]["count"], 1)

    def test_unverified_collection_fails_before_items_request(self):
        opener = FakeOpener([catalog("continuous")])
        with self.assertRaisesRegex(ValueError, "does not contain: daily"):
            self.collect(opener)
        self.assertEqual(len(opener.requests), 1)

    def test_state_is_atomic_and_must_be_outside_checkout(self):
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "state.json"
            expected = {"version": 1, "continuations": {"daily|station||14": {"url": "https://example.test/page", "offset": 0}}}
            usgs.save_state(path, expected)
            self.assertEqual(usgs.load_state(path), expected)
            self.assertFalse(path.with_name(path.name + ".tmp").exists())
        with self.assertRaisesRegex(ValueError, "outside"):
            usgs.save_state(pathlib.Path.cwd() / "state.json", {"version": 1, "continuations": {}})


if __name__ == "__main__":
    unittest.main()
