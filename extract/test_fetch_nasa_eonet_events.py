import importlib.util
import io
import json
import sys
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse


SCRIPT = Path(__file__).parent / "scripts" / "fetch_nasa_eonet_events.py"
SPEC = importlib.util.spec_from_file_location("fetch_nasa_eonet_events", SCRIPT)
assert SPEC and SPEC.loader
EONET = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = EONET
SPEC.loader.exec_module(EONET)


class FakeResponse(io.StringIO):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def feature(event_id="EONET_1234", title="Example wildfire"):
    return {
        "type": "Feature",
        "id": event_id,
        "geometry": {"type": "Point", "coordinates": [-121.5, 38.6]},
        "properties": {
            "title": title,
            "link": f"https://eonet.gsfc.nasa.gov/api/v3/events/{event_id}",
            "categories": [{"id": "wildfires", "title": "Wildfires"}],
            "sources": [{"id": "source-id", "url": "https://example.test/source"}],
            "geometry": [
                {
                    "date": "2026-09-13T00:00:00Z",
                    "type": "Point",
                    "coordinates": [-121.5, 38.6],
                    "magnitudeValue": 10,
                    "magnitudeUnit": "acres",
                }
            ],
        },
    }


def collection(features):
    return {"type": "FeatureCollection", "features": features}


class FetchEonetEventsTests(unittest.TestCase):
    def mocked_pages(self, *documents):
        requests = []
        bodies = iter(documents)

        def urlopen(request, timeout):
            requests.append((request, timeout))
            return FakeResponse(json.dumps(next(bodies)))

        return requests, urlopen

    def test_normalizes_open_event_with_attribution_and_raw_feature(self):
        raw_feature = feature()
        requests, urlopen = self.mocked_pages(collection([raw_feature]))
        with patch.object(EONET.urllib.request, "urlopen", urlopen), patch.object(
            EONET, "datetime"
        ) as clock:
            clock.now.return_value.isoformat.return_value = "2026-09-13T01:02:03+00:00"
            records = list(EONET.fetch_events(limit=2, timeout=17))

        self.assertEqual(len(records), 1)
        record = records[0]
        self.assertEqual(record["id"], "EONET_1234")
        self.assertEqual(record["title"], "Example wildfire")
        self.assertEqual(record["date"], "2026-09-13T00:00:00Z")
        self.assertEqual(record["magnitude"], {"value": 10, "unit": "acres"})
        self.assertEqual(record["categories"], raw_feature["properties"]["categories"])
        self.assertEqual(record["source_links"], ["https://example.test/source"])
        self.assertEqual(record["coordinates"], [-121.5, 38.6])
        self.assertEqual(record["attribution"], EONET.NASA_ATTRIBUTION)
        self.assertEqual(record["fetched_at"], "2026-09-13T01:02:03+00:00")
        self.assertEqual(record["raw"], raw_feature)
        self.assertEqual(len(requests), 1)
        self.assertEqual(requests[0][1], 17)

    def test_empty_feature_collection_emits_no_records(self):
        _, urlopen = self.mocked_pages(collection([]))
        with patch.object(EONET.urllib.request, "urlopen", urlopen):
            self.assertEqual(list(EONET.fetch_events()), [])

    def test_rejects_malformed_collection_and_event_id(self):
        _, urlopen = self.mocked_pages({"type": "FeatureCollection", "features": {}})
        with patch.object(EONET.urllib.request, "urlopen", urlopen):
            with self.assertRaisesRegex(ValueError, "GeoJSON FeatureCollection"):
                list(EONET.fetch_events())

        _, urlopen = self.mocked_pages(collection([feature(event_id="1234")]))
        with patch.object(EONET.urllib.request, "urlopen", urlopen):
            with self.assertRaisesRegex(ValueError, "valid EONET event ID"):
                list(EONET.fetch_events())

    def test_bounds_requests_and_rejects_out_of_range_values(self):
        requests, urlopen = self.mocked_pages(collection([]))
        with patch.object(EONET.urllib.request, "urlopen", urlopen):
            list(EONET.fetch_events(limit=EONET.MAX_LIMIT, max_pages=EONET.MAX_PAGES))
        query = parse_qs(urlparse(requests[0][0].full_url).query)
        self.assertEqual(query, {"status": ["open"], "limit": ["100"], "page": ["1"]})

        with self.assertRaisesRegex(ValueError, "limit must be"):
            list(EONET.fetch_events(limit=EONET.MAX_LIMIT + 1))
        with self.assertRaisesRegex(ValueError, "max_pages must be"):
            list(EONET.fetch_events(max_pages=EONET.MAX_PAGES + 1))

    def test_paginates_and_deduplicates_stable_event_ids(self):
        first = feature("EONET_1")
        second = feature("EONET_2")
        third = feature("EONET_3")
        requests, urlopen = self.mocked_pages(
            collection([first, second]),
            collection([second, third]),
            collection([]),
        )
        with patch.object(EONET.urllib.request, "urlopen", urlopen):
            records = list(EONET.fetch_events(limit=2, max_pages=3))

        self.assertEqual([record["id"] for record in records], ["EONET_1", "EONET_2", "EONET_3"])
        self.assertEqual(
            [parse_qs(urlparse(request[0].full_url).query)["page"] for request in requests],
            [["1"], ["2"], ["3"]],
        )


if __name__ == "__main__":
    unittest.main()
