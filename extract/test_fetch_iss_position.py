import contextlib
import importlib.util
import io
import json
from pathlib import Path
import re
import unittest
from unittest import mock


SCRIPT = Path(__file__).parent / "scripts" / "fetch_iss_position.py"
SOURCE_CONFIG = Path(__file__).parent / "sources" / "iss_position.yml"
SPEC = importlib.util.spec_from_file_location("fetch_iss_position", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


# Field layout follows the WTIA v1 satellites/[id] representation documented at
# https://wheretheiss.at/w/developer. Values are fixed local test data.
SATELLITE_POSITION = {
    "name": "iss",
    "id": 25544,
    "latitude": 42.691539376205,
    "longitude": 86.736860971946,
    "altitude": 427.86383966667,
    "velocity": 27569.100839107,
    "visibility": "daylight",
    "footprint": 4547.2567037502,
    "timestamp": 1792128726,
    "daynum": 2461329.730625,
    "solar_lat": -8.8923356681052,
    "solar_lon": 93.375799421831,
    "units": "kilometers",
}


class JsonResponse(io.BytesIO):
    def __init__(self, document):
        super().__init__(json.dumps(document).encode("utf-8"))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


class FetchIssPositionTests(unittest.TestCase):
    def fetch(self, document, **kwargs):
        with mock.patch.object(
            MODULE.urllib.request,
            "urlopen",
            return_value=JsonResponse(document),
        ) as urlopen:
            record = MODULE.fetch_position(**kwargs)
        return record, urlopen

    def test_emits_one_observation_with_source_natural_id(self):
        record, _ = self.fetch(dict(SATELLITE_POSITION))

        self.assertEqual(record["source"], "iss_position")
        self.assertEqual(record["id"], "iss-1792128726")
        self.assertEqual(record["norad_id"], 25544)
        self.assertEqual(record["name"], "iss")
        self.assertEqual(record["latitude"], SATELLITE_POSITION["latitude"])
        self.assertEqual(record["longitude"], SATELLITE_POSITION["longitude"])
        self.assertEqual(record["altitude"], SATELLITE_POSITION["altitude"])
        self.assertEqual(record["velocity"], SATELLITE_POSITION["velocity"])
        self.assertEqual(record["visibility"], "daylight")
        self.assertEqual(record["footprint"], SATELLITE_POSITION["footprint"])
        self.assertEqual(record["daynum"], SATELLITE_POSITION["daynum"])
        self.assertEqual(record["solar_lat"], SATELLITE_POSITION["solar_lat"])
        self.assertEqual(record["solar_lon"], SATELLITE_POSITION["solar_lon"])
        self.assertEqual(record["units"], "kilometers")
        self.assertIn("wheretheiss.at", record["attribution"])
        self.assertRegex(record["fetched_at"], r"\+00:00$")

    def test_request_is_single_bounded_and_identifying(self):
        _, urlopen = self.fetch(dict(SATELLITE_POSITION), timeout=17)

        urlopen.assert_called_once()
        request = urlopen.call_args.args[0]
        self.assertEqual(
            request.full_url,
            "https://api.wheretheiss.at/v1/satellites/25544",
        )
        self.assertTrue(request.get_header("User-agent"))
        self.assertEqual(urlopen.call_args.kwargs, {"timeout": 17})

    def test_invalid_bounds_fail_before_http(self):
        with mock.patch.object(MODULE.urllib.request, "urlopen") as urlopen:
            with self.assertRaises(ValueError):
                MODULE.fetch_position(timeout=0)
        urlopen.assert_not_called()

    def test_rejects_non_object_response(self):
        with self.assertRaises(TypeError):
            self.fetch([SATELLITE_POSITION])

    def test_rejects_missing_observation_fields(self):
        for field in ("latitude", "longitude", "timestamp"):
            row = dict(SATELLITE_POSITION)
            del row[field]
            with self.subTest(field=field):
                with self.assertRaisesRegex(ValueError, f"missing {field}"):
                    self.fetch(row)

    def test_main_emits_one_ndjson_record(self):
        stdout = io.StringIO()

        with mock.patch.object(
            MODULE.urllib.request,
            "urlopen",
            return_value=JsonResponse(SATELLITE_POSITION),
        ):
            with contextlib.redirect_stdout(stdout):
                MODULE.main([])

        lines = stdout.getvalue().splitlines()
        self.assertEqual(len(lines), 1)
        self.assertEqual(json.loads(lines[0])["id"], "iss-1792128726")

    def test_source_configuration_is_staggered_five_minute_and_disabled(self):
        text = SOURCE_CONFIG.read_text(encoding="utf-8")

        self.assertRegex(text, r'(?m)^schedule: "1-59/5 \* \* \* \*"')
        self.assertRegex(text, r"(?m)^enabled: false$")
        self.assertRegex(text, r"(?m)^args: \[\]$")
        self.assertIsNone(re.search(r"(?m)^enabled: true$", text))


if __name__ == "__main__":
    unittest.main()
