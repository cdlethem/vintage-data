import importlib.util
import io
import json
import pathlib
import socket
import unittest
import urllib.error
from unittest import mock


SCRIPT = pathlib.Path(__file__).parents[1] / "scripts" / "fetch_openwaters_ais.py"
SPEC = importlib.util.spec_from_file_location("fetch_openwaters_ais", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


class Response(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def snapshot_document(*, valid=True, features=None):
    if features is None:
        features = [
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [1.2, 51.3]},
                "properties": {
                    "mmsi": 367334360,
                    "name": "ROSS SEA",
                    "callsign": "WDE3122",
                    "imo": 8983105,
                    "flag": "US",
                    "type": 70,
                    "kind": "vessel",
                    "nav_status": 0,
                    "sog": 12.5,
                    "cog": 218.9,
                    "heading": 197,
                    "draught": 4,
                    "length": 30,
                    "beam": 10,
                    "destination": "Harbour",
                    "eta": "11-11 09:01",
                    "seen": "2026-11-11T05:38:31Z",
                    "msg_type": "PositionReport",
                    "source": "aisstream",
                    "station": "aisstream",
                },
            }
        ]
    if valid:
        doc = {"type": "FeatureCollection", "features": features, "attribution": {}}
    else:
        doc = {"features": "nope"}
    return json.dumps(doc).encode()


def point_feature(mmsi=123, seen="2026-11-11T05:38:31Z", geometry=None, props=None):
    feature = {
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [1.2, 51.3]}
        if geometry is None
        else geometry,
        "properties": ({"mmsi": mmsi, "seen": seen} if props is None else props),
    }
    return feature


class FetchVesselsTests(unittest.TestCase):
    def fetch(self, response_or_effect, **kwargs):
        urlopen_kwargs = (
            {"return_value": response_or_effect}
            if isinstance(response_or_effect, Response)
            else {"side_effect": response_or_effect}
        )
        with mock.patch.object(MODULE.urllib.request, "urlopen", **urlopen_kwargs) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            records = list(MODULE.fetch_vessels(**kwargs))
        return records, urlopen, sleep

    def test_immediate_success_uses_one_request_and_preserves_payload(self):
        records, urlopen, sleep = self.fetch(Response(snapshot_document()), timeout=23)

        self.assertEqual(len(records), 1)
        record = records[0]
        self.assertEqual(record["id"], "367334360:2026-11-11T05:38:31Z")
        self.assertEqual(record["source"], MODULE.SOURCE)
        self.assertEqual(record["mmsi"], 367334360)
        self.assertEqual(record["name"], "ROSS SEA")
        self.assertEqual(record["imo"], 8983105)
        self.assertEqual(record["latitude"], 51.3)
        self.assertEqual(record["longitude"], 1.2)
        self.assertEqual(record["sog_knots"], 12.5)
        self.assertEqual(record["cog_deg"], 218.9)
        self.assertEqual(record["receiver"], "aisstream")
        self.assertEqual(record["eta"], "11-11 09:01")
        self.assertNotIn("station", record)
        urlopen.assert_called_once()
        request = urlopen.call_args.args[0]
        self.assertTrue(request.full_url.startswith(f"{MODULE.BASE}?"))
        self.assertIn("bbox=51.0%2C0.5%2C51.5%2C1.5", request.full_url)
        self.assertIn("max_age=15m", request.full_url)
        self.assertEqual(urlopen.call_args.kwargs["timeout"], 23)
        sleep.assert_not_called()

    def test_empty_box_yields_no_records_without_retries(self):
        records, urlopen, sleep = self.fetch(
            Response(snapshot_document(features=[]))
        )
        self.assertEqual(records, [])
        urlopen.assert_called_once()
        sleep.assert_not_called()

    def test_retries_connection_timeout_then_succeeds(self):
        records, urlopen, sleep = self.fetch(
            [socket.timeout("read timed out"), Response(snapshot_document())]
        )
        self.assertEqual(len(records), 1)
        urlopen.assert_called()
        self.assertEqual(urlopen.call_count, 2)
        sleep.assert_called_once_with(MODULE.RETRY_DELAYS[0])

    def test_retries_urllib_wrapped_timeout_then_succeeds(self):
        error = urllib.error.URLError("timed out")
        error.reason = socket.timeout("read timed out")
        records, urlopen, sleep = self.fetch(
            [error, Response(snapshot_document())]
        )
        self.assertEqual(len(records), 1)
        self.assertEqual(urlopen.call_count, 2)
        sleep.assert_called_once_with(MODULE.RETRY_DELAYS[0])

    def test_timeout_exhaustion_is_bounded_and_reports_attempts(self):
        with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=TimeoutError("timed out")) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            with self.assertRaises(TimeoutError):
                list(MODULE.fetch_vessels())
        self.assertEqual(urlopen.call_count, MODULE.MAX_ATTEMPTS)
        self.assertEqual(
            sleep.call_args_list,
            [mock.call(delay) for delay in MODULE.RETRY_DELAYS],
        )

    def test_non_json_body_fails_without_retries(self):
        with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=Response(b"bbox not allowed for this key")) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            with self.assertRaises(ValueError):
                list(MODULE.fetch_vessels())
        urlopen.assert_called_once()
        sleep.assert_not_called()

    def test_missing_features_list_fails_without_retries(self):
        with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=Response(snapshot_document(valid=False))) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            with self.assertRaises(ValueError):
                list(MODULE.fetch_vessels())
        urlopen.assert_called_once()
        sleep.assert_not_called()

    def test_feature_without_point_geometry_fails_without_retries(self):
        doc = snapshot_document(features=[point_feature(geometry={"type": "LineString", "coordinates": []})])
        with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=Response(doc)) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            with self.assertRaises(ValueError):
                list(MODULE.fetch_vessels())
        urlopen.assert_called_once()
        sleep.assert_not_called()

    def test_feature_missing_mmsi_fails_without_retries(self):
        doc = snapshot_document(features=[point_feature(props={"seen": "2026-11-11T05:38:31Z"})])
        with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=Response(doc)) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            with self.assertRaises(ValueError):
                list(MODULE.fetch_vessels())
        urlopen.assert_called_once()
        sleep.assert_not_called()

    def test_non_timeout_transport_errors_fail_without_retries(self):
        error = urllib.error.URLError("temporary failure in name resolution")
        with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=error) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            with self.assertRaises(urllib.error.URLError):
                list(MODULE.fetch_vessels())
        urlopen.assert_called_once()
        sleep.assert_not_called()

    def test_http_errors_fail_without_retries(self):
        error = urllib.error.HTTPError(MODULE.BASE, 400, "bbox not allowed", {}, None)
        with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=error) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            with self.assertRaises(urllib.error.HTTPError):
                list(MODULE.fetch_vessels())
        urlopen.assert_called_once()
        sleep.assert_not_called()

    def test_invalid_bbox_fails_before_any_request(self):
        for bad in ("51,0.5,51.5", "95,0.5,96,1.5", "51.5,51.0,0.5,1.5"):
            with self.assertRaises(ValueError):
                list(MODULE.fetch_vessels(bbox=bad))


if __name__ == "__main__":
    unittest.main()
