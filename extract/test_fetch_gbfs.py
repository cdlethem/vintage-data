import importlib.util
import io
import json
import pathlib
import socket
import unittest
import urllib.error
from unittest import mock


SCRIPT = pathlib.Path(__file__).parent / "scripts" / "fetch_gbfs.py"
SPEC = importlib.util.spec_from_file_location("fetch_gbfs", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)

DISCOVERY_URL = "https://gbfs.example.com/gbfs/gbfs.json"


class Response(io.BytesIO):
    def __init__(self, body, *, status=200, headers=None):
        super().__init__(body)
        self.status = status
        self.headers = {} if headers is None else headers

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def response(document):
    return Response(json.dumps(document).encode("utf-8"))


def discovery():
    return {
        "data": {
            "en": {
                "feeds": [
                    {"name": "station_status", "url": "https://gbfs.example.com/gbfs/station_status.json"},
                    {"name": "station_information", "url": "https://gbfs.example.com/gbfs/station_information.json"},
                ]
            }
        }
    }


def station_status():
    return {
        "ttl": 60,
        "data": {
            "stations": [
                {
                    "station_id": "1",
                    "is_installed": 1,
                    "is_renting": 1,
                    "is_returning": 0,
                    "last_reported": 1763300000,
                    "num_bikes_available": 5,
                    "num_docks_available": 3,
                    "num_ebikes_available": 0,
                    "num_bikes_disabled": 0,
                    "num_docks_disabled": 0,
                }
            ],
        }
    }


def dns_failure():
    return urllib.error.URLError(socket.gaierror(-3, "Temporary failure in name resolution"))


class FetchGbfsTests(unittest.TestCase):
    def test_yields_station_status_records(self):
        with mock.patch.object(
            MODULE.urllib.request, "urlopen",
            side_effect=[response(discovery()), response(station_status())],
        ):
            records = list(MODULE.fetch_station_status(DISCOVERY_URL, system="citibike"))
        self.assertEqual(len(records), 1)
        rec = records[0]
        self.assertEqual(rec["source"], "gbfs_citibike")
        self.assertEqual(rec["id"], "1")
        self.assertIs(rec["is_installed"], True)
        self.assertEqual(rec["num_bikes_available"], 5)
        self.assertEqual(rec["last_reported"], 1763300000)
        self.assertEqual(rec["ttl"], 60)

    def test_recovers_from_transient_dns_failure(self):
        with mock.patch.object(
            MODULE.urllib.request, "urlopen",
            side_effect=[dns_failure(), response(discovery()), response(station_status())],
        ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            records = list(MODULE.fetch_station_status(DISCOVERY_URL, system="citibike"))
        self.assertEqual(len(records), 1)
        self.assertEqual(urlopen.call_count, 3)
        self.assertEqual(sleep.call_args_list, [mock.call(MODULE.RETRY_BASE_DELAY)])

    def test_recovers_from_read_timeout(self):
        with mock.patch.object(
            MODULE.urllib.request, "urlopen",
            side_effect=[TimeoutError("The read operation timed out"),
                         response(discovery()), response(station_status())],
        ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            records = list(MODULE.fetch_station_status(DISCOVERY_URL, system="citibike"))
        self.assertEqual(len(records), 1)
        self.assertEqual(urlopen.call_count, 3)
        self.assertEqual(sleep.call_args_list, [mock.call(MODULE.RETRY_BASE_DELAY)])

    def test_stops_after_finite_transport_failures(self):
        with mock.patch.object(
            MODULE.urllib.request, "urlopen",
            side_effect=[dns_failure()] * MODULE.MAX_ATTEMPTS,
        ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            with self.assertRaises(urllib.error.URLError):
                list(MODULE.fetch_station_status(DISCOVERY_URL, system="citibike"))
        self.assertEqual(urlopen.call_count, MODULE.MAX_ATTEMPTS)
        self.assertEqual(
            sleep.call_args_list,
            [mock.call(MODULE.RETRY_BASE_DELAY), mock.call(MODULE.RETRY_BASE_DELAY * 2)],
        )

    def test_http_errors_are_not_retried(self):
        error = urllib.error.HTTPError(DISCOVERY_URL, 404, "Not Found", {}, io.BytesIO(b"nope"))
        with mock.patch.object(
            MODULE.urllib.request, "urlopen", side_effect=error,
        ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            with self.assertRaises(urllib.error.HTTPError):
                list(MODULE.fetch_station_status(DISCOVERY_URL, system="citibike"))
        self.assertEqual(urlopen.call_count, 1)
        sleep.assert_not_called()


if __name__ == "__main__":
    unittest.main()
