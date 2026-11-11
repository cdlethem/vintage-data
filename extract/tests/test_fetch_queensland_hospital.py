import importlib.util
import io
import json
import pathlib
import urllib.error
from unittest import mock


SCRIPT = pathlib.Path(__file__).parents[1] / "scripts" / "fetch_queensland_hospital.py"
SPEC = importlib.util.spec_from_file_location("fetch_queensland_hospital", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


class Response(io.BytesIO):
    def __init__(self, body, *, status=200, headers=None):
        super().__init__(body)
        self.status = status
        self.headers = {} if headers is None else headers

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
SITEMAP = (
    "<urlset>"
    "<url><loc>https://openhospitals.health.qld.gov.au/facility/a</loc></url>"
    "<url><loc>https://openhospitals.health.qld.gov.au/facility/b</loc></url>"
    "</urlset>"
).encode("utf-8")

FACILITY = {
    "data_update_time": "2026-11-11T16:51:00Z",
    "slug": "working",
    "name": "Working Hospital",
    "address": "1 Test Street",
    "open": "24/7",
    "wait_time_all_patients": 15,
    "currently_closed": False,
    "live_data_availability": "Live",
    "api_code": "OK",
    "wait_time_non_critical": 10,
    "patients_waiting": 3,
    "treatment_spaces": 12,
    "treatment_spaces_updated_date": "2026-11-11",
}


def http_error(code):
    return urllib.error.HTTPError(MODULE.SITEMAP, code, "error", {}, io.BytesIO(b"origin failure"))


def sitemap_response():
    return Response(SITEMAP)


def facility_page(slug):
    document = dict(FACILITY, slug=slug)
    payload = json.dumps(json.dumps(document, ensure_ascii=False))
    return Response(f'<html><script>self.__next_f.push([1,{payload}])</script></html>'.encode("utf-8"))


def test_sitemap_recovers_from_transient_500s():
    with mock.patch.object(
        MODULE.urllib.request, "urlopen", side_effect=[http_error(500), http_error(500), sitemap_response()]
    ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
        assert MODULE.discover_slugs() == ["a", "b"]
        assert urlopen.call_count == 3
        assert sleep.call_args_list == [mock.call(5.0), mock.call(10.0)]


def test_sitemap_persistent_500_fails_after_bounded_attempts():
    with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=http_error(500)) as urlopen, \
            mock.patch.object(MODULE.time, "sleep") as sleep:
        try:
            MODULE.discover_slugs()
        except urllib.error.HTTPError as exc:
            assert exc.code == 500
        else:
            raise AssertionError("persistent 500 must fail the run")
        assert urlopen.call_count == MODULE.MAX_ATTEMPTS
        assert sleep.call_count == MODULE.MAX_ATTEMPTS - 1


def test_sitemap_non_retryable_error_fails_immediately():
    with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=http_error(404)) as urlopen, \
            mock.patch.object(MODULE.time, "sleep") as sleep:
        try:
            MODULE.discover_slugs()
        except urllib.error.HTTPError as exc:
            assert exc.code == 404
        else:
            raise AssertionError("404 must fail the run")
        assert urlopen.call_count == 1
        sleep.assert_not_called()


def test_sitemap_recovers_from_connection_error():
    with mock.patch.object(
        MODULE.urllib.request, "urlopen", side_effect=[urllib.error.URLError("connection reset"), sitemap_response()]
    ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
        assert MODULE.discover_slugs() == ["a", "b"]
        assert urlopen.call_count == 2
        assert sleep.call_args_list == [mock.call(5.0)]


def test_facility_page_failure_is_not_retried():
    # Per-facility failures stay tolerated and un-retried: a broken page must
    # not consume the run's time budget and cannot drop the other facilities.
    with mock.patch.object(
        MODULE.urllib.request, "urlopen", side_effect=[http_error(500), facility_page("working")]
    ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
        records = list(MODULE.fetch_pressure(["broken", "working"]))
    assert [r["facility_slug"] for r in records] == ["working"]
    assert urlopen.call_count == 2
    sleep.assert_not_called()
