import importlib.util
import io
import json
import pathlib
import ssl
import urllib.error
from unittest import mock

import pytest


SCRIPT = pathlib.Path(__file__).parents[1] / "scripts" / "fetch_socrata_civic.py"
SPEC = importlib.util.spec_from_file_location("fetch_socrata_civic", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


class Response(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def response(rows):
    return Response(json.dumps(rows).encode("utf-8"))


def cert_mismatch_error():
    # Same shape as the production failure: SSLCertVerificationError from the
    # TLS handshake, wrapped by urlopen in a URLError.
    inner = ssl.SSLCertVerificationError(
        1,
        "[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: "
        "Hostname mismatch, certificate is not valid for 'data.seattle.gov'.",
    )
    return urllib.error.URLError(inner)


def test_transient_cert_mismatch_is_retried_and_succeeds():
    rows = [{"datetime": "2026-10-12 17:55:00"}]
    with mock.patch.object(MODULE, "time"), mock.patch.object(
        MODULE.urllib.request,
        "urlopen",
        side_effect=[cert_mismatch_error(), response(rows)],
    ) as urlopen:
        got = MODULE._get("data.seattle.gov", "kzjm-xkqj", **{"$limit": 1})
    assert got == rows
    assert urlopen.call_count == 2


def test_persistent_cert_mismatch_still_fails_after_bounded_attempts():
    with mock.patch.object(MODULE, "time"), mock.patch.object(
        MODULE.urllib.request,
        "urlopen",
        side_effect=cert_mismatch_error(),
    ) as urlopen:
        with pytest.raises(urllib.error.URLError):
            MODULE._get("data.seattle.gov", "kzjm-xkqj")
    assert urlopen.call_count == MODULE.MAX_ATTEMPTS


def test_http_503_is_retried_and_succeeds():
    rows = [{"datetime": "2026-10-12 17:55:00"}]
    with mock.patch.object(MODULE, "time"), mock.patch.object(
        MODULE.urllib.request,
        "urlopen",
        side_effect=[
            urllib.error.HTTPError(
                "https://data.seattle.gov/resource/kzjm-xkqj.json",
                503,
                "unavailable",
                {},
                None,
            ),
            response(rows),
        ],
    ) as urlopen:
        got = MODULE._get("data.seattle.gov", "kzjm-xkqj")
    assert got == rows
    assert urlopen.call_count == 2


def test_http_404_is_not_retried():
    with mock.patch.object(MODULE, "time"), mock.patch.object(
        MODULE.urllib.request,
        "urlopen",
        side_effect=urllib.error.HTTPError(
            "https://data.seattle.gov/resource/kzjm-xkqj.json",
            404,
            "not found",
            {},
            None,
        ),
    ) as urlopen:
        with pytest.raises(urllib.error.HTTPError):
            MODULE._get("data.seattle.gov", "kzjm-xkqj")
    assert urlopen.call_count == 1
