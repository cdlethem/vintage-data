"""Tests for fetch_infodengue.py: the pinned IBGE intermediate CA must be loaded
into the geocode request's SSL context, and nowhere else."""

import importlib.util
import io
import json
import pathlib
import ssl
import sys
from unittest import mock

import pytest


SCRIPT = pathlib.Path(__file__).parents[1] / "scripts" / "fetch_infodengue.py"
SPEC = importlib.util.spec_from_file_location("fetch_infodengue", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules["fetch_infodengue"] = MODULE
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)

INTERMEDIATE_CN = "Sectigo Public Server Authentication CA DV R36"


class Response(io.BytesIO):
    headers: dict = {}

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


def response(payload):
    if isinstance(payload, str):
        return Response(payload.encode("utf-8"))
    return Response(json.dumps(payload).encode("utf-8"))


def test_pinned_intermediate_is_loaded_into_the_geocode_context():
    with mock.patch.object(
        MODULE.urllib.request, "urlopen",
        return_value=response([{"id": "3550308"}, {"id": "3304557"}, {"id": "3550308"}]),
    ) as urlopen:
        geocodes = MODULE.fetch_municipality_geocodes()
    assert geocodes == ["3304557", "3550308"]
    (request,), kwargs = urlopen.call_args
    assert request.full_url == MODULE.IBGE_MUNICIPIOS_URL
    context = kwargs["context"]
    assert isinstance(context, ssl.SSLContext)
    # Verification must stay on: the pin is additive to the system store.
    assert context.verify_mode == ssl.CERT_REQUIRED
    assert INTERMEDIATE_CN in json.dumps(context.get_ca_certs())


def test_pinned_intermediate_file_is_parseable():
    # load_verify_locations raises on a missing or corrupt PEM, so this guards
    # the committed file as well as the load path.
    context = MODULE._ibge_context()
    assert INTERMEDIATE_CN in json.dumps(context.get_ca_certs())


def test_non_list_ibge_response_is_rejected():
    with mock.patch.object(
        MODULE.urllib.request, "urlopen", return_value=response({"id": "1"}),
    ):
        with pytest.raises(ValueError, match="not a JSON list"):
            MODULE.fetch_municipality_geocodes()


def test_alert_requests_do_not_use_the_ibge_pin():
    csv_text = "SE,municipio_nome,casos\n202640,Rio de Janeiro,5\n"
    with mock.patch.object(
        MODULE.urllib.request, "urlopen", return_value=response(csv_text),
    ) as urlopen:
        rows, _attempts = MODULE._read_alert_rows("3304557", "dengue", 2026, 2026, 30, None)
    assert rows[0]["SE"] == "202640"
    assert "context" not in urlopen.call_args.kwargs
