import contextlib
from datetime import datetime, timezone
import importlib.util
import io
import json
from pathlib import Path
import urllib.error
from unittest import mock

import pytest


SCRIPT = Path(__file__).parent / "scripts" / "fetch_ilostat_sdmx.py"
SPEC = importlib.util.spec_from_file_location("fetch_ilostat_sdmx", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)

HEADER = (
    "STRUCTURE,STRUCTURE_ID,ACTION,FREQ,FREQ_LABEL,REF_AREA,REF_AREA_LABEL,"
    "SOURCE,SOURCE_LABEL,INDICATOR,INDICATOR_LABEL,SEX,SEX_LABEL,CLASSIF1,"
    "CLASSIF1_LABEL,TIME_PERIOD,OBS_VALUE,UNIT_MEASURE,UNIT_MEASURE_LABEL,"
    "UNIT_MULT,OBS_STATUS,OBS_STATUS_LABEL\n"
)
ROW = (
    "dataflow,ILO:DF_EMP_TEMP_SEX_AGE_NB(1.0),I,A,Annual,GBR,United Kingdom,"
    "BA,Labour force survey,EMP_TEMP_SEX_AGE_NB,Employment by sex and age,"
    "SEX_F,Female,AGE_YTHADULT_YGE15,Age 15+,2025,12345,PS,Persons,3,E,Estimated\n"
)


class Response(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def response(body):
    return Response(body.encode("utf-8"))


def fetch(body=HEADER + ROW, **kwargs):
    with mock.patch.object(
        MODULE.urllib.request, "urlopen", return_value=response(body)
    ) as urlopen:
        records = list(MODULE.fetch_observations(**kwargs))
    return records, urlopen


def test_normalizes_labelled_csv_and_preserves_complete_payload():
    records, _ = fetch(start_period="2025", end_period="2026")

    assert len(records) == 1
    record = records[0]
    assert record["source"] == "ilostat_sdmx"
    assert record["dataflow"] == "ILO,DF_EMP_TEMP_SEX_AGE_NB,1.0"
    assert record["period"] == "2025"
    assert record["status"] == "E"
    assert record["measures"] == {"OBS_VALUE": "12345"}
    assert record["units"] == {
        "UNIT_MEASURE": "PS",
        "UNIT_MEASURE_LABEL": "Persons",
        "UNIT_MULT": "3",
    }
    assert record["dimensions"] == {
        "FREQ": "A",
        "REF_AREA": "GBR",
        "SOURCE": "BA",
        "INDICATOR": "EMP_TEMP_SEX_AGE_NB",
        "SEX": "SEX_F",
        "CLASSIF1": "AGE_YTHADULT_YGE15",
        "TIME_PERIOD": "2025",
    }
    assert record["REF_AREA_LABEL"] == "United Kingdom"
    assert record["OBS_STATUS_LABEL"] == "Estimated"
    assert record["STRUCTURE_ID"] == "ILO:DF_EMP_TEMP_SEX_AGE_NB(1.0)"


def test_stable_id_uses_all_dimensions_but_not_labels_or_measure():
    records, _ = fetch()
    first = records[0]
    labelled_differently = ROW.replace("United Kingdom", "Royaume-Uni").replace(
        ",12345,", ",99999,"
    )
    changed_dimension = ROW.replace(",SEX_F,Female,", ",SEX_M,Male,")

    second, _ = fetch(HEADER + labelled_differently)
    third, _ = fetch(HEADER + changed_dimension)

    assert first["id"] == second[0]["id"]
    assert first["id"] != third[0]["id"]
    assert len(first["id"]) == 64


def test_valid_header_with_no_observations_is_empty_data():
    records, _ = fetch(HEADER)
    assert records == []


def test_request_is_bounded_labelled_csv_with_timeout_and_user_agent():
    _, urlopen = fetch(start_period="2024", end_period="2025", timeout=17)

    request = urlopen.call_args.args[0]
    assert request.full_url == (
        "https://rplumber.ilo.org/data/data/ILO,DF_EMP_TEMP_SEX_AGE_NB,1.0/.?"
        "startPeriod=2024&endPeriod=2025&labels=both"
    )
    assert request.get_header("Accept") == MODULE.ACCEPT
    assert request.get_header("User-agent") == MODULE.USER_AGENT
    assert urlopen.call_args.kwargs == {"timeout": 17}


def test_default_window_is_two_calendar_years():
    assert MODULE.default_period_window(
        datetime(2026, 9, 17, tzinfo=timezone.utc)
    ) == ("2025", "2026")


@pytest.mark.parametrize(
    ("body", "message"),
    [
        ("", "empty or has no CSV header"),
        ("not,csv\nvalue,row\n", "missing required columns"),
        (HEADER + ROW.rstrip("\n") + ",extra\n", "wrong column count"),
        (HEADER + ROW.replace(",GBR,", ",,"), "empty dimensions"),
    ],
)
def test_rejects_malformed_responses(body, message):
    with pytest.raises(ValueError, match=message):
        fetch(body)


def test_http_failures_propagate_without_emitting_records():
    error = urllib.error.HTTPError(MODULE.API_URL, 503, "unavailable", {}, None)
    with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=error):
        with pytest.raises(urllib.error.HTTPError) as caught:
            list(MODULE.fetch_observations("2025", "2026"))
    assert caught.value.code == 503


@pytest.mark.parametrize(
    ("start_period", "end_period", "timeout", "message"),
    [
        ("2026", "2025", 60, "must not be after"),
        ("", "2025", 60, "required"),
        ("2025", "2026", 0, "positive"),
    ],
)
def test_invalid_bounds_fail_before_http(start_period, end_period, timeout, message):
    with mock.patch.object(MODULE.urllib.request, "urlopen") as urlopen:
        with pytest.raises(ValueError, match=message):
            list(MODULE.fetch_observations(start_period, end_period, timeout))
    urlopen.assert_not_called()


def test_main_emits_one_ndjson_object_per_observation():
    stdout = io.StringIO()
    with mock.patch.object(
        MODULE.urllib.request, "urlopen", return_value=response(HEADER + ROW + ROW)
    ):
        with contextlib.redirect_stdout(stdout):
            MODULE.main(["--start-period", "2025", "--end-period", "2026"])

    lines = stdout.getvalue().splitlines()
    assert len(lines) == 2
    assert all(json.loads(line)["period"] == "2025" for line in lines)
