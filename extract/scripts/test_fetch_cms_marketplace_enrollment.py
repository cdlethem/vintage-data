import importlib.util
import io
import json
import pathlib
import socket
import urllib.error
from unittest import mock

import pytest
import yaml

SCRIPT = pathlib.Path(__file__).with_name("fetch_cms_marketplace_enrollment.py")
SPEC = importlib.util.spec_from_file_location("fetch_cms_marketplace_enrollment", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)
CONFIG = SCRIPT.parents[1] / "sources" / "cms_marketplace_enrollment.yml"


class Response(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


def response(document):
    return Response(json.dumps(document).encode("utf-8"))


def row(state="CA", platform="All", year="2026", month="February", enrollment="34514"):
    return {
        "State": state,
        "Platform": platform,
        "Year": year,
        "Month": month,
        "Effectuated_Enr": enrollment,
    }


def fetch(documents, **overrides):
    """Drive fetch_rows over a fixed sequence of pages; return (records, request urls)."""
    calls = []

    def fake_urlopen(request, timeout=None):
        document = documents[min(len(calls), len(documents) - 1)]
        calls.append(request.full_url)
        return response(document)

    arguments = dict(timeout=17, retries=2, retry_delay=0.25)
    arguments.update(overrides)
    with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=fake_urlopen) as urlopen:
        records = list(MODULE.fetch_rows(**arguments))
    return records, calls, urlopen


def test_normalizes_rows_and_envelope():
    records, urls, _ = fetch([[row()]])
    assert len(records) == 1
    record = records[0]
    assert record["source"] == MODULE.SOURCE
    assert record["id"] == "CA|All|2026|February"
    assert record["state"] == "CA"
    assert record["platform"] == "All"
    assert record["year"] == 2026
    assert record["month"] == "February"
    assert record["effectuated_enrollment"] == 34514
    assert isinstance(record["fetched_at"], str) and record["fetched_at"]
    assert urls == [f"{MODULE.API_URL}?size={MODULE.PAGE_SIZE}&offset=0"]


def test_stable_ids_ignore_fetch_time():
    first, _, _ = fetch([[row()]])
    second, _, _ = fetch([[row()]])
    assert [r["id"] for r in first] == [r["id"] for r in second]


def test_paginates_to_exhaustion():
    full_page = [row(state=f"S{i:02d}") for i in range(MODULE.PAGE_SIZE)]
    records, urls, _ = fetch([full_page, [row(state="CA"), row(state="NY")]])
    assert len(records) == MODULE.PAGE_SIZE + 2
    assert len(urls) == 2
    assert "offset=0" in urls[0]
    assert f"offset={MODULE.PAGE_SIZE}" in urls[1]

@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"enrollment": "N/A"}, "non-numeric Effectuated_Enr"),
        ({"month": "Spring"}, "unknown Month"),
        ({"year": "202"}, "out-of-range Year"),
    ],
)
def test_rejects_malformed_rows(overrides, message):
    with pytest.raises(ValueError, match=message):
        fetch([[row(**overrides)]])


def test_rejects_non_list_document():
    with pytest.raises(ValueError, match="not a list"):
        fetch([{"data": []}])


def test_retries_transient_http_failure_with_bounded_backoff():
    error = urllib.error.HTTPError(MODULE.API_URL, 503, "unavailable", {}, None)
    documents = [error, error, [row()]]
    calls = []

    def fake_urlopen(request, timeout=None):
        item = documents[min(len(calls), len(documents) - 1)]
        calls.append(1)
        if isinstance(item, Exception):
            raise item
        return response(item)

    with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=fake_urlopen), \
            mock.patch.object(MODULE.time, "sleep") as sleep:
        records = list(MODULE.fetch_rows(17, retries=2, retry_delay=0.25))
    assert len(records) == 1
    assert len(calls) == 3
    assert [c.args[0] for c in sleep.call_args_list] == [0.25, 0.5]


def test_does_not_retry_permanent_http_failure():
    error = urllib.error.HTTPError(MODULE.API_URL, 404, "not found", {}, None)
    with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=error) as urlopen, \
            mock.patch.object(MODULE.time, "sleep") as sleep:
        with pytest.raises(urllib.error.HTTPError):
            list(MODULE.fetch_rows(17))
    urlopen.assert_called_once()
    sleep.assert_not_called()


def test_retries_network_timeout_then_succeeds():
    with mock.patch.object(MODULE.urllib.request, "urlopen",
                           side_effect=[socket.timeout("read timed out"), response([row()])]), \
            mock.patch.object(MODULE.time, "sleep"):
        records = list(MODULE.fetch_rows(17))
    assert len(records) == 1


def test_pagination_cap_raises():
    full_page = [row(state=f"S{i:02d}") for i in range(MODULE.PAGE_SIZE)]
    with pytest.raises(RuntimeError, match="pagination cap"):
        fetch([full_page], max_pages=2)


def run_main(tmp_path, pages, *argv, **env):
    calls = []

    def fake_urlopen(request, timeout=None):
        document = pages[min(len(calls), len(pages) - 1)]
        calls.append(1)
        return response(document)

    args = list(argv)
    with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=fake_urlopen), \
            mock.patch.dict("os.environ", env, clear=False):
        code = MODULE.main(args)
    return code


def test_main_emits_new_rows_and_commits_state(tmp_path, capsys):
    state_file = tmp_path / "state.json"
    release_v1 = [[row(state="CA"), row(state="NY")]]
    assert run_main(tmp_path, release_v1, "--state-file", str(state_file)) == 0
    out = capsys.readouterr().out.splitlines()
    assert len(out) == 2
    assert all(json.loads(line)["source"] == MODULE.SOURCE for line in out)
    seen = json.loads(state_file.read_text())["seen"]
    assert seen == ["CA|All|2026|February", "NY|All|2026|February"]

    # A repeat run against an unchanged release emits nothing and rewrites nothing.
    before = state_file.read_bytes()
    assert run_main(tmp_path, release_v1, "--state-file", str(state_file)) == 0
    capsys.readouterr()
    assert state_file.read_bytes() == before

    # A new release appends a month; only the new row is emitted on the next run.
    release_v2 = [[row(state="CA"), row(state="NY"), row(state="TX")]]
    assert run_main(tmp_path, release_v2, "--state-file", str(state_file)) == 0
    out = capsys.readouterr().out.splitlines()
    assert len(out) == 1
    assert json.loads(out[0])["state"] == "TX"
    assert len(json.loads(state_file.read_text())["seen"]) == 3


def test_main_no_state_emits_everything_without_state_file(tmp_path, capsys, monkeypatch):
    monkeypatch.setenv("EXTRACT_DATA_ROOT", str(tmp_path))
    documents = [[row(state="CA"), row(state="NY")]]
    assert run_main(tmp_path, documents, "--no-state") == 0
    out = capsys.readouterr().out.splitlines()
    assert len(out) == 2
    assert not (tmp_path / "state" / f"{MODULE.SOURCE}.json").exists()


def test_main_reports_failed_summary_on_bad_row(tmp_path, capsys):
    documents = [[row(enrollment="oops")]]
    assert run_main(tmp_path, documents, "--no-state") == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    summary_lines = [line for line in captured.err.splitlines() if line.startswith(MODULE.SUMMARY_PREFIX)]
    assert len(summary_lines) == 1
    summary = json.loads(summary_lines[0][len(MODULE.SUMMARY_PREFIX):])
    assert summary["health"] == "failed"
    assert summary["records"] == 0
    assert "non-numeric" in summary["error"]


def test_main_success_summary_counts_new_rows(tmp_path, capsys):
    documents = [[row(state="CA"), row(state="NY")]]
    assert run_main(tmp_path, documents, "--no-state") == 0
    captured = capsys.readouterr()
    summary_lines = [line for line in captured.err.splitlines() if line.startswith(MODULE.SUMMARY_PREFIX)]
    summary = json.loads(summary_lines[0][len(MODULE.SUMMARY_PREFIX):])
    assert summary["health"] == "healthy"
    assert summary["records"] == 2
    assert summary["coverage"] == {"rows_total": 2, "rows_new": 2}
    assert summary["requests"]["attempted"] == 1


def test_mutually_exclusive_state_flags(tmp_path):
    with pytest.raises(SystemExit):
        MODULE.main(["--no-state", "--state-file", str(tmp_path / "state.json")])


def test_source_configuration():
    config = yaml.safe_load(CONFIG.read_text(encoding="utf-8"))
    assert config["name"] == MODULE.SOURCE
    assert config["script"] == SCRIPT.name
    assert (SCRIPT.parent / config["script"]).is_file()
    assert len(config["schedule"].split()) == 5
    for key in ("rate_limit", "licence", "attribution_required"):
        assert key in config
