import contextlib
import importlib.util
import io
import json
import pathlib
from datetime import datetime, timezone
from unittest import mock
import urllib.error
import urllib.parse

import pytest

SCRIPT = pathlib.Path(__file__).parents[1] / "scripts" / "fetch_polymarket_events.py"
SOURCE_CONFIG = pathlib.Path(__file__).parents[1] / "sources" / "polymarket_events.yml"
SPEC = importlib.util.spec_from_file_location("fetch_polymarket_events", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)

NOW = datetime(2026, 10, 6, 6, 41, 0, tzinfo=timezone.utc)


class Response(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


def response(document):
    return Response(json.dumps(document).encode("utf-8"))


def page(events, next_cursor=None):
    return {"events": events, "next_cursor": next_cursor}


def event(event_id, *, volume24hr=None, closed=False, end_date="2026-10-05T18:00:00Z",
          markets=None):
    if markets is None:
        markets = [market(f"{event_id}-1")]
    return {
        "id": str(event_id),
        "ticker": f"ev-{event_id}",
        "slug": f"ev-{event_id}",
        "title": f"Event {event_id}?",
        "startDate": "2026-09-01T00:00:00Z",
        "endDate": end_date,
        "creationDate": "2026-09-01T00:00:00Z",
        "updatedAt": "2026-10-06T00:00:00Z",
        "closed": closed,
        "negRisk": False,
        "volume24hr": volume24hr,
        "volume1wk": 12345.6,
        "volume1mo": 65432.1,
        "liquidity": "100.5",
        "description": "a long marketing description that must not be emitted",
        "image": "https://example.com/image.png",
        "markets": markets,
    }


def market(market_id, *, question="Will it happen?"):
    return {
        "id": str(market_id),
        "question": question,
        "slug": f"mkt-{market_id}",
        "conditionId": f"0x{market_id}",
        "outcomes": json.dumps(["Yes", "No"]),
        "outcomePrices": json.dumps(["0.66", "0.34"]),
        "bestBid": 0.65,
        "bestAsk": 0.67,
        "spread": 0.02,
        "lastTradePrice": 0.66,
        "liquidity": "92.1632",
        "volume": "380.167892",
        "oneDayPriceChange": -0.01,
        "oneWeekPriceChange": 0.05,
        "closed": False,
        "closedTime": None,
        "umaResolutionStatus": "proposed",
        "startDate": "2026-09-01T00:00:00Z",
        "endDate": "2026-10-05T18:00:00Z",
        "clobTokenIds": json.dumps(["123", "456"]),
        "description": "long market description",
    }


def run_fetch(documents, **kwargs):
    """Drive fetch_events against canned pages; returns (records, urlopen mock)."""
    patcher = mock.patch.object(MODULE.urllib.request, "urlopen")
    urlopen = patcher.start()
    urlopen.side_effect = [response(d) for d in documents]
    try:
        records = list(MODULE.fetch_events(now=NOW, **kwargs))
    finally:
        patcher.stop()
    return records, urlopen


def test_normalizes_event_and_packs_outcome_lists():
    records, _ = run_fetch([page([event(1, volume24hr=50000.0)]), page([])])
    assert len(records) == 1
    record = records[0]
    assert record["source"] == "polymarket_events"
    assert record["id"] == "1"
    assert record["slice"] == "active"
    assert record["fetched_at"] == NOW.isoformat()
    assert record["volume24hr"] == 50000.0
    assert record["liquidity"] == 100.5
    assert "description" not in record and "image" not in record
    market = record["markets"][0]
    assert market["id"] == "1-1"
    assert market["outcomes"] == ["Yes", "No"]
    assert market["outcomePrices"] == [0.66, 0.34]
    assert market["liquidity"] == 92.1632
    assert market["volume"] == 380.167892
    assert "clobTokenIds" not in market and "description" not in market


def test_active_slice_stops_at_volume_floor_without_extra_requests():
    documents = [
        page([
            event(1, volume24hr=50000.0),
            event(2, volume24hr=20000.0),
            event(3, volume24hr=5000.0),
            event(4, volume24hr=1000.0),
        ]),
        page([]),  # closed slice
    ]
    records, urlopen = run_fetch(documents)
    assert [r["id"] for r in records if r["slice"] == "active"] == ["1", "2"]
    # active slice stopped at the floor, then one empty closed page.
    assert urlopen.call_count == 2


def test_closed_slice_pages_to_end_and_keeps_shared_timestamp():
    documents = [
        page([]),  # active slice
        page([event(10, closed=True, end_date="2026-10-05T00:00:00Z")], next_cursor="cur-1"),
        page([event(11, closed=True, end_date="2026-10-05T12:00:00Z")], next_cursor="cur-2"),
        page([event(12, closed=True, end_date="2026-10-06T00:00:00Z")]),
    ]
    records, urlopen = run_fetch(documents)
    closed = [r for r in records if r["slice"] == "closed"]
    assert [r["id"] for r in closed] == ["10", "11", "12"]
    assert len({r["fetched_at"] for r in records}) == 1
    assert urlopen.call_count == 4


def test_requests_are_keyset_urls_with_volume_floor_and_date_window():
    captured = []

    def capturing(request, timeout=None):
        captured.append(request.full_url)
        # First request: active slice; second: closed slice; then stop.
        return response(page([event(1, volume24hr=50000.0)]))

    patcher = mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=capturing)
    patcher.start()
    try:
        list(MODULE.fetch_events(now=NOW))
    finally:
        patcher.stop()
    assert len(captured) >= 2
    active = urllib.parse.urlparse(captured[0])
    closed = urllib.parse.urlparse(captured[1])
    assert active.path == "/events/keyset"
    active_q = urllib.parse.parse_qs(active.query)
    assert active_q["closed"] == ["false"]
    assert active_q["order"] == ["volume24hr"]
    assert active_q["limit"] == ["100"]
    closed_q = urllib.parse.parse_qs(closed.query)
    assert closed_q["closed"] == ["true"]
    assert closed_q["volume_min"] == ["10000.0"]
    assert closed_q["end_date_min"] == ["2026-10-05T06:41:00Z"]
    assert closed_q["end_date_max"] == ["2026-10-07T06:41:00Z"]


def test_empty_slices_are_a_successful_empty_snapshot():
    records, urlopen = run_fetch([page([]), page([])])
    assert records == []
    assert urlopen.call_count == 2


def test_follows_keyset_cursor_chain():
    documents = [
        page([event(1, volume24hr=50000.0)], next_cursor="abc"),
        page([event(2, volume24hr=30000.0)], next_cursor="def"),
        page([]),  # end of active slice
        page([]),  # closed slice
    ]
    records, urlopen = run_fetch(documents)
    assert [r["id"] for r in records if r["slice"] == "active"] == ["1", "2"]
    assert urlopen.call_count == 4


def test_rejects_pagination_cycles():
    documents = [
        page([event(1, volume24hr=50000.0)], next_cursor="a"),
        page([event(2, volume24hr=40000.0)], next_cursor="a"),
    ]
    with pytest.raises(RuntimeError, match="repeated a cursor"):
        run_fetch(documents)


def test_stops_at_page_bound_instead_of_silently_truncating():
    documents = [page([event(1, volume24hr=50000.0)], next_cursor="more")]
    with pytest.raises(RuntimeError, match="max_pages"):
        run_fetch(documents, max_pages=1)


@pytest.mark.parametrize("bad_event, exc_type", [
    ({"markets": []}, ValueError),
    ({"id": "1"}, TypeError),
    ({"id": "1", "markets": "nope"}, TypeError),
])
def test_rejects_malformed_events(bad_event, exc_type):
    with pytest.raises(exc_type):
        run_fetch([page([bad_event]), page([])])


def test_rejects_missing_events_list():
    with pytest.raises(TypeError, match="missing events list"):
        run_fetch([{"next_cursor": None}, page([])])


def test_retries_transient_http_errors():
    http_429 = urllib.error.HTTPError(
        f"{MODULE.BASE}/events/keyset", 429, "slow down", {}, None)
    patcher = mock.patch.object(
        MODULE.urllib.request,
        "urlopen",
        side_effect=[http_429,
                     response(page([event(1, volume24hr=50000.0)])),
                     response(page([]))],
    )
    urlopen = patcher.start()
    try:
        records = list(MODULE.fetch_events(now=NOW, retry_delay=0))
    finally:
        patcher.stop()
    assert [r["id"] for r in records if r["slice"] == "active"] == ["1"]
    assert urlopen.call_count == 3


def test_propagates_non_retriable_http_errors_without_retry():
    http_404 = urllib.error.HTTPError(
        f"{MODULE.BASE}/events/keyset", 404, "gone", {}, None)
    patcher = mock.patch.object(
        MODULE.urllib.request, "urlopen", side_effect=http_404)
    urlopen = patcher.start()
    try:
        with pytest.raises(urllib.error.HTTPError) as caught:
            list(MODULE.fetch_events(now=NOW, retry_delay=0))
    finally:
        patcher.stop()
    assert caught.value.code == 404
    assert urlopen.call_count == 1


@pytest.mark.parametrize("kwargs", [
    {"max_pages": 0},
    {"window_hours": 0},
    {"min_volume": -1},
])
def test_rejects_invalid_bounds_without_a_request(kwargs):
    patcher = mock.patch.object(MODULE.urllib.request, "urlopen")
    urlopen = patcher.start()
    try:
        with pytest.raises(ValueError):
            list(MODULE.fetch_events(now=NOW, **kwargs))
        urlopen.assert_not_called()
    finally:
        patcher.stop()


def test_main_emits_ndjson_and_run_summary():
    def fake_urlopen(request, timeout=None):
        query = urllib.parse.urlparse(request.full_url).query
        if urllib.parse.parse_qs(query)["closed"] == ["false"]:
            return response(page([event(1, volume24hr=50000.0,
                                        markets=[market("1-1"), market("1-2")])]))
        return response(page([event(2, closed=True)]))

    stdout = io.StringIO()
    stderr = io.StringIO()
    with (
        mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=fake_urlopen),
        contextlib.redirect_stdout(stdout),
        contextlib.redirect_stderr(stderr),
    ):
        MODULE.main([])

    lines = stdout.getvalue().splitlines()
    assert len(lines) == 2
    assert json.loads(lines[0])["id"] == "1"
    assert json.loads(lines[0])["slice"] == "active"
    assert json.loads(lines[1])["slice"] == "closed"

    summary_lines = [
        line for line in stderr.getvalue().splitlines()
        if line.startswith(MODULE.SUMMARY_PREFIX)
    ]
    assert len(summary_lines) == 1
    summary = json.loads(summary_lines[0][len(MODULE.SUMMARY_PREFIX):])
    assert summary["events"] == 2
    assert summary["events_active"] == 1
    assert summary["events_closed"] == 1
    assert summary["markets"] == 3
    assert summary["requests"] == 2


def test_source_configuration_is_daily_and_keyless():
    text = SOURCE_CONFIG.read_text(encoding="utf-8")
    assert 'name: polymarket_events' in text
    assert "script: fetch_polymarket_events.py" in text
    assert 'schedule: "41 6 * * *"' in text
    assert "enabled: true" in text
    assert "args: []" in text
