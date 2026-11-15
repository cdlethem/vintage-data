import contextlib
import io
import importlib.util
import json
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

SCRIPT = Path(__file__).parent / "scripts" / "fetch_biorxiv_preprints.py"
SOURCE_CONFIG = Path(__file__).parent / "sources" / "biorxiv_preprints.yml"
SPEC = importlib.util.spec_from_file_location("fetch_biorxiv_preprints", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


def post(doi, date="2026-11-12", version="1", title="Example preprint", **extra):
    item = {
        "title": title,
        "authors": "Doe, J.; Roe, R.",
        "author_corresponding": "Jane Doe",
        "author_corresponding_institution": "Example Institute",
        "doi": doi,
        "date": date,
        "version": version,
        "type": "new results",
        "license": "cc_by",
        "category": "neuroscience",
        "jatsxml": (f"https://www.biorxiv.org/content/early/2026/11/12/{doi.lstrip('10.')}.source.xml"
                    if isinstance(doi, str) else ""),
        "abstract": "An abstract.",
        "published": "NA",
        "server": "biorxiv",
    }
    item.update(extra)
    return item


def page(items, total, status="ok", cursor="0"):
    message = {
        "status": status,
        "category": "all",
        "interval": "2026-11-12:2026-11-12",
        "cursor": cursor,
        "count": len(items),
        "count_new_papers": str(len(items)),
        "total": str(total),
    }
    return {"messages": [message], "collection": list(items)}


class JsonResponse(io.BytesIO):
    def __init__(self, document):
        super().__init__(json.dumps(document).encode("utf-8"))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def urlopen_sequence(*documents):
    responses = [JsonResponse(d) for d in documents]
    return mock.MagicMock(side_effect=responses)


def fetch(server="biorxiv", pages=(), **kwargs):
    requests = []
    kwargs.setdefault("timeout", 30)
    kwargs.setdefault("retries", 3)
    kwargs.setdefault("retry_budget", 60)
    kwargs.setdefault("fetched_at", "2026-11-14T06:31:00+00:00")
    kwargs.setdefault("sleep", lambda seconds: None)
    with mock.patch.object(MODULE.urllib.request, "urlopen",
                           side_effect=[JsonResponse(p) for p in pages]) as urlopen:
        records = list(MODULE.fetch_server(
            server, "2026-11-12", "2026-11-12", "", frozenset(),
            requests=requests, **kwargs))
    return records, requests, urlopen


class FetchServerTests(unittest.TestCase):
    def test_normalizes_record_with_stable_id_and_utc_fetched_at(self):
        records, _, _ = fetch(pages=[page([post("10.64898/2026.11.12.1")], 1)])

        self.assertEqual(len(records), 1)
        record = records[0][0]
        self.assertEqual(record["source"], "biorxiv_preprints")
        self.assertEqual(record["id"], "biorxiv:10.64898/2026.11.12.1:v1")
        self.assertEqual(record["server"], "biorxiv")
        self.assertEqual(record["doi"], "10.64898/2026.11.12.1")
        self.assertEqual(record["version"], 1)
        self.assertEqual(record["date"], "2026-11-12")
        self.assertIsNone(record["published"])  # "NA" is a missing value
        self.assertEqual(record["abstract"], "An abstract.")
        self.assertRegex(record["fetched_at"], r"\+00:00$")

    def test_paginates_cursor_to_the_reported_total(self):
        first = [post(f"10.64898/2026.11.12.{n}") for n in range(30)]
        second = [post(f"10.64898/2026.11.12.{30 + n}") for n in range(30)]
        third = [post("10.64898/2026.11.12.60")]
        records, requests, urlopen = fetch(
            pages=[page(first, 61, cursor="0"),
                   page(second, 61, cursor="30"),
                   page(third, 61, cursor="60")])

        self.assertEqual(len(records), 61)
        self.assertEqual(len(requests), 3)
        urls = [c.args[0].full_url for c in urlopen.call_args_list]
        self.assertIn("/0/json", urls[0])
        self.assertIn("/30/json", urls[1])
        self.assertIn("/60/json", urls[2])

    def test_stops_on_no_posts_found_without_error(self):
        records, _, _ = fetch(pages=[page([], 0, status="no posts found")])

        self.assertEqual(records, [])

    def test_rejects_unrecognized_server_status(self):
        with self.assertRaisesRegex(MODULE.BiorxivError, "not recognized"):
            list(fetch(pages=[page([], 0, status="Server not recognized")]))[0]

    def test_rejects_missing_messages_block(self):
        with self.assertRaisesRegex(MODULE.BiorxivError, "messages"):
            list(fetch(pages=[{"collection": []}]))[0]

    def test_rejects_missing_collection_list(self):
        document = page([], 1)
        del document["collection"]
        with self.assertRaisesRegex(MODULE.BiorxivError, "collection"):
            list(fetch(pages=[document]))[0]

    def test_drops_posts_before_the_watermark_and_boundary_ids(self):
        early = post("10.64898/2026.11.11.1", date="2026-11-11")
        boundary = post("10.64898/2026.11.12.9", date="2026-11-12")
        late = post("10.64898/2026.11.12.10", date="2026-11-12")
        fresh = post("10.64898/2026.11.13.1", date="2026-11-13")
        items = [early, boundary, late, fresh]
        requests = []
        with mock.patch.object(
                MODULE.urllib.request, "urlopen",
                return_value=JsonResponse(page(items, len(items)))):
            records = list(MODULE.fetch_server(
                "biorxiv", "2026-11-12", "2026-11-13", "2026-11-12",
                frozenset({"biorxiv:10.64898/2026.11.12.9:v1"}),
                timeout=30, retries=3, retry_budget=60,
                fetched_at="2026-11-14T06:31:00+00:00", requests=requests,
                sleep=lambda seconds: None))

        ids = [r[0]["id"] for r in records]
        self.assertIn("biorxiv:10.64898/2026.11.12.10:v1", ids)  # late watermark-day post
        self.assertIn("biorxiv:10.64898/2026.11.13.1:v1", ids)
        self.assertNotIn("biorxiv:10.64898/2026.11.11.1:v1", ids)
        self.assertNotIn("biorxiv:10.64898/2026.11.12.9:v1", ids)  # boundary id
        self.assertEqual(len(ids), 2)

    def test_retries_429_within_the_sleep_budget(self):
        import urllib.error

        url = "https://api.biorxiv.org/details/biorxiv/2026-11-12/2026-11-12/0/json"
        error = urllib.error.HTTPError(url, 429, "Too Many Requests", {}, None)
        sleeps = []
        with mock.patch.object(
                MODULE.urllib.request, "urlopen",
                side_effect=[error, JsonResponse(page([post("10.64898/2026.11.12.1")], 1))]) as urlopen:
            records = list(MODULE.fetch_server(
                "biorxiv", "2026-11-12", "2026-11-12", "", frozenset(),
                timeout=30, retries=3, retry_budget=60,
                fetched_at="2026-11-14T06:31:00+00:00", requests=[],
                sleep=sleeps.append))

        self.assertEqual(len(records), 1)
        self.assertEqual(urlopen.call_count, 2)
        self.assertEqual(sleeps, [MODULE.MIN_RETRY_DELAY])

    def test_non_retriable_404_fails_immediately(self):
        import urllib.error

        url = "https://api.biorxiv.org/details/biorxiv/2026-11-12/2026-11-12/0/json"
        error = urllib.error.HTTPError(url, 404, "Not Found", {}, None)
        with mock.patch.object(
                MODULE.urllib.request, "urlopen", side_effect=error):
            with self.assertRaises(MODULE.BiorxivError):
                list(MODULE.fetch_server(
                    "biorxiv", "2026-11-12", "2026-11-12", "", frozenset(),
                    timeout=30, retries=3, retry_budget=60,
                    fetched_at="2026-11-14T06:31:00+00:00", requests=[],
                    sleep=lambda seconds: None))

    def test_rejects_posts_without_a_stable_id(self):
        for doi in (None, "", "not-a-doi", "10.1234/"):
            with self.subTest(doi=doi):
                with self.assertRaisesRegex(ValueError, "stable source id"):
                    list(fetch(pages=[page([post(doi)], 1)]))[0]

    def test_rejects_invalid_dates_versions_and_titles(self):
        cases = (
            (post("10.64898/1", date="11/12/2026"), "date"),
            (post("10.64898/1", date="2026-13-40"), "date"),
            (post("10.64898/1", version="v1"), "version"),
            (post("10.64898/1", version="0"), "version"),
            (post("10.64898/1", title="  "), "title"),
        )
        for item, message in cases:
            with self.subTest(item=item.get("doi"), message=message):
                with self.assertRaisesRegex(ValueError, message):
                    list(fetch(pages=[page([item], 1)]))[0]


FROZEN_NOW = datetime(2026, 11, 14, 6, 31, 0, tzinfo=timezone.utc)


class FrozenDatetime(datetime):
    @classmethod
    def now(cls, tz=None):
        return FROZEN_NOW.astimezone(tz) if tz else FROZEN_NOW


def run_main(argv, state_file=None):
    stdout, stderr = io.StringIO(), io.StringIO()
    argv = list(argv)
    if state_file is not None:
        argv += ["--state-file", str(state_file)]
    with mock.patch.object(MODULE, "datetime", FrozenDatetime), \
         contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
        code = MODULE.main(argv)
    return code, stdout.getvalue(), stderr.getvalue()


def server_pages(day_items, total_per_day=30):
    """Build paginated documents for one day's items (30 per page)."""
    pages = []
    for i in range(0, len(day_items), 30):
        chunk = day_items[i:i + 30]
        pages.append(page(chunk, len(day_items), cursor=str(i)))
    return pages


class MainTests(unittest.TestCase):
    def test_first_run_writes_watermark_and_second_run_emits_nothing(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            state_file = Path(tmp) / "state.json"
            day_b = [post(f"10.64898/2026.11.13.{n}", date="2026-11-13")
                     for n in range(10)]
            med_b = [post(f"10.1101/2026.11.13.{n}", date="2026-11-13")
                     for n in range(4)]

            # Run 1: cold start (7-day window); the mock serves only the most
            # recent day's items, so only day_b / med_b records are emitted.
            with mock.patch.object(
                    MODULE.urllib.request, "urlopen",
                    side_effect=urlopen_sequence(
                        *server_pages(day_b), *server_pages(med_b))):
                code, out, err = run_main(["--server", "biorxiv,medrxiv"], state_file)

            self.assertEqual(code, 0)
            lines = [json.loads(line) for line in out.splitlines()]
            self.assertEqual(len(lines), 14)  # 10 bioRxiv + 4 medRxiv
            self.assertTrue(all(l["date"] == "2026-11-13" for l in lines))
            self.assertTrue(all(l["fetched_at"] == lines[0]["fetched_at"] for l in lines))

            state = json.loads(state_file.read_text(encoding="utf-8"))
            self.assertEqual(state["biorxiv"]["watermark"], "2026-11-13")
            self.assertEqual(len(state["biorxiv"]["boundary"]), 10)
            self.assertEqual(state["medrxiv"]["watermark"], "2026-11-13")
            self.assertEqual(len(state["medrxiv"]["boundary"]), 4)

            # Run 2: window re-queries the watermark day; every id is in the
            # boundary, so nothing is emitted and watermarks stay put.
            with mock.patch.object(
                    MODULE.urllib.request, "urlopen",
                    side_effect=urlopen_sequence(
                        *server_pages(day_b), *server_pages(med_b))) as urlopen:
                code, out, err = run_main(["--server", "biorxiv,medrxiv"], state_file)

            self.assertEqual(code, 0)
            self.assertEqual(out, "")
            self.assertEqual(urlopen.call_count, 2)  # one page per server
            state2 = json.loads(state_file.read_text(encoding="utf-8"))
            self.assertEqual(state2, state)

            summary = [
                line for line in err.splitlines()
                if line.startswith(MODULE.SUMMARY_PREFIX)
            ]
            self.assertEqual(len(summary), 1)
            payload = json.loads(summary[0][len(MODULE.SUMMARY_PREFIX):])
            self.assertEqual(payload["status"], "ok")
            self.assertEqual(payload["records"], 0)
            self.assertEqual(payload["requests"], 2)

    def test_late_watermark_day_post_is_caught_on_the_next_run(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            state_file = Path(tmp) / "state.json"
            state = {
                "biorxiv": {
                    "watermark": "2026-11-13",
                    "boundary": ["biorxiv:10.64898/2026.11.13.1:v1"],
                }
            }
            state_file.write_text(json.dumps(state), encoding="utf-8")
            late = post("10.64898/2026.11.13.77", date="2026-11-13")
            fresh = post("10.64898/2026.11.13.78", date="2026-11-13")
            # watermark day (11-13) has late+fresh; nothing on 11-13 for medRxiv
            with mock.patch.object(
                    MODULE.urllib.request, "urlopen",
                    side_effect=urlopen_sequence(
                        *server_pages([late, fresh]))):
                code, out, err = run_main(["--server", "biorxiv"], state_file)

            self.assertEqual(code, 0)
            lines = [json.loads(line) for line in out.splitlines()]
            self.assertEqual(
                [l["id"] for l in lines],
                ["biorxiv:10.64898/2026.11.13.77:v1",
                 "biorxiv:10.64898/2026.11.13.78:v1"])
            state2 = json.loads(state_file.read_text(encoding="utf-8"))
            self.assertEqual(state2["biorxiv"]["watermark"], "2026-11-13")
            self.assertEqual(
                len(state2["biorxiv"]["boundary"]), 3)  # old id + two new

    def test_no_state_pull_never_writes_the_state_file(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            state_file = Path(tmp) / "state.json"
            with mock.patch.object(
                    MODULE.urllib.request, "urlopen",
                    side_effect=urlopen_sequence(page([], 0, status="no posts found"))):
                code, out, err = run_main(
                    ["--no-state", "--server", "biorxiv"], state_file)

            self.assertEqual(code, 0)
            self.assertFalse(state_file.exists())

    def test_transport_failure_fails_the_run_without_writing_state(self):
        import tempfile
        import urllib.error

        with tempfile.TemporaryDirectory() as tmp:
            state_file = Path(tmp) / "state.json"
            with mock.patch.object(
                    MODULE.urllib.request, "urlopen",
                    side_effect=urllib.error.URLError("connection reset")):
                code, out, err = run_main(["--server", "biorxiv"], state_file)

            self.assertEqual(code, 1)
            self.assertEqual(out, "")
            self.assertIn("extraction failed", err)
            self.assertFalse(state_file.exists())

    def test_rejects_unknown_and_duplicate_servers(self):
        for server in ("all", "biorxiv,biorxiv", "biorxiv,genomics"):
            with self.subTest(server=server):
                with self.assertRaises(SystemExit):
                    with mock.patch.object(MODULE.urllib.request, "urlopen"):
                        run_main(["--server", server])

    def test_source_configuration_is_daily_and_vetted(self):
        text = SOURCE_CONFIG.read_text(encoding="utf-8")

        self.assertRegex(text, r'(?m)^schedule: "31 6 \* \* \*"')
        self.assertRegex(text, r"(?m)^enabled: true$")
        self.assertIn('args: ["7"]', text)
        self.assertIn("retries: 1", text)
        self.assertIn("timeout_minutes: 15", text)
        self.assertIn("cadence_note", text)
        self.assertIn("rate_limit", text)
        self.assertIn("licence", text)
        self.assertIn("attribution_required: true", text)
        self.assertIn("keyless official API", text)
        self.assertIn("Center for Open Science", text)
        self.assertIn("per-server date watermark", text)


if __name__ == "__main__":
    unittest.main()
