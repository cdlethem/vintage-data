import contextlib
import importlib.util
import io
from datetime import datetime, timezone
from pathlib import Path
import unittest
from unittest.mock import patch
import urllib.error


SCRIPT_PATH = Path(__file__).parent / "scripts" / "fetch_malaysia_pricecatcher.py"
SPEC = importlib.util.spec_from_file_location("fetch_malaysia_pricecatcher", SCRIPT_PATH)
fetch_pc = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(fetch_pc)

# The failed DAG run: 2026-10-01 02:55 UTC = 10:55 Asia/Kuala_Lumpur, so the
# defaulted month is 2026-10 while only pricecatcher_2026-09.csv is published.
OCTOBER_NOW = datetime(2026, 10, 1, 3, 0, tzinfo=timezone.utc)
BASE = "https://storage.data.gov.my/pricecatcher"


class CsvResponse(io.BytesIO):
    """In-memory stand-in for the urllib HTTP response object."""

    def __init__(self, text):
        super().__init__(text.encode("utf-8-sig"))
        self.headers = {"Last-Modified": "Wed, 30 Sep 2026 12:00:27 GMT"}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def csv_document(rows):
    lines = ["date,premise_code,item_code,price"]
    lines.extend(f"{date},{premise},{item},{price}" for date, premise, item, price in rows)
    return "\n".join(lines) + "\n"


def http_error(code, url=f"{BASE}/pricecatcher_2026-10.csv"):
    return urllib.error.HTTPError(url, code, "Not Found", None, None)


class FakeDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        return OCTOBER_NOW


class FetchMalaysiaPricecatcherTests(unittest.TestCase):
    def test_falls_back_to_previous_month_when_current_month_not_published(self):
        stderr = io.StringIO()
        with patch.object(fetch_pc, "datetime", FakeDateTime), \
                contextlib.redirect_stderr(stderr), \
                patch.object(fetch_pc.urllib.request, "urlopen",
                             side_effect=[http_error(404),
                                          CsvResponse(csv_document(
                                              [("2026-09-30", "22275", "2045", "9.99")]))]) \
                as urlopen:
            records = list(fetch_pc.fetch_prices(limit=100))

        self.assertEqual(
            [call.args[0].full_url for call in urlopen.call_args_list],
            [f"{BASE}/pricecatcher_2026-10.csv", f"{BASE}/pricecatcher_2026-09.csv"],
        )
        self.assertEqual(records, [{
            "date": "2026-09-30",
            "premise_code": "22275",
            "item_code": "2045",
            "price": "9.99",
            "source": "malaysia_pricecatcher",
            "fetched_at": "2026-10-01T03:00:00+00:00",
            "id": "2026-09-30|22275|2045",
            "publisher_updated_at": "Wed, 30 Sep 2026 12:00:27 GMT",
        }])
        self.assertIn("pricecatcher_2026-10.csv not published yet", stderr.getvalue())
        self.assertIn("pricecatcher_2026-09.csv", stderr.getvalue())

    def test_no_fallback_when_current_month_is_published(self):
        stderr = io.StringIO()
        with patch.object(fetch_pc, "datetime", FakeDateTime), \
                contextlib.redirect_stderr(stderr), \
                patch.object(fetch_pc.urllib.request, "urlopen",
                             side_effect=[CsvResponse(csv_document(
                                 [("2026-10-01", "22275", "2045", "9.99")]))]) \
                as urlopen:
            records = list(fetch_pc.fetch_prices(limit=100))

        self.assertEqual(
            [call.args[0].full_url for call in urlopen.call_args_list],
            [f"{BASE}/pricecatcher_2026-10.csv"],
        )
        self.assertEqual(records[0]["date"], "2026-10-01")
        self.assertEqual(stderr.getvalue(), "")

    def test_explicit_month_404_still_fails(self):
        with patch.object(fetch_pc, "datetime", FakeDateTime), \
                patch.object(fetch_pc.urllib.request, "urlopen",
                             side_effect=[http_error(404)]) as urlopen:
            with self.assertRaises(urllib.error.HTTPError) as raised:
                list(fetch_pc.fetch_prices("2026-10", limit=100))

        self.assertEqual(raised.exception.code, 404)
        self.assertEqual(len(urlopen.call_args_list), 1)

    def test_non_404_error_on_default_month_still_fails(self):
        with patch.object(fetch_pc, "datetime", FakeDateTime), \
                patch.object(fetch_pc.urllib.request, "urlopen",
                             side_effect=[http_error(500)]) as urlopen:
            with self.assertRaises(urllib.error.HTTPError) as raised:
                list(fetch_pc.fetch_prices(limit=100))

        self.assertEqual(raised.exception.code, 500)
        self.assertEqual(len(urlopen.call_args_list), 1)

    def test_previous_month_wraps_the_year_boundary(self):
        self.assertEqual(fetch_pc._previous_month("2026-10"), "2026-09")
        self.assertEqual(fetch_pc._previous_month("2026-02"), "2026-01")
        self.assertEqual(fetch_pc._previous_month("2026-01"), "2025-12")


if __name__ == "__main__":
    unittest.main()
