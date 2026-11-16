import importlib.util
import io
import pathlib
import unittest
import urllib.error
from unittest import mock


SCRIPT = pathlib.Path(__file__).parent / "scripts" / "fetch_sec_edgar.py"
SPEC = importlib.util.spec_from_file_location("fetch_sec_edgar", SCRIPT)
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


ATOM = """<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>Latest Filings - Mon, 16 Nov 2026 11:00:00 EDT</title>
  <updated>2026-11-16T15:00:00-04:00</updated>
  <id>https://www.sec.gov/cgi-bin/browse-edgar</id>
  <entry>
    <id>https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany</id>
    <title>144 - Apple Inc (0000320193) (Filer)</title>
    <summary type="html">&lt;b&gt;Filed:&lt;/b&gt; 2026-11-16 &lt;b&gt;AccNo:&lt;/b&gt; 0000320193-26-000001 &lt;b&gt;Size:&lt;/b&gt; 12K</summary>
    <updated>2026-11-16T10:59:59-04:00</updated>
    <link rel="alternate" href="https://www.sec.gov/Archives/edgar/data/320193/000032019326000001/p0000320193-26-000001-index.htm"/>
  </entry>
</feed>
""".encode("utf-8")


def response(body=ATOM):
    return Response(body)


def http_transient(code):
    return urllib.error.HTTPError(MODULE.BASE, code, "error", {}, io.BytesIO(b"origin failure"))


class FetchSecEdgarTests(unittest.TestCase):
    def test_yields_parsed_records_from_atom(self):
        with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=response()) as urlopen, \
                mock.patch.object(MODULE.time, "sleep") as sleep:
            records = list(MODULE.fetch_recent())
        self.assertEqual(urlopen.call_count, 1)
        sleep.assert_not_called()
        self.assertEqual(len(records), 1)
        record = records[0]
        self.assertEqual(record["source"], "sec_edgar")
        self.assertTrue(record["fetched_at"].endswith("+00:00"))
        self.assertEqual(record["id"], "0000320193-26-000001:Filer:0000320193")
        self.assertEqual(record["form_type"], "144")
        self.assertEqual(record["party_name"], "Apple Inc")
        self.assertEqual(record["cik"], "0000320193")
        self.assertEqual(record["role"], "Filer")
        self.assertEqual(record["accession_number"], "0000320193-26-000001")
        self.assertEqual(record["filed_date"], "2026-11-16")
        self.assertEqual(record["size"], "12K")
        self.assertEqual(
            record["url"],
            "https://www.sec.gov/Archives/edgar/data/320193/000032019326000001/p0000320193-26-000001-index.htm",
        )

    def test_form_type_and_count_reach_the_query(self):
        with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=response()) as urlopen:
            list(MODULE.fetch_recent("144", count=100))
        url = urlopen.call_args_list[0].args[0].full_url
        self.assertEqual(
            url,
            MODULE.BASE + "?action=getcurrent&type=144&company=&dateb=&owner=include&count=100&output=atom",
        )

    def test_recovers_from_transient_read_timeout(self):
        with mock.patch.object(
            MODULE.urllib.request, "urlopen",
            side_effect=[TimeoutError("The read operation timed out"), response()],
        ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            self.assertEqual(len(list(MODULE.fetch_recent())), 1)
        self.assertEqual(urlopen.call_count, 2)
        self.assertEqual(sleep.call_args_list, [mock.call(MODULE.RETRY_BASE_DELAY)])

    def test_recovers_from_transient_server_errors(self):
        for code in (429, 500, 502, 503, 504):
            with self.subTest(code=code), mock.patch.object(
                MODULE.urllib.request, "urlopen",
                side_effect=[http_transient(code), response()],
            ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
                self.assertEqual(len(list(MODULE.fetch_recent())), 1)
                self.assertEqual(sleep.call_args_list, [mock.call(MODULE.RETRY_BASE_DELAY)])
                self.assertEqual(urlopen.call_count, 2)

    def test_recovers_from_connection_error(self):
        with mock.patch.object(
            MODULE.urllib.request, "urlopen",
            side_effect=[urllib.error.URLError("connection reset by peer"), response()],
        ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            self.assertEqual(len(list(MODULE.fetch_recent())), 1)
        self.assertEqual(urlopen.call_count, 2)

    def test_stops_after_finite_timeout_attempts(self):
        with mock.patch.object(
            MODULE.urllib.request, "urlopen",
            side_effect=[TimeoutError("The read operation timed out")] * MODULE.MAX_ATTEMPTS,
        ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            with self.assertRaises(TimeoutError):
                list(MODULE.fetch_recent())
        self.assertEqual(urlopen.call_count, MODULE.MAX_ATTEMPTS)
        self.assertEqual(
            sleep.call_args_list,
            [mock.call(MODULE.RETRY_BASE_DELAY * (2 ** i)) for i in range(MODULE.MAX_ATTEMPTS - 1)],
        )

    def test_fails_immediately_for_non_retryable_http_errors(self):
        error = urllib.error.HTTPError(MODULE.BASE, 403, "Forbidden", {}, io.BytesIO(b"blocked"))
        with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=error) as urlopen, \
                mock.patch.object(MODULE.time, "sleep") as sleep:
            with self.assertRaises(urllib.error.HTTPError) as ctx:
                list(MODULE.fetch_recent())
        self.assertEqual(ctx.exception.code, 403)
        self.assertEqual(urlopen.call_count, 1)
        sleep.assert_not_called()


if __name__ == "__main__":
    unittest.main()
