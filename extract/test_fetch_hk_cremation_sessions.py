import contextlib
import importlib.util
import io
from pathlib import Path
import unittest
from unittest import mock


SCRIPT = Path(__file__).parent / "scripts" / "fetch_hk_cremation_sessions.py"
SPEC = importlib.util.spec_from_file_location("fetch_hk_cremation_sessions", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


# The exact English sentence FEHD serves during its daily 6:30-10:30 pm HKT
# maintenance window (observed live 2026-11-10), with no availability table.
MAINTENANCE_PAGE = """
<html lang="zh-HK"><head><title>食物環境衞生署</title></head>
<body>
<div class="message-block english">The system will be suspended from 6:30 p.m. to 10:30 p.m. due to system upgrading and maintenance. Sorry for any inconvenience caused.</div>
</body></html>
"""

AVAILABILITY_PAGE = """
<html><head><title>Available Sessions</title></head>
<body>
  <table>
    <tr><td>Last updated: 10/11/2026 06:00</td></tr>
  </table>
  <table>
    <tr>
      <th>Date (dd/mm/yyyy)</th><th>Total</th>
      <th>Crematorium A</th><th>Crematorium B</th>
    </tr>
    <tr>
      <td>11/11/2026</td><td>20</td>
      <td>12 (2)</td><td>N/A</td>
    </tr>
    <tr>
      <td>12/11/2026</td><td>15</td>
      <td>8 (0)</td><td>7 (1)</td>
    </tr>
  </table>
</body></html>
"""


class PageResponse(io.BytesIO):
    def __init__(self, text):
        super().__init__(text.encode("utf-8"))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


class FetchHkCremationSessionsTests(unittest.TestCase):
    def fetch(self, page, **kwargs):
        stderr = io.StringIO()
        with mock.patch.object(
            MODULE.urllib.request,
            "urlopen",
            return_value=PageResponse(page),
        ) as urlopen:
            with contextlib.redirect_stderr(stderr):
                records = list(MODULE.fetch_sessions(**kwargs))
        return records, stderr.getvalue(), urlopen

    def test_maintenance_notice_page_emits_no_records(self):
        records, stderr, _ = self.fetch(MAINTENANCE_PAGE)

        self.assertEqual(records, [])
        self.assertIn("maintenance", stderr)

    def test_availability_page_parses_sessions_and_update_time(self):
        records, stderr, _ = self.fetch(AVAILABILITY_PAGE)

        self.assertEqual(len(records), 4)
        self.assertEqual(stderr, "")
        first = records[0]
        self.assertEqual(first["source"], "hk_cremation_sessions")
        self.assertEqual(first["id"], "11/11/2026:Crematorium A")
        self.assertEqual(first["session_date"], "11/11/2026")
        self.assertEqual(first["crematorium"], "Crematorium A")
        self.assertEqual(first["raw_value"], "12 (2)")
        self.assertEqual(first["available_sessions"], 12)
        self.assertEqual(first["sessions_under_booking"], 2)
        self.assertEqual(first["service_state"], "in_service")
        self.assertEqual(first["day_total_sessions"], 20)
        self.assertEqual(first["publisher_updated_at"], "10/11/2026 06:00")
        na = records[1]
        self.assertEqual(na["id"], "11/11/2026:Crematorium B")
        self.assertIsNone(na["available_sessions"])
        self.assertEqual(na["service_state"], "N/A")
        self.assertEqual(records[0]["fetched_at"], records[3]["fetched_at"])
        self.assertRegex(records[0]["fetched_at"], r"\+00:00$")

    def test_page_missing_table_and_notice_still_fails(self):
        with self.assertRaises(ValueError) as caught:
            self.fetch("<html><body><p>something else entirely</p></body></html>")

        self.assertIn("availability table", str(caught.exception))


if __name__ == "__main__":
    unittest.main()
