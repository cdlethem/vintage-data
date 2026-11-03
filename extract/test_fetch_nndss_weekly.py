"""Offline behavioral tests for fetch_nndss_weekly.py."""

import importlib.util
import io
import json
import pathlib
import tempfile
import unittest
from unittest import mock
from urllib.parse import parse_qs, urlparse
from datetime import datetime, timezone

SCRIPT = pathlib.Path(__file__).parent / "scripts" / "fetch_nndss_weekly.py"
SPEC = importlib.util.spec_from_file_location("fetch_nndss_weekly", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)

FIXED_NOW = datetime(2026, 11, 2, 5, 17, 0, tzinfo=timezone.utc)


def week_row(year, week, seq, label, location, m1=None, m1_flag="-",
             m2="0.0", m2_flag=None, m3=None, m3_flag="-", m4="5.0", m4_flag="-"):
    row = {
        "sort_order": f"{year}{week:02d}{seq:05d}",
        "year": str(year),
        "week": str(week),
        "label": label,
        "states": location,
    }
    if m1 is not None:
        row["m1"] = str(m1)
    if m1_flag is not None:
        row["m1_flag"] = m1_flag
    if m2 is not None:
        row["m2"] = str(m2)
    if m2_flag is not None:
        row["m2_flag"] = m2_flag
    if m3 is not None:
        row["m3"] = str(m3)
    if m3_flag is not None:
        row["m3_flag"] = m3_flag
    if m4 is not None:
        row["m4"] = str(m4)
    if m4_flag is not None:
        row["m4_flag"] = m4_flag
    return row


W42_BATCH = [
    week_row(2026, 42, 1, "Anthrax", "U.S. Residents", m1="1.0", m1_flag=None,
             m2="1.0", m3="1.0", m4="1.0"),
    week_row(2026, 42, 2, "Anthrax", "New England"),
    week_row(2026, 42, 3, "Chikungunya virus disease", "Florida",
             m1="12.0", m1_flag="N", m3="47.5", m4=None, m4_flag=None),
]
W43_BATCH = [
    week_row(2026, 43, 1, "Anthrax", "U.S. Residents", m1="0.0", m1_flag=None),
    week_row(2026, 43, 2, "Anthrax", "Alabama"),
]


def latest_doc(year, week):
    return [{"year": str(year), "week": str(week), "n": "8400"}]


class JsonResponse(io.BytesIO):
    def __init__(self, document):
        super().__init__(json.dumps(document).encode("utf-8"))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


class FailingOutput(io.StringIO):
    def __init__(self, fail_after=1):
        super().__init__()
        self.writes = 0
        self.fail_after = fail_after

    def write(self, value):
        self.writes += 1
        if self.writes > self.fail_after:
            raise OSError("simulated output failure")
        return super().write(value)


class FetchNndssWeeklyTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.state_file = pathlib.Path(self._tmp.name) / "state.json"

    def tearDown(self):
        self._tmp.cleanup()

    def write_state(self, watermark):
        self.state_file.write_text(
            json.dumps({"watermark": watermark}, sort_keys=True) + "\n",
            encoding="utf-8")
        return self.state_file.read_bytes()

    def run_module(self, docs, **kwargs):
        output = kwargs.pop("output", io.StringIO())
        stderr = io.StringIO()
        responses = [JsonResponse(doc) for doc in docs]
        with mock.patch.object(MODULE.urllib.request, "urlopen",
                               side_effect=responses) as urlopen, \
             mock.patch.object(MODULE.sys, "stderr", new=stderr):
            count = MODULE.run(
                state_file=str(self.state_file),
                output=output,
                now=lambda: FIXED_NOW,
                **kwargs,
            )
        return count, output.getvalue(), stderr.getvalue(), urlopen

    @staticmethod
    def request_urls(urlopen):
        urls = []
        for call in urlopen.call_args_list:
            request = call.args[0]
            parsed = urlparse(request.full_url)
            urls.append((request.full_url, parse_qs(parsed.query)))
        return urls

    def test_cold_start_emits_only_latest_week(self):
        count, stdout, stderr, urlopen = self.run_module(
            [latest_doc(2026, 42), W42_BATCH])

        self.assertEqual(count, 3)
        lines = [json.loads(line) for line in stdout.splitlines()]
        self.assertEqual(len(lines), 3)
        for record in lines:
            self.assertEqual(record["source"], "nndss_weekly")
            self.assertEqual(record["fetched_at"], FIXED_NOW.isoformat())
        self.assertEqual(
            [record["id"] for record in lines],
            ["20264200001", "20264200002", "20264200003"])

        urls = self.request_urls(urlopen)
        self.assertEqual(len(urls), 2)
        # First request discovers the latest batch server-side.
        self.assertEqual(urls[0][1]["$group"], ["year,week"])
        self.assertEqual(urls[0][1]["$order"], ["year desc, week desc"])
        # Second request pulls only the latest week, ordered for stable paging.
        self.assertEqual(urls[1][1]["$where"], ["year='2026' and week='42'"])
        self.assertEqual(urls[1][1]["$order"], ["sort_order"])

        state = json.loads(self.state_file.read_text(encoding="utf-8"))
        self.assertEqual(state["watermark"], [2026, 42])
        self.assertIn("cold start", stderr)

    def test_normalizes_counts_flags_and_missing_values(self):
        _, stdout, _, _ = self.run_module([latest_doc(2026, 42), W42_BATCH])
        lines = [json.loads(line) for line in stdout.splitlines()]

        anthrax_us = lines[0]
        self.assertEqual(anthrax_us["mmwr_year"], 2026)
        self.assertEqual(anthrax_us["mmwr_week"], 42)
        self.assertEqual(anthrax_us["condition"], "Anthrax")
        self.assertEqual(anthrax_us["location"], "U.S. Residents")
        self.assertEqual(anthrax_us["current_week_cases"], 1)
        self.assertIsNone(anthrax_us["current_week_flag"])
        self.assertEqual(anthrax_us["ytd_previous_year_cases"], 1)

        chikungunya = lines[2]
        self.assertEqual(chikungunya["current_week_cases"], 12)
        self.assertEqual(chikungunya["current_week_flag"], "N")
        self.assertEqual(chikungunya["ytd_cases"], 47.5)
        self.assertIsNone(chikungunya["ytd_previous_year_cases"])
        self.assertIsNone(chikungunya["ytd_previous_year_flag"])

    def test_quiet_re_run_emits_nothing_and_keeps_state(self):
        before = self.write_state([2026, 42])
        count, stdout, _, urlopen = self.run_module([latest_doc(2026, 42)])

        self.assertEqual(count, 0)
        self.assertEqual(stdout, "")
        # Only the group-by discovery request goes out.
        self.assertEqual(len(urlopen.call_args_list), 1)
        self.assertEqual(self.state_file.read_bytes(), before)

    def test_new_week_emits_only_new_batch_and_advances_watermark(self):
        self.write_state([2026, 42])
        count, stdout, _, urlopen = self.run_module(
            [latest_doc(2026, 43), W43_BATCH])

        self.assertEqual(count, 2)
        lines = [json.loads(line) for line in stdout.splitlines()]
        self.assertEqual({line["mmwr_week"] for line in lines}, {43})
        urls = self.request_urls(urlopen)
        self.assertEqual(urls[1][1]["$where"], ["year='2026' and week='43'"])
        state = json.loads(self.state_file.read_text(encoding="utf-8"))
        self.assertEqual(state["watermark"], [2026, 43])

    def test_watermark_rolls_over_mmwr_year(self):
        self.write_state([2025, 53])
        rollover_batch = [week_row(2026, 1, 1, "Anthrax", "U.S. Residents")]
        count, _, _, _ = self.run_module([latest_doc(2026, 1), rollover_batch])

        self.assertEqual(count, 1)
        state = json.loads(self.state_file.read_text(encoding="utf-8"))
        self.assertEqual(state["watermark"], [2026, 1])

    def test_latest_older_than_watermark_emits_nothing(self):
        before = self.write_state([2026, 43])
        count, stdout, stderr, urlopen = self.run_module([latest_doc(2026, 42)])

        self.assertEqual(count, 0)
        self.assertEqual(stdout, "")
        self.assertEqual(len(urlopen.call_args_list), 1)
        self.assertEqual(self.state_file.read_bytes(), before)
        self.assertIn("older than watermark", stderr)

    def test_full_page_triggers_offset_follow_up(self):
        self.write_state([2026, 42])
        batch = [week_row(2026, 43, seq, f"Cond {seq}", f"Loc {seq}")
                 for seq in range(1, 6)]
        served = []

        def paged(request, timeout=None):
            query = parse_qs(urlparse(request.full_url).query)
            if "$group" in query:
                served.append("group")
                return JsonResponse(latest_doc(2026, 43))
            offset = int(query["$offset"][0])
            served.append(offset)
            return JsonResponse(batch[offset:offset + 2])

        output = io.StringIO()
        with mock.patch.object(MODULE.urllib.request, "urlopen",
                               side_effect=paged) as urlopen, \
             mock.patch.object(MODULE.sys, "stderr", new=io.StringIO()), \
             mock.patch.object(MODULE, "PAGE_LIMIT", 2):
            count = MODULE.run(state_file=str(self.state_file), output=output,
                               now=lambda: FIXED_NOW)

        self.assertEqual(count, 5)
        self.assertEqual(served, ["group", 0, 2, 4])
        self.assertEqual(urlopen.call_args.kwargs, {"timeout": 60})

    def test_state_not_saved_when_output_fails(self):
        self.write_state([2026, 42])
        before = self.state_file.read_bytes()
        with self.assertRaises(OSError):
            self.run_module([latest_doc(2026, 43), W43_BATCH],
                            output=FailingOutput(fail_after=1))
        self.assertEqual(self.state_file.read_bytes(), before)

    def test_no_state_never_writes(self):
        count, stdout, _, _ = self.run_module(
            [latest_doc(2026, 42), W42_BATCH], no_state=True)

        self.assertEqual(count, 3)
        self.assertEqual(len(stdout.splitlines()), 3)
        self.assertFalse(self.state_file.exists())

    def test_malformed_state_file_raises(self):
        self.state_file.write_text("[2026, 42]\n", encoding="utf-8")
        with self.assertRaises(ValueError):
            self.run_module([latest_doc(2026, 42)])

    def test_row_without_sort_order_raises(self):
        with self.assertRaises(ValueError):
            MODULE.normalize_row({"year": "2026", "week": "42"}, "ts")

    def test_empty_dataset_raises(self):
        with mock.patch.object(MODULE.urllib.request, "urlopen",
                               return_value=JsonResponse([])) as urlopen:
            with self.assertRaises(RuntimeError):
                MODULE.latest_week()
        urlopen.assert_called_once()


if __name__ == "__main__":
    unittest.main()
