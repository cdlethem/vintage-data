from contextlib import redirect_stderr, redirect_stdout
import importlib.util
import io
import json
import pathlib
import socket
import tempfile
import unittest
import urllib.error
from datetime import datetime, timedelta, timezone
from unittest import mock

SCRIPT = pathlib.Path(__file__).parents[1] / "scripts" / "fetch_cran_package_updates.py"
SPEC = importlib.util.spec_from_file_location("fetch_cran_package_updates", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)

class Response(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def http_error(code, message="error"):
    return urllib.error.HTTPError(MODULE.LIST_URL, code, message, {}, None)


def list_page(*rows, header=True):
    header_cells = "<tr> <th> Date </th> <th> Package </th> <th> Title </th> </tr>" if header else ""
    body = "\n".join(
        f'<tr> <td> {d} </td> <td> <a href="../../web/packages/{name}/index.html">'
        f'<span class="CRAN">{name}</span></a> </td> <td> {title} </td> </tr>'
        for d, name, title in rows
    )
    return f"<html><body><table>{header_cells}\n{body}\n</table></body></html>".encode()


def version_page(version):
    return (
        f"<html><body><table><tr>\n<td>Version:</td>\n<td>{version}</td>\n</tr></table></body></html>"
    ).encode()


_UTC_NOW = datetime.now(timezone.utc)
TODAY = _UTC_NOW.date().isoformat()
YESTERDAY = (_UTC_NOW.date() - timedelta(days=1)).isoformat()
OLDER = (_UTC_NOW.date() - timedelta(days=2)).isoformat()


def run_main(urlopen_effects, argv, stderr=None, stdout=None):
    """Run MODULE.main() with scripted urlopen responses; return (urlopen, sleep, stdout, stderr)."""
    if stdout is None:
        stdout = io.StringIO()
    if stderr is None:
        stderr = io.StringIO()
    with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=urlopen_effects) as urlopen, \
            mock.patch.object(MODULE.time, "sleep") as sleep, \
            mock.patch("sys.argv", argv):
        with redirect_stdout(stdout), redirect_stderr(stderr):
            MODULE.main()
    return urlopen, sleep, stdout, stderr


class CollectUpdatesTests(unittest.TestCase):
    def rows(self):
        return list(MODULE.parse_list_table(list_page(
            (TODAY, "alpha", "A Title"),
            (YESTERDAY, "Beta", "B Title"),
            (OLDER, "gamma", "C Title"),
        )))

    def test_cold_start_floor_excludes_older_dates(self):
        got = list(MODULE.collect_updates(self.rows(), watermark="", boundary=frozenset(), floor=YESTERDAY))
        self.assertEqual([p for _, p, _ in got], ["alpha", "Beta"])

    def test_watermark_skips_boundary_and_keeps_same_date_remainder(self):
        rows = list(MODULE.parse_list_table(list_page(
            (TODAY, "delta", "D"), (TODAY, "alpha", "A"), (YESTERDAY, "Beta", "B"),
        )))
        got = list(MODULE.collect_updates(rows, watermark=TODAY, boundary=frozenset({"delta"}), floor=TODAY))
        self.assertEqual([p for _, p, _ in got], ["alpha"])

    def test_dedupes_case_variants(self):
        rows = list(MODULE.parse_list_table(list_page(
            (TODAY, "Alpha", "A"), (TODAY, "alpha", "A2"),
        )))
        got = list(MODULE.collect_updates(rows, watermark="", boundary=frozenset(), floor=OLDER))
        self.assertEqual([p for _, p, _ in got], ["Alpha"])


class ParseListTableTests(unittest.TestCase):
    def test_missing_header_fails(self):
        with self.assertRaises(ValueError):
            list(MODULE.parse_list_table(list_page((TODAY, "alpha", "A"), header=False)))

    def test_bad_date_fails(self):
        with self.assertRaises(ValueError):
            list(MODULE.parse_list_table(list_page(("not-a-date", "alpha", "A"))))


class FetchTextTests(unittest.TestCase):
    def test_retries_503_then_succeeds(self):
        with mock.patch.object(MODULE.urllib.request, "urlopen",
                               side_effect=[http_error(503), Response(version_page("1.0"))]) as urlopen, \
                mock.patch.object(MODULE.time, "sleep") as sleep:
            text = MODULE.fetch_text(MODULE.LIST_URL, timeout=5)
        self.assertIn("1.0", text)
        self.assertEqual(urlopen.call_count, 2)
        sleep.assert_called_once_with(MODULE.RETRY_DELAYS[0])

    def test_404_is_not_retried(self):
        with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=http_error(404)) as urlopen, \
                mock.patch.object(MODULE.time, "sleep") as sleep:
            with self.assertRaises(urllib.error.HTTPError):
                MODULE.fetch_text(MODULE.LIST_URL, timeout=5)
        self.assertEqual(urlopen.call_count, 1)
        sleep.assert_not_called()

    def test_timeout_exhaustion_is_bounded(self):
        with mock.patch.object(MODULE.urllib.request, "urlopen",
                               side_effect=socket.timeout("timed out")) as urlopen, \
                mock.patch.object(MODULE.time, "sleep") as sleep:
            with self.assertRaises(TimeoutError):
                MODULE.fetch_text(MODULE.LIST_URL, timeout=5)
        self.assertEqual(urlopen.call_count, MODULE.MAX_ATTEMPTS)
        self.assertEqual(sleep.call_args_list, [mock.call(1), mock.call(2)])

    def test_dns_blip_is_not_retried(self):
        error = urllib.error.URLError("temporary failure in name resolution")
        with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=error) as urlopen, \
                mock.patch.object(MODULE.time, "sleep") as sleep:
            with self.assertRaises(urllib.error.URLError):
                MODULE.fetch_text(MODULE.LIST_URL, timeout=5)
        self.assertEqual(urlopen.call_count, 1)
        sleep.assert_not_called()


class MainRunTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.state_file = pathlib.Path(self._tmp.name) / "state.json"
    def test_cold_start_run_writes_watermark_and_emits_two_days(self):
        state_file = self.state_file
        effects = [
            Response(list_page((TODAY, "alpha", "A Title"), (YESTERDAY, "Beta", "B Title"), (OLDER, "gamma", "C"))),
            Response(version_page("0.1.0")),
            Response(version_page("2.3-4")),
        ]
        urlopen, sleep, out, err = run_main(effects, ["fetch", "--state-file", str(state_file)])
        records = [json.loads(line) for line in out.getvalue().splitlines()]
        self.assertEqual([r["id"] for r in records], ["alpha", "beta"])
        self.assertEqual(records[0]["package"], "alpha")
        self.assertEqual(records[1]["package"], "Beta")
        self.assertEqual(records[0]["version"], "0.1.0")
        self.assertEqual(records[1]["version"], "2.3-4")
        self.assertEqual(records[0]["published"], TODAY)
        self.assertEqual(records[1]["published"], YESTERDAY)
        self.assertEqual(len({r["source"] for r in records}), 1)
        self.assertEqual(records[0]["source"], "cran_package_updates")
        self.assertEqual(records[0]["fetched_at"], records[1]["fetched_at"])
        self.assertEqual(urlopen.call_count, 3)  # 1 index page + 2 package pages
        self.assertEqual(json.loads(state_file.read_text()),
                         {"date": TODAY, "boundary": ["alpha"]})
        summary = [l for l in err.getvalue().splitlines() if l.startswith(MODULE.SUMMARY_PREFIX)]
        self.assertEqual(json.loads(summary[0].split("\t", 1)[1]),
                         {"records": 2, "version_fetch_failures": 0})


    def test_quiet_second_run_emits_nothing_and_preserves_watermark(self):
        state_file = self.state_file
        state_file.write_text(json.dumps({"date": TODAY, "boundary": ["alpha"]}))
        effects = [Response(list_page((TODAY, "alpha", "A Title"), (YESTERDAY, "Beta", "B Title")))]
        urlopen, sleep, out, err = run_main(effects, ["fetch", "--state-file", str(state_file)])
        self.assertEqual(out.getvalue(), "")
        self.assertEqual(err.getvalue(), "")
        self.assertEqual(urlopen.call_count, 1)  # index page only, no package pages
        self.assertEqual(json.loads(state_file.read_text()),
                         {"date": TODAY, "boundary": ["alpha"]})


    def test_new_package_on_watermark_date_is_emitted(self):
        state_file = self.state_file
        state_file.write_text(json.dumps({"date": TODAY, "boundary": ["alpha"]}))
        effects = [
            Response(list_page((TODAY, "zeta", "Z Title"), (TODAY, "alpha", "A Title"))),
            Response(version_page("1.0.0")),
        ]
        urlopen, sleep, out, err = run_main(effects, ["fetch", "--state-file", str(state_file)])
        records = [json.loads(line) for line in out.getvalue().splitlines()]
        self.assertEqual([r["id"] for r in records], ["zeta"])
        self.assertEqual(json.loads(state_file.read_text()),
                         {"date": TODAY, "boundary": ["alpha", "zeta"]})


    def test_version_page_failure_yields_null_and_counts_in_summary(self):
        state_file = self.state_file
        effects = [
            Response(list_page((TODAY, "alpha", "A"), (TODAY, "flaky", "F"))),
            Response(version_page("0.1.0")),
            socket.timeout("timed out"),
            socket.timeout("timed out"),
            socket.timeout("timed out"),
        ]
        urlopen, sleep, out, err = run_main(effects, ["fetch", "--state-file", str(state_file)])
        records = [json.loads(line) for line in out.getvalue().splitlines()]
        self.assertEqual(records[0]["version"], "0.1.0")
        self.assertIsNone(records[1]["version"])
        summary = json.loads([l for l in err.getvalue().splitlines()
                              if l.startswith(MODULE.SUMMARY_PREFIX)][0].split("\t", 1)[1])
        self.assertEqual(summary, {"records": 2, "version_fetch_failures": 1})


    def test_list_page_timeout_exhaustion_fails_the_run(self):
        state_file = self.state_file
        effects = [socket.timeout("timed out")] * MODULE.MAX_ATTEMPTS
        with self.assertRaises(TimeoutError):
            run_main(effects, ["fetch", "--state-file", str(state_file)])
        self.assertFalse(state_file.exists())

    def test_malformed_list_fails_without_state_write(self):
        state_file = self.state_file
        with self.assertRaises(ValueError):
            run_main([Response(list_page((TODAY, "alpha", "A"), header=False))],
                     ["fetch", "--state-file", str(state_file)])
        self.assertFalse(state_file.exists())


if __name__ == "__main__":
    unittest.main()
