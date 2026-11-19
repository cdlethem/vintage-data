import importlib.util
import io
import json
import os
import pathlib
import tempfile
import unittest
import urllib.error
from unittest import mock


SCRIPT = pathlib.Path(__file__).parent / "scripts" / "fetch_nhc_hurdat2_atlantic.py"
SPEC = importlib.util.spec_from_file_location("fetch_nhc_hurdat2_atlantic", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


LISTING = """\
<html><body><table>
<tr><td align="right"><a href="hurdat2-1851-2025-092326.txt">hurdat2-1851-2025-092326.txt</a></td><td align="right"> 2025-09-30 01:02 </td></tr>
<tr><td align="right"><a href="hurdat2-1851-2025-02272026.txt">hurdat2-1851-2025-02272026.txt</a></td><td align="right"> 2025-02-27 01:19 </td></tr>
</table></body></html>
"""

ARCHIVE_A = """\
AL011999, TESTA,  3,
19990601, 0000,  ,  TS, 26.0N, 84.0W,  40, 1008,  60,  60,  60,  60, -999, -999, -999, -999, -999, -999, -999, -999, -999
19990601, 0600,  ,  TS, 26.2N, 84.5W,  45, 1005,  60,  60,  60,  60, -999, -999, -999, -999, -999, -999, -999, -999, -999
19990601, 1200,  ,  HU, 26.5N, 85.0W,  65,  990,  60,  60,  60,  60, -999, -999, -999, -999, -999, -999, -999, -999, -999
AL021999, TESTB,  2,
19990801, 0000,  ,  TD, 20.0N, 70.0W,  30, 1010, -999, -999, -999, -999, -999, -999, -999, -999, -999, -999, -999, -999, -999
19990801, 0600,  ,  TD, 20.5N, 70.5W,  35, 1008, -999, -999, -999, -999, -999, -999, -999, -999, -999, -999, -999, -999, -999
"""

# TESTB's second point is revised (lat 20.5 -> 20.7) and a new storm AL032000 is appended.
ARCHIVE_B = """\
AL011999, TESTA,  3,
19990601, 0000,  ,  TS, 26.0N, 84.0W,  40, 1008,  60,  60,  60,  60, -999, -999, -999, -999, -999, -999, -999, -999, -999
19990601, 0600,  ,  TS, 26.2N, 84.5W,  45, 1005,  60,  60,  60,  60, -999, -999, -999, -999, -999, -999, -999, -999, -999
19990601, 1200,  ,  HU, 26.5N, 85.0W,  65,  990,  60,  60,  60,  60, -999, -999, -999, -999, -999, -999, -999, -999, -999
AL021999, TESTB,  2,
19990801, 0000,  ,  TD, 20.0N, 70.0W,  30, 1010, -999, -999, -999, -999, -999, -999, -999, -999, -999, -999, -999, -999, -999
19990801, 0600,  ,  TD, 20.7N, 70.5W,  35, 1008, -999, -999, -999, -999, -999, -999, -999, -999, -999, -999, -999, -999, -999
AL032000, TESTC,  1,
20000101, 0000,  ,  TD, 15.0N, 60.0W,  30, 1012, -999, -999, -999, -999, -999, -999, -999, -999, -999, -999, -999, -999, -999
"""


class Response(io.BytesIO):
    def __init__(self, body, *, status=200, headers=None):
        super().__init__(body)
        self.status = status
        self.headers = {} if headers is None else headers

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def http_transient(code, retry_after=None):
    headers = {} if retry_after is None else {"Retry-After": retry_after}
    return urllib.error.HTTPError(MODULE.DIRECTORY_URL, code, "error", headers, io.BytesIO(b"origin failure"))


def listing_response():
    return Response(LISTING.encode("utf-8"))


def file_response(text, etag=None):
    return Response(text.encode("utf-8"), headers={"etag": etag or "test-etag"})


def run_main(argv, state, urlopen_side_effect):
    out, err = io.StringIO(), io.StringIO()
    with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=urlopen_side_effect) as urlopen, \
            mock.patch.object(MODULE.time, "sleep"):
        status = MODULE.main(argv + ["--state-file", str(state), "--timeout", "30"], out=out, err=err)
    return status, out.getvalue(), err.getvalue(), urlopen


def summary_from(err):
    return json.loads(err.split("VINTAGE_RUN_SUMMARY\t", 1)[1])


def temp_state_path():
    fd, name = tempfile.mkstemp(prefix="hurdat2-state-")
    os.close(fd)
    os.unlink(name)
    return pathlib.Path(name)


class FetchNhcHurdat2AtlanticTests(unittest.TestCase):
    def test_baseline_run_emits_every_point(self):
        state = temp_state_path()
        status, out, err, urlopen = run_main(["--no-state"], state, [listing_response(), file_response(ARCHIVE_A)])
        self.assertEqual(status, 0)
        records = [json.loads(line) for line in out.splitlines()]
        self.assertEqual(len(records), 5)
        first, last = records[0], records[-1]
        self.assertEqual(first["source"], "nhc_hurdat2_atlantic")
        self.assertTrue(first["fetched_at"].endswith("Z"))
        self.assertEqual(first["id"], "AL011999:19990601T0000Z")
        self.assertEqual(first["storm_id"], "AL011999")
        self.assertEqual(first["name"], "TESTA")
        self.assertEqual(first["release"], "hurdat2-1851-2025-092326.txt")
        self.assertEqual(first["lat"], 26.0)
        self.assertEqual(first["lon"], -84.0)
        self.assertEqual(first["max_wind_kt"], 40.0)
        self.assertEqual(first["wind_1min_radius_ne_km"], 60.0)
        self.assertIsNone(first["tc_radius_ne_km"])
        self.assertEqual(last["category"], "TD")
        self.assertFalse(state.exists())
        summary = summary_from(err)
        self.assertEqual(summary["status"], "ok")
        self.assertEqual(summary["state"], "baseline")
        self.assertEqual(summary["storms"], 2)
        self.assertEqual(summary["points_emitted"], 5)
        self.assertEqual(urlopen.call_count, 2)
        self.assertTrue(urlopen.call_args_list[1].args[0].full_url.endswith("hurdat2-1851-2025-092326.txt"))

    def test_unchanged_file_emits_nothing(self):
        state = temp_state_path()
        run_main([], state, [listing_response(), file_response(ARCHIVE_A)])
        first_state = state.read_text()
        status, out, err, _ = run_main([], state, [listing_response(), file_response(ARCHIVE_A)])
        self.assertEqual(status, 0)
        self.assertEqual(out, "")
        self.assertEqual(state.read_text(), first_state)
        summary = summary_from(err)
        self.assertEqual(summary["status"], "unchanged")
        self.assertEqual(summary["points_emitted"], 0)

    def test_revision_emits_only_changed_storms(self):
        state = temp_state_path()
        run_main([], state, [listing_response(), file_response(ARCHIVE_A)])
        status, out, err, _ = run_main([], state, [listing_response(), file_response(ARCHIVE_B)])
        self.assertEqual(status, 0)
        records = [json.loads(line) for line in out.splitlines()]
        ids = [record["id"] for record in records]
        self.assertEqual(ids, [
            "AL021999:19990801T0000Z",
            "AL021999:19990801T0600Z",
            "AL032000:20000101T0000Z",
        ])
        revised = next(record for record in records if record["id"] == "AL021999:19990801T0600Z")
        self.assertEqual(revised["lat"], 20.7)
        summary = summary_from(err)
        self.assertEqual(summary["status"], "ok")
        self.assertEqual(summary["state"], "updated")
        self.assertEqual(summary["storms_changed"], 2)
        self.assertEqual(summary["points_emitted"], 3)

    def test_recovers_from_transient_listing_error(self):
        state = temp_state_path()
        status, out, err, urlopen = run_main(
            [], state, [http_transient(503), listing_response(), file_response(ARCHIVE_A)]
        )
        self.assertEqual(status, 0)
        self.assertEqual(len(out.splitlines()), 5)
        self.assertEqual(urlopen.call_count, 3)

    def test_listing_without_snapshot_fails_with_summary(self):
        state = temp_state_path()
        body = b"<html><table><tr><td><a href=\"hurdat2-ep-1851-102326.txt\">hurdat2-ep-1851-102326.txt</a></td><td> 2025-09-30 01:02 </td></tr></table></html>"
        status, out, err, urlopen = run_main(["--no-state"], state, [Response(body)])
        self.assertEqual(status, 1)
        self.assertEqual(out, "")
        summary = summary_from(err)
        self.assertEqual(summary["status"], "failed")
        self.assertIn("no hurdat2-1851-*.txt", summary["error"])
        self.assertEqual(urlopen.call_count, 1)

    def test_point_count_mismatch_fails_with_summary(self):
        state = temp_state_path()
        truncated = ARCHIVE_A.replace("AL021999, TESTB,  2,", "AL021999, TESTB,  3,")
        status, out, err, _ = run_main(["--no-state"], state, [listing_response(), file_response(truncated)])
        self.assertEqual(status, 1)
        self.assertEqual(out, "")
        summary = summary_from(err)
        self.assertEqual(summary["status"], "failed")
        self.assertIn("AL021999", summary["error"])

    def test_unparseable_coordinate_fails_with_summary(self):
        state = temp_state_path()
        broken = ARCHIVE_A.replace("26.0N", "99.9X")
        status, out, err, _ = run_main(["--no-state"], state, [listing_response(), file_response(broken)])
        self.assertEqual(status, 1)
        self.assertEqual(out, "")
        summary = summary_from(err)
        self.assertEqual(summary["status"], "failed")

    def test_state_file_is_versioned_json(self):
        state = temp_state_path()
        run_main([], state, [listing_response(), file_response(ARCHIVE_A)])
        document = json.loads(state.read_text())
        self.assertEqual(document["version"], MODULE.STATE_VERSION)
        self.assertEqual(document["file"], "hurdat2-1851-2025-092326.txt")
        self.assertEqual(len(document["storm_digests"]), 2)


if __name__ == "__main__":
    unittest.main()
