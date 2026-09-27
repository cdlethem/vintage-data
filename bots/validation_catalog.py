"""Fixed, capability-scoped public source smoke commands.

This module belongs to the trusted orchestration deployment, not the candidate.
Gate data may select only a command ID; it cannot alter the executable, extractor path,
arguments, network policy, or output handling.  ``{candidate_root}`` is the sole
runner-owned placeholder and is bound read-only at ``/work``.
"""
from __future__ import annotations

from typing import Final

# The parent supplies a private per-run proxy socket. Candidate processes have
# no host network and cannot reach private, loopback, or link-local destinations.
_BWRAP_PREFIX: Final[tuple[str, ...]] = (
    "/usr/bin/bwrap",
    "--unshare-all",
    "--new-session",
    "--die-with-parent",
    "--clearenv",
    "--ro-bind", "/usr", "/usr",
    "--ro-bind", "/lib", "/lib",
    "--ro-bind", "/lib64", "/lib64",
    "--ro-bind", "/etc/ssl/certs", "/etc/ssl/certs",
    "--ro-bind", "{candidate_root}", "/work",
    "--proc", "/proc",
    "--dev", "/dev",
    "--tmpfs", "/tmp",
    "--chdir", "/work",
    "--setenv", "PATH", "/usr/bin:/bin",
    "--setenv", "LANG", "C.UTF-8",
    "--",
    "/usr/bin/python3", "-I", "-c",
)

# This literal wrapper is trusted runtime code, rather than candidate code.  It
# invokes exactly the static extractor argv that follows it, consumes all its
# NDJSON, and releases only bounded structural evidence.  Candidate stdout never
# becomes validation evidence verbatim.
_EXTRACTOR_ENVELOPE: Final[str] = r'''
import json
import subprocess
import sys
import tempfile

record_type = sys.argv[1]
expected_source = sys.argv[2]
required_string_key = sys.argv[3] or None
required_equals_key = sys.argv[4] or None
required_equals_value = sys.argv[5] or None
extractor_argv = sys.argv[6:]
stderr = tempfile.TemporaryFile()
child = subprocess.Popen(
    extractor_argv,
    stdin=subprocess.DEVNULL,
    stdout=subprocess.PIPE,
    stderr=stderr,
)
if child.stdout is None:
    raise SystemExit("extractor stdout unavailable")
seen = set()
emitted = 0
observed = 0
required_string_seen = required_string_key is None
while True:
    raw = child.stdout.readline(65537)
    if not raw:
        break
    if len(raw) > 65536 or not raw.endswith(b"\n"):
        child.kill()
        raise SystemExit("extractor output is not bounded NDJSON")
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        child.kill()
        raise SystemExit("extractor output is invalid NDJSON")
    identifier = value.get("id") if isinstance(value, dict) else None
    if type(identifier) is int:
        identifier = str(identifier)
    if not isinstance(identifier, str) or not identifier or len(identifier) > 512:
        child.kill()
        raise SystemExit("extractor record lacks a bounded stable id")
    if value.get("source") != expected_source:
        child.kill()
        raise SystemExit("extractor record source does not match its fixed smoke")
    if required_equals_key and value.get(required_equals_key) != required_equals_value:
        child.kill()
        raise SystemExit("extractor record does not satisfy fixed smoke shape")
    if required_string_key and isinstance(value.get(required_string_key), str) and value[required_string_key]:
        required_string_seen = True
    observed += 1
    if identifier in seen:
        child.kill()
        raise SystemExit("extractor emitted duplicate ids")
    seen.add(identifier)
    if emitted < 100:
        print(json.dumps({"type": record_type, "id": identifier}, separators=(",", ":")))
        emitted += 1
    if observed > 100000:
        child.kill()
        raise SystemExit("extractor result exceeds record bound")
exit_code = child.wait()
if exit_code != 0:
    stderr.seek(0, 2)
    stderr.seek(max(0, stderr.tell() - 2000))
    diagnostic = stderr.read(2000).decode("utf-8", "replace").strip()
    stderr.close()
    raise SystemExit(f"extractor failed ({exit_code}): {diagnostic or 'no stderr captured'}")
stderr.seek(0)
summary_output = stderr.read(131073)
if len(summary_output) > 131072:
    raise SystemExit("extractor stderr exceeds evidence bound")
sys.stderr.buffer.write(summary_output)
stderr.close()
if not observed:
    raise SystemExit("extractor produced no records")
if not required_string_seen:
    raise SystemExit("extractor did not satisfy fixed string-field assertion")
'''


def _public_smoke(record_type: str, expected_source: str, *extractor_argv: str, source_url: str,
                  expected_status: int, timeout_seconds: int, required_string_key: str = "",
                  required_equals_key: str = "", required_equals_value: str = "",
                  summary_required: bool = False) -> dict[str, object]:
    """Build one immutable public-source smoke entry.

    Every output-shape assertion is literal catalog data.  Gate values cannot
    influence either the candidate command or what counts as live evidence.
    """
    return {
        "recipe": "public_source_smoke",
        "capability": "public-network-readonly",
        "argv": [
            *_BWRAP_PREFIX,
            _EXTRACTOR_ENVELOPE,
            record_type,
            expected_source,
            required_string_key,
            required_equals_key,
            required_equals_value,
            "/usr/bin/python3",
            *extractor_argv,
        ],
        "timeout_seconds": timeout_seconds,
        "network_profile": "public-egress-proxy-v1",
        "source_url": source_url,
        "expected_status": expected_status,
        "summary_required": summary_required,
        "credential_env": [],
    }


COMMANDS: Final[dict[str, dict[str, object]]] = {
    # The source scripts are from the exact candidate mounted at /work.  Every
    # request is fixed and deliberately small; no ticket argument reaches argv.
    "smoke-arxiv-new": _public_smoke(
        "arxiv", "arxiv", "-c",
        "import json,runpy; module=runpy.run_path('/work/extract/scripts/fetch_arxiv_new.py')\n"
        "for row in module['fetch_new_papers'](category='cs.AI',hours_back=168,max_results=20):\n print(json.dumps(row))",
        source_url="https://export.arxiv.org/api/query", expected_status=200, timeout_seconds=120,
        required_string_key="title",
    ),
    "smoke-musicbrainz": _public_smoke(
        "musicbrainz", "musicbrainz", "/work/extract/scripts/fetch_musicbrainz.py", "2026-09-02",
        source_url="https://musicbrainz.org/ws/2/release/", expected_status=200, timeout_seconds=90,
        required_string_key="title",
    ),
    "smoke-digitraffic-rail": _public_smoke(
        "digitraffic_rail_live_trains", "digitraffic_rail_live_trains",
        "/work/extract/scripts/fetch_digitraffic_rail.py", "--station", "HKI", "--limit", "10", "--timeout", "30",
        source_url="https://rata.digitraffic.fi/api/v1/live-trains/station/HKI?minutes_before_departure=0&minutes_after_departure=60&minutes_before_arrival=0&minutes_after_arrival=60",
        expected_status=200, timeout_seconds=90, required_equals_key="requested_station", required_equals_value="HKI",
    ),
    "smoke-open-library": _public_smoke(
        "open_library", "open_library", "/work/extract/scripts/fetch_open_library.py", "add-book", "20",
        source_url="https://openlibrary.org/recentchanges/add-book.json?limit=20", expected_status=200,
        timeout_seconds=90, required_string_key="author", required_equals_key="kind", required_equals_value="add-book",
    ),
    "smoke-workday": _public_smoke(
        "workday", "job_boards", "/work/extract/scripts/fetch_job_boards.py", "--board", "workday",
        "2020companies|wd1|external_careers", "--max-per-board", "1", "--workers", "1",
        source_url="https://2020companies.wd1.myworkdayjobs.com/wday/cxs/2020companies/external_careers/jobs",
        expected_status=200, timeout_seconds=180, required_string_key="title",
        required_equals_key="provider", required_equals_value="workday", summary_required=True,
    ),
}
