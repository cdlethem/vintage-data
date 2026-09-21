"""Fixed, capability-scoped public source smoke commands.

This module is copied into the root-owned validation runtime.  Gate data may select
only one of these command IDs; it cannot alter the executable, extractor path,
arguments, network policy, or output handling.  ``{candidate_root}`` is the sole
runner-owned placeholder and is bound read-only at ``/work``.
"""
from __future__ import annotations

from typing import Final

# A public entry is executable only when the trusted outer validation launcher
# supplies this profile's root-owned proxy socket. It must reject private,
# loopback, and link-local destinations before binding the socket into bwrap.
# The command itself never shares the host network.
#
# The validation launcher provides the capability-specific network namespace.
# These commands intentionally do not share the host network themselves.
_BWRAP_PREFIX: Final[tuple[str, ...]] = (
    "/usr/bin/bwrap",
    "--unshare-all",
    "--new-session",
    "--die-with-parent",
    "--clearenv",
    "--ro-bind", "/usr", "/usr",
    "--ro-bind", "/lib", "/lib",
    "--ro-bind", "/lib64", "/lib64",
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

record_type = sys.argv[1]
extractor_argv = sys.argv[2:]
child = subprocess.Popen(
    extractor_argv,
    stdin=subprocess.DEVNULL,
    stdout=subprocess.PIPE,
    stderr=subprocess.DEVNULL,
    text=True,
    encoding="utf-8",
    errors="strict",
)
if child.stdout is None:
    raise SystemExit("extractor stdout unavailable")
seen = set()
emitted = 0
observed = 0
for raw in child.stdout:
    if not raw.endswith("\n") or len(raw.encode("utf-8")) > 65536:
        child.kill()
        raise SystemExit("extractor output is not bounded NDJSON")
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        child.kill()
        raise SystemExit("extractor output is invalid NDJSON")
    identifier = value.get("id") if isinstance(value, dict) else None
    if not isinstance(identifier, str) or not identifier or len(identifier) > 512:
        child.kill()
        raise SystemExit("extractor record lacks a bounded stable id")
    observed += 1
    if identifier in seen:
        child.kill()
        raise SystemExit("extractor emitted duplicate ids")
    seen.add(identifier)
    if emitted < 100:
        print(json.dumps({"type": record_type, "id": identifier}, separators=(",", ":")))
        emitted += 1
    if observed > 10000:
        child.kill()
        raise SystemExit("extractor result exceeds record bound")
if child.wait() != 0:
    raise SystemExit("extractor failed")
if not observed:
    raise SystemExit("extractor produced no records")
'''


def _public_smoke(record_type: str, *extractor_argv: str, source_url: str,
                  expected_status: int, timeout_seconds: int) -> dict[str, object]:
    """Build one immutable public-source smoke entry.

    ``record_type`` and extractor argv are module literals.  Do not add gate
    interpolation here: the runner replaces only ``{candidate_root}`` before
    launching the root-owned namespace policy.
    """
    return {
        "recipe": "public_source_smoke",
        "capability": "public-network-readonly",
        "argv": [
            *_BWRAP_PREFIX,
            _EXTRACTOR_ENVELOPE,
            record_type,
            "/usr/bin/python3",
            *extractor_argv,
        ],
        "timeout_seconds": timeout_seconds,
        "network_profile": "public-egress-proxy-v1",
        "source_url": source_url,
        "expected_status": expected_status,
        "credential_env": [],
    }


COMMANDS: Final[dict[str, dict[str, object]]] = {
    # The source scripts are from the exact candidate mounted at /work.  Every
    # request is fixed and deliberately small; no ticket argument reaches argv.
    "smoke-arxiv-new": _public_smoke(
        "arxiv", "/work/extract/scripts/fetch_arxiv_new.py", "cs.AI",
        source_url="https://export.arxiv.org/api/query", expected_status=200, timeout_seconds=120,
    ),
    "smoke-musicbrainz": _public_smoke(
        "musicbrainz", "/work/extract/scripts/fetch_musicbrainz.py", "2026-09-02",
        source_url="https://musicbrainz.org/ws/2/release/", expected_status=200, timeout_seconds=90,
    ),
    "smoke-digitraffic-rail": _public_smoke(
        "digitraffic_rail_live_trains", "/work/extract/scripts/fetch_digitraffic_rail.py",
        "--station", "HKI", "--limit", "10", "--timeout", "30",
        source_url="https://rata.digitraffic.fi/api/v1/live-trains/station/HKI?minutes_before_departure=0&minutes_after_departure=60&minutes_before_arrival=0&minutes_after_arrival=60",
        expected_status=200, timeout_seconds=90,
    ),
    "smoke-open-library": _public_smoke(
        "open_library", "/work/extract/scripts/fetch_open_library.py", "add-book", "20",
        source_url="https://openlibrary.org/recentchanges/add-book.json?limit=20", expected_status=200,
        timeout_seconds=90,
    ),
    "smoke-workday": _public_smoke(
        "workday", "/work/extract/scripts/fetch_job_boards.py", "--board", "workday",
        "2020companies|wd1|external_careers", "--max-per-board", "1", "--workers", "1",
        source_url="https://2020companies.wd1.myworkdayjobs.com/wday/cxs/2020companies/external_careers/jobs",
        expected_status=200, timeout_seconds=180,
    ),
    "smoke-sensor-community": _public_smoke(
        "sensor_community", "/work/extract/scripts/fetch_sensor_community.py", "--country", "DE",
        source_url="https://data.sensor.community/airrohr/v1/filter/country=DE", expected_status=200,
        timeout_seconds=240,
    ),
}
