#!/usr/bin/env python3
"""Smithsonian Global Volcanism Program — eruption database, keyless WFS/GeoJSON.

Lead #26: low frequency but high drama; a good slow-moving join dimension against
seismic data.

**Verified live 2026-09-03**:
  * `webservices.volcano.si.edu/geoserver/GVP-VOTW/wfs?service=WFS&version=2.0.0&
    request=GetFeature&typeName=GVP-VOTW:E3WebApp_Eruptions1960&outputFormat=
    application/json&count=3` — **200, 1,772 bytes, `totalFeatures: 2248`** eruptions
    since 1960, real GeoJSON features.
  * The same query with **`sortBy=StartDate+D`** (descending) — **200, 2,779 bytes,
    5 features** — top result Kikai, Japan, `StartDate: 20251229` — the most recent
    eruption in the record, genuinely close to the probe date.
  * **`maxfeatures=3` (lowercase, no separating capital) silently returned the
    entire dataset** — one query with that parameter came back **8.9 MB, 11,089
    features**, ignoring the limit entirely — confirmed and explained in quirks.

Endpoint: `https://webservices.volcano.si.edu/geoserver/GVP-VOTW/wfs`
    service=WFS&version=2.0.0&request=GetFeature&outputFormat=application/json
    typeName=GVP-VOTW:E3WebApp_Eruptions1960   (or ...Smithsonian_VOTW_Holocene_Eruptions
                                               for the full Holocene record, much larger)
    count=N              row limit — the correct WFS 2.0 parameter name
    sortBy=StartDate+D     D=descending, A=ascending — space-encoded as `+` in the URL

Quirks that will cost someone an afternoon:
    * **The row-limit parameter is `count`, not `maxfeatures`, in WFS 2.0** — this is
      a real, silent trap: `maxfeatures=3` (the WFS 1.x parameter name, wrong case for
      2.0) returned the *entire* 2,248-row or 11,089-row dataset instead of erroring
      or limiting, confirmed live twice at different scales (1.15 MB and 8.9 MB
      responses). The correctly-cased `count=3` limited properly. Always use `count`.
    * `sortBy` needs a direction suffix and the space before it must be `+`-encoded
      in the URL (`sortBy=StartDate+D`) — without it you get the database's default
      (apparently insertion) order, not chronological.
    * `StartDate`/`EndDate` are **packed `YYYYMMDD` strings**, with separate
      `StartDateYear`/`Month`/`Day` integer fields alongside them — pick one
      representation and stick with it rather than mixing.
    * `ContinuingEruption` is a **stringified boolean** (`"True"`/`"False"`), not a
      real JSON bool — and it can read `"True"` on a record whose `EndDate` is
      already in the past, so don't treat it as a reliable "still erupting right now"
      signal without cross-checking the date.
    * Two typeNames cover very different scales: `E3WebApp_Eruptions1960` (2,248
      rows, since 1960) is the practical "recent activity" table; the full
      `Smithsonian_VOTW_Holocene_Eruptions` typeName returns 11,000+ rows spanning
      the Holocene — fetching it unfiltered is an 8.9 MB response.

Etiquette: keyless, no published rate limit found. This is a small institutional
service (Smithsonian) — cache aggressively (eruption records are historical once
entered) and avoid unfiltered full-table pulls given the 8.9 MB size observed.

Stdlib only.
"""
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
BASE = "https://webservices.volcano.si.edu/geoserver/GVP-VOTW/wfs"
REQUEST_TIMEOUT_SECONDS = 45.0
REQUEST_BUDGET_SECONDS = 120.0
MAX_ATTEMPTS = 3
RETRYABLE_HTTP_STATUS = frozenset({408, 425, 429, 500, 502, 503, 504})
ATTEMPT_PREFIX = "VINTAGE_REQUEST_ATTEMPT\t"
SUMMARY_PREFIX = "VINTAGE_RUN_SUMMARY\t"


class SmithsonianFetchError(RuntimeError):
    """A redacted fetch failure with enough context for final telemetry."""

    def __init__(self, message, *, attempts, elapsed, status, byte_count, failure_phase):
        super().__init__(message)
        self.attempts = attempts
        self.elapsed = elapsed
        self.status = status
        self.byte_count = byte_count
        self.failure_phase = failure_phase

class RequestDeadlineError(TimeoutError):
    """The run-wide request budget expired during an operation."""



def _elapsed(clock, started):
    return max(0.0, clock() - started)


def _emit_attempt(*, attempt, elapsed, status, byte_count, records, failure_phase, outcome, retrying):
    print(
        ATTEMPT_PREFIX
        + json.dumps(
            {
                "attempt": attempt,
                "elapsed_seconds": round(elapsed, 3),
                "status": status,
                "bytes": byte_count,
                "records": records,
                "failure_phase": failure_phase,
                "outcome": outcome,
                "retrying": retrying,
            },
            sort_keys=True,
        ),
        file=sys.stderr,
    )


def _validate_document(document, requested_count):
    if not isinstance(document, dict):
        raise ValueError("response must be a GeoJSON object")
    if document.get("type") not in (None, "FeatureCollection"):
        raise ValueError("response is not a GeoJSON FeatureCollection")
    features = document.get("features")
    if not isinstance(features, list):
        raise ValueError("response is missing a GeoJSON features list")
    if len(features) > requested_count:
        raise ValueError("response exceeded the requested feature count")
    number_returned = document.get("numberReturned")
    if number_returned is not None and (
        isinstance(number_returned, bool)
        or not isinstance(number_returned, int)
        or number_returned != len(features)
    ):
        raise ValueError("response numberReturned does not match its features")
    for feature in features:
        if not isinstance(feature, dict) or not isinstance(feature.get("properties"), dict):
            raise ValueError("response contains an invalid GeoJSON feature")
    return features
def _set_response_timeout(response, timeout):
    """Clip the underlying socket operation to the run-wide remaining budget."""
    stream = getattr(response, "fp", None)
    raw = getattr(stream, "raw", None)
    sock = getattr(raw, "_sock", None)
    if sock is not None:
        sock.settimeout(timeout)


def _read_response(response, clock, started, chunks):
    while True:
        remaining = REQUEST_BUDGET_SECONDS - _elapsed(clock, started)
        if remaining <= 0:
            raise RequestDeadlineError("request deadline exhausted while reading response")
        _set_response_timeout(response, min(REQUEST_TIMEOUT_SECONDS, remaining))
        chunk = response.read(64 * 1024)
        if not chunk:
            return
        chunks.append(chunk)




def _fetch_document(params, requested_count, diagnostics=None):
    """Fetch and validate one complete response within a run-wide retry budget."""
    url = f"{BASE}?{urllib.parse.urlencode(params)}"
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    clock = time.monotonic
    sleeper = time.sleep
    started = clock()
    total_bytes = 0
    last_status = None

    def fail(message, attempts, phase):
        raise SmithsonianFetchError(
            message,
            attempts=attempts,
            elapsed=_elapsed(clock, started),
            status=last_status,
            byte_count=total_bytes,
            failure_phase=phase,
        )

    for attempt in range(1, MAX_ATTEMPTS + 1):
        remaining = REQUEST_BUDGET_SECONDS - _elapsed(clock, started)
        if remaining <= 0:
            fail("request deadline exhausted", attempt - 1, "request_deadline")

        status = None
        body = b""
        chunks = []
        failure_phase = "open"
        failure_kind = None
        retryable = False
        try:
            response = urllib.request.urlopen(
                request, timeout=min(REQUEST_TIMEOUT_SECONDS, remaining)
            )
            with response:
                status = getattr(response, "status", None)
                last_status = status
                if status is not None and not 200 <= status < 300:
                    retryable = status in RETRYABLE_HTTP_STATUS
                    failure_kind = "transient_http" if retryable else "http"
                    failure_phase = "status"
                else:
                    failure_phase = "read"
                    _read_response(response, clock, started, chunks)
        except urllib.error.HTTPError as error:
            status = error.code
            last_status = status
            retryable = status in RETRYABLE_HTTP_STATUS
            failure_kind = "transient_http" if retryable else "http"
            failure_phase = "status"
            error.close()
        except RequestDeadlineError:
            failure_kind = "deadline"
            failure_phase = "read_deadline"
        except (TimeoutError, urllib.error.URLError, OSError) as error:
            retryable = True
            failure_kind = "timeout" if isinstance(error, TimeoutError) else "transport"
        body = b"".join(chunks)
        total_bytes += len(body)

        elapsed = _elapsed(clock, started)
        if failure_kind == "deadline":
            _emit_attempt(
                attempt=attempt,
                elapsed=_elapsed(clock, started),
                status=status,
                byte_count=len(body),
                records=0,
                failure_phase=failure_phase,
                outcome="failed",
                retrying=False,
            )
            fail("request deadline exhausted while reading response", attempt, failure_phase)
        if failure_kind is None and elapsed >= REQUEST_BUDGET_SECONDS:
            _emit_attempt(
                attempt=attempt,
                elapsed=elapsed,
                status=status,
                byte_count=len(body),
                records=0,
                failure_phase="read_deadline",
                outcome="failed",
                retrying=False,
            )
            fail("request deadline exhausted while reading response", attempt, "read_deadline")

        if failure_kind is None:
            try:
                document = json.loads(body)
                features = _validate_document(document, requested_count)
            except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
                phase = "parse" if isinstance(error, (UnicodeDecodeError, json.JSONDecodeError)) else "validate"
                _emit_attempt(
                    attempt=attempt,
                    elapsed=_elapsed(clock, started),
                    status=status,
                    byte_count=len(body),
                    records=0,
                    failure_phase=phase,
                    outcome="failed",
                    retrying=False,
                )
                fail(str(error), attempt, phase)
            elapsed = _elapsed(clock, started)
            if elapsed >= REQUEST_BUDGET_SECONDS:
                _emit_attempt(
                    attempt=attempt,
                    elapsed=elapsed,
                    status=status,
                    byte_count=len(body),
                    records=0,
                    failure_phase="parse_deadline",
                    outcome="failed",
                    retrying=False,
                )
                fail("request deadline exhausted while validating response", attempt, "parse_deadline")
            _emit_attempt(
                attempt=attempt,
                elapsed=elapsed,
                status=status,
                byte_count=len(body),
                records=len(features),
                failure_phase=None,
                outcome="succeeded",
                retrying=False,
            )
            if diagnostics is not None:
                diagnostics.update(
                    attempts=attempt,
                    elapsed=elapsed,
                    status=status,
                    byte_count=total_bytes,
                )
            return document

        delay = float(2 ** (attempt - 1))
        remaining = REQUEST_BUDGET_SECONDS - elapsed
        retrying = retryable and attempt < MAX_ATTEMPTS and delay < remaining
        _emit_attempt(
            attempt=attempt,
            elapsed=elapsed,
            status=status,
            byte_count=len(body),
            records=0,
            failure_phase=failure_phase,
            outcome=failure_kind,
            retrying=retrying,
        )
        if not retryable:
            fail("request failed with a non-transient HTTP status", attempt, failure_phase)
        if attempt == MAX_ATTEMPTS:
            fail("transient request failures exhausted all attempts", attempt, failure_phase)
        if not retrying:
            fail("request deadline cannot accommodate retry backoff", attempt, "backoff_deadline")
        sleeper(delay)
        if _elapsed(clock, started) >= REQUEST_BUDGET_SECONDS:
            fail("request deadline exhausted during retry backoff", attempt, "backoff_deadline")

    raise AssertionError("unreachable")


def fetch_recent_eruptions(count: int = 25, *, diagnostics=None):
    """Return fully validated recent eruptions, newest first."""
    document = _fetch_document(
        {
            "service": "WFS",
            "version": "2.0.0",
            "request": "GetFeature",
            "typeName": "GVP-VOTW:E3WebApp_Eruptions1960",
            "outputFormat": "application/json",
            "count": count,
            "sortBy": "StartDate D",
        },
        count,
        diagnostics,
    )
    fetched_at = datetime.now(timezone.utc).isoformat()
    records = []
    for feature in document["features"]:
        properties = feature["properties"]
        records.append(
            {
                "source": "smithsonian_volcanism",
                "fetched_at": fetched_at,
                "id": properties.get("Activity_ID"),
                "volcano_number": properties.get("VolcanoNumber"),
                "volcano_name": properties.get("VolcanoName"),
                "start_date": properties.get("StartDate"),
                "end_date": properties.get("EndDate"),
                "continuing": properties.get("ContinuingEruption") == "True",
                "explosivity_index_max": properties.get("ExplosivityIndexMax"),
                "latitude": properties.get("LatitudeDecimal"),
                "longitude": properties.get("LongitudeDecimal"),
            }
        )
    return records


def main(argv=None):
    arguments = sys.argv[1:] if argv is None else argv
    count = int(arguments[0]) if arguments else 25
    diagnostics = {}
    try:
        records = fetch_recent_eruptions(count=count, diagnostics=diagnostics)
    except SmithsonianFetchError as error:
        summary = {
            "health": "failed",
            "completeness": "failed",
            "records": 0,
            "bytes": error.byte_count,
            "requests": {"attempted": error.attempts, "maximum": MAX_ATTEMPTS},
            "metrics": {
                "elapsed_seconds": round(error.elapsed, 3),
                "status": error.status,
                "failure_phase": error.failure_phase,
            },
            "error": str(error),
        }
        print(SUMMARY_PREFIX + json.dumps(summary, sort_keys=True), file=sys.stderr)
        return 1

    for record in records:
        print(json.dumps(record, ensure_ascii=False))
    summary = {
        "health": "healthy",
        "completeness": "complete",
        "records": len(records),
        "bytes": diagnostics["byte_count"],
        "requests": {"attempted": diagnostics["attempts"], "maximum": MAX_ATTEMPTS},
        "metrics": {
            "elapsed_seconds": round(diagnostics["elapsed"], 3),
            "status": diagnostics["status"],
            "failure_phase": None,
        },
    }
    print(SUMMARY_PREFIX + json.dumps(summary, sort_keys=True), file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
