#!/usr/bin/env python3
"""Fetch one deliberately bounded Wikidata Query Service SPARQL result set.

The service has no cursor protocol.  Each run therefore makes exactly one GET
request and requires an explicit LIMIT of at most ``MAX_RESULTS`` in the query.
"""
import argparse
import hashlib
import json
import os
import re
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any, Callable, Iterator

ENDPOINT = "https://query.wikidata.org/sparql"
SOURCE = "wikidata_query_service"
DEFAULT_TIMEOUT_SECONDS = 30
MAX_TIMEOUT_SECONDS = 60
MAX_QUERY_CHARS = 12_000
MAX_RESULTS = 100
SUMMARY_PREFIX = "VINTAGE_RUN_SUMMARY\t"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
_LIMIT_RE = re.compile(r"\bLIMIT\s+(\d+)\b", re.IGNORECASE)


class ResponseError(ValueError):
    """The endpoint returned JSON that is not a SPARQL results response."""


def query_limit(query: str) -> int:
    """Validate the fixed upper bound required for every service request."""
    if not isinstance(query, str) or not query.strip():
        raise ValueError("query must be a non-empty string")
    if len(query) > MAX_QUERY_CHARS:
        raise ValueError(f"query exceeds {MAX_QUERY_CHARS} characters")
    limits = _LIMIT_RE.findall(query)
    if len(limits) != 1:
        raise ValueError("query must contain exactly one explicit LIMIT")
    limit = int(limits[0])
    if not 1 <= limit <= MAX_RESULTS:
        raise ValueError(f"query LIMIT must be between 1 and {MAX_RESULTS}")
    return limit


def normalize_response(payload: Any) -> tuple[list[str], list[dict[str, str | None]]]:
    """Validate SPARQL JSON and flatten RDF terms to their string values."""
    if not isinstance(payload, dict):
        raise ResponseError("response must be a JSON object")
    head = payload.get("head")
    results = payload.get("results")
    if not isinstance(head, dict) or not isinstance(results, dict):
        raise ResponseError("response must contain head and results objects")
    variables = head.get("vars")
    bindings = results.get("bindings")
    if (not isinstance(variables, list) or not variables
            or any(not isinstance(variable, str) or not variable for variable in variables)
            or len(set(variables)) != len(variables)):
        raise ResponseError("head.vars must be a non-empty list of unique strings")
    if not isinstance(bindings, list):
        raise ResponseError("results.bindings must be a list")

    variable_set = set(variables)
    normalized: list[dict[str, str | None]] = []
    for index, binding in enumerate(bindings):
        if not isinstance(binding, dict):
            raise ResponseError(f"binding {index} must be an object")
        unknown = set(binding) - variable_set
        if unknown:
            raise ResponseError(f"binding {index} contains undeclared variables")
        row: dict[str, str | None] = {}
        for variable in variables:
            term = binding.get(variable)
            if term is None:
                row[variable] = None
                continue
            if not isinstance(term, dict) or not isinstance(term.get("value"), str):
                raise ResponseError(f"binding {index}.{variable} must contain a string value")
            row[variable] = term["value"]
        normalized.append(row)
    return variables, normalized


def stable_record_id(binding: dict[str, str | None]) -> str:
    """Return a source-scoped identifier independent of response key ordering."""
    canonical = json.dumps(binding, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return f"{SOURCE}:{hashlib.sha256(canonical.encode('utf-8')).hexdigest()}"


def fetch(query: str, *, timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
          opener: Callable[..., Any] = urllib.request.urlopen,
          fetched_at: str | None = None) -> tuple[list[str], Iterator[dict[str, Any]]]:
    """Request and normalize one bounded query result; never follows pagination."""
    limit = query_limit(query)
    if not isinstance(timeout_seconds, int) or not 1 <= timeout_seconds <= MAX_TIMEOUT_SECONDS:
        raise ValueError(f"timeout_seconds must be between 1 and {MAX_TIMEOUT_SECONDS}")
    params = urllib.parse.urlencode({"query": query, "format": "json"})
    request = urllib.request.Request(
        f"{ENDPOINT}?{params}",
        headers={"Accept": "application/sparql-results+json", "User-Agent": USER_AGENT},
    )
    with opener(request, timeout=timeout_seconds) as response:
        payload = json.load(response)
    variables, bindings = normalize_response(payload)
    timestamp = fetched_at or datetime.now(timezone.utc).isoformat()

    def records() -> Iterator[dict[str, Any]]:
        for binding in bindings:
            yield {
                "source": SOURCE,
                "fetched_at": timestamp,
                "id": stable_record_id(binding),
                **binding,
            }

    # `limit` is deliberately evaluated before the request so invalid queries
    # cannot reach the shared service.
    del limit
    return variables, records()


def run(query: str, timeout_seconds: int) -> int:
    """Emit NDJSON records and structured run metadata."""
    try:
        limit = query_limit(query)
        variables, records = fetch(query, timeout_seconds=timeout_seconds)
        count = 0
        for record in records:
            print(json.dumps(record, ensure_ascii=False, separators=(",", ":")))
            count += 1
    except Exception as exc:
        print(SUMMARY_PREFIX + json.dumps({
            "health": "failed", "completeness": "failed", "records": 0,
            "requests": {"attempted": 0 if isinstance(exc, ValueError) else 1},
            "error": str(exc),
        }, separators=(",", ":")), file=sys.stderr)
        return 1
    print(SUMMARY_PREFIX + json.dumps({
        "health": "healthy", "completeness": "complete", "records": count,
        "requests": {"attempted": 1},
        "metrics": {"endpoint": ENDPOINT, "limit": limit, "variables": variables,
                    "pagination": "none"},
    }, separators=(",", ":")), file=sys.stderr)
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--query", required=True, help="SPARQL query with an explicit bounded LIMIT")
    parser.add_argument("--timeout-seconds", type=int, default=DEFAULT_TIMEOUT_SECONDS)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    raise SystemExit(run(args.query, args.timeout_seconds))
