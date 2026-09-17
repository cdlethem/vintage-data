#!/usr/bin/env python3
"""Fetch one explicitly bounded SPARQL SELECT query from Wikidata.

The query is configuration, not a built-in bulk export.  Scheduled queries must end
with a numeric LIMIT no greater than ``MAX_QUERY_LIMIT``.  Each run makes exactly one
GET request; pagination is intentionally unsupported so a query cannot silently grow
into an unbounded Wikidata crawl.

Stdlib only.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any, Iterable

SOURCE = "wikidata_query_service"
URL = "https://query.wikidata.org/sparql"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
SUMMARY_PREFIX = "VINTAGE_RUN_SUMMARY\t"
DEFAULT_TIMEOUT_SECONDS = 30
MAX_TIMEOUT_SECONDS = 120
MAX_QUERY_BYTES = 8_192
MAX_URL_LENGTH = 32_768
MAX_RESPONSE_BYTES = 2 * 2**20
MAX_QUERY_LIMIT = 100
RESERVED_FIELDS = frozenset(("source", "fetched_at", "id", "binding_metadata"))
STRICT_LIMIT = re.compile(r"\bLIMIT\s+([0-9]+)\s*\Z", re.IGNORECASE)


def validate_query(query: str) -> tuple[str, int]:
    """Return the normalized query and its mandatory final LIMIT."""
    if not isinstance(query, str) or not query.strip():
        raise ValueError("SPARQL query must be a non-empty string")
    query = query.strip()
    if len(query.encode("utf-8")) > MAX_QUERY_BYTES:
        raise ValueError(f"SPARQL query exceeds {MAX_QUERY_BYTES} bytes")
    match = STRICT_LIMIT.search(query)
    if match is None:
        raise ValueError("SPARQL query must end with an explicit numeric LIMIT")
    limit = int(match.group(1))
    if not 1 <= limit <= MAX_QUERY_LIMIT:
        raise ValueError(f"SPARQL LIMIT must be between 1 and {MAX_QUERY_LIMIT}")
    return query, limit


def validate_timeout(timeout: int) -> int:
    if isinstance(timeout, bool) or not isinstance(timeout, int):
        raise ValueError("request timeout must be an integer number of seconds")
    if not 1 <= timeout <= MAX_TIMEOUT_SECONDS:
        raise ValueError(f"request timeout must be between 1 and {MAX_TIMEOUT_SECONDS} seconds")
    return timeout


def _get(query: str, timeout: int) -> Any:
    params = urllib.parse.urlencode({"query": query, "format": "json"})
    url = f"{URL}?{params}"
    if len(url) > MAX_URL_LENGTH:
        raise ValueError(f"encoded request URL exceeds {MAX_URL_LENGTH} characters")
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "application/sparql-results+json",
        },
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        body = response.read(MAX_RESPONSE_BYTES + 1)
    if len(body) > MAX_RESPONSE_BYTES:
        raise ValueError(f"Wikidata response exceeds {MAX_RESPONSE_BYTES} bytes")
    try:
        return json.loads(body)
    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        raise ValueError("Wikidata response is not valid JSON") from error


def _binding_value(binding: Any, variable: str, row_number: int) -> tuple[str, dict[str, str]]:
    if not isinstance(binding, dict):
        raise ValueError(f"binding {row_number}.{variable} is not an object")
    binding_type = binding.get("type")
    value = binding.get("value")
    if not isinstance(binding_type, str) or not binding_type:
        raise ValueError(f"binding {row_number}.{variable} has no type")
    if not isinstance(value, str):
        raise ValueError(f"binding {row_number}.{variable} has no string value")

    metadata = {"type": binding_type}
    datatype = binding.get("datatype")
    language = binding.get("xml:lang")
    if datatype is not None:
        if not isinstance(datatype, str):
            raise ValueError(f"binding {row_number}.{variable} has a non-string datatype")
        metadata["datatype"] = datatype
    if language is not None:
        if not isinstance(language, str):
            raise ValueError(f"binding {row_number}.{variable} has a non-string language")
        metadata["language"] = language
    return value, metadata


def normalize_response(
    document: Any,
    *,
    id_variable: str,
    fetched_at: str,
    query_limit: int,
) -> list[dict[str, Any]]:
    """Validate SPARQL Results JSON and flatten each binding into an envelope."""
    if not isinstance(document, dict):
        raise ValueError("Wikidata response is not an object")
    head = document.get("head")
    results = document.get("results")
    if not isinstance(head, dict) or not isinstance(results, dict):
        raise ValueError("Wikidata response is missing head or results")
    variables = head.get("vars")
    bindings = results.get("bindings")
    if (
        not isinstance(variables, list)
        or not variables
        or not all(isinstance(variable, str) and variable for variable in variables)
        or len(set(variables)) != len(variables)
    ):
        raise ValueError("Wikidata response has invalid head.vars")
    if id_variable not in variables:
        raise ValueError(f"Wikidata response does not declare ID variable {id_variable!r}")
    collisions = RESERVED_FIELDS.intersection(variables)
    if collisions:
        raise ValueError(f"Wikidata variables collide with envelope fields: {sorted(collisions)}")
    if not isinstance(bindings, list):
        raise ValueError("Wikidata response has invalid results.bindings")
    if len(bindings) > query_limit:
        raise ValueError("Wikidata response contains more rows than the query LIMIT")

    records: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    declared = set(variables)
    for row_number, binding_row in enumerate(bindings):
        if not isinstance(binding_row, dict):
            raise ValueError(f"Wikidata binding row {row_number} is not an object")
        unexpected = set(binding_row).difference(declared)
        if unexpected:
            raise ValueError(f"Wikidata binding row {row_number} has undeclared variables: {sorted(unexpected)}")

        values: dict[str, str | None] = {}
        metadata: dict[str, dict[str, str]] = {}
        for variable in variables:
            binding = binding_row.get(variable)
            if binding is None:
                values[variable] = None
                continue
            value, details = _binding_value(binding, variable, row_number)
            values[variable] = value
            metadata[variable] = details

        record_id = values[id_variable]
        if not record_id:
            raise ValueError(f"Wikidata binding row {row_number} has no value for ID variable {id_variable!r}")
        if record_id in seen_ids:
            raise ValueError(f"Wikidata response contains duplicate ID {record_id!r}")
        seen_ids.add(record_id)
        record: dict[str, Any] = {
            "source": SOURCE,
            "fetched_at": fetched_at,
            "id": record_id,
        }
        record.update(values)
        record["binding_metadata"] = metadata
        records.append(record)
    return records


def fetch_query(
    query: str,
    *,
    id_variable: str = "item",
    timeout: int = DEFAULT_TIMEOUT_SECONDS,
    fetched_at: str | None = None,
) -> Iterable[dict[str, Any]]:
    query, query_limit = validate_query(query)
    timeout = validate_timeout(timeout)
    if not isinstance(id_variable, str) or not id_variable:
        raise ValueError("ID variable must be a non-empty string")
    timestamp = fetched_at or datetime.now(timezone.utc).isoformat()
    document = _get(query, timeout)
    yield from normalize_response(
        document,
        id_variable=id_variable,
        fetched_at=timestamp,
        query_limit=query_limit,
    )


def _summary(*, health: str, completeness: str, records: int, requests: int, query_limit: int | None, error: BaseException | None = None) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "health": health,
        "completeness": completeness,
        "records": records,
        "requests": {"attempted": requests},
        "metrics": {
            "endpoint": URL,
            "query_limit": query_limit,
            "pagination": "disabled",
        },
    }
    if error is not None:
        summary["error"] = type(error).__name__
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--query", required=True)
    parser.add_argument("--id-variable", default="item")
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT_SECONDS)
    args = parser.parse_args(argv)

    requests = 0
    query_limit: int | None = None
    try:
        _, query_limit = validate_query(args.query)
        validate_timeout(args.timeout)
        requests = 1
        records = list(
            fetch_query(
                args.query,
                id_variable=args.id_variable,
                timeout=args.timeout,
            )
        )
    except Exception as error:
        print(
            SUMMARY_PREFIX
            + json.dumps(
                _summary(
                    health="failed",
                    completeness="failed",
                    records=0,
                    requests=requests,
                    query_limit=query_limit,
                    error=error,
                ),
                separators=(",", ":"),
            ),
            file=sys.stderr,
        )
        return 1

    for record in records:
        print(json.dumps(record, ensure_ascii=False, separators=(",", ":")))
    print(
        SUMMARY_PREFIX
        + json.dumps(
            _summary(
                health="healthy",
                completeness="complete",
                records=len(records),
                requests=requests,
                query_limit=query_limit,
            ),
            separators=(",", ":"),
        ),
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
