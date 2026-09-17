#!/usr/bin/env python3
"""Run one bounded SELECT query against Wikidata Query Service and emit NDJSON.

The supported query contract is deliberately narrow: one outer SELECT/WHERE query
with one positive outer LIMIT. A small lexer distinguishes that LIMIT from text in
comments, literals, IRIs, and nested subqueries before any network access occurs.
Each invocation performs exactly one request and never follows SPARQL pagination.

Wikidata data is provided under CC0. Attribute Wikidata and its contributors when
redistributing extracts, and follow the WDQS usage policy. Stdlib only.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any, Iterator, Sequence

SOURCE = "wikidata_query_service"
ENDPOINT = "https://query.wikidata.org/sparql"
SUMMARY_PREFIX = "VINTAGE_RUN_SUMMARY\t"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or (
    "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
)

DEFAULT_QUERY = """SELECT ?country ?countryLabel ?capital ?capitalLabel WHERE {
  VALUES ?country { wd:Q30 wd:Q145 wd:Q183 wd:Q142 wd:Q38 }
  ?country wdt:P36 ?capital .
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
}
ORDER BY ?country ?capital
LIMIT 100"""
DEFAULT_TIMEOUT = 60.0
MAX_TIMEOUT = 120.0
MAX_QUERY_LENGTH = 10_000
MAX_URL_LENGTH = 30_000
MAX_RESPONSE_BYTES = 4 * 1024 * 1024
MAX_ROWS = 500

# Mutable in place so monitoring callers may retain a reference between runs.
LAST_RUN_METADATA: dict[str, Any] = {}

_TOKEN_BREAKS = frozenset("{}()[]'\"<#;,.")
_QUERY_FORMS = frozenset(
    {
        "ASK",
        "CONSTRUCT",
        "DESCRIBE",
        "INSERT",
        "DELETE",
        "LOAD",
        "CLEAR",
        "CREATE",
        "DROP",
        "COPY",
        "MOVE",
        "ADD",
        "WITH",
    }
)
_HEX = frozenset("0123456789abcdefABCDEF")


def _consume_escape(query: str, position: int, *, iri: bool) -> int:
    """Return the first position after one checked SPARQL escape."""
    if position + 1 >= len(query):
        raise ValueError("query ends with an incomplete escape")
    marker = query[position + 1]
    if marker in {"u", "U"}:
        width = 4 if marker == "u" else 8
        digits = query[position + 2 : position + 2 + width]
        if len(digits) != width or any(char not in _HEX for char in digits):
            raise ValueError("query contains an invalid Unicode escape")
        code_point = int(digits, 16)
        if code_point > 0x10FFFF or 0xD800 <= code_point <= 0xDFFF:
            raise ValueError("query contains an invalid Unicode code point")
        return position + 2 + width
    if iri:
        raise ValueError("IRI contains an invalid escape")
    if marker not in "tbnrf\\\"'":
        raise ValueError("quoted literal contains an invalid escape")
    return position + 2


def _lex_query(query: str) -> list[tuple[str, int, int, int]]:
    """Lex only enough SPARQL to locate clauses at the outer query level.

    Returned depths describe the position immediately before each token. Quoted
    values, IRIs, and comments are consumed as opaque lexical units. This is not a
    general SPARQL parser; malformed or unsupported lexical constructs fail closed.
    """
    tokens: list[tuple[str, int, int, int]] = []
    stack: list[str] = []
    depths = {"{": 0, "(": 0, "[": 0}
    closing = {"}": "{", ")": "(", "]": "["}
    position = 0

    def emit(text: str) -> None:
        tokens.append((text, depths["{"], depths["("], depths["["]))

    while position < len(query):
        char = query[position]
        if char.isspace():
            position += 1
            continue
        if char == "#":
            newline = query.find("\n", position + 1)
            position = len(query) if newline < 0 else newline + 1
            continue
        if char in {"'", '"'}:
            delimiter = char * 3 if query.startswith(char * 3, position) else char
            position += len(delimiter)
            while position < len(query):
                if query.startswith(delimiter, position):
                    position += len(delimiter)
                    emit("<literal>")
                    break
                current = query[position]
                if current == "\\":
                    position = _consume_escape(query, position, iri=False)
                    continue
                if len(delimiter) == 1 and current in "\r\n":
                    raise ValueError("short quoted literal contains a newline")
                position += 1
            else:
                raise ValueError("query contains an unterminated quoted literal")
            continue
        if char == "<":
            position += 1
            while position < len(query):
                current = query[position]
                if current == ">":
                    position += 1
                    emit("<iri>")
                    break
                if current == "\\":
                    position = _consume_escape(query, position, iri=True)
                    continue
                if ord(current) <= 0x20 or current in '<"{}|^`':
                    raise ValueError("query contains a malformed IRI")
                position += 1
            else:
                raise ValueError("query contains an unterminated IRI")
            continue
        if char in "{([":
            emit(char)
            stack.append(char)
            depths[char] += 1
            position += 1
            continue
        if char in "})]":
            expected = closing[char]
            if not stack or stack[-1] != expected:
                raise ValueError("query contains unbalanced delimiters")
            emit(char)
            stack.pop()
            depths[expected] -= 1
            position += 1
            continue
        if char in ";,.":
            emit(char)
            position += 1
            continue

        start = position
        while position < len(query):
            current = query[position]
            if current.isspace() or current in _TOKEN_BREAKS or current in ")>]":
                break
            position += 1
        if position == start:
            emit(char)
            position += 1
        else:
            emit(query[start:position])

    if stack:
        raise ValueError("query contains unbalanced delimiters")
    return tokens


def validate_query(query: str) -> int:
    """Validate the supported query shape and return its outer LIMIT."""
    if not isinstance(query, str):
        raise TypeError("query must be a string")
    if not query.strip():
        raise ValueError("query must not be empty")
    if len(query) > MAX_QUERY_LENGTH:
        raise ValueError(f"query exceeds the {MAX_QUERY_LENGTH}-character limit")
    if any(0xD800 <= ord(char) <= 0xDFFF for char in query):
        raise ValueError("query contains an invalid Unicode code point")

    tokens = _lex_query(query)

    def outer(token: tuple[str, int, int, int]) -> bool:
        return token[1:] == (0, 0, 0)

    outer_tokens = [
        (index, token[0])
        for index, token in enumerate(tokens)
        if outer(token)
    ]
    outer_words = [(index, text.upper()) for index, text in outer_tokens]
    prohibited = [word for _, word in outer_words if word in _QUERY_FORMS]
    if prohibited:
        raise ValueError(f"unsupported outer query form: {prohibited[0]}")

    selects = [index for index, word in outer_words if word == "SELECT"]
    if len(selects) != 1:
        raise ValueError("query must contain exactly one outer SELECT")
    select_index = selects[0]

    # Only PREFIX/BASE declarations may precede SELECT. Parsing the complete
    # graph pattern is intentionally left to WDQS, but the outer skeleton stays
    # small enough that clause depth and the row bound are unambiguous.
    prologue = [text for index, text in outer_tokens if index < select_index]
    position = 0
    while position < len(prologue):
        keyword = prologue[position].upper()
        if keyword == "PREFIX" and position + 2 < len(prologue):
            if re.fullmatch(r"(?:[A-Za-z][A-Za-z0-9_-]*)?:", prologue[position + 1]) is None:
                raise ValueError("unsupported or malformed query prologue")
            if prologue[position + 2] != "<iri>":
                raise ValueError("unsupported or malformed query prologue")
            position += 3
        elif keyword == "BASE" and position + 1 < len(prologue):
            if prologue[position + 1] != "<iri>":
                raise ValueError("unsupported or malformed query prologue")
            position += 2
        else:
            raise ValueError("unsupported or malformed query prologue")

    wheres = [
        index for index, word in outer_words if word == "WHERE" and index > select_index
    ]
    if len(wheres) != 1:
        raise ValueError("outer SELECT must contain exactly one WHERE clause")
    where_index = wheres[0]

    projection = [text for index, text in outer_tokens if select_index < index < where_index]
    if projection and projection[0].upper() in {"DISTINCT", "REDUCED"}:
        projection = projection[1:]
    variable = re.compile(r"[?$][A-Za-z_][A-Za-z0-9_]*").fullmatch
    if not projection or not (
        projection == ["*"] or all(variable(item) is not None for item in projection)
    ):
        raise ValueError("outer SELECT projection must contain only variables or *")

    limits = [index for index, word in outer_words if word == "LIMIT"]
    if len(limits) != 1:
        raise ValueError("query must contain exactly one outer LIMIT")
    limit_index = limits[0]

    outer_groups = [
        index
        for index, token in enumerate(tokens)
        if token[0] == "{" and outer(token) and where_index < index < limit_index
    ]
    if len(outer_groups) != 1:
        raise ValueError("outer WHERE must contain exactly one graph-pattern group")
    group_start = outer_groups[0]
    if group_start != where_index + 1:
        raise ValueError("outer WHERE must be followed immediately by its graph pattern")
    group_ends = [
        index
        for index, token in enumerate(tokens)
        if token[0] == "}"
        and token[1:] == (1, 0, 0)
        and group_start < index < limit_index
    ]
    if len(group_ends) != 1:
        raise ValueError("outer LIMIT must follow the complete WHERE graph pattern")
    group_end = group_ends[0]

    suffix = [token[0] for token in tokens[group_end + 1 : limit_index]]
    if suffix:
        if len(suffix) < 3 or [item.upper() for item in suffix[:2]] != ["ORDER", "BY"]:
            raise ValueError("only an outer ORDER BY may appear before LIMIT")
        if not all(variable(item) is not None for item in suffix[2:]):
            raise ValueError("outer ORDER BY supports variables only")

    if limit_index + 1 >= len(tokens):
        raise ValueError("outer LIMIT must have an integer value")
    limit_token = tokens[limit_index + 1]
    if not outer(limit_token) or re.fullmatch(r"[0-9]+", limit_token[0]) is None:
        raise ValueError("outer LIMIT must have a positive decimal integer value")
    if limit_index + 2 != len(tokens):
        raise ValueError("unsupported syntax after outer LIMIT")

    limit = int(limit_token[0])
    if not 1 <= limit <= MAX_ROWS:
        raise ValueError(f"outer LIMIT must be between 1 and {MAX_ROWS}")
    return limit


def _bounded_body(response: Any) -> bytes:
    headers = getattr(response, "headers", None)
    content_length = headers.get("Content-Length") if headers is not None else None
    if content_length is not None:
        try:
            declared_length = int(content_length)
        except (TypeError, ValueError) as exc:
            raise ValueError("WDQS response has an invalid Content-Length") from exc
        if declared_length < 0 or declared_length > MAX_RESPONSE_BYTES:
            raise ValueError("WDQS response exceeds the response-size limit")

    content_type = headers.get("Content-Type") if headers is not None else None
    if content_type:
        media_type = content_type.split(";", 1)[0].strip().lower()
        if media_type not in {"application/json", "application/sparql-results+json"}:
            raise ValueError(f"WDQS returned unsupported content type {media_type!r}")

    body = response.read(MAX_RESPONSE_BYTES + 1)
    if len(body) > MAX_RESPONSE_BYTES:
        raise ValueError("WDQS response exceeds the response-size limit")
    return body


def _normalize_binding(binding: Any, variables: tuple[str, ...], row_number: int) -> dict[str, Any]:
    if not isinstance(binding, dict):
        raise ValueError(f"WDQS binding {row_number} is not an object")
    unknown = set(binding) - set(variables)
    if unknown:
        raise ValueError(f"WDQS binding {row_number} contains undeclared variables")

    normalized: dict[str, Any] = {}
    for variable in variables:
        if variable not in binding:
            continue
        value = binding[variable]
        if not isinstance(value, dict):
            raise ValueError(f"WDQS binding {row_number}.{variable} is not an object")
        if set(value) - {"type", "value", "xml:lang", "datatype"}:
            raise ValueError(f"WDQS binding {row_number}.{variable} has unknown fields")
        value_type = value.get("type")
        text = value.get("value")
        if value_type not in {"uri", "literal", "typed-literal", "bnode"}:
            raise ValueError(f"WDQS binding {row_number}.{variable} has an invalid type")
        if not isinstance(text, str):
            raise ValueError(f"WDQS binding {row_number}.{variable} has an invalid value")
        language = value.get("xml:lang")
        datatype = value.get("datatype")
        if language is not None and (not isinstance(language, str) or not language):
            raise ValueError(f"WDQS binding {row_number}.{variable} has an invalid language")
        if datatype is not None and (not isinstance(datatype, str) or not datatype):
            raise ValueError(f"WDQS binding {row_number}.{variable} has an invalid datatype")
        if value_type in {"uri", "bnode"} and (language is not None or datatype is not None):
            raise ValueError(f"WDQS binding {row_number}.{variable} has invalid annotations")
        if language is not None and datatype is not None:
            raise ValueError(f"WDQS binding {row_number}.{variable} has conflicting annotations")
        normalized[variable] = dict(value)
    return normalized


def parse_response(
    document: Any, *, query_sha256: str, fetched_at: str, row_limit: int
) -> list[dict[str, Any]]:
    """Validate a SPARQL Results JSON document and build stable envelopes."""
    if not isinstance(document, dict):
        raise ValueError("WDQS response is not an object")
    head = document.get("head")
    results = document.get("results")
    if not isinstance(head, dict) or not isinstance(results, dict):
        raise ValueError("WDQS response is missing head or results")
    variables_value = head.get("vars")
    bindings = results.get("bindings")
    if not isinstance(variables_value, list) or not all(
        isinstance(variable, str) and variable for variable in variables_value
    ):
        raise ValueError("WDQS response head.vars is invalid")
    variables = tuple(variables_value)
    if len(set(variables)) != len(variables):
        raise ValueError("WDQS response head.vars contains duplicates")
    if not isinstance(bindings, list):
        raise ValueError("WDQS response results.bindings is invalid")
    if len(bindings) > row_limit or len(bindings) > MAX_ROWS:
        raise ValueError("WDQS response exceeds the bounded row count")

    records: list[dict[str, Any]] = []
    for row_number, binding in enumerate(bindings, start=1):
        normalized = _normalize_binding(binding, variables, row_number)
        identity = json.dumps(
            {"query_sha256": query_sha256, "binding": normalized},
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        records.append(
            {
                "source": SOURCE,
                "fetched_at": fetched_at,
                "id": hashlib.sha256(identity).hexdigest(),
                "query_sha256": query_sha256,
                "binding": normalized,
            }
        )
    return records


def fetch_wikidata_query_service(
    query: str = DEFAULT_QUERY, timeout: float = DEFAULT_TIMEOUT
) -> Iterator[dict[str, Any]]:
    """Execute one validated, bounded WDQS request and yield normalized records."""
    LAST_RUN_METADATA.clear()
    LAST_RUN_METADATA.update(
        {
            "source": SOURCE,
            "endpoint": ENDPOINT,
            "records": 0,
            "response_bytes": 0,
            "requests": {"attempted": 0},
        }
    )
    if (
        isinstance(timeout, bool)
        or not isinstance(timeout, (int, float))
        or not math.isfinite(timeout)
        or not 0 < timeout <= MAX_TIMEOUT
    ):
        raise ValueError(f"timeout must be greater than zero and at most {MAX_TIMEOUT:g} seconds")

    row_limit = validate_query(query)
    query_sha256 = hashlib.sha256(query.encode("utf-8")).hexdigest()
    parameters = urllib.parse.urlencode({"query": query, "format": "json"})
    url = f"{ENDPOINT}?{parameters}"
    if len(url) > MAX_URL_LENGTH:
        raise ValueError(f"encoded query URL exceeds the {MAX_URL_LENGTH}-character limit")

    LAST_RUN_METADATA.update(
        {"query_sha256": query_sha256, "row_limit": row_limit, "url_length": len(url)}
    )
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/sparql-results+json",
            "User-Agent": USER_AGENT,
        },
    )
    fetched_at = datetime.now(timezone.utc).isoformat()
    LAST_RUN_METADATA["requests"]["attempted"] = 1
    with urllib.request.urlopen(request, timeout=timeout) as response:
        body = _bounded_body(response)
        status = getattr(response, "status", None)
    LAST_RUN_METADATA["response_bytes"] = len(body)
    if status is not None:
        LAST_RUN_METADATA["status"] = status

    try:
        document = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("WDQS response is not valid UTF-8 JSON") from exc
    records = parse_response(
        document,
        query_sha256=query_sha256,
        fetched_at=fetched_at,
        row_limit=row_limit,
    )
    LAST_RUN_METADATA["records"] = len(records)
    yield from records


def _summary(*, failed: bool = False, error: str | None = None) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "health": "failed" if failed else "healthy",
        "completeness": "failed" if failed else "complete",
        "records": LAST_RUN_METADATA.get("records", 0),
        "requests": dict(LAST_RUN_METADATA.get("requests", {"attempted": 0})),
        "metrics": {
            key: value
            for key, value in LAST_RUN_METADATA.items()
            if key not in {"source", "records", "requests"}
        },
    }
    if error is not None:
        summary["error"] = error
    return summary


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--query", default=DEFAULT_QUERY)
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT)
    args = parser.parse_args(argv)

    try:
        for record in fetch_wikidata_query_service(args.query, args.timeout):
            print(json.dumps(record, ensure_ascii=False, separators=(",", ":")))
    except Exception as exc:
        print(SUMMARY_PREFIX + json.dumps(_summary(failed=True, error=str(exc))), file=sys.stderr)
        return 1

    print(SUMMARY_PREFIX + json.dumps(_summary()), file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
