"""Automatic schema detection, in a type vocabulary no warehouse owns.

Inference runs in Python over a sample of each file's records and emits
*canonical* types; a destination maps those to its own DDL. That split is what
keeps the layer portable — DuckDB today, BigQuery or Snowflake later, with the
inference untouched.

Two rules make schema drift survivable without a user-supplied schema:

* every record is also stored verbatim in ``_payload``, so a column that was
  never inferred (a key absent from the sample, a type that widened later) is
  always recoverable downstream;
* conflicting observations widen to the most permissive type rather than
  failing the load, and values that don't fit a column land as NULL via the
  destination's safe cast — never as a failed batch.
"""
from __future__ import annotations

import re
from dataclasses import dataclass

# Canonical types. A destination maps each to one native type.
STRING = "string"
INTEGER = "integer"
DOUBLE = "double"
BOOLEAN = "boolean"
DATE = "date"
TIMESTAMP = "timestamp"
JSON = "json"

CANONICAL_TYPES = (STRING, INTEGER, DOUBLE, BOOLEAN, DATE, TIMESTAMP, JSON)

_TS_RE = re.compile(r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(:\d{2}(\.\d+)?)?(Z|[+-]\d{2}:?\d{2})?$")
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

_IDENT_RE = re.compile(r"[^a-z0-9_]+")

#: Reserved prefix. Metadata columns own it; a payload key starting with ``_``
#: is renamed so lineage columns can never be shadowed by source data.
META_PREFIX = "_"


@dataclass(frozen=True)
class Column:
    """One warehouse column and where its value comes from."""

    name: str                 # column name in the warehouse
    type: str                 # canonical type
    key: str | None = None    # top-level JSON key, None for metadata columns
    meta: bool = False
    #: the sample saw this key, but only ever as null — so its type is a guess.
    #: A tentative column is never created and never widens anything; the
    #: layer waits for a real value rather than pinning the column to VARCHAR
    #: (or, worse, widening a good column to VARCHAR because one batch of a
    #: chatty source happened to be all nulls).
    tentative: bool = False


#: Row-level lineage, written on every row of every RAW table.
#:
#: ``_row_id`` is deterministic (file + line), so the same physical record
#: always gets the same identity; ``_batch_id`` is the extract run that
#: produced the file, and joins to ``_load.files``; ``_load_id`` is the loader
#: job that inserted it, and joins to ``_load.jobs``.
META_COLUMNS: tuple[Column, ...] = (
    Column("_row_id", STRING, meta=True),
    Column("_source", STRING, meta=True),
    Column("_batch_id", STRING, meta=True),
    Column("_source_file", STRING, meta=True),
    Column("_file_row_num", INTEGER, meta=True),
    Column("_dt", DATE, meta=True),
    Column("_extract_started_at", TIMESTAMP, meta=True),
    Column("_load_id", STRING, meta=True),
    Column("_loaded_at", TIMESTAMP, meta=True),
    Column("_content_hash", STRING, meta=True),
)

PAYLOAD_COLUMN = Column("_payload", JSON, meta=True)


def sanitize(name: str, *, fallback: str = "field") -> str:
    """Fold an arbitrary JSON key into a portable lowercase identifier."""
    out = _IDENT_RE.sub("_", name.strip().lower()).strip("_")
    if not out:
        out = fallback
    if out[0].isdigit():
        out = f"f_{out}"
    return out


def _scalar_type(value, detect_temporal: bool) -> str:
    if isinstance(value, bool):
        return BOOLEAN
    if isinstance(value, int):
        return INTEGER
    if isinstance(value, float):
        return DOUBLE
    if isinstance(value, str):
        if detect_temporal and _TS_RE.match(value):
            return TIMESTAMP
        if detect_temporal and _DATE_RE.match(value):
            return DATE
        return STRING
    return JSON  # dict or list


def widen(a: str, b: str) -> str:
    """Least common type of two observations.

    ``string`` is the absorbing element: it can hold any value, and a
    destination can always widen an existing column to it. Deliberately *not*
    string-to-json, which would need every historical value to be valid JSON.
    """
    if a == b:
        return a
    pair = {a, b}
    if pair == {INTEGER, DOUBLE}:
        return DOUBLE
    if pair == {DATE, TIMESTAMP}:
        return TIMESTAMP
    return STRING


def infer_columns(records, settings) -> list[Column]:
    """Infer typed columns from sampled records.

    Keys are kept in first-seen order so a table's column order mirrors the
    shape of the source's records. Forced ``column_types`` win outright, and
    are emitted even if the sample never saw the key (the envelope keys are
    configured this way, so every RAW table has ``source``/``id``/``fetched_at``
    with the same type).
    """
    forced = {k: v.lower() for k, v in (settings.column_types or {}).items()}
    for key, type_ in forced.items():
        if type_ not in CANONICAL_TYPES:
            raise ValueError(f"{settings.name}: column_types[{key!r}] = {type_!r} "
                             f"is not one of {CANONICAL_TYPES}")
    excluded = set(settings.exclude_keys or ())

    observed: dict[str, str | None] = {k: None for k in forced if k not in excluded}
    if settings.schema_detection == "auto":
        for record in records:
            if not isinstance(record, dict):
                continue
            for key, value in record.items():
                if key in excluded or key in forced:
                    observed.setdefault(key, None)
                    continue
                if value is None:
                    observed.setdefault(key, None)
                    continue
                seen = _scalar_type(value, settings.detect_temporal)
                current = observed.get(key)
                observed[key] = seen if current is None else widen(current, seen)
    elif settings.schema_detection != "payload_only":
        raise ValueError(f"{settings.name}: unknown schema_detection "
                         f"{settings.schema_detection!r} (auto | payload_only)")

    columns: list[Column] = []
    used = {c.name for c in META_COLUMNS} | {PAYLOAD_COLUMN.name}
    for i, (key, type_) in enumerate(observed.items()):
        name = sanitize(key, fallback=f"field_{i}")
        if name.startswith(META_PREFIX):
            name = f"f{name}"
        base, n = name, 2
        while name in used:
            name, n = f"{base}_{n}", n + 1
        used.add(name)
        columns.append(Column(name, forced.get(key) or type_ or STRING, key=key,
                              tentative=key not in forced and type_ is None))
    return columns


def table_columns(inferred: list[Column], settings) -> list[Column]:
    """Full column list for a RAW table: lineage, payload, then source fields."""
    columns = list(META_COLUMNS)
    if settings.keep_payload:
        columns.append(PAYLOAD_COLUMN)
    return columns + inferred


def plan_evolution(existing: dict[str, str], wanted: list[Column]):
    """Diff a live table against what inference wants now.

    Returns ``(added, widened)``. Existing columns are never narrowed and never
    dropped — this is an insert-only RAW layer, so history stays readable.
    """
    added: list[Column] = []
    widened: list[tuple[Column, str, str]] = []
    for column in wanted:
        current = existing.get(column.name)
        if column.tentative:
            continue  # all-null in the sample: no basis to create or widen
        if current is None:
            added.append(column)
        elif current != column.type:
            target = widen(current, column.type)
            if target != current:
                widened.append((column, current, target))
    return added, widened
