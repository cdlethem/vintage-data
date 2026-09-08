"""The portability seam.

Everything above this file — discovery, inference, the queue, the service loop —
is warehouse-agnostic. A new destination is one subclass: map the canonical
types to native DDL, say how an NDJSON file becomes a relation of JSON records,
and the shared SQL generation here does the rest.

The contract the service relies on:

* one connection, owned by one process, writes applied serially;
* ``load_file`` inserts the rows **and** the ledger row in a single
  transaction, so a file is never half-loaded — that is what makes the
  insert-only RAW layer safe to retry;
* the ledger (``<meta_schema>.files``) lives in the destination itself, so
  "what is already loaded?" is answered by the same store that holds the data,
  with no second source of truth to drift.
"""
from __future__ import annotations

import abc
from dataclasses import dataclass, replace
from datetime import datetime
from typing import ClassVar

from ..discovery import SinkFile
from ..schema import Column, plan_evolution


@dataclass
class LoadRequest:
    """One file, ready to insert."""

    source: str
    table: str
    file: SinkFile
    columns: list[Column]
    load_id: str
    loaded_at: datetime
    keep_payload: bool
    #: drop unparseable NDJSON lines instead of failing the whole file
    ignore_malformed_lines: bool = False


@dataclass
class LoadResult:
    rows: int
    duration_s: float


class Destination(abc.ABC):
    """A warehouse the load layer can write to."""

    type: str = "abstract"

    #: canonical type -> native column type
    TYPE_MAP: ClassVar[dict[str, str]] = {}

    def __init__(self, config: dict, raw_schema: str, meta_schema: str):
        self.config = config
        self.name = config.get("name", self.type)
        self.raw_schema = raw_schema
        self.meta_schema = meta_schema
        self.max_attempts = int(config.get("max_attempts", 3))

    # -- lifecycle -------------------------------------------------------
    @abc.abstractmethod
    def connect(self) -> None:
        """Open the connection. Called by the writer service only."""

    @abc.abstractmethod
    def close(self) -> None:
        """Release the connection so other processes can read."""

    @property
    @abc.abstractmethod
    def connected(self) -> bool: ...

    # -- schema ----------------------------------------------------------
    @abc.abstractmethod
    def ensure_schemas(self) -> None:
        """Create the RAW and metadata schemas plus the ledger tables."""

    @abc.abstractmethod
    def list_columns(self, schema: str, table: str) -> dict[str, str] | None:
        """Live columns as ``{name: canonical_type}``; ``None`` if no table."""

    @abc.abstractmethod
    def create_table(self, schema: str, table: str, columns: list[Column]) -> None: ...

    @abc.abstractmethod
    def add_column(self, schema: str, table: str, column: Column) -> None: ...

    def widen_column(self, schema: str, table: str, name: str, to_type: str) -> bool:
        """Widen an existing column in place.

        Returning ``False`` means "not supported here" — the loader then keeps
        the narrower column and lets non-conforming values land as NULL, with
        the full record still available in ``_payload``. Warehouses that cannot
        alter column types (BigQuery, mostly) simply don't override this.
        """
        return False

    # -- data ------------------------------------------------------------
    @abc.abstractmethod
    def load_file(self, request: LoadRequest) -> LoadResult:
        """Insert one file's rows and its ledger row, atomically."""

    # -- ledger ----------------------------------------------------------
    @abc.abstractmethod
    def ledger(self) -> dict[str, tuple[str, int]]:
        """``{path: (status, attempts)}`` for every file the ledger knows."""

    @abc.abstractmethod
    def record_failure(self, file: SinkFile, table: str, load_id: str, error: str) -> None: ...

    @abc.abstractmethod
    def record_job(self, job: dict) -> None: ...

    @abc.abstractmethod
    def record_schema_change(self, load_id: str, table: str, column: str,
                             change: str, from_type: str | None, to_type: str) -> None: ...

    @abc.abstractmethod
    def summary(self) -> list[dict]:
        """Per-table row counts and load recency, for ``status`` output."""

    # -- cadence ---------------------------------------------------------
    # Scheduling cadence detection reads RAW and writes only its own two
    # tables, so a destination that cannot answer these cheaply is free to
    # raise: the cadence job then reports the source as skipped and the load
    # path is untouched.
    @abc.abstractmethod
    def recent_batches(self, source: str, limit: int = 2) -> list[dict]:
        """The source's newest *scheduled* runs, newest first.

        Backfill batches are excluded on purpose: a historical unit says
        nothing about how often the upstream publishes now.
        """

    @abc.abstractmethod
    def probe_novelty(self, table: str, batch_id: str, prev_batch_id: str, *,
                      key: str, volatile_keys: tuple[str, ...]) -> dict:
        """Compare two batches of one table: ``{distinct_rows, novel_rows}``.

        Must read only those two batches — this runs after every load pass.
        ``key`` is ``content`` (record hash, volatile envelope keys removed) or
        ``ids`` (the envelope ``id`` column).
        """

    @abc.abstractmethod
    def cadence_states(self) -> dict[str, dict]:
        """Per-source cadence state, keyed by source name."""

    @abc.abstractmethod
    def save_cadence_state(self, state) -> None: ...

    @abc.abstractmethod
    def record_cadence_decision(self, load_id: str, decision) -> None:
        """Append one decision, with the evidence it was made on."""

    def should_skip(self, path: str, ledger: dict[str, tuple[str, int]]) -> str | None:
        """Why this file is not a load candidate, or None if it is one."""
        entry = ledger.get(path)
        if entry is None:
            return None
        status, attempts = entry
        if status == "loaded":
            return "already loaded"
        if attempts >= self.max_attempts:
            return f"failed {attempts}x (>= max_attempts)"
        return None

    # -- shared SQL helpers ----------------------------------------------
    def quote(self, ident: str) -> str:
        return '"' + ident.replace('"', '""') + '"'

    def qualify(self, schema: str, table: str) -> str:
        return f"{self.quote(schema)}.{self.quote(table)}"

    def native_type(self, canonical: str) -> str:
        try:
            return self.TYPE_MAP[canonical]
        except KeyError:
            raise ValueError(f"{self.type}: no native type for {canonical!r}") from None

    def column_ddl(self, column: Column) -> str:
        return f"{self.quote(column.name)} {self.native_type(column.type)}"

    # -- schema synchronisation (dialect-independent) ---------------------
    def sync_table(self, table: str, columns: list[Column], load_id: str) -> list[Column]:
        """Make the live table able to hold ``columns``; return what it now has.

        Additive only. New keys become new columns; a key whose observed type
        outgrew its column widens if the warehouse can do that in place, and
        otherwise keeps the old column (values that don't fit arrive as NULL,
        recoverable from ``_payload``). Nothing is ever dropped or narrowed,
        which is what lets RAW stay append-only and re-readable.
        """
        existing = self.list_columns(self.raw_schema, table)
        if existing is None:
            columns = [c for c in columns if not c.tentative]
            self.create_table(self.raw_schema, table, columns)
            for column in columns:
                self.record_schema_change(load_id, table, column.name,
                                          "create", None, column.type)
            return columns

        added, widened = plan_evolution(existing, columns)
        for column in added:
            self.add_column(self.raw_schema, table, column)
            self.record_schema_change(load_id, table, column.name, "add", None, column.type)
            existing[column.name] = column.type
        for column, from_type, to_type in widened:
            if self.widen_column(self.raw_schema, table, column.name, to_type):
                self.record_schema_change(load_id, table, column.name,
                                          "widen", from_type, to_type)
                existing[column.name] = to_type
            else:
                self.record_schema_change(load_id, table, column.name,
                                          "widen_skipped", from_type, to_type)

        # Write with the types the table actually has, not the ones inferred —
        # and drop tentative columns the table doesn't have yet, so an all-null
        # key doesn't get pinned to a type before a real value shows up.
        return [replace(c, type=existing[c.name]) for c in columns if c.name in existing]
