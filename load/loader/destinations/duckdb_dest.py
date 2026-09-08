"""DuckDB destination — the local-dev warehouse.

DuckDB takes an exclusive file lock: while this process holds a write
connection, nothing else can even read the database. That is exactly the
single-writer model the load layer wants, but it would also lock a human out
of their own warehouse, so the service closes the connection when it goes idle
and reopens (with backoff) on the next job. Connecting is therefore allowed to
wait for a reader to finish, not just fail.
"""
from __future__ import annotations

import json
import logging
import pathlib
import time
from datetime import datetime, timezone
from typing import ClassVar

import duckdb

from ..cadence import volatile_patch
from ..schema import (
    BOOLEAN,
    DATE,
    DOUBLE,
    INTEGER,
    JSON,
    PAYLOAD_COLUMN,
    STRING,
    TIMESTAMP,
    Column,
)
from .base import Destination, LoadRequest, LoadResult

log = logging.getLogger(__name__)

# One record may be large (some sources emit fat objects); the default 16MB
# object cap is raised rather than letting a single row fail a file.
MAX_OBJECT_SIZE = 128 * 1024 * 1024


class DuckDBDestination(Destination):
    type = "duckdb"

    TYPE_MAP: ClassVar[dict[str, str]] = {
        STRING: "VARCHAR",
        INTEGER: "BIGINT",
        DOUBLE: "DOUBLE",
        BOOLEAN: "BOOLEAN",
        DATE: "DATE",
        TIMESTAMP: "TIMESTAMP WITH TIME ZONE",
        JSON: "JSON",
    }

    #: information_schema.data_type -> canonical. Anything unlisted is treated
    #: as a string, which is the safe assumption: strings hold everything.
    FROM_NATIVE: ClassVar[dict[str, str]] = {
        "VARCHAR": STRING, "TEXT": STRING,
        "BIGINT": INTEGER, "INTEGER": INTEGER, "HUGEINT": INTEGER,
        "UBIGINT": INTEGER, "SMALLINT": INTEGER,
        "DOUBLE": DOUBLE, "FLOAT": DOUBLE, "DECIMAL": DOUBLE,
        "BOOLEAN": BOOLEAN,
        "DATE": DATE,
        "TIMESTAMP WITH TIME ZONE": TIMESTAMP, "TIMESTAMP": TIMESTAMP,
        "JSON": JSON,
    }

    def __init__(self, config, raw_schema, meta_schema):
        super().__init__(config, raw_schema, meta_schema)
        self.database = pathlib.Path(config["database"]).expanduser()
        self.read_only = bool(config.get("read_only", False))
        self.threads = config.get("threads")
        self.memory_limit = config.get("memory_limit")
        self.lock_timeout_s = float(config.get("lock_timeout_s", 120))
        self._con: duckdb.DuckDBPyConnection | None = None

    # -- lifecycle -------------------------------------------------------
    @property
    def connected(self) -> bool:
        return self._con is not None

    def connect(self) -> None:
        if self._con is not None:
            return
        if not self.read_only:
            self.database.parent.mkdir(parents=True, exist_ok=True)
        deadline = time.monotonic() + self.lock_timeout_s
        delay = 0.5
        while True:
            try:
                self._con = duckdb.connect(str(self.database), read_only=self.read_only)
                break
            except duckdb.IOException as exc:
                if "lock" not in str(exc).lower() or time.monotonic() >= deadline:
                    raise
                log.warning("warehouse locked by another process; retrying in %.1fs", delay)
                time.sleep(delay)
                delay = min(delay * 2, 10)
        if self.threads:
            self._con.execute(f"SET threads={int(self.threads)}")
        if self.memory_limit:
            self._con.execute("SET memory_limit=?", [str(self.memory_limit)])

    def close(self) -> None:
        if self._con is not None:
            self._con.close()
            self._con = None

    @property
    def con(self) -> duckdb.DuckDBPyConnection:
        if self._con is None:
            raise RuntimeError("destination is not connected")
        return self._con

    # -- schema ----------------------------------------------------------
    def ensure_schemas(self) -> None:
        self.con.execute(f"CREATE SCHEMA IF NOT EXISTS {self.quote(self.raw_schema)}")
        self.con.execute(f"CREATE SCHEMA IF NOT EXISTS {self.quote(self.meta_schema)}")
        meta = self.quote(self.meta_schema)
        self.con.execute(f"""
            CREATE TABLE IF NOT EXISTS {meta}."files" (
                path              VARCHAR PRIMARY KEY,
                source            VARCHAR,
                table_name        VARCHAR,
                batch_id          VARCHAR,
                dt                DATE,
                size_bytes        BIGINT,
                file_mtime        TIMESTAMP WITH TIME ZONE,
                manifest_records  BIGINT,
                rows_loaded       BIGINT,
                load_id           VARCHAR,
                loaded_at         TIMESTAMP WITH TIME ZONE,
                duration_s        DOUBLE,
                status            VARCHAR,
                attempts          INTEGER,
                error             VARCHAR
            )""")
        self.con.execute(f"""
            CREATE TABLE IF NOT EXISTS {meta}."jobs" (
                load_id      VARCHAR PRIMARY KEY,
                kind         VARCHAR,
                submitted_at TIMESTAMP WITH TIME ZONE,
                started_at   TIMESTAMP WITH TIME ZONE,
                finished_at  TIMESTAMP WITH TIME ZONE,
                duration_s   DOUBLE,
                files_seen   BIGINT,
                files_loaded BIGINT,
                files_failed BIGINT,
                rows_loaded  BIGINT,
                status       VARCHAR,
                error        VARCHAR
            )""")
        self.con.execute(f"""
            CREATE TABLE IF NOT EXISTS {meta}."schema_changes" (
                changed_at  TIMESTAMP WITH TIME ZONE,
                load_id     VARCHAR,
                table_name  VARCHAR,
                column_name VARCHAR,
                change      VARCHAR,
                from_type   VARCHAR,
                to_type     VARCHAR
            )""")
        self.con.execute(f"""
            CREATE TABLE IF NOT EXISTS {meta}."cadence_state" (
                source            VARCHAR PRIMARY KEY,
                interval_minutes  INTEGER,
                cron              VARCHAR,
                last_batch_id     VARCHAR,
                changed_streak    INTEGER,
                unchanged_streak  INTEGER,
                updated_at        TIMESTAMP WITH TIME ZONE
            )""")
        # The decision log: one row per evaluation that had new evidence,
        # holds included, so a schedule is always explainable after the fact.
        self.con.execute(f"""
            CREATE TABLE IF NOT EXISTS {meta}."cadence_decisions" (
                decided_at     TIMESTAMP WITH TIME ZONE,
                load_id        VARCHAR,
                source         VARCHAR,
                decision       VARCHAR,
                from_minutes   INTEGER,
                to_minutes     INTEGER,
                cron           VARCHAR,
                batch_id       VARCHAR,
                prev_batch_id  VARCHAR,
                rows_new       BIGINT,
                distinct_rows  BIGINT,
                novel_rows     BIGINT,
                novel_ratio    DOUBLE,
                changed        BOOLEAN,
                signal         VARCHAR,
                reason         VARCHAR
            )""")
        # Migration-safe: older DBs created without the schema-v2 manifest
        # evidence columns are upgraded in place. The existing manifest
        # counter columns (manifest_records, rows_loaded) and the novelty /
        # state-change columns (rows_new, distinct_rows, novel_rows, changed)
        # keep their meaning; these are purely additive.
        self._add_column_if_missing("files", "extract_status", "VARCHAR")
        self._add_column_if_missing("files", "extract_health", "VARCHAR")
        self._add_column_if_missing("files", "extract_completeness", "VARCHAR")
        self._add_column_if_missing("files", "extract_duration_s", "DOUBLE")
        self._add_column_if_missing("files", "partition_attempted", "BIGINT")
        self._add_column_if_missing("files", "partition_succeeded", "BIGINT")
        self._add_column_if_missing("files", "partition_failed", "BIGINT")
        self._add_column_if_missing("files", "extract_metrics", "JSON")
        self._add_column_if_missing("jobs", "files_held", "BIGINT")
        self._add_column_if_missing("jobs", "files_partial", "BIGINT")

    def _add_column_if_missing(self, table: str, column: str, type_: str) -> None:
        """``ALTER TABLE ... ADD COLUMN IF NOT EXISTS``; the ledger is append-only,
        so a missing row is not an error — a fresh install already has it."""
        meta = self.quote(self.meta_schema)
        self.con.execute(
            f"ALTER TABLE {meta}.{self.quote(table)} "
            f"ADD COLUMN IF NOT EXISTS {column} {type_}"
        )

    def list_columns(self, schema: str, table: str) -> dict[str, str] | None:
        rows = self.con.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position",
            [schema, table]).fetchall()
        if not rows:
            return None
        return {name: self.FROM_NATIVE.get(str(dtype).upper(), STRING) for name, dtype in rows}

    def create_table(self, schema: str, table: str, columns: list[Column]) -> None:
        ddl = ", ".join(self.column_ddl(c) for c in columns)
        self.con.execute(f"CREATE TABLE IF NOT EXISTS {self.qualify(schema, table)} ({ddl})")
        log.info("created %s.%s with %d columns", schema, table, len(columns))

    def add_column(self, schema: str, table: str, column: Column) -> None:
        self.con.execute(f"ALTER TABLE {self.qualify(schema, table)} "
                         f"ADD COLUMN IF NOT EXISTS {self.column_ddl(column)}")
        log.info("%s.%s: + column %s %s", schema, table, column.name, column.type)

    def widen_column(self, schema: str, table: str, name: str, to_type: str) -> bool:
        try:
            self.con.execute(f"ALTER TABLE {self.qualify(schema, table)} "
                             f"ALTER COLUMN {self.quote(name)} "
                             f"TYPE {self.native_type(to_type)}")
        except duckdb.Error as exc:
            log.warning("%s.%s: cannot widen %s to %s: %s", schema, table, name, to_type, exc)
            return False
        log.info("%s.%s: widened %s to %s", schema, table, name, to_type)
        return True

    # -- value extraction -------------------------------------------------
    @staticmethod
    def _json_path(key: str) -> str:
        return '$."' + key.replace('\\', '\\\\').replace('"', '\\"') + '"'

    def _extract(self, column: Column) -> str:
        """SQL for one source field, read out of the record's JSON.

        Every cast is a *try* cast: a value that doesn't fit the column becomes
        NULL instead of failing the file, and stays intact in ``_payload``.
        """
        literal = "'" + self._json_path(column.key).replace("'", "''") + "'"
        if column.type == JSON:
            return f"json_extract(payload, {literal})"
        text = f"json_extract_string(payload, {literal})"
        if column.type == STRING:
            return text
        return f"TRY_CAST({text} AS {self.native_type(column.type)})"

    def _relation(self, ignore_malformed: bool = False) -> str:
        """One NDJSON file as ``(payload, _rn)``, in file order.

        ``preserve_insertion_order`` is on by default, so ``row_number()``
        matches the physical line number — that is what makes ``_row_id``
        (file + line) a stable identity for a record.
        """
        # ignore_errors drops unparseable lines rather than failing the file --
        # off by default, because a malformed line means a broken fetcher and
        # should be loud, but reachable via a source's on_malformed_lines.
        return (f"(SELECT json AS payload, row_number() OVER () AS _rn "
                f"FROM read_ndjson_objects(?, maximum_object_size={MAX_OBJECT_SIZE}, "
                f"ignore_errors={'true' if ignore_malformed else 'false'}))")

    # -- data -------------------------------------------------------------
    def load_file(self, request: LoadRequest) -> LoadResult:
        file = request.file
        path = str(file.path)
        target = self.qualify(self.raw_schema, request.table)
        started = time.monotonic()

        select: list[str] = []
        params: list = []
        for column in request.columns:
            if column.key is not None:
                select.append(self._extract(column))
                continue
            if column.name == "_row_id":
                select.append("md5(? || ':' || _rn::VARCHAR)"); params.append(path)
            elif column.name == "_source":
                select.append("?"); params.append(request.source)
            elif column.name == "_batch_id":
                select.append("?"); params.append(file.batch_id)
            elif column.name == "_source_file":
                select.append("?"); params.append(path)
            elif column.name == "_file_row_num":
                select.append("_rn")
            elif column.name == "_dt":
                select.append("TRY_CAST(? AS DATE)"); params.append(file.dt)
            elif column.name == "_extract_started_at":
                select.append("TRY_CAST(? AS TIMESTAMP WITH TIME ZONE)")
                params.append(file.extract_started_at)
            elif column.name == "_load_id":
                select.append("?"); params.append(request.load_id)
            elif column.name == "_loaded_at":
                select.append("TRY_CAST(? AS TIMESTAMP WITH TIME ZONE)")
                params.append(request.loaded_at.isoformat())
            elif column.name == "_content_hash":
                select.append("md5(payload::VARCHAR)")
            elif column.name == PAYLOAD_COLUMN.name:
                select.append("payload")
            else:
                raise ValueError(f"no source for metadata column {column.name!r}")

        columns_sql = ", ".join(self.quote(c.name) for c in request.columns)
        sql = (f"INSERT INTO {target} ({columns_sql}) SELECT {', '.join(select)} "
               f"FROM {self._relation(request.ignore_malformed_lines)}")
        params.append(path)

        self.con.execute("BEGIN TRANSACTION")
        try:
            rows = 0 if file.size_bytes == 0 else self.con.execute(sql, params).fetchone()[0]
            duration = round(time.monotonic() - started, 3)
            self._upsert_file(file, request, rows=rows, status="loaded",
                              error=None, duration_s=duration)
            self.con.execute("COMMIT")
        except Exception:
            self.con.execute("ROLLBACK")
            raise
        return LoadResult(rows=rows, duration_s=duration)

    # -- ledger -----------------------------------------------------------
    def _upsert_file(self, file, request: LoadRequest, *, rows, status, error,
                     duration_s) -> None:
        meta = self.quote(self.meta_schema)
        f = file
        self.con.execute(f"""
            INSERT INTO {meta}."files" (path, source, table_name, batch_id, dt,
                size_bytes, file_mtime, manifest_records, rows_loaded, load_id,
                loaded_at, duration_s, status, attempts, error,
                extract_status, extract_health, extract_completeness,
                extract_duration_s, partition_attempted, partition_succeeded,
                partition_failed, extract_metrics)
            VALUES (?, ?, ?, ?, TRY_CAST(? AS DATE), ?, to_timestamp(?), ?, ?, ?,
                    TRY_CAST(? AS TIMESTAMP WITH TIME ZONE), ?, ?, 1, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (path) DO UPDATE SET
                rows_loaded             = EXCLUDED.rows_loaded,
                load_id                 = EXCLUDED.load_id,
                loaded_at               = EXCLUDED.loaded_at,
                duration_s              = EXCLUDED.duration_s,
                status                  = EXCLUDED.status,
                attempts                = {meta}."files".attempts + 1,
                error                   = EXCLUDED.error,
                extract_status          = COALESCE(EXCLUDED.extract_status, {meta}."files".extract_status),
                extract_health          = COALESCE(EXCLUDED.extract_health, {meta}."files".extract_health),
                extract_completeness    = COALESCE(EXCLUDED.extract_completeness, {meta}."files".extract_completeness),
                extract_duration_s      = COALESCE(EXCLUDED.extract_duration_s, {meta}."files".extract_duration_s),
                partition_attempted     = COALESCE(EXCLUDED.partition_attempted, {meta}."files".partition_attempted),
                partition_succeeded     = COALESCE(EXCLUDED.partition_succeeded, {meta}."files".partition_succeeded),
                partition_failed        = COALESCE(EXCLUDED.partition_failed, {meta}."files".partition_failed),
                extract_metrics         = COALESCE(EXCLUDED.extract_metrics, {meta}."files".extract_metrics)
            """, [str(file.path), request.source, request.table, file.batch_id, file.dt,
                  file.size_bytes, file.mtime, file.records, rows, request.load_id,
                  request.loaded_at.isoformat(), duration_s, status, error,
                  f.extract_status, f.extract_health, f.extract_completeness,
                  f.extract_duration_s, f.partition_attempted, f.partition_succeeded,
                  f.partition_failed,
                  json.dumps(f.extract_metrics) if f.extract_metrics is not None else None])

    def ledger(self) -> dict[str, tuple[str, int]]:
        rows = self.con.execute(
            f'SELECT path, status, attempts FROM {self.quote(self.meta_schema)}."files"'
        ).fetchall()
        return {path: (status, attempts or 0) for path, status, attempts in rows}

    def record_failure(self, file, table: str, load_id: str, error: str) -> None:
        request = LoadRequest(source=file.source, table=table, file=file, columns=[],
                              load_id=load_id, loaded_at=datetime.now(timezone.utc),
                              keep_payload=False)
        self._upsert_file(file, request, rows=0, status="failed",
                          error=error[:2000], duration_s=None)

    def record_job(self, job: dict) -> None:
        meta = self.quote(self.meta_schema)
        self.con.execute(f"""
            INSERT OR REPLACE INTO {meta}."jobs" (load_id, kind, submitted_at,
                started_at, finished_at, duration_s, files_seen, files_loaded,
                files_failed, rows_loaded, status, error, files_held, files_partial)
            VALUES (?, ?, TRY_CAST(? AS TIMESTAMP WITH TIME ZONE),
                    TRY_CAST(? AS TIMESTAMP WITH TIME ZONE),
                    TRY_CAST(? AS TIMESTAMP WITH TIME ZONE), ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [job["load_id"], job.get("kind"), job.get("submitted_at"),
                  job.get("started_at"), job.get("finished_at"), job.get("duration_s"),
                  job.get("files_seen"), job.get("files_loaded"), job.get("files_failed"),
                  job.get("rows_loaded"), job.get("status"),
                  (job.get("error") or None) and str(job["error"])[:2000],
                  job.get("files_held"), job.get("files_partial")])

    def record_schema_change(self, load_id, table, column, change, from_type, to_type) -> None:
        self.con.execute(
            f'INSERT INTO {self.quote(self.meta_schema)}."schema_changes" VALUES '
            f'(now(), ?, ?, ?, ?, ?, ?)',
            [load_id, table, column, change, from_type, to_type])

    def summary(self) -> list[dict]:
        rows = self.con.execute(f"""
            SELECT source, table_name,
                   count(*) FILTER (WHERE status = 'loaded')  AS files_loaded,
                   count(*) FILTER (WHERE status = 'failed')  AS files_failed,
                   sum(rows_loaded)                     AS rows_loaded,
                   max(loaded_at)                       AS last_loaded_at
            FROM {self.quote(self.meta_schema)}."files"
            GROUP BY 1, 2 ORDER BY 1
        """).fetchall()
        return [dict(zip(("source", "table", "files_loaded", "files_failed",
                          "rows_loaded", "last_loaded_at"), r)) for r in rows]

    # -- cadence ----------------------------------------------------------
    def recent_batches(self, source: str, limit: int = 2) -> list[dict]:
        rows = self.con.execute(f"""
            SELECT batch_id, rows_loaded, file_mtime
            FROM {self.quote(self.meta_schema)}."files"
            WHERE source = ? AND status = 'loaded'
              AND regexp_matches(batch_id, '_[0-9]{{8}}T[0-9]{{6}}Z$')
            ORDER BY file_mtime DESC, batch_id DESC
            LIMIT ?
        """, [source, limit]).fetchall()
        return [{"batch_id": b, "rows_loaded": n or 0, "file_mtime": t} for b, n, t in rows]

    def probe_novelty(self, table: str, batch_id: str, prev_batch_id: str, *,
                      key: str, volatile_keys: tuple[str, ...]) -> dict:
        """Distinct records in one batch, and how many the previous batch lacked.

        Both batches are selected by ``_batch_id`` with literal parameters, so
        DuckDB's row-group min/max statistics prune the rest of the table: RAW
        is insert-only and loaded batch by batch, which makes ``_batch_id``
        effectively clustered. Reading two batches of an 8.3M-row table costs
        well under a second.
        """
        if key == "content":
            # json_merge_patch drops a key when the patch maps it to null:
            # `fetched_at` (and any other per-poll field, nested paths
            # included) moves every run, so hashing the raw record would
            # report every run as changed.
            patch = json.dumps(volatile_patch(volatile_keys))
            identity = "md5(json_merge_patch(_payload, ?::JSON)::VARCHAR)"
            extra: list = [patch]
        elif key == "ids":
            identity = "id"
            extra = []
        else:
            raise ValueError(f"unknown novelty key {key!r}")

        sql = f"""
            WITH pair AS (
                SELECT _batch_id AS b, {identity} AS k
                FROM {self.qualify(self.raw_schema, table)}
                WHERE _batch_id IN (?, ?)
            ),
            newest AS (SELECT DISTINCT k FROM pair WHERE b = ?),
            previous AS (SELECT DISTINCT k FROM pair WHERE b = ?)
            SELECT (SELECT count(*) FROM newest) AS distinct_rows,
                   (SELECT count(*) FROM newest n
                    WHERE NOT EXISTS (SELECT 1 FROM previous p WHERE p.k IS NOT DISTINCT FROM n.k)
                   ) AS novel_rows
        """
        params = extra + [batch_id, prev_batch_id, batch_id, prev_batch_id]
        distinct_rows, novel_rows = self.con.execute(sql, params).fetchone()
        return {"distinct_rows": distinct_rows or 0, "novel_rows": novel_rows or 0}

    def cadence_states(self) -> dict[str, dict]:
        columns = ("source", "interval_minutes", "cron", "last_batch_id",
                   "changed_streak", "unchanged_streak", "updated_at")
        rows = self.con.execute(
            f'SELECT {", ".join(columns)} FROM {self.quote(self.meta_schema)}."cadence_state"'
        ).fetchall()
        return {row[0]: dict(zip(columns, row)) for row in rows}

    def save_cadence_state(self, state) -> None:
        self.con.execute(f"""
            INSERT OR REPLACE INTO {self.quote(self.meta_schema)}."cadence_state"
                (source, interval_minutes, cron, last_batch_id, changed_streak,
                 unchanged_streak, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, now())
        """, [state.source, state.interval_minutes, state.cron, state.last_batch_id,
              state.changed_streak, state.unchanged_streak])

    def record_cadence_decision(self, load_id: str, decision) -> None:
        obs = decision.observation
        self.con.execute(f"""
            INSERT INTO {self.quote(self.meta_schema)}."cadence_decisions" VALUES
                (now(), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [load_id, decision.source, decision.decision, decision.from_minutes,
              decision.to_minutes, decision.cron, obs.batch_id, obs.prev_batch_id,
              obs.rows, obs.distinct_rows, obs.novel_rows, obs.novel_ratio,
              obs.changed, obs.signal, decision.reason])
