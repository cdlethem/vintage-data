"""Immutable DuckDB snapshots and atomic, ordered Postgres publication."""
from __future__ import annotations

import contextlib
import fcntl
import hashlib
import json
import os
import pathlib
import shutil
import tempfile
import time
from datetime import datetime, timezone

from visualization.project import columns, digest, identifier, mart_nodes


def state_root() -> pathlib.Path:
    return pathlib.Path(os.environ.get("LIGHTDASH_STATE_ROOT", pathlib.Path.home() / ".local/share/vintage-data/lightdash")).expanduser()


def file_digest(path: pathlib.Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()


@contextlib.contextmanager
def warehouse_reader(warehouse: pathlib.Path, timeout: float = 120):
    import duckdb
    deadline = time.monotonic() + timeout
    while True:
        try:
            con = duckdb.connect(str(warehouse), read_only=True)
            break
        except duckdb.IOException as exc:
            if "lock" not in str(exc).lower() or time.monotonic() >= deadline:
                raise
            time.sleep(min(1, max(0, deadline - time.monotonic())))
    try:
        con.execute("SET TimeZone='UTC'")
        con.execute("BEGIN TRANSACTION")
        yield con
        con.execute("COMMIT")
    finally:
        con.close()


def capture(manifest: dict, results: dict, warehouse: pathlib.Path, root: pathlib.Path | None = None) -> dict:
    """Caller holds the transform lock from build through capture."""
    root = root or state_root()
    marts = mart_nodes(manifest)
    if not results.get("results"):
        raise ValueError("cannot snapshot without successful dbt run results")
    if any(row.get("status") not in ("success", "pass", "warn") for row in results["results"]):
        raise ValueError("cannot publish a failed or partially skipped dbt build")
    invocation = results.get("metadata", {}).get("invocation_id")
    if not invocation or invocation != manifest.get("metadata", {}).get("invocation_id"):
        raise ValueError("manifest and run results do not belong to the same dbt invocation")
    selected = {row["unique_id"]: marts[row["unique_id"]] for row in results["results"] if row["unique_id"] in marts and row["status"] == "success"}
    if not selected:
        raise ValueError("build produced no publishable marts")
    batches = root / "batches"
    batches.mkdir(parents=True, exist_ok=True)
    batch_id = digest({"invocation": invocation, "manifest": digest(manifest), "models": sorted(selected)})
    final = batches / batch_id
    with (root / "capture.lock").open("a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if final.exists():
            return {"batch_id": batch_id, "path": str(final)}
        counter = root / "sequence"
        sequence = max(time.time_ns(), int(counter.read_text()) + 1 if counter.exists() else 0)
        counter.write_text(str(sequence))
        temporary = pathlib.Path(tempfile.mkdtemp(prefix=".capture-", dir=batches))
        try:
            rows = []
            with warehouse_reader(warehouse) as con:
                for uid, node in sorted(selected.items()):
                    table = node.get("alias") or node["name"]
                    schema = node["schema"]
                    relation = f"{identifier(schema)}.{identifier(table)}"
                    cols = columns(node)
                    names = ", ".join(identifier(col["name"]) for col in cols)
                    path = temporary / f"{table}.csv"
                    # Every non-null value is quoted; a literal backslash-N is
                    # therefore distinct from the unquoted SQL-null marker.
                    literal = "'" + str(path).replace("'", "''") + "'"
                    count = con.execute(f"COPY (SELECT {names} FROM {relation}) TO {literal} (FORMAT CSV, HEADER false, NULL '\\N', FORCE_QUOTE *)").fetchone()[0]
                    rows.append({"unique_id": uid, "table": table, "columns": cols, "schema_sha256": digest(cols),
                                 "rows": count, "file": path.name, "sha256": file_digest(path)})
            info = {"schema_version": 1, "batch_id": batch_id, "sequence": sequence,
                    "captured_at": datetime.now(timezone.utc).isoformat(), "invocation_id": invocation,
                    "manifest_sha256": digest(manifest), "tables": rows}
            (temporary / "manifest.json").write_text(json.dumps(manifest))
            (temporary / "run_results.json").write_text(json.dumps(results))
            (temporary / "batch.json").write_text(json.dumps(info, indent=2) + "\n")
            temporary.rename(final)
        except BaseException:
            shutil.rmtree(temporary)
            raise
    return {"batch_id": batch_id, "path": str(final)}


def connection_args(*, reader=False) -> dict:
    return {"host": os.environ.get("LIGHTDASH_PG_HOST", "127.0.0.1"), "port": int(os.environ.get("LIGHTDASH_PG_PORT", "5433")),
            "dbname": os.environ.get("LIGHTDASH_SERVING_DB", "vintage_serving"),
            "user": "mart_reader" if reader else "mart_publisher",
            "password": os.environ["LIGHTDASH_READER_PASSWORD" if reader else "LIGHTDASH_PUBLISHER_PASSWORD"], "connect_timeout": 10}


def time_index(name: str) -> str:
    """Deterministic, length-bounded name for a mart's time index."""
    suffix = "_time_brin"
    return name[: 63 - len(suffix)] + suffix


def time_columns(columns: list[dict]) -> list[str]:
    return [column["name"] for column in columns if str(column["type"]).lower().startswith(("date", "timestamp"))]


def index_statements(name: str, columns: list[dict]):
    """Drop-then-create a single multi-column BRIN index over the time columns.

    Dashboard queries filter and group on time, and every mart is time-grained.
    BRIN costs kilobytes and one scan to build, and a multi-column BRIN answers
    a predicate on any of its columns, so one index covers every trend axis.
    It is dropped before the reload so the bulk INSERT does no index
    maintenance, and recreated on the finished table.
    """
    from psycopg import sql
    fields = time_columns(columns)
    drop = sql.SQL("DROP INDEX IF EXISTS {}").format(sql.Identifier("transform_marts", time_index(name)))
    if not fields:
        return drop, None
    create = sql.SQL("CREATE INDEX {} ON {} USING brin ({})").format(
        sql.Identifier(time_index(name)), sql.Identifier("transform_marts", name),
        sql.SQL(", ").join(sql.Identifier(field) for field in fields))
    return drop, create


def publish(batch_path: pathlib.Path, *, connection=None) -> dict:
    import psycopg
    from psycopg import sql
    batch_path = batch_path.resolve()
    batch = json.loads((batch_path / "batch.json").read_text())
    if batch.get("schema_version") != 1 or not isinstance(batch.get("sequence"), int):
        raise ValueError("invalid publication batch")
    for table in batch["tables"]:
        path = batch_path / table["file"]
        if path.parent != batch_path or not path.is_file() or path.is_symlink() or file_digest(path) != table["sha256"]:
            raise ValueError(f"snapshot integrity failure: {table['table']}")
    owned = connection is None
    con = connection or psycopg.connect(**connection_args())
    published, skipped = [], []
    try:
        with con.transaction():
            con.execute("SET LOCAL statement_timeout = '20min'")
            con.execute("SET LOCAL lock_timeout = '60s'")
            con.execute("SELECT pg_advisory_xact_lock(863529174)")
            con.execute("CREATE SCHEMA IF NOT EXISTS _publish")
            con.execute("CREATE SCHEMA IF NOT EXISTS transform_marts")
            con.execute("""CREATE TABLE IF NOT EXISTS _publish.models (
                model text PRIMARY KEY, sequence bigint NOT NULL, batch_id text NOT NULL,
                schema_sha256 text NOT NULL, row_count bigint NOT NULL,
                captured_at timestamptz NOT NULL, published_at timestamptz NOT NULL DEFAULT now())""")
            for table in batch["tables"]:
                name = table["table"]
                identifier(name)
                previous = con.execute("SELECT sequence, schema_sha256 FROM _publish.models WHERE model=%s", (name,)).fetchone()
                if previous and previous[0] >= batch["sequence"]:
                    skipped.append(name)
                    continue
                if previous and previous[1] != table["schema_sha256"]:
                    raise ValueError(f"{name}: schema changed; coordinate a serving migration and content deployment")
                cols = table["columns"]
                if digest(cols) != table["schema_sha256"]:
                    raise ValueError("column fingerprint mismatch")
                from visualization.project import pg_type
                definition = sql.SQL(", ").join(sql.SQL("{} {}").format(sql.Identifier(c["name"]), sql.SQL(pg_type(c["type"]))) for c in cols)
                target = sql.Identifier("transform_marts", name)
                temp = sql.Identifier("incoming_mart")
                con.execute(sql.SQL("CREATE TEMP TABLE {} ({}) ON COMMIT DROP").format(temp, definition))
                with con.cursor().copy(sql.SQL("COPY {} FROM STDIN WITH (FORMAT CSV, NULL '\\N')").format(temp)) as stream:
                    with (batch_path / table["file"]).open("rb") as data:
                        for chunk in iter(lambda: data.read(1024 * 1024), b""):
                            stream.write(chunk)
                count = con.execute(sql.SQL("SELECT count(*) FROM {}").format(temp)).fetchone()[0]
                if count != table["rows"]:
                    raise ValueError(f"{name}: row count mismatch {count} != {table['rows']}")
                con.execute(sql.SQL("CREATE TABLE IF NOT EXISTS {} ({})").format(target, definition))
                drop_index, create_index = index_statements(name, cols)
                con.execute(drop_index)
                # DELETE/INSERT in a transaction preserves table identity and
                # MVCC visibility, unlike TRUNCATE or rename-based replacement.
                con.execute(sql.SQL("DELETE FROM {}").format(target))
                con.execute(sql.SQL("INSERT INTO {} SELECT * FROM {}").format(target, temp))
                con.execute(sql.SQL("DROP TABLE {}").format(temp))
                if create_index is not None:
                    con.execute(create_index)
                con.execute(sql.SQL("ANALYZE {}").format(target))
                con.execute("""INSERT INTO _publish.models(model, sequence, batch_id, schema_sha256, row_count, captured_at)
                    VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT(model) DO UPDATE SET
                    sequence=excluded.sequence, batch_id=excluded.batch_id, schema_sha256=excluded.schema_sha256,
                    row_count=excluded.row_count, captured_at=excluded.captured_at, published_at=now()""",
                    (name, batch["sequence"], batch["batch_id"], table["schema_sha256"], count, batch["captured_at"]))
                published.append(name)
        return {"batch_id": batch["batch_id"], "published": published, "skipped": skipped}
    finally:
        if owned:
            con.close()


def reindex(*, connection=None) -> dict:
    """Rebuild time indexes on the already-published marts.

    Publication creates them, but an installation published before the index
    existed needs them without a full recapture and reload.
    """
    import psycopg
    from psycopg import sql
    owned = connection is None
    con = connection or psycopg.connect(**connection_args())
    indexed = []
    try:
        names = [row[0] for row in con.execute("SELECT model FROM _publish.models ORDER BY model").fetchall()]
        for name in names:
            identifier(name)
            rows = con.execute("SELECT column_name, data_type FROM information_schema.columns"
                               " WHERE table_schema='transform_marts' AND table_name=%s ORDER BY ordinal_position",
                               (name,)).fetchall()
            columns = [{"name": column, "type": data_type} for column, data_type in rows]
            if not columns:
                continue
            drop_index, create_index = index_statements(name, columns)
            with con.transaction():
                con.execute("SET LOCAL statement_timeout = '20min'")
                con.execute("SET LOCAL lock_timeout = '60s'")
                con.execute(drop_index)
                if create_index is not None:
                    con.execute(create_index)
                    con.execute(sql.SQL("ANALYZE {}").format(sql.Identifier("transform_marts", name)))
                    indexed.append(name)
        return {"indexed": indexed, "index_count": len(indexed)}
    finally:
        if owned:
            con.close()


def status() -> dict:
    import psycopg
    from psycopg.rows import dict_row
    with psycopg.connect(**connection_args(reader=True), row_factory=dict_row) as con:
        rows = con.execute("SELECT *, extract(epoch FROM now()-captured_at)::bigint AS age_seconds FROM _publish.models ORDER BY model").fetchall()
    return {"models": rows, "published_count": len(rows)}
