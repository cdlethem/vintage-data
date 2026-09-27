"""Streamed DuckDB-to-Postgres publication with metadata-only batch records."""
from __future__ import annotations

import contextlib
import fcntl
import hashlib
import json
import os
import pathlib
import re
import shutil
import tempfile
import time
from datetime import datetime, timezone

from visualization.project import columns, digest, family_mart_nodes, identifier, pg_type, vintage_scope


def state_root() -> pathlib.Path:
    return pathlib.Path(os.environ.get("LIGHTDASH_STATE_ROOT", pathlib.Path.home() / ".local/share/vintage-data/lightdash")).expanduser()


def file_digest(path: pathlib.Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()

def content_digest(content_root: pathlib.Path) -> str:
    """Fingerprint the reviewed Lightdash files captured with a production batch."""
    files = []
    for kind in ("charts", "dashboards"):
        directory = content_root / kind
        if not directory.is_dir():
            raise ValueError(f"missing reviewed Lightdash {kind} directory")
        for path in sorted(directory.glob("*.yml")):
            if not path.is_file() or path.is_symlink():
                raise ValueError(f"unsafe reviewed Lightdash content: {path}")
            files.append({"path": f"{kind}/{path.name}", "sha256": file_digest(path)})
    if not files:
        raise ValueError("reviewed Lightdash content is empty")
    return digest(files)


def snapshot_content(content_root: pathlib.Path, destination: pathlib.Path) -> str:
    """Copy reviewed content into an immutable batch after it has been checked."""
    content_sha256 = content_digest(content_root)
    target = destination / "content"
    for kind in ("charts", "dashboards"):
        source = content_root / kind
        output = target / kind
        output.mkdir(parents=True, exist_ok=True)
        for path in sorted(source.glob("*.yml")):
            shutil.copy2(path, output / path.name)
    return content_sha256


@contextlib.contextmanager
def deployment_lock(root: pathlib.Path | None = None):
    """Serialize Lightdash deploys across cadences and explicit operators."""
    root = root or state_root()
    root.mkdir(parents=True, exist_ok=True)
    with (root / "deployment.lock").open("a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


# DuckDB's Python driver materializes a whole result before handing rows back,
# so one query per mart would hold the entire table in memory. Reading fixed
# row-id spans keeps that buffer proportional to the span: row-group pruning
# makes each span cost the same wherever it sits in a large mart.
TRANSFER_CHUNK_ROWS = 16384
# The span bounds the result; this bounds the buffer pool that would otherwise
# cache the whole mart while scanning it. A copy scan also needs no more
# parallelism than the writer it follows, and fewer threads means fewer
# per-thread buffers while the transform lock is held.
TRANSFER_MEMORY_LIMIT = "128MB"
TRANSFER_THREADS = "4"


@contextlib.contextmanager
def warehouse_reader(warehouse: pathlib.Path, timeout: float = 120):
    import duckdb
    deadline = time.monotonic() + timeout
    while True:
        try:
            con = duckdb.connect(str(warehouse), read_only=True,
                                 config={"memory_limit": TRANSFER_MEMORY_LIMIT, "threads": TRANSFER_THREADS})
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



SUCCESSFUL_RESULT_STATUSES = frozenset({"success", "pass", "warn"})


def accepted_marts(manifest: dict, results: dict) -> tuple[dict, dict]:
    """Successful scoped marts whose own tests and build ancestors also succeeded."""
    scope = vintage_scope(manifest)
    candidates = family_mart_nodes(manifest, scope["family"], scope["cadence"])
    rows = {row.get("unique_id"): row.get("status") for row in results.get("results", [])}

    def dependencies(uid: str) -> set[str]:
        found, pending = set(), [uid]
        while pending:
            current = pending.pop()
            if current in found:
                continue
            found.add(current)
            pending.extend(manifest.get("nodes", {}).get(current, {}).get("depends_on", {}).get("nodes", []))
        found.discard(uid)
        return found

    selected = {}
    for uid, node in candidates.items():
        if rows.get(uid) != "success":
            continue
        ancestors = dependencies(uid)
        dependency_failed = any(
            dependency in rows and rows[dependency] not in SUCCESSFUL_RESULT_STATUSES
            for dependency in ancestors
        )
        test_failed = any(
            test.get("resource_type") == "test"
            and uid in test.get("depends_on", {}).get("nodes", [])
            and rows.get(test_id) not in SUCCESSFUL_RESULT_STATUSES
            for test_id, test in manifest.get("nodes", {}).items()
            if test_id in rows
        )
        if not dependency_failed and not test_failed:
            selected[uid] = node
    return scope, selected

class StaleBatch(Exception):
    """Abort the publication transaction without touching a sibling table."""

    def __init__(self, tables: list[str]):
        super().__init__("stale publication batch: " + ", ".join(tables))
        self.tables = tables


def serving_tables(selected: dict) -> list[dict]:
    """Validate every accepted mart's identity and columns before any transfer."""
    tables: list[dict] = []
    seen: set[str] = set()
    for uid, node in sorted(selected.items()):
        name = node.get("alias") or node["name"]
        identifier(name)
        identifier(node["schema"])
        cols = columns(node)
        if not cols:
            raise ValueError(f"{name}: accepted mart declares no columns")
        for column in cols:
            identifier(column["name"])
            pg_type(column["type"])
        if name in seen:
            raise ValueError(f"{name}: two accepted marts claim the same serving table")
        seen.add(name)
        tables.append({
            "unique_id": uid,
            "table": name,
            "source_schema": node["schema"],
            "columns": cols,
            "schema_sha256": digest(cols),
        })
    return tables


def batch_identity(invocation: str, manifest_sha256: str, scope: dict, tables: list[dict], content_sha256: str | None) -> str:
    """Keep the publication identity of the retired capture path unchanged."""
    return digest({
        "invocation": invocation,
        "manifest": manifest_sha256,
        "scope": scope,
        "models": sorted(table["unique_id"] for table in tables),
        "content": content_sha256,
    })


def allocate_sequence(root: pathlib.Path) -> int:
    """Mint the next monotonic publication sequence under deployment serialization."""
    counter = root / "sequence"
    sequence = max(time.time_ns(), int(counter.read_text()) + 1 if counter.exists() else 0)
    counter.write_text(str(sequence))
    return sequence


def existing_metadata(final: pathlib.Path, batch_id: str, tables: list[dict]) -> dict:
    """Reuse a previously recorded identity instead of minting a second one."""
    info = json.loads((final / "batch.json").read_text())
    if info.get("schema_version") not in (1, 2) or not isinstance(info.get("sequence"), int):
        raise ValueError(f"invalid publication batch metadata: {final}")
    if info.get("batch_id") != batch_id:
        raise ValueError(f"batch metadata identity does not match its directory: {final}")
    recorded = {row["table"]: row["schema_sha256"] for row in info["tables"]}
    if recorded != {table["table"]: table["schema_sha256"] for table in tables}:
        raise ValueError(f"batch metadata does not describe this build's accepted marts: {final}")
    return info


def fsync_path(path: pathlib.Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def stage_metadata(
    staged: pathlib.Path,
    info: dict,
    manifest: dict,
    results: dict,
    content_root: pathlib.Path | None,
) -> None:
    """Write the durable catalog record and force it out before Postgres commits."""
    if content_root:
        snapshot_content(content_root, staged)
    (staged / "manifest.json").write_text(json.dumps(manifest))
    (staged / "run_results.json").write_text(json.dumps(results))
    (staged / "batch.json").write_text(json.dumps(info, indent=2) + "\n")
    for path in sorted(staged.rglob("*"), reverse=True):
        fsync_path(path)
    fsync_path(staged)


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


def incoming_names(slot: int) -> tuple[str, str]:
    """Scratch names for one batch position's freshly loaded table and index."""
    return f"incoming_mart_{slot}", f"incoming_mart_{slot}_time_brin"


def transfer_table(
    duck, con, entry: dict, slot: int, *, sequence: int, batch_id: str, captured_at: str, expected: int | None = None,
) -> int:
    """Stream one mart from DuckDB into a fresh table inside the open transaction.

    DuckDB serializes every value to text itself: routing dates, timestamps and
    decimals through Python conversions would lose precision and special
    values. Rows are read in bounded row-id spans and written 1024 at a time, so
    no mart is ever buffered whole and no export file is ever written. The new
    table replaces the serving one only in `swap_table`.
    """
    from psycopg import sql

    name = entry["table"]
    cols = entry["columns"]
    definition = sql.SQL(", ").join(
        sql.SQL("{} {}").format(sql.Identifier(column["name"]), sql.SQL(pg_type(column["type"])))
        for column in cols
    )
    table_name, index_name = incoming_names(slot)
    incoming = sql.Identifier("transform_marts", table_name)
    con.execute(sql.SQL("CREATE TABLE {} ({})").format(incoming, definition))
    relation = f"{identifier(entry['source_schema'])}.{identifier(name)}"
    projection = ", ".join(f"CAST({identifier(column['name'])} AS VARCHAR)" for column in cols)
    bounds = duck.execute(f"SELECT min(rowid), max(rowid) FROM {relation}").fetchone()
    sent = 0
    with con.cursor() as cursor, cursor.copy(sql.SQL("COPY {} FROM STDIN").format(incoming)) as stream:
        start = bounds[0]
        while start is not None and start <= bounds[1]:
            stop = min(start + TRANSFER_CHUNK_ROWS - 1, bounds[1])
            duck.execute(f"SELECT {projection} FROM {relation} WHERE rowid BETWEEN {start} AND {stop}")
            while True:
                rows = duck.fetchmany(1024)
                if not rows:
                    break
                for row in rows:
                    stream.write_row(row)
                sent += len(rows)
            start = stop + 1
    count = con.execute(sql.SQL("SELECT count(*) FROM {}").format(incoming)).fetchone()[0]
    if count != sent:
        raise ValueError(f"{name}: transferred {sent} rows but Postgres received {count}")
    if expected is not None and count != expected:
        raise ValueError(f"{name}: warehouse now holds {count} rows, not the {expected} this batch recorded")
    fields = time_columns(cols)
    if fields:
        con.execute(sql.SQL("CREATE INDEX {} ON {} USING brin ({})").format(
            sql.Identifier(index_name), incoming, sql.SQL(", ").join(sql.Identifier(field) for field in fields)))
    con.execute(sql.SQL("ANALYZE {}").format(incoming))
    con.execute("""INSERT INTO _publish.models(model, sequence, batch_id, schema_sha256, row_count, captured_at)
        VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT(model) DO UPDATE SET
        sequence=excluded.sequence, batch_id=excluded.batch_id, schema_sha256=excluded.schema_sha256,
        row_count=excluded.row_count, captured_at=excluded.captured_at, published_at=now()""",
        (name, sequence, batch_id, entry["schema_sha256"], count, captured_at))
    return count


def swap_table(con, entry: dict, slot: int) -> None:
    """Replace a serving table with its freshly loaded copy.

    Dropping the previous table frees its files when the transaction commits;
    replacing its rows in place would instead leave a whole mart of dead tuples
    for vacuum to chase on every publication. Readers wait only for this swap
    and the commit, not for the transfer.
    """
    from psycopg import sql

    name = entry["table"]
    table_name, index_name = incoming_names(slot)
    con.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier("transform_marts", name)))
    con.execute(sql.SQL("ALTER TABLE {} RENAME TO {}").format(
        sql.Identifier("transform_marts", table_name), sql.Identifier(name)))
    if time_columns(entry["columns"]):
        con.execute(sql.SQL("ALTER INDEX {} RENAME TO {}").format(
            sql.Identifier("transform_marts", index_name), sql.Identifier(time_index(name))))


def publish(
    manifest: dict,
    results: dict,
    warehouse: pathlib.Path,
    root: pathlib.Path | None = None,
    *,
    content_root: pathlib.Path | None = None,
    connection=None,
) -> dict:
    """Publish one build's accepted marts in a single streamed transaction.

    The caller holds the transform lock from build through transfer and the
    deployment lock for publication. Only kilobytes of catalog metadata reach
    the disk: a failed transfer is retried by rebuilding from raw data, never
    by replaying a retained export.
    """
    import psycopg

    root = pathlib.Path(root) if root is not None else state_root()
    warehouse = pathlib.Path(warehouse)
    if not results.get("results"):
        raise ValueError("cannot publish without successful dbt run results")
    invocation = results.get("metadata", {}).get("invocation_id")
    if not invocation or invocation != manifest.get("metadata", {}).get("invocation_id"):
        raise ValueError("manifest and run results do not belong to the same dbt invocation")
    scope, selected = accepted_marts(manifest, results)
    if not selected:
        raise ValueError("build produced no successful, fully verified marts for its vintage scope")
    tables = serving_tables(selected)
    content_sha256 = content_digest(content_root) if content_root else None
    manifest_sha256 = digest(manifest)
    batch_id = batch_identity(invocation, manifest_sha256, scope, tables, content_sha256)
    batches = root / "batches"
    batches.mkdir(parents=True, exist_ok=True)
    final = batches / batch_id
    recorded = existing_metadata(final, batch_id, tables) if final.exists() else None
    sequence = recorded["sequence"] if recorded else allocate_sequence(root)
    captured_at = recorded["captured_at"] if recorded else datetime.now(timezone.utc).isoformat()
    expected = {row["table"]: row["rows"] for row in recorded["tables"]} if recorded else {}

    owned = connection is None
    con = connection or psycopg.connect(**connection_args())
    published: list[str] = []
    skipped: list[str] = []
    staged: pathlib.Path | None = None
    try:
        with con.transaction():
            con.execute("SET LOCAL statement_timeout = '20min'")
            con.execute("SET LOCAL lock_timeout = '60s'")
            # Postgres parses the warehouse's own text serialization; pin the
            # interpretation instead of inheriting a server or role default.
            con.execute("SET LOCAL DateStyle = 'ISO, YMD'")
            con.execute("SET LOCAL TimeZone = 'UTC'")
            con.execute("SELECT pg_advisory_xact_lock(863529174)")
            con.execute("CREATE SCHEMA IF NOT EXISTS _publish")
            con.execute("CREATE SCHEMA IF NOT EXISTS transform_marts")
            con.execute("""CREATE TABLE IF NOT EXISTS _publish.models (
                model text PRIMARY KEY, sequence bigint NOT NULL, batch_id text NOT NULL,
                schema_sha256 text NOT NULL, row_count bigint NOT NULL,
                captured_at timestamptz NOT NULL, published_at timestamptz NOT NULL DEFAULT now())""")
            # Classify every model before reading the warehouse: without a
            # retained payload, replaying part of an older batch would label
            # today's DuckDB rows as that old batch.
            pending, stale = [], []
            for entry in tables:
                previous = con.execute(
                    "SELECT sequence, schema_sha256 FROM _publish.models WHERE model=%s",
                    (entry["table"],),
                ).fetchone()
                if previous and previous[0] > sequence:
                    stale.append(entry["table"])
                elif previous and previous[0] == sequence:
                    skipped.append(entry["table"])
                elif previous and previous[1] != entry["schema_sha256"]:
                    raise ValueError(f"{entry['table']}: schema changed; coordinate a serving migration and content deployment")
                else:
                    pending.append(entry)
            if stale:
                raise StaleBatch(stale)
            counts = {}
            if pending:
                with warehouse_reader(warehouse) as duck:
                    for slot, entry in enumerate(pending):
                        counts[entry["table"]] = transfer_table(
                            duck, con, entry, slot, sequence=sequence, batch_id=batch_id,
                            captured_at=captured_at, expected=expected.get(entry["table"]),
                        )
                        published.append(entry["table"])
                for slot, entry in enumerate(pending):
                    swap_table(con, entry, slot)
            if recorded is None:
                # The ledger must never commit a reference to absent metadata.
                staged = pathlib.Path(tempfile.mkdtemp(prefix=".publish-", dir=batches))
                stage_metadata(staged, {
                    "schema_version": 2,
                    "batch_id": batch_id,
                    "sequence": sequence,
                    "captured_at": captured_at,
                    "invocation_id": invocation,
                    "manifest_sha256": manifest_sha256,
                    "vintage_scope": scope,
                    "content_sha256": content_sha256,
                    "tables": [{
                        "unique_id": entry["unique_id"],
                        "table": entry["table"],
                        "columns": entry["columns"],
                        "schema_sha256": entry["schema_sha256"],
                        "rows": counts[entry["table"]],
                    } for entry in tables],
                }, manifest, results, content_root)
                staged.rename(final)
                staged = None
                fsync_path(batches)
        return {"batch_id": batch_id, "path": str(final), "published": published, "skipped": skipped, "stale": []}
    except StaleBatch as superseded:
        return {"batch_id": batch_id, "path": str(final), "published": [], "skipped": [], "stale": superseded.tables}
    except BaseException:
        # Retain metadata already renamed into place: an ambiguous commit still
        # needs its catalog record, and no payload exists to leak.
        if staged is not None:
            shutil.rmtree(staged, ignore_errors=True)
        raise
    finally:
        if owned:
            con.close()


def publication_state(batch_path: pathlib.Path, *, connection=None) -> dict:
    """Report whether every table in a batch is still the serving version."""
    import psycopg

    batch = json.loads((batch_path / "batch.json").read_text())
    owned = connection is None
    con = connection or psycopg.connect(**connection_args())
    current, stale, missing = [], [], []
    try:
        for table in batch["tables"]:
            row = con.execute(
                "SELECT sequence FROM _publish.models WHERE model=%s",
                (table["table"],),
            ).fetchone()
            if not row:
                missing.append(table["table"])
            elif row[0] == batch["sequence"]:
                current.append(table["table"])
            elif row[0] > batch["sequence"]:
                stale.append(table["table"])
            else:
                missing.append(table["table"])
        return {"current": current, "stale": stale, "missing": missing}
    finally:
        if owned:
            con.close()


def serving_model_ledger(*, connection=None) -> dict[str, dict]:
    """Current serving tables and their immutable publication batches."""
    import psycopg

    owned = connection is None
    con = connection or psycopg.connect(**connection_args())
    try:
        rows = con.execute(
            "SELECT model, batch_id, sequence, schema_sha256 FROM _publish.models ORDER BY model"
        ).fetchall()
        return {
            model: {"batch_id": batch_id, "sequence": sequence, "schema_sha256": schema_sha256}
            for model, batch_id, sequence, schema_sha256 in rows
        }
    except psycopg.errors.UndefinedTable:
        return {}
    finally:
        if owned:
            con.close()


def reindex(*, connection=None) -> dict:
    """Rebuild time indexes on the already-published marts.

    Publication creates them, but an installation published before the index
    existed needs them without rebuilding and republishing every mart.
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


LEGACY_TEMPORARY_PREFIX = ".capture-"
METADATA_NAMES = frozenset({"batch.json", "manifest.json", "run_results.json", "content"})


@contextlib.contextmanager
def legacy_capture_lock(root: pathlib.Path | None = None, wait_seconds: float = 120):
    """Hold the retired capture lock, proving no old exporter is still writing."""
    root = root or state_root()
    root.mkdir(parents=True, exist_ok=True)
    with (root / "capture.lock").open("a+") as lock:
        deadline = time.monotonic() + wait_seconds
        while True:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise TimeoutError(
                        "a retired capture still holds capture.lock; drain old transform tasks first"
                    )
                time.sleep(min(1.0, max(0.0, deadline - time.monotonic())))
        try:
            yield
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def allocated_bytes(path: pathlib.Path) -> int:
    return path.lstat().st_blocks * 512


def _directory_payload(path: pathlib.Path) -> tuple[list[pathlib.Path], int]:
    """Every entry of a retired directory, rejecting anything outside the root."""
    root = path.resolve()
    entries, total = [], allocated_bytes(path)
    for child in sorted(path.rglob("*")):
        if child.is_symlink():
            raise ValueError(f"refusing to follow a symlink inside retired export data: {child}")
        if not child.resolve().is_relative_to(root):
            raise ValueError(f"retired export entry escapes its directory: {child}")
        entries.append(child)
        total += allocated_bytes(child)
    return entries, total


def _batch_payloads(batch: pathlib.Path) -> tuple[list[pathlib.Path], list[str]]:
    """Legacy CSV payloads a valid batch.json explicitly lists, and nothing else."""
    try:
        info = json.loads((batch / "batch.json").read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"unreadable batch metadata, refusing to guess: {batch}") from exc
    if not isinstance(info, dict) or info.get("batch_id") != batch.name or not isinstance(info.get("tables"), list):
        raise ValueError(f"invalid batch metadata, refusing to guess: {batch}")
    payloads, listed = [], set()
    for table in info["tables"]:
        name = table.get("file") if isinstance(table, dict) else None
        if name is None:
            continue
        if not isinstance(name, str) or pathlib.PurePosixPath(name).name != name or not name.endswith(".csv"):
            raise ValueError(f"unsafe legacy payload name in {batch}: {name!r}")
        path = batch / name
        listed.add(name)
        if path.is_symlink() or (path.exists() and not path.is_file()):
            raise ValueError(f"unsafe legacy payload in {batch}: {name!r}")
        if path.exists():
            payloads.append(path)
    unknown = []
    for child in sorted(batch.iterdir()):
        if child.is_symlink():
            unknown.append(str(child))
        elif child.name in listed and child.is_file():
            continue
        elif child.name == "content" and child.is_dir():
            # Reviewed content is retained metadata; a payload hidden inside it
            # is unexpected and stays untouched for an operator to judge.
            unknown.extend(str(nested) for nested in sorted(child.rglob("*.csv")))
        elif child.name not in METADATA_NAMES:
            unknown.append(str(child))
    return payloads, unknown


def referenced_metadata(root: pathlib.Path) -> dict:
    """Batches and bundles the live catalog still needs, with anything missing."""
    batches, bundles = set(), set()
    for entry in serving_model_ledger().values():
        batches.add(entry["batch_id"])
    for path in (root / "deployments" / "aggregate.json", root / "deployments" / "current.json"):
        if not path.is_file():
            continue
        value = json.loads(path.read_text())
        if value.get("batch_id"):
            batches.add(value["batch_id"])
        # The aggregate keys its models by name; the receipt lists names only.
        models = value.get("models")
        for record in (models.values() if isinstance(models, dict) else []):
            if isinstance(record, dict) and record.get("batch_id"):
                batches.add(record["batch_id"])
            if isinstance(record, dict) and record.get("baseline_release"):
                bundles.add(record["baseline_release"])
        if value.get("release_sha256"):
            bundles.add(value["release_sha256"])
    missing = [
        f"batch {identity}" for identity in sorted(batches)
        if not (root / "batches" / identity / "batch.json").is_file()
    ] + [
        f"bundle {identity}" for identity in sorted(bundles)
        if not (root / "bundles" / identity / "bundle.json").is_file()
    ]
    return {"batches": sorted(batches), "bundles": sorted(bundles), "missing": missing}


def discard_export_data(root: pathlib.Path | None = None, *, apply: bool = False) -> dict:
    """Remove the retired full-table CSV exports; a dry run is the default.

    Deletion safety comes from the retired data path, not from re-hashing
    hundreds of gibibytes: the caller holds the transform, deployment and
    legacy capture locks, so no old exporter or consumer can still be running.
    """
    root = pathlib.Path(root) if root is not None else state_root()
    batches = root / "batches"
    references = referenced_metadata(root)
    if references["missing"]:
        raise ValueError(
            "refusing to discard export data: live catalog references are missing: "
            + ", ".join(references["missing"])
        )
    entries, unknown = [], []
    if batches.is_dir():
        for child in sorted(batches.iterdir()):
            if child.is_symlink() or not child.is_dir():
                unknown.append(str(child))
                continue
            if child.name.startswith(LEGACY_TEMPORARY_PREFIX):
                paths, total = _directory_payload(child)
                entries.append({"path": str(child), "files": len(paths), "bytes": total,
                                "paths": paths + [child], "directory": True})
                continue
            if not re.fullmatch("[a-f0-9]{64}", child.name):
                unknown.append(str(child))
                continue
            payloads, strays = _batch_payloads(child)
            unknown.extend(strays)
            if payloads:
                entries.append({"path": str(child), "files": len(payloads),
                                "bytes": sum(allocated_bytes(path) for path in payloads),
                                "paths": payloads, "directory": False})
    failed = root / "failed-batches"
    if failed.is_dir():
        for child in sorted(failed.iterdir()):
            if child.is_symlink():
                unknown.append(str(child))
                continue
            if child.is_dir():
                paths, total = _directory_payload(child)
                entries.append({"path": str(child), "files": len(paths), "bytes": total,
                                "paths": paths + [child], "directory": True})
            else:
                entries.append({"path": str(child), "files": 1, "bytes": allocated_bytes(child),
                                "paths": [child], "directory": False})
    failures = []
    removed_files = removed_bytes = 0
    if apply:
        for entry in entries:
            try:
                for path in sorted(entry["paths"], reverse=True):
                    size = allocated_bytes(path)
                    if path.is_dir() and not path.is_symlink():
                        path.rmdir()
                    else:
                        path.unlink()
                        removed_files += 1
                    removed_bytes += size
            except OSError as exc:
                failures.append(f"{entry['path']}: {exc}")
    return {
        "ok": not failures,
        "apply": apply,
        "root": str(root),
        "candidate_files": sum(entry["files"] for entry in entries),
        "candidate_bytes": sum(entry["bytes"] for entry in entries),
        "removed_files": removed_files,
        "removed_bytes": removed_bytes,
        "entries": [{k: v for k, v in entry.items() if k != "paths"} for entry in entries],
        "unknown": unknown,
        "failures": failures,
        "references": {"batches": references["batches"], "bundles": references["bundles"]},
    }
