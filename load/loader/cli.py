"""Command line for the load layer.

Invoke it through ``load/bin/loader``, which works from any directory
and sources ``orchestration/airflow.env`` first — so the CLI is always pointed at
the same warehouse and queue as the running service. ``python -m loader`` works
too, but only from ``load/`` or with it on ``PYTHONPATH``.

    python -m loader service              run the single writer (systemd uses this)
    python -m loader submit --wait        enqueue a scan job and wait for it
    python -m loader backfill             everything the ledger hasn't seen yet
    python -m loader run-once             load inline, without the service
    python -m loader status               service, queue and warehouse at a glance
    python -m loader inspect <source>     show the schema that would be inferred
    python -m loader sql "SELECT ..."     read-only query against the warehouse

``backfill`` and the scheduled incremental run are deliberately the same
operation: "load every file the ledger doesn't already have". The first run
just happens to find three thousand of them.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import uuid
from dataclasses import replace

from .config import load_config
from .destinations import get_destination
from .discovery import scan
from .queue import Job, JobQueue
from .schema import infer_columns, sanitize, table_columns
from .service import LoaderService, sample_records


def _job_body(args, kind="scan") -> dict:
    body = {"kind": kind, "load_id": uuid.uuid4().hex,
            "max_files": args.max_files if args.max_files > 0 else None}
    if getattr(args, "sources", None):
        body["sources"] = args.sources
    if getattr(args, "paths", None):
        body["kind"] = "files"
        body["paths"] = args.paths
    return body


def _print_result(result: dict) -> int:
    print(json.dumps(result, indent=2, default=str))
    return 0 if result.get("status") in ("ok",) else 1


def cmd_service(args) -> int:
    LoaderService(load_config(args.config)).run_forever()
    return 0


def cmd_submit(args) -> int:
    config = load_config(args.config)
    queue = JobQueue(config.queue_dir)
    status = queue.service_status()
    if status is None:
        print("warning: no service heartbeat found — is extract-loader running?",
              file=sys.stderr)
    job_id = queue.submit(_job_body(args))
    print(f"submitted {job_id} (queue depth {queue.depth()})", file=sys.stderr)
    if not args.wait:
        return 0
    result = queue.wait(job_id, timeout_s=args.timeout, poll_s=2.0)
    if result is None:
        print(f"timed out after {args.timeout}s waiting for {job_id}", file=sys.stderr)
        return 2
    return _print_result(result)


def cmd_run_once(args) -> int:
    """Do a load in this process. Fails if the service already holds the lock."""
    config = load_config(args.config)
    service = LoaderService(config)
    job = Job(job_id=uuid.uuid4().hex, body=_job_body(args), path=config.queue_dir / "inline")
    result = service.run_job(job)
    service.destination.close()
    return _print_result(result)


#: Read-only commands wait only briefly for the lock: the point of `status` and
#: `sql` is a quick answer, and "the service is mid-job" is itself the answer.
READONLY_LOCK_TIMEOUT_S = 20


def _readonly_destination(config):
    return get_destination({**config.destination, "read_only": True,
                            "lock_timeout_s": READONLY_LOCK_TIMEOUT_S},
                           config.raw_schema, config.meta_schema)


def cmd_status(args) -> int:
    config = load_config(args.config)
    queue = JobQueue(config.queue_dir)
    print(f"config      {config.path}")
    print(f"destination {config.destination.get('name')} "
          f"({config.destination.get('type')}) -> {config.destination.get('database')}")
    print(f"sink        {config.source_root}")
    print(f"queue       {config.queue_dir}  depth={queue.depth()}")
    print(f"service     {json.dumps(queue.service_status() or {'state': 'unknown'})}")

    files = scan(config.source_root, min_age_s=0)
    print(f"sink files  {len(files)} across {len({f.source for f in files})} sources")

    destination = _readonly_destination(config)
    try:
        destination.connect()
        rows = destination.summary()
    except Exception as exc:
        # Also covers a warehouse that exists but has no ledger yet.
        print(f"warehouse   unreadable right now ({exc})")
        destination.close()
        return 0
    try:
        loaded = sum(r["files_loaded"] or 0 for r in rows)
        total = sum(r["rows_loaded"] or 0 for r in rows)
        print(f"warehouse   {len(rows)} tables, {loaded} files loaded, {total:,} rows")
        if args.verbose:
            for row in sorted(rows, key=lambda r: -(r["rows_loaded"] or 0)):
                print(f"  {row['table']:<40} {row['rows_loaded'] or 0:>12,} rows  "
                      f"{row['files_loaded'] or 0:>4} files  last={row['last_loaded_at']}")
    finally:
        destination.close()
    return 0


def cmd_inspect(args) -> int:
    """Show the schema inference result for a source without touching the warehouse."""
    config = load_config(args.config)
    settings = config.for_source(args.source)
    if args.all_lines:
        settings = replace(settings, sample_lines=0)
    files = scan(config.source_root, sources=[args.source], min_age_s=0)
    if not files:
        print(f"no files under {config.source_root}/source={args.source}", file=sys.stderr)
        return 1
    files = files[-args.files:]
    sample: list[dict] = []
    for file in files:
        records, _ = sample_records(file.path, settings.sample_lines)
        sample.extend(records)
    columns = table_columns(infer_columns(sample, settings), settings)
    table = settings.table or sanitize(args.source)
    print(f"-- {config.raw_schema}.{table}  "
          f"(inferred from {len(sample)} records in {len(files)} file(s))")
    for column in columns:
        origin = f"  -- {column.key}" if column.key and column.key != column.name else ""
        print(f"  {column.name:<40} {column.type}{origin}")
    return 0


def cmd_sql(args) -> int:
    """Run one read-only query. Handy because the standalone duckdb CLI is a
    separate install, while this venv already has the driver."""
    config = load_config(args.config)
    query = sys.stdin.read() if args.query == "-" else args.query
    destination = _readonly_destination(config)
    try:
        destination.connect()
    except Exception as exc:
        print(f"warehouse unreadable right now ({exc})", file=sys.stderr)
        return 2
    try:
        if args.utc:
            destination.con.execute("SET timezone = 'UTC'")
        result = destination.con.execute(query)
        names = [d[0] for d in result.description or []]
        rows = result.fetchall()
    finally:
        destination.close()

    if not names:
        return 0
    widths = [max(len(n), *(len(str(r[i])) for r in rows)) if rows else len(n)
              for i, n in enumerate(names)]
    print("  ".join(n.ljust(w) for n, w in zip(names, widths)))
    print("  ".join("-" * w for w in widths))
    for row in rows:
        print("  ".join(str(v).ljust(w) for v, w in zip(row, widths)))
    print(f"({len(rows)} rows)", file=sys.stderr)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="loader", description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--config", default=None, help="path to load.yml")
    parser.add_argument("--log-level", default="INFO")
    sub = parser.add_subparsers(dest="command", required=True)

    def with_job_args(p, default_max=0):
        p.add_argument("--sources", nargs="*", help="limit to these source names")
        p.add_argument("--paths", nargs="*", help="load these sink files specifically")
        p.add_argument("--max-files", type=int, default=default_max,
                       help="cap files per job (0 = no cap)")
        return p

    sub.add_parser("service", help="run the single writer").set_defaults(func=cmd_service)

    p = with_job_args(sub.add_parser("submit", help="enqueue a load job"))
    p.add_argument("--wait", action="store_true")
    p.add_argument("--timeout", type=float, default=3600)
    p.set_defaults(func=cmd_submit)

    p = with_job_args(sub.add_parser("backfill", help="enqueue an uncapped load job"))
    p.add_argument("--wait", action="store_true", default=True)
    p.add_argument("--timeout", type=float, default=14400)
    p.set_defaults(func=cmd_submit)

    p = with_job_args(sub.add_parser("run-once", help="load inline, without the service"))
    p.set_defaults(func=cmd_run_once)

    p = sub.add_parser("status", help="service, queue and warehouse summary")
    p.add_argument("-v", "--verbose", action="store_true", help="per-table breakdown")
    p.set_defaults(func=cmd_status)

    p = sub.add_parser("sql", help="run a read-only query against the warehouse")
    p.add_argument("query", help='SQL to run, or "-" to read it from stdin')
    p.add_argument("--utc", action="store_true",
                   help="render TIMESTAMPTZ in UTC instead of the local timezone")
    p.set_defaults(func=cmd_sql)

    p = sub.add_parser("inspect", help="show the schema inferred for a source")
    p.add_argument("source")
    p.add_argument("--files", type=int, default=3, help="how many recent files to sample")
    p.add_argument("--all-lines", action="store_true", help="sample every line")
    p.set_defaults(func=cmd_inspect)
    return parser


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(level=args.log_level.upper(),
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    return args.func(args)
