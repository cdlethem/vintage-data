"""Read-only exploratory access to the serving marts.

Analysts must look at real data before proposing a chart, but no specialist or
confined executor may hold a warehouse credential. This module is the
capability: it loads the reader password itself, opens a read-only transaction
as `mart_reader`, and refuses anything that is not a single bounded SELECT
against `transform_marts`.

The reader role cannot write, and the read-only transaction plus statement
timeout bound the damage a bad query can do to the serving database that
Lightdash queries.
"""
from __future__ import annotations

import argparse
import json
import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
SCHEMA = "transform_marts"
MAX_ROWS = 500
STATEMENT_TIMEOUT_MS = 60_000
# One statement, read-only, no server-side side effects. Rejecting the verbs
# outright is belt-and-braces over the read-only role and transaction.
FORBIDDEN = re.compile(
    r"(?is)\b(insert|update|delete|truncate|drop|alter|create|grant|revoke|copy|vacuum|analyze"
    r"|reindex|cluster|comment|call|do|set|reset|begin|commit|rollback|lock|listen|notify"
    r"|prepare|execute|deallocate|discard|refresh|import|attach|detach|pg_sleep|pg_read_file"
    r"|pg_ls_dir|lo_import|lo_export|dblink|postgres_fdw)\b")


def load_environment() -> None:
    sys.path.insert(0, str(ROOT))
    sys.path.insert(0, str(ROOT / "orchestration" / "include"))
    import deployment
    deployment.load_env()
    deployment.load_env(ROOT / "orchestration" / "airflow.secrets.env")


def check(sql: str) -> str:
    statement = sql.strip().rstrip(";").strip()
    if not statement:
        raise ValueError("empty query")
    if ";" in statement:
        raise ValueError("one statement per call; ';' is not allowed")
    if not re.match(r"(?is)^(select|with)\b", statement):
        raise ValueError("only SELECT or WITH ... SELECT queries are allowed")
    found = FORBIDDEN.search(statement)
    if found:
        raise ValueError(f"disallowed keyword {found.group(0)!r} in a read-only query")
    return statement


def run(sql: str, *, limit: int = MAX_ROWS) -> dict:
    import psycopg
    from visualization.publisher import connection_args
    statement = check(sql)
    limit = max(1, min(int(limit), MAX_ROWS))
    with psycopg.connect(**connection_args(reader=True)) as connection:
        connection.read_only = True
        with connection.cursor() as cursor:
            cursor.execute(f"SET LOCAL statement_timeout = {STATEMENT_TIMEOUT_MS}")
            cursor.execute(f"SET LOCAL search_path = {SCHEMA}")
            cursor.execute(statement)
            columns = [description.name for description in cursor.description or []]
            rows = cursor.fetchmany(limit + 1)
    truncated = len(rows) > limit
    return {"columns": columns, "row_count": min(len(rows), limit), "truncated": truncated,
            "rows": [[None if value is None else str(value) for value in row] for row in rows[:limit]]}


def profile(model: str, *, sample_rows: int = 200_000) -> dict:
    """Row count, per-column null share and cardinality, and date-column spans.

    One pass over the table computes every column at once: a column-at-a-time
    loop needs as many scans as there are columns, which no statement timeout
    survives on a hundred-million-row mart. Above `sample_rows` the aggregates
    run over a deterministic head of the table so the shape is still measured
    rather than guessed; `sampled_rows` says so in the result.
    """
    import psycopg
    from visualization.publisher import connection_args
    if not re.fullmatch(r"[a-z_][a-z0-9_]*", model):
        raise ValueError(f"invalid mart name {model!r}")
    with psycopg.connect(**connection_args(reader=True)) as connection:
        connection.read_only = True
        with connection.cursor() as cursor:
            cursor.execute(f"SET LOCAL statement_timeout = {STATEMENT_TIMEOUT_MS}")
            cursor.execute("select column_name, data_type from information_schema.columns"
                           " where table_schema = %s and table_name = %s order by ordinal_position",
                           (SCHEMA, model))
            columns = cursor.fetchall()
            if not columns:
                raise ValueError(f"{model} is not a published mart in {SCHEMA}")
            cursor.execute("select coalesce(row_count, 0) from _publish.models where model = %s", (model,))
            declared = cursor.fetchone()
            rows = int(declared[0]) if declared else 0
            if not rows:
                cursor.execute(f'select count(*) from {SCHEMA}."{model}"')
                rows = cursor.fetchone()[0]
            sampled = rows > sample_rows
            source = (f'(select * from {SCHEMA}."{model}" limit {sample_rows}) sampled'
                      if sampled else f'{SCHEMA}."{model}"')
            selects, plan = [], []
            for name, data_type in columns:
                quoted = f'"{name}"'
                selects += [f"count({quoted})", f"count(distinct {quoted})"]
                temporal = data_type.startswith(("date", "timestamp"))
                if temporal:
                    selects += [f"min({quoted})::text", f"max({quoted})::text",
                                f"count(distinct date_trunc('month', {quoted}))",
                                f"count(distinct date_trunc('day', {quoted}))"]
                plan.append((name, data_type, temporal))
            cursor.execute(f"select {', '.join(selects)} from {source}")
            values = list(cursor.fetchone() or [])
    scanned = min(rows, sample_rows) if sampled else rows
    fields, dates, offset = [], [], 0
    for name, data_type, temporal in plan:
        filled, distinct = values[offset], values[offset + 1]
        offset += 2
        entry = {"column": name, "type": data_type, "non_null": filled, "distinct": distinct,
                 "null_share": None if not scanned else round(1 - filled / scanned, 4)}
        if temporal:
            low, high, months, days = values[offset:offset + 4]
            offset += 4
            if filled:
                entry.update(min=low, max=high, distinct_months=months, distinct_days=days)
                dates.append(name)
        fields.append(entry)
    result = {"model": model, "rows": rows, "date_columns": dates, "columns": fields}
    if sampled:
        # Cardinalities and spans come from a head sample, so a rare value or an
        # older date can sit outside it. Say so rather than letting an analyst
        # read a sampled span as the published history.
        result["sampled_rows"] = scanned
        result["note"] = (f"aggregates cover the first {scanned:,} of {rows:,} rows;"
                          " confirm exact spans and cardinalities with eda query")
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["query", "profile", "models"])
    parser.add_argument("argument", nargs="?", help="SQL for query, mart name for profile")
    parser.add_argument("--limit", type=int, default=MAX_ROWS)
    args = parser.parse_args()
    load_environment()
    if args.command == "models":
        result = run("select table_name from information_schema.tables"
                     f" where table_schema = '{SCHEMA}' order by table_name", limit=MAX_ROWS)
    elif args.command == "profile":
        if not args.argument:
            parser.error("profile requires a mart name")
        result = profile(args.argument)
    else:
        if not args.argument:
            parser.error("query requires SQL")
        result = run(args.argument, limit=args.limit)
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
