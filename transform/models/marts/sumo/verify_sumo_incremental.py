#!/usr/bin/env python3
"""Exercise the Sumo mart through initial, incremental-replay, and full-refresh dbt runs."""
from __future__ import annotations

import os
import pathlib
import shutil
import subprocess
import sys
import tempfile
from typing import Any

import duckdb
import yaml

ROOT = pathlib.Path(__file__).resolve().parents[4]
PROJECT_DIR = ROOT / "transform"
MODEL = "fct_sumo_rikishi_observation"
MAX_DIAGNOSTIC_LINES = 24
MAX_DIAGNOSTIC_CHARS = 6_000

COLUMNS = (
    "_row_id",
    "_source",
    "_batch_id",
    "_source_file",
    "_file_row_num",
    "_dt",
    "_extract_started_at",
    "_load_id",
    "_loaded_at",
    "_content_hash",
    "_payload",
    "source",
    "id",
    "fetched_at",
    "shikona_en",
    "shikona_jp",
    "current_rank",
    "heya",
    "height_cm",
    "weight_kg",
    "debut",
    "updated_at",
)

INITIAL_ROWS = (
    (
        "stale-row", "sumo", "stale-batch", "01-stale.jsonl", 1, "2026-09-01",
        "2026-09-01 00:00:00+00", "stale-load", "2026-09-01 00:05:00+00",
        "stale-hash", "{}", "sumo_rikishi", "42", "2026-09-01 00:00:00+00",
        "Stale name", None, "Juryo 1 East", "Stale-beya", 179.0, 149.0,
        "2020.01", "2026-08-29 00:00:00+00",
    ),
    (
        "tie-stale", "sumo", "tie-stale-batch", "00-tie.jsonl", 1, "2026-09-01",
        "2026-09-01 00:00:00+00", "tie-stale-load", "2026-09-01 00:10:00+00",
        "tie-stale-hash", "{}", "sumo_rikishi", "43", "2026-09-01 00:00:00+00",
        "Tie stale", None, "Makushita 1 East", "Tie-beya", 180.0, 150.0,
        "2021.01", "2026-08-29 00:00:00+00",
    ),
)

REPLAY_ROWS = (
    (
        "replay-row", "sumo", "replay-batch", "02-replay.jsonl", 7, "2026-09-01",
        "2026-09-01 00:10:00+00", "replay-load", "2026-09-01 00:15:00+00",
        "replay-hash", "{}", "sumo_rikishi", "42", "2026-09-01 00:00:00+00",
        "Corrected name", "corrected-jp", "Maegashira 3 West", "Corrected-beya", 181.0,
        151.0, "2020.01", "2026-09-01 00:12:00+00",
    ),
    (
        "next-snapshot", "sumo", "next-batch", "03-next.jsonl", 1, "2026-09-02",
        "2026-09-02 00:00:00+00", "next-load", "2026-09-02 00:05:00+00",
        "next-hash", "{}", "sumo_rikishi", "42", "2026-09-02 00:00:00+00",
        "Next snapshot", None, "Maegashira 3 West", "Corrected-beya", 181.0, 151.0,
        "2020.01", "2026-09-01 00:12:00+00",
    ),
    (
        "tie-file-a", "sumo", "tie-a-batch", "01-tie.jsonl", 99, "2026-09-01",
        "2026-09-01 00:10:00+00", "tie-a-load", "2026-09-01 00:15:00+00",
        "tie-a-hash", "{}", "sumo_rikishi", "43", "2026-09-01 00:00:00+00",
        "Tie file A", None, "Makushita 2 East", "Tie-beya", 180.0, 150.0,
        "2021.01", "2026-09-01 00:12:00+00",
    ),
    (
        "tie-line-one", "sumo", "tie-line-batch", "02-tie.jsonl", 1, "2026-09-01",
        "2026-09-01 00:10:00+00", "tie-line-load", "2026-09-01 00:15:00+00",
        "tie-line-hash", "{}", "sumo_rikishi", "43", "2026-09-01 00:00:00+00",
        "Tie line one", None, "Makushita 2 West", "Tie-beya", 180.0, 150.0,
        "2021.01", "2026-09-01 00:12:00+00",
    ),
    (
        "tie-row-a", "sumo", "tie-row-a-batch", "02-tie.jsonl", 2, "2026-09-01",
        "2026-09-01 00:10:00+00", "tie-row-a-load", "2026-09-01 00:15:00+00",
        "tie-row-a-hash", "{}", "sumo_rikishi", "43", "2026-09-01 00:00:00+00",
        "Tie row A", None, "Makushita 3 East", "Tie-beya", 180.0, 150.0,
        "2021.01", "2026-09-01 00:12:00+00",
    ),
    (
        "tie-row-z", "sumo", "tie-row-z-batch", "02-tie.jsonl", 2, "2026-09-01",
        "2026-09-01 00:10:00+00", "tie-row-z-load", "2026-09-01 00:15:00+00",
        "tie-row-z-hash", "{}", "sumo_rikishi", "43", "2026-09-01 00:00:00+00",
        "Tie winner", None, "Makushita 4 West", "Winning-beya", 182.0, 152.0,
        "2021.01", "2026-09-01 00:12:00+00",
    ),
    (
        "basho-row", "sumo", "basho-batch", "basho.jsonl", 1, "2026-09-02",
        "2026-09-02 00:00:00+00", "basho-load", "2026-09-02 00:06:00+00",
        "basho-hash", "{}", "sumo_basho", "202609", "2026-09-01 00:00:00+00",
        None, None, None, None, None, None, None, None,
    ),
)


def model(dbt: Any, session: Any) -> Any:
    """Keep dbt's Python-model parser from treating this operator script as a model."""
    dbt.config(enabled=False)
    return session.sql("select 1 where false")


def _create_extract(path: pathlib.Path) -> None:
    with duckdb.connect(str(path)) as connection:
        connection.execute("create schema raw")
        connection.execute(
            """
            create table raw.sumo (
                _row_id varchar,
                _source varchar,
                _batch_id varchar,
                _source_file varchar,
                _file_row_num bigint,
                _dt date,
                _extract_started_at timestamp with time zone,
                _load_id varchar,
                _loaded_at timestamp with time zone,
                _content_hash varchar,
                _payload json,
                source varchar,
                id varchar,
                fetched_at timestamp with time zone,
                shikona_en varchar,
                shikona_jp varchar,
                current_rank varchar,
                heya varchar,
                height_cm double,
                weight_kg double,
                debut varchar,
                updated_at timestamp with time zone
            )
            """
        )
        _insert_rows(connection, INITIAL_ROWS)


def _insert_rows(connection: duckdb.DuckDBPyConnection, rows: tuple[tuple[Any, ...], ...]) -> None:
    placeholders = ", ".join("?" for _ in COLUMNS)
    connection.executemany(
        f"insert into raw.sumo ({', '.join(COLUMNS)}) values ({placeholders})",
        rows,
    )


def _append_replay(path: pathlib.Path) -> None:
    with duckdb.connect(str(path)) as connection:
        _insert_rows(connection, REPLAY_ROWS)


def _write_profile(path: pathlib.Path, warehouse: pathlib.Path, extract: pathlib.Path) -> None:
    profile = {
        "vintage_data": {
            "target": "harness",
            "outputs": {
                "harness": {
                    "type": "duckdb",
                    "path": str(warehouse),
                    "schema": "harness",
                    "threads": 1,
                    "attach": [{"path": str(extract), "alias": "extract", "read_only": True}],
                }
            },
        }
    }
    path.write_text(yaml.safe_dump(profile, sort_keys=False))


def _category(output: str) -> str:
    lowered = output.lower()
    if "compilation error" in lowered or "parsing error" in lowered:
        return "dbt compilation"
    if "constraint error" in lowered or "database error" in lowered or "runtime error" in lowered:
        return "dbt database execution"
    if "profile" in lowered or "credentials" in lowered:
        return "isolated profile setup"
    return "dbt child process"


def _bounded_diagnostic(output: str, temporary_root: pathlib.Path) -> str:
    redacted = output.replace(str(temporary_root), "<temp>").replace(str(ROOT), "<repo>")
    lines = [line for line in redacted.splitlines() if line.strip()]
    bounded = "\n".join(lines[-MAX_DIAGNOSTIC_LINES:])
    if len(bounded) > MAX_DIAGNOSTIC_CHARS:
        bounded = "[earlier diagnostic text omitted]\n" + bounded[-MAX_DIAGNOSTIC_CHARS:]
    return bounded


def _run_dbt(
    executable: str,
    temporary_root: pathlib.Path,
    profiles_dir: pathlib.Path,
    arguments: tuple[str, ...],
) -> int:
    command = (
        executable,
        *arguments,
        "--target",
        "harness",
        "--project-dir",
        str(PROJECT_DIR),
        "--profiles-dir",
        str(profiles_dir),
        "--target-path",
        str(temporary_root / "target"),
        "--log-path",
        str(temporary_root / "logs"),
        "--no-use-colors",
        "--no-static-parser",
    )
    environment = {
        "HOME": str(temporary_root),
        "PATH": os.pathsep.join((str(pathlib.Path(executable).parent), "/usr/bin", "/bin")),
        "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
    }
    completed = subprocess.run(
        command,
        cwd=temporary_root,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode:
        output = "\n".join(part for part in (completed.stderr, completed.stdout) if part)
        print(
            f"Sumo incremental verification failed: {_category(output)} "
            f"(child exit {completed.returncode})",
            file=sys.stderr,
        )
        diagnostic = _bounded_diagnostic(output, temporary_root)
        if diagnostic:
            print(diagnostic, file=sys.stderr)
    return completed.returncode


def _rows(warehouse: pathlib.Path) -> list[tuple[Any, ...]]:
    with duckdb.connect(str(warehouse), read_only=True) as connection:
        return connection.execute(
            f"select * from harness_marts.{MODEL} "
            "order by source_system, rikishi_id, observed_at"
        ).fetchall()


def _summary(rows: list[tuple[Any, ...]]) -> list[tuple[Any, ...]]:
    return [(row[1], row[2], row[3], row[4], row[13], row[14], row[15], row[21]) for row in rows]


def _assert_initial(rows: list[tuple[Any, ...]]) -> None:
    summary = _summary(rows)
    if len(summary) != 2 or {row[4] for row in summary} != {"stale-row", "tie-stale"}:
        raise AssertionError("initial dbt run did not retain the two initial roster grains")


def _assert_reconciled(rows: list[tuple[Any, ...]]) -> None:
    summary = _summary(rows)
    grains = [(row[0], row[1], row[2]) for row in summary]
    if len(grains) != 3 or len(set(grains)) != 3:
        raise AssertionError("incremental result does not contain exactly three unique roster grains")
    by_row_id = {row[4]: row for row in summary}
    if set(by_row_id) != {"replay-row", "next-snapshot", "tie-row-z"}:
        raise AssertionError("incremental result retained the wrong deterministic replay winners")
    replay = by_row_id["replay-row"]
    if replay[3] != "Corrected name" or replay[5:] != (
        "replay-batch", "02-replay.jsonl", "replay-hash"
    ):
        raise AssertionError("replay winner did not replace stale attributes and lineage")
    if by_row_id["next-snapshot"][2] == replay[2]:
        raise AssertionError("separate observation timestamps were collapsed")


def verify() -> int:
    executable = shutil.which("dbt")
    if executable is None:
        print("Sumo incremental verification failed: dbt executable unavailable", file=sys.stderr)
        return 127

    with tempfile.TemporaryDirectory(prefix="sumo-incremental-") as directory:
        temporary_root = pathlib.Path(directory)
        extract = temporary_root / "extract.duckdb"
        warehouse = temporary_root / "warehouse.duckdb"
        profiles_dir = temporary_root / "profiles"
        profiles_dir.mkdir()
        _create_extract(extract)
        _write_profile(profiles_dir / "profiles.yml", warehouse, extract)

        status = _run_dbt(executable, temporary_root, profiles_dir, ("run", "--select", f"+{MODEL}"))
        if status:
            return status
        _assert_initial(_rows(warehouse))

        _append_replay(extract)
        status = _run_dbt(executable, temporary_root, profiles_dir, ("run", "--select", MODEL))
        if status:
            return status
        incremental_rows = _rows(warehouse)
        _assert_reconciled(incremental_rows)

        status = _run_dbt(
            executable,
            temporary_root,
            profiles_dir,
            ("run", "--full-refresh", "--select", MODEL),
        )
        if status:
            return status
        full_refresh_rows = _rows(warehouse)
        _assert_reconciled(full_refresh_rows)
        if incremental_rows != full_refresh_rows:
            raise AssertionError("incremental replay result differs from combined-input full refresh")

    print("Sumo incremental verified: initial + replay rows match combined-input full refresh")
    return 0


def main() -> int:
    try:
        return verify()
    except (AssertionError, duckdb.Error, OSError, ValueError, yaml.YAMLError) as error:
        print(f"Sumo incremental verification failed: {type(error).__name__}: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
