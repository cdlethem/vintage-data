"""Behavioral dbt contract tests for fct_uk_parliament_procedural_activity.

Renders the actual model SQL (this file's sibling
``fct_uk_parliament_procedural_activity.sql``) with a minimal Jinja stub for
``ref``/``config`` and executes it against deterministic in-memory DuckDB
fixtures. No external database, network, Git metadata, credentials, or
production access is required or used.

This file also declares a disabled dbt Python model (see ``model`` below) so
that ``dbt parse`` accepts it as a project file under ``models/`` without
treating it as a build target: the node lands in ``manifest['disabled']``,
never in ``manifest['nodes']``, so it is invisible to mart/contract policy
checks (``transform/scripts/validate_project.py``) and to Lightdash coverage
validation (``visualization/project.py: mart_nodes``, which iterates
``manifest['nodes']``), and dbt never attempts to build it.
"""
from __future__ import annotations

import json
import pathlib
import unittest

import duckdb
import jinja2


def model(dbt, session):  # dbt Python-model entrypoint; never executed.
    dbt.config(enabled=False)
    return None


MART_SQL_PATH = pathlib.Path(__file__).parent / "fct_uk_parliament_procedural_activity.sql"
BASE_FIXTURE_TABLE = "base_fixture"

# Columns `source_rows` in the mart SQL reads from
# `ref('base_uk_parliament_procedural_activity')`, matching the raw source
# columns forwarded unmodified by that base view.
BASE_FIXTURE_DDL = f"""
create table {BASE_FIXTURE_TABLE} (
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
    source varchar,
    id varchar,
    fetched_at varchar,
    localid varchar,
    businessitemdate varchar,
    layingdate varchar
)
"""

BASE_FIXTURE_COLUMNS = (
    "_row_id", "_source", "_batch_id", "_source_file", "_file_row_num", "_dt",
    "_extract_started_at", "_load_id", "_loaded_at", "_content_hash",
    "source", "id", "fetched_at", "localid", "businessitemdate", "layingdate",
)


def render_mart_sql() -> str:
    """Render the real mart SQL, substituting `ref` and a no-op `config`."""
    raw = MART_SQL_PATH.read_text()
    return jinja2.Template(raw).render(ref=lambda *args: BASE_FIXTURE_TABLE, config=lambda **kwargs: "")


def business_item_dates(*entries: dict) -> str:
    return json.dumps(list(entries))


def fixture_row(
    *,
    row_id: str,
    procedural_item_id: str,
    fetched_at: str | None,
    business_item_date_json: str,
    laying_date: str | None = None,
    source_file: str = "file-1.ndjson",
    file_row_num: int = 1,
    loaded_at: str = "2026-01-01T00:00:00Z",
    batch_id: str = "batch-1",
    source_relation: str = "uk_parliament_procedural_activity",
    source_name: str = "uk_parliament_procedural_activity",
) -> tuple:
    return (
        row_id, source_relation, batch_id, source_file, file_row_num, "2026-01-01",
        "2026-01-01T00:00:00Z", "load-1", loaded_at, f"hash-{row_id}",
        source_name, procedural_item_id, fetched_at, procedural_item_id,
        business_item_date_json, laying_date,
    )


# Deterministic fixture rows covering every required behavioral case.
FIXTURE_ROWS: dict[str, tuple] = {
    # Raw JSON preservation: business_item_dates must round-trip verbatim and
    # business_item_date_count must equal the source array length.
    "json_preservation": fixture_row(
        row_id="row-json", procedural_item_id="item-json", fetched_at="2026-01-01T10:00:00Z",
        business_item_date_json=business_item_dates(
            {"BusinessItemDate": "2026-01-01T10:00:00Z"}, {"BusinessItemDate": "2026-01-05T15:30:00Z"},
        ),
    ),
    # Empty schedule-date array: count zero, no first/last scheduled date.
    "empty_array": fixture_row(
        row_id="row-empty", procedural_item_id="item-empty", fetched_at="2026-01-01T10:00:00Z",
        business_item_date_json=business_item_dates(),
    ),
    # Mixed valid/malformed entries: count reflects the raw array length, but
    # first/last scheduled dates skip the malformed entry entirely.
    "mixed_valid_malformed": fixture_row(
        row_id="row-mixed", procedural_item_id="item-mixed", fetched_at="2026-01-01T10:00:00Z",
        business_item_date_json=business_item_dates(
            {"BusinessItemDate": "2026-01-02T00:00:00Z"},
            {"BusinessItemDate": "not-a-date"},
            {"BusinessItemDate": "2026-01-09T00:00:00Z"},
        ),
    ),
    # Offset-to-UTC normalization: -05:00 at 23:00 local is 04:00 UTC the next day.
    "offset_normalization": fixture_row(
        row_id="row-offset", procedural_item_id="item-offset", fetched_at="2026-01-01T10:00:00Z",
        business_item_date_json=business_item_dates({"BusinessItemDate": "2026-01-03T23:00:00-05:00"}),
    ),
    # Null/invalid timestamps: unparseable fetched_at and absent laying_date
    # tolerantly resolve to NULL rather than rejecting the row; an
    # unparseable schedule-date entry still counts toward the raw array
    # length but contributes no first/last scheduled date.
    "null_invalid_timestamps": fixture_row(
        row_id="row-null", procedural_item_id="item-null", fetched_at="not-a-timestamp",
        laying_date=None, business_item_date_json=business_item_dates({"BusinessItemDate": "garbage"}),
    ),
    # Duplicate grain (same source_relation, procedural_item_id, observed_at)
    # with different lineage: the later `_loaded_at` record wins.
    "dup_lineage_a": fixture_row(
        row_id="row-dup-a", procedural_item_id="item-dup", fetched_at="2026-01-01T10:00:00Z",
        business_item_date_json=business_item_dates({"BusinessItemDate": "2026-01-01T00:00:00Z"}),
        source_file="file-1.ndjson", file_row_num=6, loaded_at="2026-01-01T00:00:00Z",
    ),
    "dup_lineage_b": fixture_row(
        row_id="row-dup-b", procedural_item_id="item-dup", fetched_at="2026-01-01T10:00:00Z",
        business_item_date_json=business_item_dates(
            {"BusinessItemDate": "2026-01-01T00:00:00Z"}, {"BusinessItemDate": "2026-01-02T00:00:00Z"},
        ),
        source_file="file-2.ndjson", file_row_num=6, loaded_at="2026-01-02T00:00:00Z",
    ),
    # Deterministic tie, level 1: same source_loaded_at, differ by
    # _source_file; the lexicographically greater file name wins.
    "tie_file_a": fixture_row(
        row_id="row-tie-file-a", procedural_item_id="item-tie-file", fetched_at="2026-01-01T10:00:00Z",
        business_item_date_json=business_item_dates({"BusinessItemDate": "2026-01-01T00:00:00Z"}),
        source_file="file-a.ndjson", file_row_num=7, loaded_at="2026-01-01T00:00:00Z",
    ),
    "tie_file_b": fixture_row(
        row_id="row-tie-file-b", procedural_item_id="item-tie-file", fetched_at="2026-01-01T10:00:00Z",
        business_item_date_json=business_item_dates(
            {"BusinessItemDate": "2026-01-01T00:00:00Z"}, {"BusinessItemDate": "2026-01-02T00:00:00Z"},
        ),
        source_file="file-b.ndjson", file_row_num=7, loaded_at="2026-01-01T00:00:00Z",
    ),
    # Deterministic tie, level 2: same source_loaded_at and _source_file,
    # differ by _file_row_num; the greater row number wins.
    "tie_row_num_a": fixture_row(
        row_id="row-tie-rownum-a", procedural_item_id="item-tie-rownum", fetched_at="2026-01-01T10:00:00Z",
        business_item_date_json=business_item_dates({"BusinessItemDate": "2026-01-01T00:00:00Z"}),
        source_file="file-c.ndjson", file_row_num=8, loaded_at="2026-01-01T00:00:00Z",
    ),
    "tie_row_num_b": fixture_row(
        row_id="row-tie-rownum-b", procedural_item_id="item-tie-rownum", fetched_at="2026-01-01T10:00:00Z",
        business_item_date_json=business_item_dates(
            {"BusinessItemDate": "2026-01-01T00:00:00Z"}, {"BusinessItemDate": "2026-01-02T00:00:00Z"},
        ),
        source_file="file-c.ndjson", file_row_num=9, loaded_at="2026-01-01T00:00:00Z",
    ),
    # Deterministic tie, level 3: everything above ties too, differ only by
    # _row_id; the lexicographically greater row id wins.
    "tie_row_id_a": fixture_row(
        row_id="row-a", procedural_item_id="item-tie-rowid", fetched_at="2026-01-01T10:00:00Z",
        business_item_date_json=business_item_dates({"BusinessItemDate": "2026-01-01T00:00:00Z"}),
        source_file="file-d.ndjson", file_row_num=10, loaded_at="2026-01-01T00:00:00Z",
    ),
    "tie_row_id_b": fixture_row(
        row_id="row-b", procedural_item_id="item-tie-rowid", fetched_at="2026-01-01T10:00:00Z",
        business_item_date_json=business_item_dates(
            {"BusinessItemDate": "2026-01-01T00:00:00Z"}, {"BusinessItemDate": "2026-01-02T00:00:00Z"},
        ),
        source_file="file-d.ndjson", file_row_num=10, loaded_at="2026-01-01T00:00:00Z",
    ),
    # Primary defect fixture: uk_parliament_procedural_activity/300, observed
    # twice at genuinely different `observed_at` timestamps. The grain is
    # (source_relation, procedural_item_id, observed_at); two distinct
    # observed_at values are two legitimate rows, not a duplicate.
    "grain_version_1": fixture_row(
        row_id="row-300a", procedural_item_id="300", fetched_at="2026-01-01T09:00:00Z",
        business_item_date_json=business_item_dates({"BusinessItemDate": "2026-01-05T00:00:00Z"}),
        file_row_num=11,
    ),
    "grain_version_2": fixture_row(
        row_id="row-300b", procedural_item_id="300", fetched_at="2026-01-01T15:00:00Z",
        business_item_date_json=business_item_dates(
            {"BusinessItemDate": "2026-01-05T00:00:00Z"}, {"BusinessItemDate": "2026-01-06T00:00:00Z"},
        ),
        file_row_num=12,
    ),
    # Out-of-order valid entries: first/last scheduled date must reflect the
    # chronological min/max, not array position.
    "out_of_order_dates": fixture_row(
        row_id="row-order", procedural_item_id="item-order", fetched_at="2026-01-01T10:00:00Z",
        business_item_date_json=business_item_dates(
            {"BusinessItemDate": "2026-03-01T00:00:00Z"},
            {"BusinessItemDate": "2026-01-15T00:00:00Z"},
            {"BusinessItemDate": "2026-02-10T00:00:00Z"},
        ),
    ),
    # Larger array: count equality must hold beyond the two/three-entry cases
    # already covered above.
    "larger_array": fixture_row(
        row_id="row-larger", procedural_item_id="item-larger", fetched_at="2026-01-01T10:00:00Z",
        business_item_date_json=business_item_dates(
            {"BusinessItemDate": "2026-01-01T00:00:00Z"},
            {"BusinessItemDate": "2026-01-02T00:00:00Z"},
            {"BusinessItemDate": "2026-01-03T00:00:00Z"},
            {"BusinessItemDate": "2026-01-04T00:00:00Z"},
        ),
    ),
}


def build_mart() -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect(":memory:")
    connection.execute(BASE_FIXTURE_DDL)
    placeholders = ", ".join(["?"] * len(BASE_FIXTURE_COLUMNS))
    connection.executemany(
        f"insert into {BASE_FIXTURE_TABLE} values ({placeholders})", list(FIXTURE_ROWS.values()),
    )
    connection.execute(f"create table mart as ({render_mart_sql()})")
    return connection


def one_row_at_grain(connection, source_relation: str, procedural_item_id: str, observed_at):
    """The declared grain is (source_relation, procedural_item_id, observed_at);
    a correct helper must select on the full tuple, not a subset of it."""
    cursor = connection.execute(
        "select * from mart where source_relation is not distinct from ? "
        "and procedural_item_id is not distinct from ? and observed_at is not distinct from ?",
        [source_relation, procedural_item_id, observed_at],
    )
    columns = [description[0] for description in cursor.description]
    rows = cursor.fetchall()
    if len(rows) != 1:
        raise AssertionError(
            f"expected exactly one row for {source_relation}/{procedural_item_id}/{observed_at}, "
            f"found {len(rows)}"
        )
    return dict(zip(columns, rows[0]))


class ProceduralActivityContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.connection = build_mart()

    @classmethod
    def tearDownClass(cls):
        cls.connection.close()

    def _row(self, procedural_item_id: str, observed_at: str = "2026-01-01T10:00:00Z"):
        return one_row_at_grain(
            self.connection, "uk_parliament_procedural_activity", procedural_item_id, observed_at,
        )

    def test_raw_json_preserved_and_count_equals_array_length(self):
        row = self._row("item-json")
        self.assertEqual(row["business_item_date_count"], 2)
        self.assertEqual(
            json.loads(row["business_item_dates"]),
            [{"BusinessItemDate": "2026-01-01T10:00:00Z"}, {"BusinessItemDate": "2026-01-05T15:30:00Z"}],
        )

    def test_empty_schedule_dates(self):
        row = self._row("item-empty")
        self.assertEqual(row["business_item_date_count"], 0)
        self.assertIsNone(row["first_scheduled_at"])
        self.assertIsNone(row["last_scheduled_at"])

    def test_mixed_valid_and_malformed_schedule_dates(self):
        row = self._row("item-mixed")
        self.assertEqual(row["business_item_date_count"], 3)
        self.assertEqual(str(row["first_scheduled_at"]), "2026-01-02 00:00:00+00:00")
        self.assertEqual(str(row["last_scheduled_at"]), "2026-01-09 00:00:00+00:00")

    def test_offset_normalized_to_utc(self):
        row = self._row("item-offset")
        self.assertEqual(str(row["first_scheduled_at"]), "2026-01-04 04:00:00+00:00")
        self.assertEqual(row["first_scheduled_at"], row["last_scheduled_at"])

    def test_first_last_scheduled_dates_ignore_array_order(self):
        row = self._row("item-order")
        self.assertEqual(row["business_item_date_count"], 3)
        self.assertEqual(str(row["first_scheduled_at"]), "2026-01-15 00:00:00+00:00")
        self.assertEqual(str(row["last_scheduled_at"]), "2026-03-01 00:00:00+00:00")

    def test_count_equals_array_length_for_larger_arrays(self):
        row = self._row("item-larger")
        self.assertEqual(row["business_item_date_count"], 4)
        self.assertEqual(str(row["first_scheduled_at"]), "2026-01-01 00:00:00+00:00")
        self.assertEqual(str(row["last_scheduled_at"]), "2026-01-04 00:00:00+00:00")

    def test_null_and_invalid_timestamps_are_tolerated(self):
        row = self._row("item-null", observed_at=None)
        self.assertIsNone(row["observed_at"])
        self.assertIsNone(row["laid_at"])
        self.assertEqual(row["business_item_date_count"], 1)
        self.assertIsNone(row["first_scheduled_at"])
        self.assertIsNone(row["last_scheduled_at"])

    def test_duplicate_grain_selects_latest_loaded_lineage(self):
        row = self._row("item-dup")
        self.assertEqual(row["_row_id"], "row-dup-b")
        self.assertEqual(row["business_item_date_count"], 2)
        self.assertEqual(str(row["source_loaded_at"]), "2026-01-02 00:00:00+00:00")

    def test_tie_breaks_on_source_file_when_loaded_at_matches(self):
        row = self._row("item-tie-file")
        self.assertEqual(row["_row_id"], "row-tie-file-b")
        self.assertEqual(row["_source_file"], "file-b.ndjson")
        self.assertEqual(row["business_item_date_count"], 2)

    def test_tie_breaks_on_file_row_num_when_file_matches(self):
        row = self._row("item-tie-rownum")
        self.assertEqual(row["_row_id"], "row-tie-rownum-b")
        self.assertEqual(row["_file_row_num"], 9)
        self.assertEqual(row["business_item_date_count"], 2)

    def test_tie_breaks_on_row_id_as_final_tiebreaker(self):
        row = self._row("item-tie-rowid")
        self.assertEqual(row["_row_id"], "row-b")
        self.assertEqual(row["business_item_date_count"], 2)

    def test_source_relation_is_part_of_the_grain(self):
        """Regression: the grain is (source_relation, procedural_item_id,
        observed_at). Item 300 has two genuinely distinct observed_at
        versions, so both survive dedup as separate rows; a helper that
        checks uniqueness on (source_relation, procedural_item_id) alone
        wrongly reports two rows as a duplicate."""
        version_1 = one_row_at_grain(
            self.connection, "uk_parliament_procedural_activity", "300", "2026-01-01T09:00:00Z",
        )
        version_2 = one_row_at_grain(
            self.connection, "uk_parliament_procedural_activity", "300", "2026-01-01T15:00:00Z",
        )
        self.assertNotEqual(version_1["observed_at"], version_2["observed_at"])
        self.assertEqual(version_1["business_item_date_count"], 1)
        self.assertEqual(version_2["business_item_date_count"], 2)

    def test_grain_is_not_satisfied_by_source_relation_and_item_alone(self):
        total = self.connection.execute(
            "select count(*) from mart where source_relation = 'uk_parliament_procedural_activity' "
            "and procedural_item_id = '300'"
        ).fetchone()[0]
        self.assertEqual(total, 2, "two distinct observed_at versions are two rows, not a duplicate")

    def test_no_grain_key_has_more_than_one_row(self):
        duplicates = self.connection.execute(
            "select source_relation, procedural_item_id, observed_at, count(*) as n "
            "from mart group by 1, 2, 3 having count(*) > 1"
        ).fetchall()
        self.assertEqual(duplicates, [], f"grain violated: {duplicates}")

    def test_mart_key_is_unique(self):
        total = self.connection.execute("select count(*) from mart").fetchone()[0]
        distinct = self.connection.execute(
            "select count(distinct uk_parliament_procedural_activity_key) from mart"
        ).fetchone()[0]
        self.assertEqual(total, distinct)


if __name__ == "__main__":
    unittest.main()
