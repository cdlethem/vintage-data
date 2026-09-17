import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

import duckdb
import yaml


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "extract" / "scripts" / "fetch_who_gho_odata.py"
RAW_DECLARATIONS = REPO_ROOT / "transform" / "models" / "base" / "_raw_sources.yml"
MODEL_SCHEMA = REPO_ROOT / "transform" / "models" / "base" / "base_who_gho_odata.yml"
TRANSFORM = REPO_ROOT / "transform"
SPEC = importlib.util.spec_from_file_location("fetch_who_gho_odata_pipeline", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


def bounded_process(argv, *, cwd, env, label):
    """Run a required child and retain its status plus bounded, path-redacted stderr."""
    try:
        result = subprocess.run(
            argv,
            cwd=cwd,
            env=env,
            text=True,
            capture_output=True,
            timeout=180,
            check=False,
        )
    except FileNotFoundError as error:
        raise AssertionError(f"{label}: required executable is unavailable: {argv[0]}") from error
    except subprocess.TimeoutExpired as error:
        stderr = (error.stderr or "")[-4000:]
        raise AssertionError(f"{label}: timed out; stderr={stderr!r}") from error
    if result.returncode:
        stderr = result.stderr.replace(str(cwd), "<tmp>")
        stdout = result.stdout.replace(str(cwd), "<tmp>")
        stderr = "\n".join(stderr.splitlines()[-40:])[-8000:]
        stdout = "\n".join(stdout.splitlines()[-40:])[-8000:]
        raise AssertionError(
            f"{label}: exit status {result.returncode}; stderr={stderr!r}; stdout={stdout!r}"
        )
    return result


def fixture_observation(
    *, upstream_id, period, dim1, numeric_value, display_value, low, populated_optionals
):
    return {
        "Id": upstream_id,
        "IndicatorCode": "WHOSIS_000001",
        "SpatialDimType": "COUNTRY",
        "SpatialDim": "USA",
        "ParentLocationCode": "AMR" if populated_optionals else None,
        "ParentLocation": "Americas" if populated_optionals else None,
        "TimeDimType": "YEAR",
        "TimeDim": period,
        "Dim1Type": "SEX" if populated_optionals else None,
        "Dim1": dim1,
        "Dim2Type": None,
        "Dim2": None,
        "Dim3Type": None,
        "Dim3": None,
        "DataSourceDimType": None,
        "DataSourceDim": None,
        "NumericValue": numeric_value,
        "Value": display_value,
        "Low": low,
        "High": 82.0 if populated_optionals else None,
        "Comments": "revised estimate" if populated_optionals else None,
        "Date": "2022-08-10T16:16:12.68+02:00" if populated_optionals else None,
        "TimeDimensionValue": str(period) if populated_optionals else None,
        "TimeDimensionBegin": (
            f"{period}-01-01T00:00:00+01:00" if populated_optionals else None
        ),
        "TimeDimensionEnd": (
            f"{period}-12-31T00:00:00+01:00" if populated_optionals else None
        ),
    }


class WhoGhoPipelineTests(unittest.TestCase):
    def test_actual_loader_and_dbt_model_preserve_values_and_lineage(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            raw_root = root / "sink"
            queue = root / "queue"
            raw_db = root / "extract.duckdb"
            transform_db = root / "transform.duckdb"
            source_dir = raw_root / "source=who_gho_odata" / "dt=2026-09-17"
            source_dir.mkdir(parents=True)
            source_url = MODULE._initial_url(
                "WHOSIS_000001",
                MODULE._filter_for("WHOSIS_000001", "COUNTRY", "USA", 2020, 2022),
                100,
            )
            fetched_at = "2026-09-17T12:00:00+00:00"

            def normalized(row):
                return MODULE.normalize_observation(
                    row,
                    fetched_at=fetched_at,
                    source_url=source_url,
                    indicator="WHOSIS_000001",
                    geography_type="COUNTRY",
                    geography="USA",
                    start_year=2020,
                    end_year=2022,
                )

            initial_record = normalized(
                fixture_observation(
                    upstream_id=None,
                    period=2020,
                    dim1=None,
                    numeric_value=None,
                    display_value=None,
                    low=None,
                    populated_optionals=False,
                )
            )
            initial_file = source_dir / "who_gho_odata_20260917T120000Z.ndjson"
            initial_file.write_text(
                json.dumps(initial_record, sort_keys=True) + "\n", encoding="utf-8"
            )

            load_config = root / "load.yml"
            load_config.write_text(
                yaml.safe_dump(
                    {
                        "destination": "local",
                        "destinations": {
                            "local": {
                                "type": "duckdb",
                                "database": str(raw_db),
                                "threads": 1,
                                "memory_limit": "1GB",
                                "lock_timeout_s": 5,
                                "max_attempts": 1,
                            }
                        },
                        "paths": {"source_root": str(raw_root), "queue_dir": str(queue)},
                        "schemas": {"raw": "raw", "meta": "_load"},
                        "cadence": {"enabled": False},
                        "defaults": {
                            "enabled": True,
                            "table": None,
                            "schema_detection": "auto",
                            "sample_lines": 500,
                            "keep_payload": True,
                            "detect_temporal": True,
                            "min_age_s": 0,
                            "on_malformed_lines": "fail",
                            "column_types": {
                                "source": "string",
                                "id": "string",
                                "fetched_at": "timestamp",
                            },
                            "exclude_keys": [],
                        },
                    },
                    sort_keys=False,
                ),
                encoding="utf-8",
            )
            env = os.environ.copy()
            env["PYTHONPATH"] = os.pathsep.join(
                filter(None, (str(REPO_ROOT / "load"), env.get("PYTHONPATH")))
            )

            def run_loader(expected_rows):
                loaded = bounded_process(
                    [
                        sys.executable,
                        "-m",
                        "loader",
                        "--config",
                        str(load_config),
                        "run-once",
                        "--sources",
                        "who_gho_odata",
                    ],
                    cwd=root,
                    env=env,
                    label="repository loader",
                )
                load_result = json.loads(loaded.stdout)
                self.assertEqual(load_result["status"], "ok")
                self.assertEqual(load_result["files_loaded"], 1)
                self.assertEqual(load_result["rows_loaded"], expected_rows)

            run_loader(1)
            optional_columns = {
                "upstream_id",
                "parent_geography_code",
                "parent_geography_name",
                "numeric_value",
                "display_value",
                "low",
                "high",
                "comments",
                "date",
                "time_dimension_value",
                "time_dimension_begin",
                "time_dimension_end",
            }
            raw_connection = duckdb.connect(str(raw_db), read_only=True)
            try:
                initial_columns = {
                    row[0]
                    for row in raw_connection.execute(
                        """
                        select column_name
                        from information_schema.columns
                        where table_schema = 'raw' and table_name = 'who_gho_odata'
                        """
                    ).fetchall()
                }
            finally:
                raw_connection.close()
            self.assertTrue(optional_columns.isdisjoint(initial_columns))

            profiles = root / "profiles"
            profiles.mkdir()
            (profiles / "profiles.yml").write_text(
                yaml.safe_dump(
                    {
                        "vintage_data": {
                            "target": "dev",
                            "outputs": {
                                "dev": {
                                    "type": "duckdb",
                                    "path": str(transform_db),
                                    "schema": "transform",
                                    "threads": 1,
                                    "attach": [
                                        {
                                            "path": str(raw_db),
                                            "alias": "extract",
                                            "read_only": True,
                                        }
                                    ],
                                }
                            },
                        }
                    },
                    sort_keys=False,
                ),
                encoding="utf-8",
            )
            dbt = shutil.which("dbt")
            if dbt is None:
                self.fail("dbt is a required PATH dependency")
            dbt_support = root / "dbt-support"
            dbt_support.mkdir()
            (dbt_support / "sitecustomize.py").write_text(
                "from dbt.adapters.duckdb.connections import DuckDBConnectionManager\n"
                "import dbt.task.runnable as runnable\n"
                "class SerialPool:\n"
                "    def __init__(self, processes, *args, **kwargs):\n"
                "        self.max_threads = processes\n"
                "        self.max_microbatch_models = 1\n"
                "        self.closed = False\n"
                "    def apply_async(self, *args, **kwargs):\n"
                "        raise AssertionError('dbt left single-threaded execution')\n"
                "    def close(self): self.closed = True\n"
                "    def terminate(self): self.closed = True\n"
                "    def join(self): pass\n"
                "    def is_closed(self): return self.closed\n"
                "DuckDBConnectionManager.release = lambda self: None\n"
                "runnable.DbtThreadPool = SerialPool\n",
                encoding="utf-8",
            )
            dbt_env = env.copy()
            dbt_env["DBT_LOG_PATH"] = str(root / "dbt-logs")
            dbt_env.update(
                {
                    "DBT_SINGLE_THREADED": "true",
                    "DBT_INTROSPECT": "false",
                    "OPENBLAS_NUM_THREADS": "1",
                    "OMP_NUM_THREADS": "1",
                    "MKL_NUM_THREADS": "1",
                    "NUMEXPR_NUM_THREADS": "1",
                    "DBT_USE_V2_PARSER": "false",
                    "RAYON_NUM_THREADS": "1",
                }
            )
            dbt_env["PYTHONPATH"] = os.pathsep.join(
                filter(None, (str(dbt_support), env.get("PYTHONPATH")))
            )
            target = root / "dbt-target"
            common = [dbt, "--quiet", "--no-use-colors"]
            paths = [
                "--project-dir",
                str(TRANSFORM),
                "--profiles-dir",
                str(profiles),
                "--target-path",
                str(target),
            ]

            def run_dbt(command):
                bounded_process(
                    common
                    + [command, *paths, "--select", "base_who_gho_odata"],
                    cwd=root,
                    env=dbt_env,
                    label=f"dbt WHO model {command}",
                )

            def model_rows():
                connection = duckdb.connect(str(transform_db), read_only=True)
                try:
                    raw_db_sql = str(raw_db).replace("'", "''")
                    connection.execute(f"ATTACH '{raw_db_sql}' AS extract (READ_ONLY)")
                    return connection.execute(
                        """
                        select
                            source, id, observation_identity, upstream_id,
                            indicator_code, geography_type, geography_code, period,
                            parent_geography_code, parent_geography_name,
                            numeric_value, display_value, low, high, comments, date,
                            time_dimension_value, time_dimension_begin, time_dimension_end,
                            dimensions::varchar, source_url, _row_id, _batch_id,
                            _source_file, _load_id, _loaded_at, _payload::varchar
                        from transform_base.base_who_gho_odata
                        order by period
                        """
                    ).fetchall()
                finally:
                    connection.close()

            run_dbt("run")
            run_dbt("test")
            initial_rows = model_rows()
            self.assertEqual(len(initial_rows), 1)
            initial = initial_rows[0]
            self.assertEqual(initial[0], "who_gho_odata")
            self.assertEqual(initial[1], initial[2])
            self.assertIsNone(initial[3])
            self.assertEqual(
                initial[4:8], ("WHOSIS_000001", "COUNTRY", "USA", 2020)
            )
            self.assertEqual(initial[8:19], (None,) * 11)
            self.assertTrue(all(initial[index] for index in (20, 21, 22, 23, 24, 25)))
            self.assertIsNone(json.loads(initial[26])["numeric_value"])

            subsequent_record = normalized(
                fixture_observation(
                    upstream_id=102,
                    period=2021,
                    dim1="FMLE",
                    numeric_value=78.25,
                    display_value="78.3",
                    low=77.5,
                    populated_optionals=True,
                )
            )
            subsequent_file = source_dir / "who_gho_odata_20260917T130000Z.ndjson"
            subsequent_file.write_text(
                json.dumps(subsequent_record, sort_keys=True) + "\n", encoding="utf-8"
            )
            run_loader(1)
            run_dbt("run")
            run_dbt("test")

            rows = model_rows()
            self.assertEqual(len(rows), 2)
            first, second = rows
            self.assertEqual(first[8:19], (None,) * 11)
            self.assertEqual(second[3], "102")
            self.assertEqual(second[7], 2021)
            self.assertEqual(second[8:15], (
                "AMR",
                "Americas",
                78.25,
                "78.3",
                77.5,
                82.0,
                "revised estimate",
            ))
            self.assertIsNotNone(second[15])
            self.assertEqual(second[16], "2021")
            self.assertIsNotNone(second[17])
            self.assertIsNotNone(second[18])
            self.assertEqual(
                json.loads(second[19])["dim1"], {"type": "SEX", "code": "FMLE"}
            )
            self.assertEqual(second[20], source_url)
            self.assertTrue(all(second[index] for index in (21, 22, 23, 24, 25)))
            self.assertNotEqual(first[1], second[1])

    def test_who_raw_declaration_and_model_assertions_are_scoped(self):
        raw = yaml.safe_load(RAW_DECLARATIONS.read_text(encoding="utf-8"))
        raw_source = next(source for source in raw["sources"] if source["name"] == "raw")
        tables = [table for table in raw_source["tables"] if table["name"] == "who_gho_odata"]
        self.assertEqual(len(tables), 1)
        column_names = {column["name"] for column in tables[0]["columns"]}
        self.assertTrue(
            {
                "_row_id",
                "_batch_id",
                "_source_file",
                "_load_id",
                "_loaded_at",
                "id",
                "indicator_code",
                "geography_type",
                "geography_code",
                "period",
                "numeric_value",
                "display_value",
                "dimensions",
                "source_url",
            }.issubset(column_names)
        )
        schema = yaml.safe_load(MODEL_SCHEMA.read_text(encoding="utf-8"))
        self.assertEqual([model["name"] for model in schema["models"]], ["base_who_gho_odata"])
        columns = {column["name"]: column for column in schema["models"][0]["columns"]}
        self.assertIn("unique", columns["_row_id"]["data_tests"])
        self.assertIn("not_null", columns["id"]["data_tests"])
        self.assertIn("not_null", columns["source_url"]["data_tests"])


if __name__ == "__main__":
    unittest.main()
