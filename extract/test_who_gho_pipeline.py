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


def fixture_observation(*, upstream_id, period, dim1, numeric_value, display_value, low):
    return {
        "Id": upstream_id,
        "IndicatorCode": "WHOSIS_000001",
        "SpatialDimType": "COUNTRY",
        "SpatialDim": "USA",
        "ParentLocationCode": "AMR",
        "ParentLocation": "Americas",
        "TimeDimType": "YEAR",
        "TimeDim": period,
        "Dim1Type": "SEX",
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
        "High": 82.0,
        "Comments": None,
        "Date": "2022-08-10T16:16:12.68+02:00",
        "TimeDimensionValue": str(period),
        "TimeDimensionBegin": f"{period}-01-01T00:00:00+01:00",
        "TimeDimensionEnd": f"{period}-12-31T00:00:00+01:00",
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
            sink_file = source_dir / "who_gho_odata_20260917T120000Z.ndjson"
            source_url = MODULE._initial_url(
                "WHOSIS_000001",
                MODULE._filter_for("WHOSIS_000001", "COUNTRY", "USA", 2020, 2022),
                100,
            )
            fetched_at = "2026-09-17T12:00:00+00:00"
            records = [
                MODULE.normalize_observation(
                    fixture_observation(
                        upstream_id=101,
                        period=2020,
                        dim1="BTSX",
                        numeric_value=78.25,
                        display_value="78.3",
                        low=77.5,
                    ),
                    fetched_at=fetched_at,
                    source_url=source_url,
                    indicator="WHOSIS_000001",
                    geography_type="COUNTRY",
                    geography="USA",
                    start_year=2020,
                    end_year=2022,
                ),
                MODULE.normalize_observation(
                    fixture_observation(
                        upstream_id=102,
                        period=2021,
                        dim1="FMLE",
                        numeric_value=None,
                        display_value=None,
                        low=None,
                    ),
                    fetched_at=fetched_at,
                    source_url=source_url,
                    indicator="WHOSIS_000001",
                    geography_type="COUNTRY",
                    geography="USA",
                    start_year=2020,
                    end_year=2022,
                ),
            ]
            sink_file.write_text(
                "".join(json.dumps(record, sort_keys=True) + "\n" for record in records),
                encoding="utf-8",
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
                            "sample_lines": 0,
                            "keep_payload": True,
                            "detect_temporal": True,
                            "min_age_s": 0,
                            "on_malformed_lines": "fail",
                            "column_types": {
                                "source": "string",
                                "id": "string",
                                "fetched_at": "timestamp",
                                "observation_identity": "string",
                                "upstream_id": "string",
                                "indicator_code": "string",
                                "geography_type": "string",
                                "geography_code": "string",
                                "parent_geography_code": "string",
                                "parent_geography_name": "string",
                                "period_type": "string",
                                "period": "integer",
                                "numeric_value": "double",
                                "display_value": "string",
                                "low": "double",
                                "high": "double",
                                "comments": "string",
                                "date": "timestamp",
                                "time_dimension_value": "string",
                                "time_dimension_begin": "timestamp",
                                "time_dimension_end": "timestamp",
                                "dimensions": "json",
                                "source_url": "string",
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
            self.assertEqual(load_result["rows_loaded"], 2)

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
            # This sandbox cannot create dbt's worker thread. dbt's hidden serial
            # mode still constructs an unused pool and shares the master connection
            # identity with node runners. The process-local support module removes
            # only those two serial-mode incompatibilities; dbt still parses,
            # materializes, and tests the model itself.
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
            bounded_process(
                common
                + [
                    "run",
                    *paths,
                    "--select",
                    "base_who_gho_odata",
                ],
                cwd=root,
                env=dbt_env,
                label="dbt WHO model run",
            )
            bounded_process(
                common
                + [
                    "test",
                    *paths,
                    "--select",
                    "base_who_gho_odata",
                ],
                cwd=root,
                env=dbt_env,
                label="dbt WHO model tests",
            )

            connection = duckdb.connect(str(transform_db), read_only=True)
            try:
                raw_db_sql = str(raw_db).replace("'", "''")
                connection.execute(
                    f"ATTACH '{raw_db_sql}' AS extract (READ_ONLY)"
                )
                rows = connection.execute(
                    """
                    select
                        source, id, observation_identity, indicator_code,
                        geography_type, geography_code, period, numeric_value,
                        display_value, low, high, dimensions::varchar, source_url,
                        _row_id, _batch_id, _source_file, _load_id, _loaded_at,
                        _payload::varchar
                    from transform_base.base_who_gho_odata
                    order by period
                    """
                ).fetchall()
            finally:
                connection.close()

            self.assertEqual(len(rows), 2)
            first, second = rows
            self.assertEqual(first[0], "who_gho_odata")
            self.assertEqual(first[1], first[2])
            self.assertEqual(first[3:7], ("WHOSIS_000001", "COUNTRY", "USA", 2020))
            self.assertEqual(first[7:11], (78.25, "78.3", 77.5, 82.0))
            self.assertEqual(json.loads(first[11])["dim1"], {"type": "SEX", "code": "BTSX"})
            self.assertEqual(first[12], source_url)
            self.assertTrue(all(first[index] for index in (13, 14, 15, 16, 17)))
            self.assertEqual(json.loads(first[18])["upstream_id"], "101")
            self.assertEqual(second[6], 2021)
            self.assertIsNone(second[7])
            self.assertIsNone(second[8])
            self.assertIsNone(second[9])
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
