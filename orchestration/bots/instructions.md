## About this repository

This is vintage-data: public, keyless data sources flow through Airflow extract DAGs into raw NDJSON, then a loader into DuckDB, then dbt models. Read `agents.md` at the repository root before changing anything; it is the project's rulebook (fetcher conventions, source YAML, state files, retired sources).

- Each `extract__<name>` DAG runs the script named in `extract/sources/<name>.yml` through `orchestration/include/extract_runner.py`. The fetcher's stderr and its `VINTAGE_RUN_SUMMARY` line appear in the task log.
- Fetcher tests live in `extract/` (`extract/test_*.py` and `extract/tests/`). Virtualenvs are not part of this checkout; run tests with `uv run --no-project --with pytest --with pyyaml python -m pytest <paths>` and add `--with <package>` if an import is missing.
- You may call a source's public endpoint a few times to reproduce a problem. Use temporary state (`--no-state` or a temp `--state-file`), never production state.
- Never re-enable, restore, or modify anything listed in `extract/retired_sources.yml`.
- If a source is permanently gone, now requires a key, or its licence changed, don't delete it: answer `ask` and recommend what to do.
