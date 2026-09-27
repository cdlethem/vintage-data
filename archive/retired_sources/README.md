# Retired sources

Sources in this directory were deliberately switched off. The authoritative
list, with the reason for each retirement and where its collected data is kept,
is `extract/retired_sources.yml`.

**Automated agents must not bring these back.** Do not move files out of this
directory, recreate their extract configs, scripts, base views or marts,
propose them (or another integration of the same upstream) in source discovery,
model their raw tables, add smoke tests for them, or backfill them. The guards
listed in `extract/retired_sources.yml` reject them; do not weaken those guards.

Each source directory mirrors the repository paths its files came from, so a
human-approved resume is a move back rather than a rewrite:

| Archived path | Original location |
|---|---|
| `<name>/extract/sources/<name>.yml` | `extract/sources/` |
| `<name>/extract/scripts/fetch_<name>.py` | `extract/scripts/` |
| `<name>/discovery/staged_scripts/fetch_<name>.py` | `discovery/staged_scripts/` (staged copy) |
| `<name>/transform/models/base/base_<name>.sql` | `transform/models/base/` |
| `<name>/transform/models/marts/<name>/` | `transform/models/marts/` |

## Collected data

Nothing collected was deleted. Raw NDJSON and manifests stay under
`$EXTRACT_DATA_ROOT/raw/source=<name>/`, and the DuckDB `raw.<name>` table and
the source's mart stay in the warehouse. They are no longer loaded or rebuilt,
and they are not migrated to BigQuery.

## Resuming a source (human decision only)

Resuming costs money in the cloud warehouse; see the reason recorded in
`extract/retired_sources.yml` and re-estimate before deciding.

1. Remove the source's entry from `extract/retired_sources.yml` and its
   `enabled: false` override from `load/config/load.yml`.
2. `git mv` every file under `archive/retired_sources/<name>/` back to the
   original location shown above, then delete the empty archive directory.
3. Set `enabled: true` in the restored extract config once collection should
   restart. Review the cadence first: both retired sources were polled far more
   often than the near-free target allows.
4. Regenerate the raw declarations (`transform/bin/sync_raw_sources --write-all`)
   and the Lightdash content (`visualization/bin/viz content`), then run the
   project validation and tests.
5. Restore any smoke test or monitoring metadata noted in the source's
   `RETIRED.md`.
