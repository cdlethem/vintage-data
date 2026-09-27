# openactive_feeds — retired 2026-09-22

Do not re-enable. The reason, preserved data locations and the rules for
automated agents are in `extract/retired_sources.yml`; the resume procedure is
in `../README.md`.

The extractor's RPDE cursors remain in `$EXTRACT_DATA_ROOT/state/openactive_rpde.json`.
Resuming from them continues where collection stopped; delete the file to start
the feeds from the beginning instead.

Removed alongside the archived files, to restore only on a human-approved resume:

- `monitoring/source_types.json`:

  ```json
  "openactive_feeds": {
   "type": "feed",
   "notes": "RPDE change feed; updated and deleted item states are both expected and cursors advance only after flushed output"
  }
  ```

- Generated Lightdash charts `fct-openactive-feed*.yml` and the
  `openactive-feeds` dashboard, recreated by `viz content` once the mart is back.
