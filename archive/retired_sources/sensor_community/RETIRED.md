# sensor_community — retired 2026-09-22

Do not re-enable. The reason, preserved data locations and the rules for
automated agents are in `extract/retired_sources.yml`; the resume procedure is
in `../README.md`.

Removed alongside the archived files, to restore only on a human-approved resume:

- `monitoring/source_types.json`:

  ```json
  "sensor_community": {
   "type": "status",
   "notes": "global latest-reading snapshot; reading id/timestamp define novelty, never fetched_at"
  }
  ```

- Generated Lightdash charts `fct-sensor-community-measurement*.yml` and the
  `sensor-community` dashboard, recreated by `viz content` once the mart is back.
