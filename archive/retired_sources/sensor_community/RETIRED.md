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

- `bots/validation_catalog.py` `COMMANDS`:

  ```python
  "smoke-sensor-community": _public_smoke(
      "sensor_community", "sensor_community", "/work/extract/scripts/fetch_sensor_community.py", "--country", "DE",
      source_url="https://data.sensor.community/airrohr/v1/filter/country=DE", expected_status=200,
      timeout_seconds=240, required_equals_key="country", required_equals_value="DE", summary_required=True,
  ),
  ```

- `orchestration/provider_bot_dashboard/.../validation_recipes.py` `DEFAULT_RECIPE_CATALOG`:

  ```python
  "smoke-sensor-community": {"recipe": "public_source_smoke", "capability": "public-network-readonly",
                             "source_url": "https://data.sensor.community/airrohr/v1/filter/country=DE", "expected_status": 200},
  ```

- Generated Lightdash charts `fct-sensor-community-measurement*.yml` and the
  `sensor-community` dashboard, recreated by `viz content` once the mart is back.
