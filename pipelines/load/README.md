# load (planned)

Raw NDJSON files → data warehouse.

Responsibilities when built:
- ingest `$EXTRACT_DATA_ROOT/raw/source=<name>/dt=<date>/*.ndjson` incrementally
- deduplicate on `(source, id)` across runs (the extract stage deliberately re-fetches
  overlapping windows rather than tracking state)
- land typed tables the transform stage can build on
