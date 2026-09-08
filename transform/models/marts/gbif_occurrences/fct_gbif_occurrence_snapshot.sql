{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['daily']
) }}

with source_rows as (
    select
        md5(to_json(struct_pack(
            source := cast(source as varchar),
            occurrence_id := cast(id as varchar),
            fetched_at := cast(fetched_at as timestamp with time zone)
        ))) as gbif_occurrence_snapshot_key,
        cast(source as varchar) as source,
        cast(id as varchar) as occurrence_id,
        cast(fetched_at as timestamp with time zone) as fetched_at,
        cast(species as varchar) as species,
        cast(scientific_name as varchar) as scientific_name,
        cast(kingdom as varchar) as kingdom,
        cast(event_date as timestamp with time zone) as event_date,
        cast(ingested as timestamp with time zone) as ingested,
        cast(lat as double) as latitude,
        cast(lon as double) as longitude,
        cast(country as varchar) as country,
        cast(basis as varchar) as basis_of_record,
        cast(_batch_id as varchar) as _batch_id,
        cast(_load_id as varchar) as _load_id,
        cast(_source_file as varchar) as _source_file,
        cast(_file_row_num as bigint) as _file_row_num,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as _content_hash,
        row_number() over (
            partition by source, id, fetched_at
            order by _loaded_at desc, _source_file desc, _file_row_num desc
        ) as _dedupe_rank
    from {{ ref('base_gbif_occurrences') }}
)

select
    gbif_occurrence_snapshot_key,
    source,
    occurrence_id,
    fetched_at,
    species,
    scientific_name,
    kingdom,
    event_date,
    ingested,
    latitude,
    longitude,
    country,
    basis_of_record,
    _batch_id,
    _load_id,
    _source_file,
    _file_row_num,
    source_loaded_at,
    _content_hash
from source_rows
where _dedupe_rank = 1
