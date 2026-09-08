{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['daily']
) }}

with source_rows as (
    select
        md5(to_json(struct_pack(
            source := cast(source as varchar),
            event_id := cast(id as varchar),
            observed_at := cast(fetched_at as timestamp with time zone)
        )))::varchar as smithsonian_volcanism_snapshot_key,
        cast(source as varchar) as source,
        cast(id as varchar) as event_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(volcano_number as bigint) as volcano_number,
        cast(volcano_name as varchar) as volcano_name,
        try_strptime(nullif(trim(start_date), ''), '%Y%m%d')::date as event_start_date,
        try_strptime(nullif(trim(end_date), ''), '%Y%m%d')::date as event_end_date,
        cast(continuing as boolean) as continuing,
        cast(explosivity_index_max as bigint) as explosivity_index_max,
        cast(latitude as double) as latitude,
        cast(longitude as double) as longitude,
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
    from {{ ref('base_smithsonian_volcanism') }}
)

select
    smithsonian_volcanism_snapshot_key,
    source,
    event_id,
    observed_at,
    volcano_number,
    volcano_name,
    event_start_date,
    event_end_date,
    continuing,
    explosivity_index_max,
    latitude,
    longitude,
    _batch_id,
    _load_id,
    _source_file,
    _file_row_num,
    source_loaded_at,
    _content_hash
from source_rows
where _dedupe_rank = 1
