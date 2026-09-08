{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['twice_hourly']
) }}

with source_rows as (
    select
        md5(
            to_json(
                struct_pack(
                    airport_id := cast(id as varchar),
                    observed_at := cast(fetched_at as timestamp with time zone)
                )
            )
        )::varchar as faa_nas_status_observation_key,
        cast(source as varchar) as source_name,
        cast(id as varchar) as airport_id,
        cast(airport as varchar) as airport,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(has_ground_stop as boolean) as has_ground_stop,
        cast(has_ground_delay as boolean) as has_ground_delay,
        cast(ground_stop_reason as varchar) as ground_stop_reason,
        cast(ground_delay_reason as varchar) as ground_delay_reason,
        cast(ground_delay_avg_min as double) as ground_delay_avg_min,
        cast(ground_delay_max_min as bigint) as ground_delay_max_min,
        cast(source_timestamp as timestamp with time zone) as source_timestamp,
        cast(_row_id as varchar) as _row_id,
        cast(_batch_id as varchar) as _batch_id,
        cast(_source_file as varchar) as _source_file,
        cast(_file_row_num as bigint) as _file_row_num,
        cast(_dt as date) as _dt,
        cast(_extract_started_at as timestamp with time zone) as _extract_started_at,
        cast(_load_id as varchar) as _load_id,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as _content_hash,
        row_number() over (
            partition by id, fetched_at
            order by _loaded_at desc, _source_file desc, _file_row_num desc, _row_id desc
        ) as observation_rank
    from {{ ref('base_faa_nas_status') }}
)

select
    faa_nas_status_observation_key,
    source_name,
    airport_id,
    airport,
    observed_at,
    has_ground_stop,
    has_ground_delay,
    ground_stop_reason,
    ground_delay_reason,
    ground_delay_avg_min,
    ground_delay_max_min,
    source_timestamp,
    _row_id,
    _batch_id,
    _source_file,
    _file_row_num,
    _dt,
    _extract_started_at,
    _load_id,
    source_loaded_at,
    _content_hash
from source_rows
where observation_rank = 1
