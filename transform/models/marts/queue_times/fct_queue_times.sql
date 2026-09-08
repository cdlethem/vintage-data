{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['twice_hourly']
) }}

with normalized as (
    select
        cast(source as varchar) as source,
        cast(id as varchar) as observation_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(park_id as bigint) as park_id,
        cast(park as varchar) as park,
        cast(land_id as bigint) as land_id,
        cast(land as varchar) as land,
        cast(ride_id as bigint) as ride_id,
        cast(ride as varchar) as ride,
        cast(is_open as boolean) as is_open,
        cast(wait_minutes as bigint) as wait_minutes,
        cast(last_updated as timestamp with time zone) as last_updated_at,
        cast(_row_id as varchar) as _row_id,
        cast(_batch_id as varchar) as _batch_id,
        cast(_source_file as varchar) as _source_file,
        cast(_file_row_num as bigint) as _file_row_num,
        cast(_dt as date) as _dt,
        cast(_extract_started_at as timestamp with time zone) as _extract_started_at,
        cast(_load_id as varchar) as _load_id,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as _content_hash
    from {{ ref('base_queue_times') }}
),

keyed as (
    select
        cast(md5(to_json(struct_pack(
            source := source,
            park_id := park_id,
            land_id := land_id,
            ride_id := ride_id,
            observed_at := observed_at
        ))) as varchar) as queue_time_observation_key,
        source,
        observation_id,
        observed_at,
        park_id,
        park,
        land_id,
        land,
        ride_id,
        ride,
        is_open,
        wait_minutes,
        last_updated_at,
        _row_id,
        _batch_id,
        _source_file,
        _file_row_num,
        _dt,
        _extract_started_at,
        _load_id,
        source_loaded_at,
        _content_hash
    from normalized
),

deduplicated as (
    select *
    from keyed
    qualify row_number() over (
        partition by queue_time_observation_key
        order by source_loaded_at desc, _source_file desc, _file_row_num desc, _row_id desc
    ) = 1
)

select
    cast(queue_time_observation_key as varchar) as queue_time_observation_key,
    cast(source as varchar) as source,
    cast(observation_id as varchar) as observation_id,
    cast(observed_at as timestamp with time zone) as observed_at,
    cast(park_id as bigint) as park_id,
    cast(park as varchar) as park,
    cast(land_id as bigint) as land_id,
    cast(land as varchar) as land,
    cast(ride_id as bigint) as ride_id,
    cast(ride as varchar) as ride,
    cast(is_open as boolean) as is_open,
    cast(wait_minutes as bigint) as wait_minutes,
    cast(last_updated_at as timestamp with time zone) as last_updated_at,
    cast(_row_id as varchar) as _row_id,
    cast(_batch_id as varchar) as _batch_id,
    cast(_source_file as varchar) as _source_file,
    cast(_file_row_num as bigint) as _file_row_num,
    cast(_dt as date) as _dt,
    cast(_extract_started_at as timestamp with time zone) as _extract_started_at,
    cast(_load_id as varchar) as _load_id,
    cast(source_loaded_at as timestamp with time zone) as source_loaded_at,
    cast(_content_hash as varchar) as _content_hash
from deduplicated
