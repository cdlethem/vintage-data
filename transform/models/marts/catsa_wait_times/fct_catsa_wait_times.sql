{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['twice_hourly']
) }}

with normalized as (
    select
        cast(source as varchar) as source,
        cast(id as varchar) as checkpoint_id,
        cast(airport_slug as varchar) as airport_slug,
        cast(checkpoint as varchar) as checkpoint,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(wait_time as varchar) as wait_time_text,
        cast(
            case
                when trim(wait_time) = 'Not available' then false
                else true
            end as boolean
        ) as wait_time_available,
        cast(
            case
                when regexp_matches(trim(wait_time), '^[0-9]+[^0-9]+[0-9]+ min$') then
                    try_cast(regexp_extract(trim(wait_time), '^([0-9]+)[^0-9]+([0-9]+) min$', 1) as bigint)
                else null
            end as bigint
        ) as wait_time_min_minutes,
        cast(
            case
                when regexp_matches(trim(wait_time), '^[0-9]+[^0-9]+[0-9]+ min$') then
                    try_cast(regexp_extract(trim(wait_time), '^([0-9]+)[^0-9]+([0-9]+) min$', 2) as bigint)
                else null
            end as bigint
        ) as wait_time_max_minutes,
        cast(_source_file as varchar) as _source_file,
        cast(_file_row_num as bigint) as _file_row_num,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as _content_hash
    from {{ ref('base_catsa_wait_times') }}
),

keyed as (
    select
        cast(
            md5(
                to_json(
                    struct_pack(
                        airport_slug := airport_slug,
                        checkpoint := checkpoint,
                        observed_at := observed_at
                    )
                )
            ) as varchar
        ) as wait_time_observation_key,
        source,
        checkpoint_id,
        airport_slug,
        checkpoint,
        observed_at,
        wait_time_text,
        wait_time_available,
        wait_time_min_minutes,
        wait_time_max_minutes,
        _source_file,
        _file_row_num,
        source_loaded_at,
        _content_hash
    from normalized
),

deduplicated as (
    select *
    from keyed
    qualify row_number() over (
        partition by wait_time_observation_key
        order by source_loaded_at desc, _source_file desc, _file_row_num desc
    ) = 1
)

select
    cast(wait_time_observation_key as varchar) as wait_time_observation_key,
    cast(source as varchar) as source,
    cast(checkpoint_id as varchar) as checkpoint_id,
    cast(airport_slug as varchar) as airport_slug,
    cast(checkpoint as varchar) as checkpoint,
    cast(observed_at as timestamp with time zone) as observed_at,
    cast(wait_time_text as varchar) as wait_time_text,
    cast(wait_time_available as boolean) as wait_time_available,
    cast(wait_time_min_minutes as bigint) as wait_time_min_minutes,
    cast(wait_time_max_minutes as bigint) as wait_time_max_minutes,
    cast(_source_file as varchar) as _source_file,
    cast(_file_row_num as bigint) as _file_row_num,
    cast(source_loaded_at as timestamp with time zone) as source_loaded_at,
    cast(_content_hash as varchar) as _content_hash
from deduplicated
