{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['hourly']
) }}

with source_rows as (
    select
        md5(to_json(struct_pack(
            source := cast(source as varchar),
            superevent_id := cast(id as varchar),
            observed_at := cast(fetched_at as timestamp with time zone)
        ))) as gracedb_superevent_observation_key,
        cast(source as varchar) as source,
        cast(id as varchar) as superevent_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(event_time_utc as timestamp with time zone) as event_time_utc,
        cast(gps_t0 as double) as gps_t0,
        cast(far_hz as double) as false_alarm_rate_hz,
        cast(category as varchar) as category,
        cast(labels as json) as labels,
        cast(retracted as boolean) as retracted,
        cast(links as varchar) as links,
        cast(_row_id as varchar) as source_row_id,
        cast(_batch_id as varchar) as source_batch_id,
        cast(_source_file as varchar) as source_file,
        cast(_file_row_num as bigint) as source_file_row_number,
        cast(_dt as date) as source_date,
        cast(_extract_started_at as timestamp with time zone) as extract_started_at,
        cast(_load_id as varchar) as load_id,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as content_hash
    from {{ ref('base_gracedb_superevents') }}
),

deduplicated as (
    select *
    from source_rows
    qualify row_number() over (
        partition by gracedb_superevent_observation_key
        order by source_loaded_at desc, source_file desc, source_file_row_number desc
    ) = 1
)

select
    gracedb_superevent_observation_key,
    source,
    superevent_id,
    observed_at,
    event_time_utc,
    gps_t0,
    false_alarm_rate_hz,
    category,
    labels,
    retracted,
    links,
    source_row_id,
    source_batch_id,
    source_file,
    source_file_row_number,
    source_date,
    extract_started_at,
    load_id,
    source_loaded_at,
    content_hash
from deduplicated
