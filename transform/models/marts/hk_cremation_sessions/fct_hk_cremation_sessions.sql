{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['hourly']
) }}

with source_rows as (
    select
        cast(md5(to_json(struct_pack(
            source := cast(source as varchar),
            session_id := cast(id as varchar),
            observed_at := cast(fetched_at as timestamp with time zone)
        ))) as varchar) as cremation_session_observation_key,
        cast(source as varchar) as source,
        cast(id as varchar) as session_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        try_strptime(cast(session_date as varchar), '%d/%m/%Y')::date as session_date,
        cast(crematorium as varchar) as crematorium,
        cast(raw_value as varchar) as raw_value,
        cast(available_sessions as bigint) as available_sessions,
        cast(sessions_under_booking as bigint) as sessions_under_booking,
        cast(service_state as varchar) as service_state,
        cast(day_total_sessions as bigint) as day_total_sessions,
        (
            try_strptime(cast(publisher_updated_at as varchar), '%d/%m/%Y %H:%M:%S')
            at time zone 'Asia/Hong_Kong'
        )::timestamp with time zone as publisher_updated_at,
        cast(_row_id as varchar) as _row_id,
        cast(_batch_id as varchar) as _batch_id,
        cast(_load_id as varchar) as _load_id,
        cast(_source_file as varchar) as _source_file,
        cast(_file_row_num as bigint) as _file_row_num,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as _content_hash
    from {{ ref('base_hk_cremation_sessions') }}
),

deduplicated as (
    select *
    from source_rows
    qualify row_number() over (
        partition by cremation_session_observation_key
        order by source_loaded_at desc, _source_file desc, _file_row_num desc
    ) = 1
)

select
    cremation_session_observation_key,
    source,
    session_id,
    observed_at,
    session_date,
    crematorium,
    raw_value,
    available_sessions,
    sessions_under_booking,
    service_state,
    day_total_sessions,
    publisher_updated_at,
    _row_id,
    _batch_id,
    _load_id,
    _source_file,
    _file_row_num,
    source_loaded_at,
    _content_hash
from deduplicated
