{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='ietf_datatracker_event_observation_key',
    on_schema_change='fail',
    tags=['twice_hourly']
) }}

with source_rows as (
    select
        cast(_source as varchar) as source_system,
        cast(id as varchar) as event_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(event_type as varchar) as event_type,
        cast(draft as varchar) as draft_name,
        cast(description as varchar) as event_description,
        cast(revision as varchar) as revision,
        cast("time" as timestamp with time zone) as event_time,
        cast(_row_id as varchar) as source_row_id,
        cast(_batch_id as varchar) as source_batch_id,
        cast(_source_file as varchar) as source_file,
        cast(_file_row_num as bigint) as source_file_row_number,
        cast(_dt as date) as source_date,
        cast(_extract_started_at as timestamp with time zone) as extract_started_at,
        cast(_load_id as varchar) as load_id,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as content_hash
    from {{ ref('base_ietf_datatracker') }}
    {% if is_incremental() %}
    where _loaded_at >= (
        select coalesce(max(source_loaded_at), timestamp '1900-01-01')
        from {{ this }}
    )
    {% endif %}
),

identified as (
    select
        cast(md5(to_json(struct_pack(
            source_system := source_system,
            event_id := event_id,
            observed_at := observed_at
        ))) as varchar) as ietf_datatracker_event_observation_key,
        source_rows.*
    from source_rows
),

deduplicated as (
    select *
    from identified
    qualify row_number() over (
        partition by source_system, event_id, observed_at
        order by source_loaded_at desc, source_file desc, source_file_row_number desc, source_row_id desc
    ) = 1
)

select
    cast(ietf_datatracker_event_observation_key as varchar) as ietf_datatracker_event_observation_key,
    cast(source_system as varchar) as source_system,
    cast(event_id as varchar) as event_id,
    cast(observed_at as timestamp with time zone) as observed_at,
    cast(event_type as varchar) as event_type,
    cast(draft_name as varchar) as draft_name,
    cast(event_description as varchar) as event_description,
    cast(revision as varchar) as revision,
    cast(event_time as timestamp with time zone) as event_time,
    cast(source_row_id as varchar) as source_row_id,
    cast(source_batch_id as varchar) as source_batch_id,
    cast(source_file as varchar) as source_file,
    cast(source_file_row_number as bigint) as source_file_row_number,
    cast(source_date as date) as source_date,
    cast(extract_started_at as timestamp with time zone) as extract_started_at,
    cast(load_id as varchar) as load_id,
    cast(source_loaded_at as timestamp with time zone) as source_loaded_at,
    cast(content_hash as varchar) as content_hash
from deduplicated
