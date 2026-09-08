{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='drivebc_event_observation_key',
    on_schema_change='fail',
    tags=['twice_hourly']
) }}

with selected_source as (
    select
        cast(source as varchar) as source_name,
        cast(id as varchar) as event_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(jurisdiction_url as varchar) as jurisdiction_url,
        cast(url as varchar) as event_url,
        cast(headline as varchar) as headline,
        cast(status as varchar) as status,
        cast(created as timestamp with time zone) as event_created_at,
        cast(updated as timestamp with time zone) as event_updated_at,
        cast(description as varchar) as description,
        cast(ivr_message as varchar) as ivr_message,
        cast(linear_reference_km as double) as linear_reference_km,
        cast(schedule as json) as schedule,
        cast(event_type as varchar) as event_type,
        cast(event_subtypes as json) as event_subtypes,
        cast(severity as varchar) as severity,
        cast(geography as json) as geography,
        cast(roads as json) as roads,
        cast(areas as json) as areas,
        cast(_row_id as varchar) as source_row_id,
        cast(_batch_id as varchar) as source_batch_id,
        cast(_source_file as varchar) as source_file,
        cast(_file_row_num as bigint) as source_file_row_number,
        cast(_dt as date) as source_date,
        cast(_extract_started_at as timestamp with time zone) as extract_started_at,
        cast(_load_id as varchar) as source_load_id,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as content_hash
    from {{ ref('base_drivebc_events') }}
    {% if is_incremental() %}
    where cast(_loaded_at as timestamp with time zone) >= (
        select max(source_loaded_at)
        from {{ this }}
    )
    {% endif %}
),

keyed as (
    select
        cast(md5(to_json(struct_pack(
            source_name := source_name,
            event_id := event_id,
            observed_at := observed_at
        ))) as varchar) as drivebc_event_observation_key,
        *
    from selected_source
),

deduplicated as (
    select *
    from keyed
    qualify row_number() over (
        partition by drivebc_event_observation_key
        order by source_loaded_at desc, source_file desc, source_file_row_number desc, source_row_id desc
    ) = 1
)

select
    cast(drivebc_event_observation_key as varchar) as drivebc_event_observation_key,
    cast(source_name as varchar) as source_name,
    cast(event_id as varchar) as event_id,
    cast(observed_at as timestamp with time zone) as observed_at,
    cast(jurisdiction_url as varchar) as jurisdiction_url,
    cast(event_url as varchar) as event_url,
    cast(headline as varchar) as headline,
    cast(status as varchar) as status,
    cast(event_created_at as timestamp with time zone) as event_created_at,
    cast(event_updated_at as timestamp with time zone) as event_updated_at,
    cast(description as varchar) as description,
    cast(ivr_message as varchar) as ivr_message,
    cast(linear_reference_km as double) as linear_reference_km,
    cast(schedule as json) as schedule,
    cast(event_type as varchar) as event_type,
    cast(event_subtypes as json) as event_subtypes,
    cast(severity as varchar) as severity,
    cast(geography as json) as geography,
    cast(roads as json) as roads,
    cast(areas as json) as areas,
    cast(source_row_id as varchar) as source_row_id,
    cast(source_batch_id as varchar) as source_batch_id,
    cast(source_file as varchar) as source_file,
    cast(source_file_row_number as bigint) as source_file_row_number,
    cast(source_date as date) as source_date,
    cast(extract_started_at as timestamp with time zone) as extract_started_at,
    cast(source_load_id as varchar) as source_load_id,
    cast(source_loaded_at as timestamp with time zone) as source_loaded_at,
    cast(content_hash as varchar) as content_hash
from deduplicated
