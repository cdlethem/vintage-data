{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='thames_discharge_key',
    on_schema_change='fail',
    tags=['twice_hourly']
) }}

with source_rows as (
    select
        cast(source as varchar) as source_system,
        cast(id as varchar) as discharge_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(locationname as varchar) as location_name,
        cast(permitnumber as varchar) as permit_number,
        cast(locationgridref as varchar) as location_grid_reference,
        cast(x as bigint) as easting,
        cast(y as bigint) as northing,
        cast(receivingwatercourse as varchar) as receiving_watercourse,
        cast(alertstatus as varchar) as alert_status,
        cast(statuschanged as timestamp with time zone) as status_changed_at,
        cast(alertpast48hours as boolean) as alert_past_48_hours,
        cast(mostrecentdischargealertstart as timestamp with time zone) as most_recent_discharge_alert_start,
        cast(mostrecentdischargealertstop as timestamp with time zone) as most_recent_discharge_alert_stop,
        cast(uniqueid as varchar) as unique_id,
        cast(_row_id as varchar) as source_row_id,
        cast(_batch_id as varchar) as source_batch_id,
        cast(_source_file as varchar) as source_file,
        cast(_file_row_num as bigint) as source_file_row_number,
        cast(_dt as date) as source_date,
        cast(_extract_started_at as timestamp with time zone) as extract_started_at,
        cast(_load_id as varchar) as load_id,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as content_hash
    from {{ ref('base_thames_discharge') }}
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
            discharge_id := discharge_id,
            observed_at := observed_at
        ))) as varchar) as thames_discharge_key,
        source_rows.*
    from source_rows
),

deduplicated as (
    select *
    from identified
    qualify row_number() over (
        partition by source_system, discharge_id, observed_at
        order by source_loaded_at desc, source_file desc, source_file_row_number desc, source_row_id desc
    ) = 1
)

select
    cast(thames_discharge_key as varchar) as thames_discharge_key,
    cast(source_system as varchar) as source_system,
    cast(discharge_id as varchar) as discharge_id,
    cast(observed_at as timestamp with time zone) as observed_at,
    cast(location_name as varchar) as location_name,
    cast(permit_number as varchar) as permit_number,
    cast(location_grid_reference as varchar) as location_grid_reference,
    cast(easting as bigint) as easting,
    cast(northing as bigint) as northing,
    cast(receiving_watercourse as varchar) as receiving_watercourse,
    cast(alert_status as varchar) as alert_status,
    cast(status_changed_at as timestamp with time zone) as status_changed_at,
    cast(alert_past_48_hours as boolean) as alert_past_48_hours,
    cast(most_recent_discharge_alert_start as timestamp with time zone) as most_recent_discharge_alert_start,
    cast(most_recent_discharge_alert_stop as timestamp with time zone) as most_recent_discharge_alert_stop,
    cast(unique_id as varchar) as unique_id,
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
