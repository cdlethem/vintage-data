{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['daily']
) }}

with radiation_measurements as (
    select
        md5(to_json(struct_pack(
            source := cast(source as varchar),
            measurement_id := cast(id as varchar)
        ))) as safecast_radiation_measurement_key,
        cast(source as varchar) as source,
        cast(id as varchar) as measurement_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(captured_at as timestamp with time zone) as captured_at,
        cast(value as double) as measurement_value,
        cast(unit as varchar) as measurement_unit,
        cast(latitude as double) as latitude,
        cast(longitude as double) as longitude,
        cast(device_id as bigint) as device_id,
        cast(height as bigint) as height,
        cast(_batch_id as varchar) as source_batch_id,
        cast(_load_id as varchar) as source_load_id,
        cast(_source_file as varchar) as source_file,
        cast(_file_row_num as bigint) as source_file_row_number,
        cast(_dt as date) as source_date,
        cast(_extract_started_at as timestamp with time zone) as extract_started_at,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as content_hash
    from {{ ref('base_safecast') }}
    where cast(is_radiation as boolean)
),

deduplicated as (
    select *
    from radiation_measurements
    qualify row_number() over (
        partition by safecast_radiation_measurement_key
        order by source_loaded_at desc, source_file desc, source_file_row_number desc
    ) = 1
)

select
    safecast_radiation_measurement_key,
    source,
    measurement_id,
    observed_at,
    captured_at,
    measurement_value,
    measurement_unit,
    latitude,
    longitude,
    device_id,
    height,
    source_batch_id,
    source_load_id,
    source_file,
    source_file_row_number,
    source_date,
    extract_started_at,
    source_loaded_at,
    content_hash
from deduplicated
