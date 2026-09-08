{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='sensor_community_measurement_key',
    on_schema_change='fail',
    tags=['twice_hourly']
) }}

with source_rows as (
    select
        cast(source as varchar) as source_name,
        cast(id as varchar) as measurement_id,
        cast(sensor_id as bigint) as sensor_id,
        cast(ts as timestamp with time zone) as observed_at,
        cast(fetched_at as timestamp with time zone) as fetched_at,
        cast(location_id as bigint) as location_id,
        cast(sensor_type as varchar) as sensor_type,
        cast(manufacturer as varchar) as manufacturer,
        cast(country as varchar) as country,
        cast(latitude as double) as latitude,
        cast(longitude as double) as longitude,
        cast(altitude as double) as altitude,
        cast(indoor as boolean) as is_indoor,
        cast(exact_location as boolean) as has_exact_location,
        try_cast(json_extract_string(values, '$.pm1') as double) as pm1,
        try_cast(json_extract_string(values, '$.pm25') as double) as pm25,
        try_cast(json_extract_string(values, '$.pm4') as double) as pm4,
        try_cast(json_extract_string(values, '$.pm10') as double) as pm10,
        try_cast(json_extract_string(values, '$.temperature') as double) as temperature,
        try_cast(json_extract_string(values, '$.humidity') as double) as humidity,
        try_cast(json_extract_string(values, '$.pressure') as double) as pressure,
        try_cast(coalesce(
            json_extract_string(values, '$.pressure_at_sealevel'),
            json_extract_string(values, '$.pressure_sealevel')
        ) as double) as pressure_at_sea_level,
        try_cast(json_extract_string(values, '$.co2_ppm') as double) as co2_ppm,
        try_cast(json_extract_string(values, '$.noise_LAeq') as double) as noise_laeq,
        try_cast(json_extract_string(values, '$.noise_LA_max') as double) as noise_la_max,
        try_cast(json_extract_string(values, '$.noise_LA_min') as double) as noise_la_min,
        try_cast(json_extract_string(values, '$.radiation_msi') as double) as radiation_msi,
        cast(values as json) as measurement_values,
        cast(quality_flags as json) as quality_flags,
        cast(is_clean as boolean) as is_clean,
        cast(_row_id as varchar) as source_row_id,
        cast(_batch_id as varchar) as source_batch_id,
        cast(_source_file as varchar) as source_file,
        cast(_file_row_num as bigint) as source_file_row_number,
        cast(_dt as date) as source_date,
        cast(_extract_started_at as timestamp with time zone) as extract_started_at,
        cast(_load_id as varchar) as source_load_id,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as content_hash
    from {{ ref('base_sensor_community') }}
    {% if is_incremental() %}
    where _loaded_at >= (
        select coalesce(
            max(source_loaded_at),
            timestamp with time zone '1900-01-01 00:00:00+00'
        )
        from {{ this }}
    )
    {% endif %}
),

keyed as (
    select
        md5(to_json(struct_pack(
            source_name := source_name,
            sensor_id := sensor_id,
            observed_at := observed_at
        ))) as sensor_community_measurement_key,
        *
    from source_rows
),

deduplicated as (
    select *
    from keyed
    qualify row_number() over (
        partition by sensor_community_measurement_key
        order by source_loaded_at desc, source_file desc, source_file_row_number desc, source_row_id desc
    ) = 1
)

select
    sensor_community_measurement_key,
    source_name,
    measurement_id,
    sensor_id,
    observed_at,
    fetched_at,
    location_id,
    sensor_type,
    manufacturer,
    country,
    latitude,
    longitude,
    altitude,
    is_indoor,
    has_exact_location,
    pm1,
    pm25,
    pm4,
    pm10,
    temperature,
    humidity,
    pressure,
    pressure_at_sea_level,
    co2_ppm,
    noise_laeq,
    noise_la_max,
    noise_la_min,
    radiation_msi,
    measurement_values,
    quality_flags,
    is_clean,
    source_row_id,
    source_batch_id,
    source_file,
    source_file_row_number,
    source_date,
    extract_started_at,
    source_load_id,
    source_loaded_at,
    content_hash
from deduplicated
