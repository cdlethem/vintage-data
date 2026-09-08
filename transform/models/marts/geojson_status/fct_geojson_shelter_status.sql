{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='geojson_shelter_status_key',
    on_schema_change='fail',
    tags=['twice_hourly']
) }}

with source_rows as (
    select
        cast(source as varchar) as source_name,
        cast(id as varchar) as feature_id,
        try_cast(json_extract_string(properties, '$.shelter_id') as bigint) as shelter_id,
        try_cast(json_extract_string(properties, '$.objectid') as bigint) as object_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(type as varchar) as feature_type,
        cast(json_extract_string(geometry, '$.type') as varchar) as geometry_type,
        cast(geometry as json) as geometry,
        try_cast(json_extract(geometry, '$.coordinates[0]') as double) as longitude,
        try_cast(json_extract(geometry, '$.coordinates[1]') as double) as latitude,
        json_extract_string(properties, '$.shelter_name') as shelter_name,
        json_extract_string(properties, '$.address') as address,
        json_extract_string(properties, '$.city') as city,
        json_extract_string(properties, '$.state') as state,
        json_extract_string(properties, '$.zip') as postal_code,
        json_extract_string(properties, '$.shelter_status') as shelter_status,
        try_cast(json_extract_string(properties, '$.evacuation_capacity') as bigint) as evacuation_capacity,
        try_cast(json_extract_string(properties, '$.post_impact_capacity') as bigint) as post_impact_capacity,
        try_cast(json_extract_string(properties, '$.total_population') as bigint) as total_population,
        json_extract_string(properties, '$.hours_open') as hours_open,
        json_extract_string(properties, '$.hours_close') as hours_close,
        json_extract_string(properties, '$.org_name') as organization_name,
        try_cast(json_extract_string(properties, '$.org_id') as bigint) as organization_id,
        json_extract_string(properties, '$.match_type') as match_type,
        json_extract_string(properties, '$.subfacility_code') as subfacility_code,
        json_extract_string(properties, '$.ada_compliant') as ada_compliant,
        json_extract_string(properties, '$.pet_accommodations_code') as pet_accommodations_code,
        json_extract_string(properties, '$.wheelchair_accessible') as wheelchair_accessible,
        cast(_row_id as varchar) as source_row_id,
        cast(_batch_id as varchar) as source_batch_id,
        cast(_source_file as varchar) as source_file,
        cast(_file_row_num as bigint) as source_file_row_number,
        cast(_dt as date) as source_date,
        cast(_extract_started_at as timestamp with time zone) as extract_started_at,
        cast(_load_id as varchar) as load_id,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as content_hash
    from {{ ref('base_geojson_status') }}
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

keyed_rows as (
    select
        md5(to_json(struct_pack(
            source_name := source_name,
            feature_id := feature_id,
            observed_at := observed_at
        ))) as geojson_shelter_status_key,
        *
    from source_rows
),

deduplicated as (
    select *
    from keyed_rows
    qualify row_number() over (
        partition by geojson_shelter_status_key
        order by source_loaded_at desc, source_file desc, source_file_row_number desc, source_row_id desc
    ) = 1
)

select
    geojson_shelter_status_key,
    source_name,
    feature_id,
    shelter_id,
    object_id,
    observed_at,
    feature_type,
    geometry_type,
    geometry,
    longitude,
    latitude,
    shelter_name,
    address,
    city,
    state,
    postal_code,
    shelter_status,
    evacuation_capacity,
    post_impact_capacity,
    total_population,
    hours_open,
    hours_close,
    organization_name,
    organization_id,
    match_type,
    subfacility_code,
    ada_compliant,
    pet_accommodations_code,
    wheelchair_accessible,
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
