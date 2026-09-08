{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='digitraffic_marine_aton_fault_observation_key',
    on_schema_change='fail',
    tags=['twice_hourly']
) }}

with selected_source as (
    select
        cast(source as varchar) as source_name,
        cast(id as varchar) as fault_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(json_extract_string(properties, '$.type') as varchar) as fault_type,
        cast(json_extract_string(properties, '$.domain') as varchar) as domain,
        cast(json_extract_string(properties, '$.state') as varchar) as state,
        cast(json_extract_string(properties, '$.fixed') as boolean) as is_fixed,
        cast(json_extract_string(properties, '$.entry_timestamp') as timestamp with time zone) as entered_at,
        cast(json_extract_string(properties, '$.fixed_timestamp') as timestamp with time zone) as fixed_at,
        cast(json_extract_string(properties, '$.aton_id') as bigint) as aton_id,
        cast(json_extract_string(properties, '$.aton_name_fi') as varchar) as aton_name_fi,
        cast(json_extract_string(properties, '$.aton_name_sv') as varchar) as aton_name_sv,
        cast(json_extract_string(properties, '$.aton_type') as varchar) as aton_type,
        cast(json_extract_string(properties, '$.fairway_number') as bigint) as fairway_number,
        cast(json_extract_string(properties, '$.fairway_name_fi') as varchar) as fairway_name_fi,
        cast(json_extract_string(properties, '$.fairway_name_sv') as varchar) as fairway_name_sv,
        cast(json_extract_string(properties, '$.area_number') as bigint) as area_number,
        cast(json_extract_string(properties, '$.area_description') as varchar) as area_description,
        cast(json_extract_string(geometry, '$.type') as varchar) as geometry_type,
        cast(json_extract(geometry, '$.coordinates[0]') as double) as longitude,
        cast(json_extract(geometry, '$.coordinates[1]') as double) as latitude,
        cast(publisher_updated_at as timestamp with time zone) as publisher_updated_at,
        cast(_row_id as varchar) as source_row_id,
        cast(_batch_id as varchar) as source_batch_id,
        cast(_source_file as varchar) as source_file,
        cast(_file_row_num as bigint) as source_file_row_number,
        cast(_dt as date) as source_date,
        cast(_extract_started_at as timestamp with time zone) as extract_started_at,
        cast(_load_id as varchar) as source_load_id,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as content_hash
    from {{ ref('base_digitraffic_marine') }}
    {% if is_incremental() %}
    where cast(_loaded_at as timestamp with time zone) >= (
        select max(source_loaded_at)
        from {{ this }}
    )
    {% endif %}
),

keyed as (
    select
        md5(to_json(struct_pack(
            source_name := source_name,
            fault_id := fault_id,
            observed_at := observed_at
        ))) as digitraffic_marine_aton_fault_observation_key,
        selected_source.*
    from selected_source
),

deduplicated as (
    select *
    from keyed
    qualify row_number() over (
        partition by digitraffic_marine_aton_fault_observation_key
        order by source_loaded_at desc, source_file desc, source_file_row_number desc, source_row_id desc
    ) = 1
)

select
    digitraffic_marine_aton_fault_observation_key,
    source_name,
    fault_id,
    observed_at,
    fault_type,
    domain,
    state,
    is_fixed,
    entered_at,
    fixed_at,
    aton_id,
    aton_name_fi,
    aton_name_sv,
    aton_type,
    fairway_number,
    fairway_name_fi,
    fairway_name_sv,
    area_number,
    area_description,
    geometry_type,
    longitude,
    latitude,
    publisher_updated_at,
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
