{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['twice_hourly']
) }}

with source_rows as (
    select
        cast(source as varchar) as source,
        cast(id as varchar) as parking_id,
        cast(park_id as varchar) as park_id,
        cast(vehicle_type as varchar) as vehicle_type,
        cast(vacancy_type as varchar) as vacancy_type,
        cast(category as varchar) as category,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(vacancyev as bigint) as electric_vacancy_count,
        cast(vacancydis as bigint) as disabled_vacancy_count,
        cast(vacancy as bigint) as vacancy_count,
        cast(lastupdate as timestamp with time zone) as publisher_updated_at,
        cast(_row_id as varchar) as _row_id,
        cast(_batch_id as varchar) as _batch_id,
        cast(_load_id as varchar) as _load_id,
        cast(_source_file as varchar) as _source_file,
        cast(_file_row_num as bigint) as _file_row_num,
        cast(_dt as date) as source_date,
        cast(_extract_started_at as timestamp with time zone) as extract_started_at,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as _content_hash
    from {{ ref('base_hk_parking_vacancy') }}
),

keyed as (
    select
        cast(md5(to_json(struct_pack(
            source := source,
            park_id := park_id,
            vehicle_type := vehicle_type,
            vacancy_type := vacancy_type,
            category := category,
            observed_at := observed_at
        ))) as varchar) as parking_vacancy_observation_key,
        source,
        parking_id,
        park_id,
        vehicle_type,
        vacancy_type,
        category,
        observed_at,
        electric_vacancy_count,
        disabled_vacancy_count,
        vacancy_count,
        publisher_updated_at,
        _row_id,
        _batch_id,
        _load_id,
        _source_file,
        _file_row_num,
        source_date,
        extract_started_at,
        source_loaded_at,
        _content_hash
    from source_rows
),

deduplicated as (
    select *
    from keyed
    qualify row_number() over (
        partition by parking_vacancy_observation_key
        order by source_loaded_at desc, _source_file desc, _file_row_num desc, _row_id desc
    ) = 1
)

select
    cast(parking_vacancy_observation_key as varchar) as parking_vacancy_observation_key,
    cast(source as varchar) as source,
    cast(parking_id as varchar) as parking_id,
    cast(park_id as varchar) as park_id,
    cast(vehicle_type as varchar) as vehicle_type,
    cast(vacancy_type as varchar) as vacancy_type,
    cast(category as varchar) as category,
    cast(observed_at as timestamp with time zone) as observed_at,
    cast(electric_vacancy_count as bigint) as electric_vacancy_count,
    cast(disabled_vacancy_count as bigint) as disabled_vacancy_count,
    cast(vacancy_count as bigint) as vacancy_count,
    cast(publisher_updated_at as timestamp with time zone) as publisher_updated_at,
    cast(_row_id as varchar) as _row_id,
    cast(_batch_id as varchar) as _batch_id,
    cast(_load_id as varchar) as _load_id,
    cast(_source_file as varchar) as _source_file,
    cast(_file_row_num as bigint) as _file_row_num,
    cast(source_date as date) as source_date,
    cast(extract_started_at as timestamp with time zone) as extract_started_at,
    cast(source_loaded_at as timestamp with time zone) as source_loaded_at,
    cast(_content_hash as varchar) as _content_hash
from deduplicated
