{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['hourly']
) }}

with source_rows as (
    select
        cast(source as varchar) as source,
        cast(id as varchar) as shortage_id,
        cast(fetched_at as timestamp with time zone) as fetched_at,
        nullif(trim(cast(availability_of_alternatives as varchar)), '') as availability_of_alternatives,
        nullif(trim(cast(category as varchar)), '') as category,
        nullif(trim(cast(change as varchar)), '') as change_type,
        nullif(trim(cast(expected_resolution as varchar)), '') as expected_resolution,
        nullif(trim(cast(expected_resolution_date as varchar)), '') as expected_resolution_date_text,
        cast(try_strptime(nullif(trim(cast(expected_resolution_date as varchar)), ''), '%d/%m/%Y') as date) as expected_resolution_date,
        nullif(trim(cast(first_published_date as varchar)), '') as first_published_date_raw,
        cast(try_strptime(nullif(trim(cast(first_published_date as varchar)), ''), '%d/%m/%Y') as date) as first_published_date,
        nullif(trim(cast(international_non_proprietary_name_inn_or_common_name as varchar)), '') as inn_or_common_name,
        nullif(trim(cast(last_updated_date as varchar)), '') as last_updated_date_raw,
        cast(try_strptime(nullif(trim(cast(last_updated_date as varchar)), ''), '%d/%m/%Y') as date) as last_updated_date,
        nullif(trim(cast(medicine_affected as varchar)), '') as medicine_affected,
        nullif(trim(cast(pharmaceutical_forms_affected as varchar)), '') as pharmaceutical_forms_affected,
        cast(publisher_timestamp as timestamp with time zone) as publisher_timestamp,
        nullif(trim(cast(shortage_url as varchar)), '') as shortage_url,
        nullif(trim(cast(snapshot_sha256 as varchar)), '') as snapshot_sha256,
        nullif(trim(cast(start_of_shortage_date as varchar)), '') as start_of_shortage_date_raw,
        cast(try_strptime(nullif(trim(cast(start_of_shortage_date as varchar)), ''), '%d/%m/%Y') as date) as start_of_shortage_date,
        nullif(trim(cast(strengths_affected as varchar)), '') as strengths_affected,
        nullif(trim(cast(supply_shortage_status as varchar)), '') as supply_shortage_status,
        nullif(trim(cast(therapeutic_area_mesh as varchar)), '') as therapeutic_area_mesh,
        cast(_row_id as varchar) as _row_id,
        cast(_source as varchar) as _source,
        cast(_batch_id as varchar) as _batch_id,
        cast(_source_file as varchar) as _source_file,
        cast(_file_row_num as bigint) as _file_row_num,
        cast(_dt as date) as _dt,
        cast(_extract_started_at as timestamp with time zone) as _extract_started_at,
        cast(_load_id as varchar) as _load_id,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as _content_hash
    from {{ ref('base_ema_medicine_shortages') }}
),

keyed as (
    select
        cast(md5(to_json(struct_pack(
            source := source,
            shortage_id := shortage_id,
            fetched_at := fetched_at
        ))) as varchar) as ema_medicine_shortage_key,
        *
    from source_rows
),

deduplicated as (
    select *
    from keyed
    qualify row_number() over (
        partition by source, shortage_id, fetched_at
        order by source_loaded_at desc, publisher_timestamp desc, _source_file desc, _file_row_num desc, _row_id desc
    ) = 1
)

select
    cast(ema_medicine_shortage_key as varchar) as ema_medicine_shortage_key,
    cast(source as varchar) as source,
    cast(shortage_id as varchar) as shortage_id,
    cast(fetched_at as timestamp with time zone) as fetched_at,
    cast(availability_of_alternatives as varchar) as availability_of_alternatives,
    cast(category as varchar) as category,
    cast(change_type as varchar) as change_type,
    cast(expected_resolution as varchar) as expected_resolution,
    cast(expected_resolution_date_text as varchar) as expected_resolution_date_text,
    cast(expected_resolution_date as date) as expected_resolution_date,
    cast(first_published_date_raw as varchar) as first_published_date_raw,
    cast(first_published_date as date) as first_published_date,
    cast(inn_or_common_name as varchar) as inn_or_common_name,
    cast(last_updated_date_raw as varchar) as last_updated_date_raw,
    cast(last_updated_date as date) as last_updated_date,
    cast(medicine_affected as varchar) as medicine_affected,
    cast(pharmaceutical_forms_affected as varchar) as pharmaceutical_forms_affected,
    cast(publisher_timestamp as timestamp with time zone) as publisher_timestamp,
    cast(shortage_url as varchar) as shortage_url,
    cast(snapshot_sha256 as varchar) as snapshot_sha256,
    cast(start_of_shortage_date_raw as varchar) as start_of_shortage_date_raw,
    cast(start_of_shortage_date as date) as start_of_shortage_date,
    cast(strengths_affected as varchar) as strengths_affected,
    cast(supply_shortage_status as varchar) as supply_shortage_status,
    cast(therapeutic_area_mesh as varchar) as therapeutic_area_mesh,
    cast(_row_id as varchar) as _row_id,
    cast(_source as varchar) as _source,
    cast(_batch_id as varchar) as _batch_id,
    cast(_source_file as varchar) as _source_file,
    cast(_file_row_num as bigint) as _file_row_num,
    cast(_dt as date) as _dt,
    cast(_extract_started_at as timestamp with time zone) as _extract_started_at,
    cast(_load_id as varchar) as _load_id,
    cast(source_loaded_at as timestamp with time zone) as source_loaded_at,
    cast(_content_hash as varchar) as _content_hash
from deduplicated
