{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['twice_hourly']
) }}

with source_rows as (
    select
        cast(source as varchar) as source,
        cast(id as varchar) as parking_id,
        cast(siteid as varchar) as site_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast("timestamp" as timestamp with time zone) as status_updated_at,
        cast(timestampstatic as timestamp with time zone) as status_static_updated_at,
        cast(reportedavailable as varchar) as reported_available_text,
        try_cast(nullif(trim(cast(reportedavailable as varchar)), '') as bigint) as reported_available_count,
        cast(trend as varchar) as availability_trend,
        cast("open" as boolean) as is_open,
        cast(trustdata as boolean) as trusts_data,
        cast(capacity as bigint) as capacity,
        cast(region as varchar) as region,
        cast(_row_id as varchar) as _row_id,
        cast(_batch_id as varchar) as _batch_id,
        cast(_source_file as varchar) as _source_file,
        cast(_file_row_num as bigint) as _file_row_num,
        cast(_dt as date) as source_date,
        cast(_extract_started_at as timestamp with time zone) as extract_started_at,
        cast(_load_id as varchar) as _load_id,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as _content_hash
    from {{ ref('base_tpims_parking') }}
),

keyed as (
    select
        cast(md5(to_json(struct_pack(
            source := source,
            site_id := site_id,
            observed_at := observed_at
        ))) as varchar) as tpims_parking_status_key,
        source,
        parking_id,
        site_id,
        observed_at,
        status_updated_at,
        status_static_updated_at,
        reported_available_text,
        reported_available_count,
        availability_trend,
        is_open,
        trusts_data,
        capacity,
        region,
        _row_id,
        _batch_id,
        _source_file,
        _file_row_num,
        source_date,
        extract_started_at,
        _load_id,
        source_loaded_at,
        _content_hash
    from source_rows
),

deduplicated as (
    select *
    from keyed
    qualify row_number() over (
        partition by tpims_parking_status_key
        order by source_loaded_at desc, _source_file desc, _file_row_num desc, _row_id desc
    ) = 1
)

select
    cast(tpims_parking_status_key as varchar) as tpims_parking_status_key,
    cast(source as varchar) as source,
    cast(parking_id as varchar) as parking_id,
    cast(site_id as varchar) as site_id,
    cast(observed_at as timestamp with time zone) as observed_at,
    cast(status_updated_at as timestamp with time zone) as status_updated_at,
    cast(status_static_updated_at as timestamp with time zone) as status_static_updated_at,
    cast(reported_available_text as varchar) as reported_available_text,
    cast(reported_available_count as bigint) as reported_available_count,
    cast(availability_trend as varchar) as availability_trend,
    cast(is_open as boolean) as is_open,
    cast(trusts_data as boolean) as trusts_data,
    cast(capacity as bigint) as capacity,
    cast(region as varchar) as region,
    cast(_row_id as varchar) as _row_id,
    cast(_batch_id as varchar) as _batch_id,
    cast(_source_file as varchar) as _source_file,
    cast(_file_row_num as bigint) as _file_row_num,
    cast(source_date as date) as source_date,
    cast(extract_started_at as timestamp with time zone) as extract_started_at,
    cast(_load_id as varchar) as _load_id,
    cast(source_loaded_at as timestamp with time zone) as source_loaded_at,
    cast(_content_hash as varchar) as _content_hash
from deduplicated
