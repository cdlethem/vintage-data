{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='open311_request_observation_key',
    on_schema_change='fail',
    tags=['twice_hourly']
) }}

with source_rows as (
    select
        cast(source as varchar) as source_name,
        cast(id as varchar) as request_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(service_request_id as varchar) as service_request_id,
        cast(status as varchar) as status,
        cast(status_notes as varchar) as status_notes,
        cast(service_name as varchar) as service_name,
        cast(service_code as varchar) as service_code,
        cast(description as varchar) as description,
        cast(requested_datetime as timestamp with time zone) as requested_at,
        cast(updated_datetime as timestamp with time zone) as updated_at,
        cast(address as varchar) as address,
        cast(lat as double) as latitude,
        cast("long" as double) as longitude,
        cast(token as varchar) as token,
        cast(city as varchar) as city,
        cast(_row_id as varchar) as source_row_id,
        cast(_batch_id as varchar) as source_batch_id,
        cast(_source_file as varchar) as source_file,
        cast(_file_row_num as bigint) as source_file_row_number,
        cast(_dt as date) as source_date,
        cast(_extract_started_at as timestamp with time zone) as extract_started_at,
        cast(_load_id as varchar) as load_id,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as content_hash
    from {{ ref('base_open311') }}
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
            request_id := request_id,
            observed_at := observed_at
        ))) as open311_request_observation_key,
        *
    from source_rows
),

deduplicated as (
    select *
    from keyed_rows
    qualify row_number() over (
        partition by open311_request_observation_key
        order by source_loaded_at desc, source_file desc, source_file_row_number desc, source_row_id desc
    ) = 1
)

select
    open311_request_observation_key,
    source_name,
    request_id,
    observed_at,
    service_request_id,
    status,
    status_notes,
    service_name,
    service_code,
    description,
    requested_at,
    updated_at,
    address,
    latitude,
    longitude,
    token,
    city,
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
