{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='socrata_civic_incident_key',
    on_schema_change='fail',
    tags=['twice_hourly']
) }}

with source_rows as (
    select
        cast(source as varchar) as source,
        cast(_source as varchar) as source_relation,
        cast(id as varchar) as record_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(ts as timestamp with time zone) as event_at,
        cast(json_extract_string(raw, '$.incident_number') as varchar) as incident_number,
        cast(json_extract_string(raw, '$.type') as varchar) as incident_type,
        cast(json_extract_string(raw, '$.address') as varchar) as address,
        try_cast(json_extract_string(raw, '$.latitude') as double) as latitude,
        try_cast(json_extract_string(raw, '$.longitude') as double) as longitude,
        cast(json_extract(raw, '$.report_location') as json) as report_location,
        cast(_row_id as varchar) as source_row_id,
        cast(_batch_id as varchar) as source_batch_id,
        cast(_source_file as varchar) as source_file,
        cast(_file_row_num as bigint) as source_file_row_number,
        cast(_dt as date) as source_date,
        cast(_extract_started_at as timestamp with time zone) as extract_started_at,
        cast(_load_id as varchar) as load_id,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as content_hash
    from {{ ref('base_socrata_civic') }}
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
        cast(md5(to_json(struct_pack(
            source_relation := source_relation,
            record_id := record_id,
            observed_at := observed_at
        ))) as varchar) as socrata_civic_incident_key,
        *
    from source_rows
),

deduplicated as (
    select *
    from keyed
    qualify row_number() over (
        partition by socrata_civic_incident_key
        order by source_loaded_at desc, source_file desc, source_file_row_number desc, source_row_id desc
    ) = 1
)

select
    cast(socrata_civic_incident_key as varchar) as socrata_civic_incident_key,
    cast(source_relation as varchar) as source_relation,
    cast(source as varchar) as source,
    cast(record_id as varchar) as record_id,
    cast(observed_at as timestamp with time zone) as observed_at,
    cast(event_at as timestamp with time zone) as event_at,
    cast(incident_number as varchar) as incident_number,
    cast(incident_type as varchar) as incident_type,
    cast(address as varchar) as address,
    cast(latitude as double) as latitude,
    cast(longitude as double) as longitude,
    cast(report_location as json) as report_location,
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
