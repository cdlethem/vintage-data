{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='globe_measurement_key',
    on_schema_change='fail',
    tags=['daily']
) }}

with source_rows as (
    select
        cast(_row_id as varchar) as source_row_id,
        cast(_source as varchar) as source_relation,
        cast(source as varchar) as source,
        cast(id as varchar) as measurement_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(protocol as varchar) as protocol,
        cast(measureddate as date) as measured_date,
        cast(createdate as timestamp with time zone) as created_at,
        cast(updatedate as timestamp with time zone) as updated_at,
        cast(publishdate as timestamp with time zone) as published_at,
        cast(organizationid as bigint) as organization_id,
        cast(organizationname as varchar) as organization_name,
        cast(siteid as bigint) as site_id,
        cast(sitename as varchar) as site_name,
        cast(countryname as varchar) as country_name,
        cast(countrycode as varchar) as country_code,
        cast(latitude as double) as latitude,
        cast(longitude as double) as longitude,
        cast(elevation as double) as elevation,
        cast(pid as bigint) as parent_measurement_id,
        cast(data as json) as measurement_data,
        cast(_batch_id as varchar) as source_batch_id,
        cast(_load_id as varchar) as source_load_id,
        cast(_source_file as varchar) as source_file,
        cast(_file_row_num as bigint) as source_file_row_number,
        cast(_dt as date) as source_date,
        cast(_extract_started_at as timestamp with time zone) as extract_started_at,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as content_hash
    from {{ ref('base_globe_measurements') }}
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
            measurement_id := measurement_id,
            observed_at := observed_at,
            content_hash := content_hash
        ))) as globe_measurement_key,
        *
    from source_rows
),

deduplicated as (
    select *
    from keyed
    qualify row_number() over (
        partition by globe_measurement_key
        order by source_loaded_at desc, source_file desc, source_file_row_number desc
    ) = 1
)

select
    globe_measurement_key,
    source_row_id,
    source_relation,
    source,
    measurement_id,
    observed_at,
    protocol,
    measured_date,
    created_at,
    updated_at,
    published_at,
    organization_id,
    organization_name,
    site_id,
    site_name,
    country_name,
    country_code,
    latitude,
    longitude,
    elevation,
    parent_measurement_id,
    measurement_data,
    source_batch_id,
    source_load_id,
    source_file,
    source_file_row_number,
    source_date,
    extract_started_at,
    source_loaded_at,
    content_hash
from deduplicated
