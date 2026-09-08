{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['daily']
) }}

with source_rows as (
    select
        cast(source as varchar) as source,
        cast(id as varchar) as certificate_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(common_name as varchar) as common_name,
        cast(names as json) as names,
        cast(issuer as varchar) as issuer,
        cast(not_before as timestamp with time zone) as not_before,
        cast(not_after as timestamp with time zone) as not_after,
        cast(serial_number as varchar) as serial_number,
        cast(_row_id as varchar) as source_row_id,
        cast(_batch_id as varchar) as source_batch_id,
        cast(_source_file as varchar) as source_file,
        cast(_file_row_num as bigint) as source_file_row_number,
        cast(_dt as date) as source_date,
        cast(_extract_started_at as timestamp with time zone) as extract_started_at,
        cast(_load_id as varchar) as load_id,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as content_hash,
        cast(md5(to_json(struct_pack(
            source := cast(source as varchar),
            certificate_id := cast(id as varchar),
            observed_at := cast(fetched_at as timestamp with time zone)
        ))) as varchar) as certificate_observation_key
    from {{ ref('base_certificate_transparency') }}
),

deduplicated as (
    select *
    from source_rows
    qualify row_number() over (
        partition by source, certificate_id, observed_at
        order by source_loaded_at desc, source_file desc, source_file_row_number desc
    ) = 1
)

select
    cast(certificate_observation_key as varchar) as certificate_observation_key,
    cast(source as varchar) as source,
    cast(certificate_id as varchar) as certificate_id,
    cast(observed_at as timestamp with time zone) as observed_at,
    cast(common_name as varchar) as common_name,
    cast(names as json) as names,
    cast(issuer as varchar) as issuer,
    cast(not_before as timestamp with time zone) as not_before,
    cast(not_after as timestamp with time zone) as not_after,
    cast(serial_number as varchar) as serial_number,
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
