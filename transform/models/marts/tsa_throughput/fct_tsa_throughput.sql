{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['daily']
) }}

with source_rows as (
    select
        md5(to_json(struct_pack(
            source := cast(source as varchar),
            throughput_id := cast(id as varchar),
            throughput_date := cast(date as date),
            observed_at := cast(fetched_at as timestamp with time zone)
        ))) as throughput_observation_key,
        cast(source as varchar) as source,
        cast(id as varchar) as throughput_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(date as date) as throughput_date,
        cast(passengers as bigint) as passengers,
        cast(publisher_updated_at as varchar) as publisher_updated_at,
        cast(_dt as date) as source_date,
        cast(_extract_started_at as timestamp with time zone) as extract_started_at,
        cast(_batch_id as varchar) as source_batch_id,
        cast(_load_id as varchar) as load_id,
        cast(_source_file as varchar) as source_file,
        cast(_file_row_num as bigint) as source_file_row_number,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as content_hash
    from {{ ref('base_tsa_throughput') }}
),

deduplicated as (
    select *
    from source_rows
    qualify row_number() over (
        partition by throughput_observation_key
        order by source_loaded_at desc, source_file desc, source_file_row_number desc
    ) = 1
)

select
    throughput_observation_key,
    source,
    throughput_id,
    observed_at,
    throughput_date,
    passengers,
    publisher_updated_at,
    source_date,
    extract_started_at,
    source_batch_id,
    load_id,
    source_file,
    source_file_row_number,
    source_loaded_at,
    content_hash
from deduplicated
