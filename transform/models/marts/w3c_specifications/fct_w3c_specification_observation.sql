{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['daily']
) }}

with source_rows as (
    select
        md5(to_json(struct_pack(
            source := cast(source as varchar),
            specification_id := cast(id as varchar),
            observed_at := cast(fetched_at as timestamp with time zone)
        ))) as specification_observation_key,
        cast(source as varchar) as source,
        cast(id as varchar) as specification_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(href as varchar) as specification_url,
        cast(title as varchar) as specification_title,
        cast(_dt as date) as source_date,
        cast(_extract_started_at as timestamp with time zone) as extract_started_at,
        cast(_batch_id as varchar) as source_batch_id,
        cast(_load_id as varchar) as load_id,
        cast(_source_file as varchar) as source_file,
        cast(_file_row_num as bigint) as source_file_row_number,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as content_hash
    from {{ ref('base_w3c_specifications') }}
),

deduplicated as (
    select *
    from source_rows
    qualify row_number() over (
        partition by specification_observation_key
        order by source_loaded_at desc, source_file desc, source_file_row_number desc
    ) = 1
)

select
    specification_observation_key,
    source,
    specification_id,
    observed_at,
    specification_url,
    specification_title,
    source_date,
    extract_started_at,
    source_batch_id,
    load_id,
    source_file,
    source_file_row_number,
    source_loaded_at,
    content_hash
from deduplicated
