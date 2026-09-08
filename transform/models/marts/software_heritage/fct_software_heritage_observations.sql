{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['twice_hourly']
) }}

with source_rows as (
    select
        md5(to_json(struct_pack(
            archive_source := cast(source as varchar),
            repository_id := cast(id as varchar),
            fetched_at := cast(fetched_at as timestamp with time zone)
        ))) as software_heritage_observation_key,
        cast(source as varchar) as archive_source,
        cast(id as varchar) as repository_id,
        cast(fetched_at as timestamp with time zone) as fetched_at,
        cast(url as varchar) as repository_url,
        cast(visit_types as json) as visit_types,
        cast(has_visits as boolean) as has_visits,
        cast(nb_visits as bigint) as visit_count,
        cast(last_visit_date as timestamp with time zone) as last_visit_at,
        cast(_batch_id as varchar) as source_batch_id,
        cast(_source_file as varchar) as source_file,
        cast(_file_row_num as bigint) as source_file_row_number,
        cast(_dt as date) as source_date,
        cast(_extract_started_at as timestamp with time zone) as extract_started_at,
        cast(_load_id as varchar) as load_id,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as content_hash
    from {{ ref('base_software_heritage') }}
),

deduplicated as (
    select *
    from source_rows
    qualify row_number() over (
        partition by software_heritage_observation_key
        order by source_loaded_at desc, source_file desc, source_file_row_number desc
    ) = 1
)

select
    software_heritage_observation_key,
    archive_source,
    repository_id,
    fetched_at,
    repository_url,
    visit_types,
    has_visits,
    visit_count,
    last_visit_at,
    source_batch_id,
    source_file,
    source_file_row_number,
    source_date,
    extract_started_at,
    load_id,
    source_loaded_at,
    content_hash
from deduplicated
