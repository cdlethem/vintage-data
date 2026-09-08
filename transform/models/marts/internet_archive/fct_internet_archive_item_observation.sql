{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['hourly']
) }}

with source_rows as (
    select
        md5(to_json(struct_pack(
            archive_source := cast(source as varchar),
            item_id := cast(id as varchar),
            observed_at := cast(fetched_at as timestamp with time zone)
        ))) as internet_archive_item_observation_key,
        cast(source as varchar) as archive_source,
        cast(id as varchar) as item_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(title as varchar) as title,
        cast(mediatype as varchar) as media_type,
        cast(collections as json) as collections,
        cast(creator as varchar) as creator,
        cast(added_date as timestamp with time zone) as added_at,
        cast(url as varchar) as item_url,
        cast(_row_id as varchar) as source_row_id,
        cast(_batch_id as varchar) as source_batch_id,
        cast(_source_file as varchar) as source_file,
        cast(_file_row_num as bigint) as source_file_row_number,
        cast(_dt as date) as source_date,
        cast(_extract_started_at as timestamp with time zone) as extract_started_at,
        cast(_load_id as varchar) as load_id,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as content_hash
    from {{ ref('base_internet_archive') }}
),

deduplicated as (
    select *
    from source_rows
    qualify row_number() over (
        partition by internet_archive_item_observation_key
        order by source_loaded_at desc, source_file desc, source_file_row_number desc, source_row_id desc
    ) = 1
)

select
    internet_archive_item_observation_key,
    archive_source,
    item_id,
    observed_at,
    title,
    media_type,
    collections,
    creator,
    added_at,
    item_url,
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
