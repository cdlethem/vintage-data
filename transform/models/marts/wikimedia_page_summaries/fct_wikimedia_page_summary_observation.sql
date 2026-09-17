{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['weekly']
) }}

with source_rows as (
    select
        cast(md5(to_json(struct_pack(
            source_name := cast(source as varchar),
            page_identity := cast(id as varchar),
            observed_at := cast(fetched_at as timestamp with time zone)
        ))) as varchar) as wikimedia_page_summary_observation_key,
        cast(source as varchar) as source_name,
        cast(id as varchar) as page_identity,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(language as varchar) as language,
        cast(project as varchar) as project,
        cast(requested_title as varchar) as requested_title,
        cast(title as varchar) as title,
        cast(is_redirect as boolean) as is_redirect,
        cast(page_id as bigint) as page_id,
        cast(revision_id as varchar) as revision_id,
        cast(revision_timestamp as timestamp with time zone) as revision_timestamp,
        cast(summary as varchar) as summary,
        cast(description as varchar) as description,
        cast(wikibase_item as varchar) as wikibase_item,
        cast(page_url as varchar) as page_url,
        cast(mobile_page_url as varchar) as mobile_page_url,
        cast(api_url as varchar) as api_url,
        cast(thumbnail_source as varchar) as thumbnail_source,
        cast(thumbnail_width as bigint) as thumbnail_width,
        cast(thumbnail_height as bigint) as thumbnail_height,
        cast(original_image_source as varchar) as original_image_source,
        cast(original_image_width as bigint) as original_image_width,
        cast(original_image_height as bigint) as original_image_height,
        cast(_row_id as varchar) as source_row_id,
        cast(_batch_id as varchar) as source_batch_id,
        cast(_source_file as varchar) as source_file,
        cast(_file_row_num as bigint) as source_file_row_number,
        cast(_dt as date) as source_date,
        cast(_extract_started_at as timestamp with time zone) as extract_started_at,
        cast(_load_id as varchar) as load_id,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as content_hash,
        row_number() over (
            partition by source, id, fetched_at
            order by _loaded_at desc, _source_file desc, _file_row_num desc, _row_id desc
        ) as _dedupe_rank
    from {{ ref('base_wikimedia_page_summaries') }}
)

select
    wikimedia_page_summary_observation_key,
    source_name,
    page_identity,
    observed_at,
    language,
    project,
    requested_title,
    title,
    is_redirect,
    page_id,
    revision_id,
    revision_timestamp,
    summary,
    description,
    wikibase_item,
    page_url,
    mobile_page_url,
    api_url,
    thumbnail_source,
    thumbnail_width,
    thumbnail_height,
    original_image_source,
    original_image_width,
    original_image_height,
    source_row_id,
    source_batch_id,
    source_file,
    source_file_row_number,
    source_date,
    extract_started_at,
    load_id,
    source_loaded_at,
    content_hash
from source_rows
where _dedupe_rank = 1
