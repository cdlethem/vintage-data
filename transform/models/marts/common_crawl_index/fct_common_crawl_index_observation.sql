{{ config(
    enabled=var('common_crawl_index_enabled', false),
    materialized='table',
    on_schema_change='fail',
    tags=['daily']
) }}

with source_rows as (
    select
        md5(to_json(struct_pack(
            source_name := cast(source as varchar),
            collection_id := cast(collection_id as varchar),
            capture_id := cast(id as varchar),
            observed_at := cast(fetched_at as timestamp with time zone)
        ))) as common_crawl_index_observation_key,
        cast(source as varchar) as source_name,
        cast(collection_id as varchar) as collection_id,
        cast(id as varchar) as capture_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        timezone('UTC', try_strptime(cast(crawl_timestamp as varchar), '%Y%m%d%H%M%S')) as crawled_at,
        cast(url as varchar) as url,
        cast(urlkey as varchar) as urlkey,
        cast(mime_type as varchar) as mime_type,
        cast(status_code as varchar) as status_code,
        cast(digest as varchar) as digest,
        cast(content_length as bigint) as content_length,
        cast(query_url_pattern as varchar) as query_url_pattern,
        cast(index_url as varchar) as index_url,
        cast(query_page as bigint) as query_page,
        cast(_batch_id as varchar) as _batch_id,
        cast(_load_id as varchar) as _load_id,
        cast(_source_file as varchar) as _source_file,
        cast(_file_row_num as bigint) as _file_row_num,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as _content_hash
    from {{ ref('base_common_crawl_index') }}
),

deduplicated as (
    select *
    from source_rows
    qualify row_number() over (
        partition by common_crawl_index_observation_key
        order by source_loaded_at desc, _source_file desc, _file_row_num desc
    ) = 1
)

select
    common_crawl_index_observation_key,
    source_name,
    collection_id,
    capture_id,
    observed_at,
    crawled_at,
    url,
    urlkey,
    mime_type,
    status_code,
    digest,
    content_length,
    query_url_pattern,
    index_url,
    query_page,
    _batch_id,
    _load_id,
    _source_file,
    _file_row_num,
    source_loaded_at,
    _content_hash
from deduplicated
