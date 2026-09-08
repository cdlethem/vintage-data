{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='bluesky_appview_post_observation_key',
    on_schema_change='fail',
    tags=['twice_hourly']
) }}

with selected_source as (
    select
        cast(source as varchar) as source_name,
        cast(id as varchar) as post_uri,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(author as varchar) as author_handle,
        cast(created as timestamp with time zone) as created_at,
        cast(text as varchar) as post_text,
        cast(langs as json) as languages,
        cast(likes as bigint) as like_count,
        cast(reposts as bigint) as repost_count,
        cast(replies as bigint) as reply_count,
        cast(quotes as bigint) as quote_count,
        cast(bookmarks as bigint) as bookmark_count,
        cast(_row_id as varchar) as source_row_id,
        cast(_batch_id as varchar) as source_batch_id,
        cast(_source_file as varchar) as source_file,
        cast(_file_row_num as bigint) as source_file_row_number,
        cast(_dt as date) as source_date,
        cast(_extract_started_at as timestamp with time zone) as extract_started_at,
        cast(_load_id as varchar) as source_load_id,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as content_hash
    from {{ ref('base_bluesky_appview') }}
    {% if is_incremental() %}
    where cast(_loaded_at as timestamp with time zone) >= (
        select max(source_loaded_at)
        from {{ this }}
    )
    {% endif %}
),

keyed as (
    select
        cast(
            md5(
                to_json(
                    struct_pack(
                        source_name := source_name,
                        post_uri := post_uri,
                        observed_at := observed_at
                    )
                )
            ) as varchar
        ) as bluesky_appview_post_observation_key,
        selected_source.*
    from selected_source
),

deduplicated as (
    select *
    from keyed
    qualify row_number() over (
        partition by bluesky_appview_post_observation_key
        order by source_loaded_at desc, source_file desc, source_file_row_number desc, source_row_id desc
    ) = 1
)

select
    bluesky_appview_post_observation_key,
    source_name,
    post_uri,
    observed_at,
    author_handle,
    created_at,
    post_text,
    languages,
    like_count,
    repost_count,
    reply_count,
    quote_count,
    bookmark_count,
    source_row_id,
    source_batch_id,
    source_file,
    source_file_row_number,
    source_date,
    extract_started_at,
    source_load_id,
    source_loaded_at,
    content_hash
from deduplicated
