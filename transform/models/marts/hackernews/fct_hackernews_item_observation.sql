{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='hackernews_item_observation_key',
    on_schema_change='fail',
    tags=['twice_hourly']
) }}

with source_rows as (
    select
        cast(_source as varchar) as source_system,
        cast(id as varchar) as item_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(type as varchar) as item_type,
        cast("by" as varchar) as author,
        cast(created as timestamp with time zone) as created_at,
        cast(title as varchar) as title,
        cast(url as varchar) as url,
        cast(score as bigint) as score,
        cast(descendants as bigint) as descendants,
        cast(dead as boolean) as is_dead,
        cast(deleted as boolean) as is_deleted,
        cast(text as varchar) as text,
        cast(_row_id as varchar) as source_row_id,
        cast(_batch_id as varchar) as source_batch_id,
        cast(_source_file as varchar) as source_file,
        cast(_file_row_num as bigint) as source_file_row_number,
        cast(_dt as date) as source_date,
        cast(_extract_started_at as timestamp with time zone) as extract_started_at,
        cast(_load_id as varchar) as load_id,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as content_hash
    from {{ ref('base_hackernews') }}
    {% if is_incremental() %}
    where _loaded_at >= (
        select coalesce(max(source_loaded_at), timestamp '1900-01-01')
        from {{ this }}
    )
    {% endif %}
),
deduplicated as (
    select
        cast(md5(to_json(struct_pack(
            source_system := source_system,
            item_id := item_id,
            observed_at := observed_at
        ))) as varchar) as hackernews_item_observation_key,
        *,
        row_number() over (
            partition by source_system, item_id, observed_at
            order by source_loaded_at desc, source_file desc, source_file_row_number desc, source_row_id desc
        ) as _dedupe_rank
    from source_rows
)

select
    cast(hackernews_item_observation_key as varchar) as hackernews_item_observation_key,
    cast(source_system as varchar) as source_system,
    cast(item_id as varchar) as item_id,
    cast(observed_at as timestamp with time zone) as observed_at,
    cast(item_type as varchar) as item_type,
    cast(author as varchar) as author,
    cast(created_at as timestamp with time zone) as created_at,
    cast(title as varchar) as title,
    cast(url as varchar) as url,
    cast(score as bigint) as score,
    cast(descendants as bigint) as descendants,
    cast(is_dead as boolean) as is_dead,
    cast(is_deleted as boolean) as is_deleted,
    cast(text as varchar) as text,
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
where _dedupe_rank = 1
