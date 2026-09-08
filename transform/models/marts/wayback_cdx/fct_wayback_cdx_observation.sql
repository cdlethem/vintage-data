{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['daily']
) }}

with source_rows as (
    select
        md5(to_json(struct_pack(
            source_name := cast(source as varchar),
            capture_id := cast(id as varchar),
            observed_at := cast(fetched_at as timestamp with time zone)
        ))) as wayback_cdx_observation_key,
        cast(source as varchar) as source_name,
        cast(id as varchar) as capture_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        timezone('UTC', try_strptime(cast(captured_at as varchar), '%Y%m%d%H%M%S')) as captured_at,
        cast(original_url as varchar) as original_url,
        cast(status_code as varchar) as status_code,
        cast(mimetype as varchar) as mimetype,
        cast(digest as varchar) as digest,
        cast(length as bigint) as content_length,
        cast(watched_url as varchar) as watched_url,
        cast(_batch_id as varchar) as _batch_id,
        cast(_load_id as varchar) as _load_id,
        cast(_source_file as varchar) as _source_file,
        cast(_file_row_num as bigint) as _file_row_num,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as _content_hash
    from {{ ref('base_wayback_cdx') }}
),

deduplicated as (
    select *
    from source_rows
    qualify row_number() over (
        partition by wayback_cdx_observation_key
        order by source_loaded_at desc, _source_file desc, _file_row_num desc
    ) = 1
)

select
    wayback_cdx_observation_key,
    source_name,
    capture_id,
    observed_at,
    captured_at,
    original_url,
    status_code,
    mimetype,
    digest,
    content_length,
    watched_url,
    _batch_id,
    _load_id,
    _source_file,
    _file_row_num,
    source_loaded_at,
    _content_hash
from deduplicated
