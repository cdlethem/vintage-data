{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['hourly']
) }}

with source_rows as (
    select
        md5(
            to_json(
                struct_pack(
                    source_name := cast(source as varchar),
                    notice_id := cast(id as varchar),
                    observed_at := cast(fetched_at as timestamp with time zone)
                )
            )
        ) as gazette_insolvency_notice_observation_key,
        cast(source as varchar) as source_name,
        cast(id as varchar) as notice_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(f_status as varchar) as notice_status,
        cast(f_notice_code as varchar) as notice_code,
        cast(f_name as varchar) as name,
        cast(f_familyname as varchar) as family_name,
        cast(title as varchar) as title,
        cast(link as json) as links,
        cast(author as json) as author,
        cast(category as json) as category,
        cast(geo_point as json) as geo_point,
        cast(content as varchar) as content,
        cast(updated as timestamp with time zone) as updated_at,
        cast(published as timestamp with time zone) as published_at,
        cast(feed_updated_at as timestamp with time zone) as feed_updated_at,
        cast(feed_total as bigint) as feed_total,
        cast(feed_total_errors as bigint) as feed_total_errors,
        cast(_row_id as varchar) as _row_id,
        cast(_batch_id as varchar) as _batch_id,
        cast(_load_id as varchar) as _load_id,
        cast(_source_file as varchar) as _source_file,
        cast(_file_row_num as bigint) as _file_row_num,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as _content_hash
    from {{ ref('base_gazette_insolvency') }}
),

deduplicated as (
    select *
    from source_rows
    qualify row_number() over (
        partition by source_name, notice_id, observed_at
        order by source_loaded_at desc, _source_file desc, _file_row_num desc
    ) = 1
)

select
    gazette_insolvency_notice_observation_key,
    source_name,
    notice_id,
    observed_at,
    notice_status,
    notice_code,
    name,
    family_name,
    title,
    links,
    author,
    category,
    geo_point,
    content,
    updated_at,
    published_at,
    feed_updated_at,
    feed_total,
    feed_total_errors,
    _row_id,
    _batch_id,
    _load_id,
    _source_file,
    _file_row_num,
    source_loaded_at,
    _content_hash
from deduplicated
