{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['twice_hourly']
) }}

with typed_rows as (
    select
        cast(source as varchar) as source_name,
        cast(id as varchar) as topic_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(title as varchar) as title,
        cast(slug as varchar) as slug,
        cast(created_at as timestamp with time zone) as created_at,
        cast(bumped_at as timestamp with time zone) as bumped_at,
        cast(author as varchar) as author,
        cast(tags as json) as tags,
        cast(reply_count as bigint) as reply_count,
        cast(views as bigint) as views,
        cast(url as varchar) as url,
        cast(_row_id as varchar) as _row_id,
        cast(_batch_id as varchar) as _batch_id,
        cast(_source_file as varchar) as _source_file,
        cast(_file_row_num as bigint) as _file_row_num,
        cast(_dt as date) as _dt,
        cast(_extract_started_at as timestamp with time zone) as _extract_started_at,
        cast(_load_id as varchar) as _load_id,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as _content_hash
    from {{ ref('base_discourse_meta') }}
), keyed_rows as (
    select
        md5(to_json(struct_pack(
            source_name := source_name,
            topic_id := topic_id,
            observed_at := observed_at
        ))) as discourse_topic_observation_key,
        *,
        row_number() over (
            partition by source_name, topic_id, observed_at
            order by source_loaded_at desc, _source_file desc, _file_row_num desc, _row_id desc
        ) as _dedupe_rank
    from typed_rows
)

select
    discourse_topic_observation_key,
    source_name,
    topic_id,
    observed_at,
    title,
    slug,
    created_at,
    bumped_at,
    author,
    tags,
    reply_count,
    views,
    url,
    _row_id,
    _batch_id,
    _source_file,
    _file_row_num,
    _dt,
    _extract_started_at,
    _load_id,
    source_loaded_at,
    _content_hash
from keyed_rows
where _dedupe_rank = 1
