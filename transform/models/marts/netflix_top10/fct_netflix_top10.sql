{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['daily']
) }}

with source_rows as (
    select
        md5(to_json(struct_pack(
            week := cast(week as date),
            scope := cast(scope as varchar),
            category := cast(category as varchar),
            weekly_rank := cast(weekly_rank as integer),
            show_title := cast(show_title as varchar)
        ))) as netflix_top10_key,
        cast(scope as varchar) as scope,
        cast(week as date) as week,
        cast(category as varchar) as category,
        cast(weekly_rank as integer) as weekly_rank,
        cast(show_title as varchar) as show_title,
        cast(season_title as varchar) as season_title,
        try_cast(weekly_hours_viewed as bigint) as weekly_hours_viewed,
        try_cast(runtime as decimal(18, 4)) as runtime,
        try_cast(weekly_views as bigint) as weekly_views,
        try_cast(cumulative_weeks_in_top_10 as integer) as cumulative_weeks_in_top_10,
        cast(source as varchar) as source_name,
        cast(id as varchar) as title_id,
        cast(fetched_at as timestamp with time zone) as fetched_at,
        cast(publisher_updated_at as varchar) as publisher_updated_at,
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
            partition by week, scope, category, weekly_rank, show_title
            order by _loaded_at desc, _source_file desc, _file_row_num desc
        ) as _dedupe_rank
    from {{ ref('base_netflix_top10') }}
)

select
    netflix_top10_key,
    scope,
    week,
    category,
    weekly_rank,
    show_title,
    season_title,
    weekly_hours_viewed,
    runtime,
    weekly_views,
    cumulative_weeks_in_top_10,
    source_name,
    title_id,
    fetched_at,
    publisher_updated_at,
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
