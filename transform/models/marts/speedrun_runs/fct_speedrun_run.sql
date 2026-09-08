{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['twice_hourly']
) }}

with source_rows as (
    select
        cast(id as varchar) as run_id,
        cast(source as varchar) as source,
        cast(game as varchar) as game_name,
        cast(category as varchar) as category_name,
        cast(platform as varchar) as platform_name,
        cast(players as json) as players,
        cast(run_date as date) as run_date,
        cast(submitted as timestamp with time zone) as submitted_at,
        cast(verify_date as timestamp with time zone) as verified_at,
        cast(status as varchar) as status,
        cast(examiner as varchar) as examiner_id,
        cast(duration_s as double) as duration_seconds,
        cast(duration_iso as varchar) as duration_iso,
        cast(emulated as boolean) as is_emulated,
        cast(video as boolean) as has_video,
        cast(url as varchar) as run_url,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(_batch_id as varchar) as source_batch_id,
        cast(_source_file as varchar) as source_file,
        cast(_file_row_num as bigint) as source_file_row_number,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as content_hash
    from {{ ref('base_speedrun_runs') }}
),

keyed as (
    select
        cast(
            md5(
                to_json(
                    struct_pack(
                        run_id := run_id
                    )
                )
            ) as varchar
        ) as speedrun_run_key,
        source_rows.*
    from source_rows
),

deduplicated as (
    select *
    from keyed
    qualify row_number() over (
        partition by speedrun_run_key
        order by source_loaded_at desc, source_file desc, source_file_row_number desc
    ) = 1
)

select
    cast(speedrun_run_key as varchar) as speedrun_run_key,
    cast(run_id as varchar) as run_id,
    cast(source as varchar) as source,
    cast(game_name as varchar) as game_name,
    cast(category_name as varchar) as category_name,
    cast(platform_name as varchar) as platform_name,
    cast(players as json) as players,
    cast(run_date as date) as run_date,
    cast(submitted_at as timestamp with time zone) as submitted_at,
    cast(verified_at as timestamp with time zone) as verified_at,
    cast(status as varchar) as status,
    cast(examiner_id as varchar) as examiner_id,
    cast(duration_seconds as double) as duration_seconds,
    cast(duration_iso as varchar) as duration_iso,
    cast(is_emulated as boolean) as is_emulated,
    cast(has_video as boolean) as has_video,
    cast(run_url as varchar) as run_url,
    cast(observed_at as timestamp with time zone) as observed_at,
    cast(source_batch_id as varchar) as source_batch_id,
    cast(source_file as varchar) as source_file,
    cast(source_file_row_number as bigint) as source_file_row_number,
    cast(source_loaded_at as timestamp with time zone) as source_loaded_at,
    cast(content_hash as varchar) as content_hash
from deduplicated
