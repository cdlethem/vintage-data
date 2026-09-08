{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['daily']
) }}

with source_rows as (
    select
        md5(to_json(struct_pack(
            archive_source := cast(_source as varchar),
            game_id := cast(id as varchar)
        ))) as chess_archive_gothamchess_game_key,
        cast(_source as varchar) as archive_source,
        cast(source as varchar) as source_system,
        cast(id as varchar) as game_id,
        cast(fetched_at as timestamp with time zone) as fetched_at,
        cast("user" as varchar) as player_user,
        cast(color as varchar) as player_color,
        cast("end" as timestamp with time zone) as ended_at,
        cast(time_class as varchar) as time_class,
        cast(time_control as varchar) as time_control,
        cast(rated as boolean) as is_rated,
        cast(my_rating as bigint) as player_rating,
        cast(opp_rating as bigint) as opponent_rating,
        cast(result as varchar) as player_result,
        cast(opp_result as varchar) as opponent_result,
        cast(rules as varchar) as rules,
        cast(eco as varchar) as opening,
        cast(n_moves_timed as bigint) as timed_move_count,
        cast(median_move_time_s as double) as median_move_time_seconds,
        cast(max_move_time_s as double) as max_move_time_seconds,
        cast(_row_id as varchar) as source_row_id,
        cast(_batch_id as varchar) as source_batch_id,
        cast(_source_file as varchar) as source_file,
        cast(_file_row_num as bigint) as source_file_row_number,
        cast(_dt as date) as source_date,
        cast(_extract_started_at as timestamp with time zone) as extract_started_at,
        cast(_load_id as varchar) as load_id,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as content_hash
    from {{ ref('base_chess_archives_gothamchess') }}
),

deduplicated as (
    select *
    from source_rows
    qualify row_number() over (
        partition by chess_archive_gothamchess_game_key
        order by source_loaded_at desc, source_file desc, source_file_row_number desc, source_row_id desc
    ) = 1
)

select
    chess_archive_gothamchess_game_key,
    archive_source,
    source_system,
    game_id,
    fetched_at,
    player_user,
    player_color,
    ended_at,
    time_class,
    time_control,
    is_rated,
    player_rating,
    opponent_rating,
    player_result,
    opponent_result,
    rules,
    opening,
    timed_move_count,
    median_move_time_seconds,
    max_move_time_seconds,
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
