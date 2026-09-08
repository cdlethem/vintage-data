{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='lichess_game_observation_key',
    on_schema_change='fail',
    tags=['twice_hourly']
) }}

with source_rows as (
    select
        cast(
            md5(
                to_json(
                    struct_pack(
                        source := cast(source as varchar),
                        game_id := cast(game_id as varchar),
                        variant := cast(variant as varchar),
                        player := cast(player as varchar),
                        color := cast(color as varchar),
                        observed_at := cast(fetched_at as timestamp with time zone)
                    )
                )
            ) as varchar
        ) as lichess_game_observation_key,
        cast(source as varchar) as source,
        cast(game_id as varchar) as game_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(variant as varchar) as variant,
        cast(player as varchar) as player,
        cast(title as varchar) as title,
        cast(rating as bigint) as rating,
        cast(color as varchar) as color,
        cast(_row_id as varchar) as source_row_id,
        cast(_batch_id as varchar) as source_batch_id,
        cast(_source_file as varchar) as source_file,
        cast(_file_row_num as bigint) as source_file_row_number,
        cast(_dt as date) as source_date,
        cast(_extract_started_at as timestamp with time zone) as extract_started_at,
        cast(_load_id as varchar) as load_id,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as content_hash
    from {{ ref('base_lichess') }}
    {% if is_incremental() %}
    where cast(_loaded_at as timestamp with time zone) >= (
        select max(source_loaded_at)
        from {{ this }}
    )
    {% endif %}
),

deduplicated as (
    select *
    from source_rows
    qualify row_number() over (
        partition by lichess_game_observation_key
        order by source_loaded_at desc, source_file desc, source_file_row_number desc, source_row_id desc
    ) = 1
)

select
    lichess_game_observation_key,
    source,
    game_id,
    observed_at,
    variant,
    player,
    title,
    rating,
    color,
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
