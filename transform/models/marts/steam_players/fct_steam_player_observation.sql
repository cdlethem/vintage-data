{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='steam_player_observation_key',
    on_schema_change='fail',
    tags=['twice_hourly']
) }}

with source_rows as (
    select
        cast(source as varchar) as source,
        cast(id as varchar) as source_id,
        cast(appid as bigint) as game_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(ok as boolean) as request_succeeded,
        cast(player_count as bigint) as concurrent_player_count,
        cast(_batch_id as varchar) as source_batch_id,
        cast(_load_id as varchar) as source_load_id,
        cast(_source_file as varchar) as source_file,
        cast(_file_row_num as bigint) as source_file_row_number,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as content_hash
    from {{ ref('base_steam_players') }}
    {% if is_incremental() %}
    where cast(_loaded_at as timestamp with time zone) >= (
        select coalesce(
            max(source_loaded_at),
            timestamp with time zone '1900-01-01 00:00:00+00'
        )
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
                        source := source,
                        game_id := game_id,
                        observed_at := observed_at
                    )
                )
            ) as varchar
        ) as steam_player_observation_key,
        source_rows.*
    from source_rows
),

deduplicated as (
    select *
    from keyed
    qualify row_number() over (
        partition by steam_player_observation_key
        order by source_loaded_at desc, source_file desc, source_file_row_number desc
    ) = 1
)

select
    cast(steam_player_observation_key as varchar) as steam_player_observation_key,
    cast(source as varchar) as source,
    cast(source_id as varchar) as source_id,
    cast(game_id as bigint) as game_id,
    cast(observed_at as timestamp with time zone) as observed_at,
    cast(request_succeeded as boolean) as request_succeeded,
    cast(concurrent_player_count as bigint) as concurrent_player_count,
    cast(source_batch_id as varchar) as source_batch_id,
    cast(source_load_id as varchar) as source_load_id,
    cast(source_file as varchar) as source_file,
    cast(source_file_row_number as bigint) as source_file_row_number,
    cast(source_loaded_at as timestamp with time zone) as source_loaded_at,
    cast(content_hash as varchar) as content_hash
from deduplicated
