{{ config(
    materialized='table',
    tags=['twice_hourly']
) }}

with normalized as (
    select
        cast(source as varchar) as source_system,
        cast(id as varchar) as game_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(date as date) as game_date,
        cast(start_time_utc as timestamp with time zone) as scheduled_at,
        cast(game_state as varchar) as game_state,
        cast(venue as varchar) as venue,
        cast(away_team as varchar) as away_team,
        cast(away_team_abbrev as varchar) as away_team_abbrev,
        cast(home_team as varchar) as home_team,
        cast(home_team_abbrev as varchar) as home_team_abbrev,
        cast(_row_id as varchar) as source_row_id,
        cast(_batch_id as varchar) as source_batch_id,
        cast(_source_file as varchar) as source_file,
        cast(_file_row_num as bigint) as source_file_row_number,
        cast(_dt as date) as source_date,
        cast(_extract_started_at as timestamp with time zone) as extract_started_at,
        cast(_load_id as varchar) as load_id,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as content_hash
    from {{ ref('base_nhl') }}
),
deduplicated as (
    select
        cast(md5(to_json(struct_pack(
            source_system := source_system,
            game_id := game_id,
            observed_at := observed_at
        ))) as varchar) as nhl_game_observation_key,
        *
    from normalized
    qualify row_number() over (
        partition by source_system, game_id, observed_at
        order by source_loaded_at desc, source_file desc, source_file_row_number desc, source_row_id desc
    ) = 1
)

select
    cast(nhl_game_observation_key as varchar) as nhl_game_observation_key,
    cast(source_system as varchar) as source_system,
    cast(game_id as varchar) as game_id,
    cast(observed_at as timestamp with time zone) as observed_at,
    cast(game_date as date) as game_date,
    cast(scheduled_at as timestamp with time zone) as scheduled_at,
    cast(game_state as varchar) as game_state,
    cast(venue as varchar) as venue,
    cast(away_team as varchar) as away_team,
    cast(away_team_abbrev as varchar) as away_team_abbrev,
    cast(home_team as varchar) as home_team,
    cast(home_team_abbrev as varchar) as home_team_abbrev,
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
