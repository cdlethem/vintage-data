{{ config(
    materialized='table',
    tags=['twice_hourly']
) }}

with normalized as (
    select
        cast(source as varchar) as source_system,
        cast(id as varchar) as game_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(game_date as timestamp with time zone) as scheduled_at,
        cast(status as varchar) as game_status,
        cast(abstract_status as varchar) as game_abstract_status,
        cast(home_team as varchar) as home_team,
        cast(away_team as varchar) as away_team,
        cast(home_score as bigint) as home_score,
        cast(away_score as bigint) as away_score,
        cast(venue as varchar) as venue,
        cast(home_probable as varchar) as home_probable_pitcher,
        cast(away_probable as varchar) as away_probable_pitcher,
        cast(_batch_id as varchar) as source_batch_id,
        cast(_source_file as varchar) as source_file,
        cast(_file_row_num as bigint) as source_file_row_number,
        cast(_dt as date) as source_date,
        cast(_extract_started_at as timestamp with time zone) as extract_started_at,
        cast(_load_id as varchar) as load_id,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as content_hash
    from {{ ref('base_mlb_statsapi') }}
),
deduplicated as (
    select
        md5(to_json(struct_pack(game_id := game_id, observed_at := observed_at))) as mlb_game_observation_key,
        *
    from normalized
    qualify row_number() over (
        partition by game_id, observed_at
        order by source_loaded_at desc, source_file desc, source_file_row_number desc
    ) = 1
)
select
    mlb_game_observation_key,
    source_system,
    game_id,
    observed_at,
    scheduled_at,
    game_status,
    game_abstract_status,
    home_team,
    away_team,
    home_score,
    away_score,
    venue,
    home_probable_pitcher,
    away_probable_pitcher,
    source_batch_id,
    source_file,
    source_file_row_number,
    source_date,
    extract_started_at,
    load_id,
    source_loaded_at,
    content_hash
from deduplicated
