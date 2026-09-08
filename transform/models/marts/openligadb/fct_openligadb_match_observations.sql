{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['hourly']
) }}

with ranked as (
    select
        md5(to_json(struct_pack(
            source_system := cast(source as varchar),
            match_id := cast(id as varchar),
            observed_at := cast(fetched_at as timestamp with time zone)
        ))) as openligadb_match_observation_key,
        cast(source as varchar) as source_system,
        cast(id as varchar) as match_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(league as varchar) as league,
        cast(match_datetime as timestamp with time zone) as scheduled_at,
        cast(is_finished as boolean) as is_finished,
        cast(team1 as varchar) as home_team,
        cast(team2 as varchar) as away_team,
        cast(team1_score as bigint) as home_score,
        cast(team2_score as bigint) as away_score,
        cast(last_updated as timestamp with time zone) as match_updated_at,
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
            partition by source, id, fetched_at
            order by _loaded_at desc, _source_file desc, _file_row_num desc, _row_id desc
        ) as row_number
    from {{ ref('base_openligadb') }}
)

select
    openligadb_match_observation_key,
    source_system,
    match_id,
    observed_at,
    league,
    scheduled_at,
    is_finished,
    home_team,
    away_team,
    home_score,
    away_score,
    match_updated_at,
    source_row_id,
    source_batch_id,
    source_file,
    source_file_row_number,
    source_date,
    extract_started_at,
    load_id,
    source_loaded_at,
    content_hash
from ranked
where row_number = 1
