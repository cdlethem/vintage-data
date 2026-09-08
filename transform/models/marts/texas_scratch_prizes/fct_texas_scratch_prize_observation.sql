{{ config(
    materialized='table',
    tags=['twice_hourly']
) }}

with ranked_observations as (
    select
        cast(md5(to_json(struct_pack(
            source_system := cast(source as varchar),
            prize_id := cast(id as varchar),
            observed_at := cast(fetched_at as timestamp with time zone)
        ))) as varchar) as texas_scratch_prize_observation_key,
        cast(source as varchar) as source_system,
        cast(id as varchar) as prize_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(game_number as varchar) as game_number,
        cast(game_name as varchar) as game_name,
        try_strptime(nullif(trim(game_close_date), ''), '%m/%d/%Y')::date as game_close_date,
        cast(ticket_price as bigint) as ticket_price,
        cast(prize_level as varchar) as prize_level_label,
        case
            when upper(trim(prize_level)) = 'TOTAL' then null
            else try_cast(trim(prize_level) as bigint)
        end as prize_level_number,
        upper(trim(prize_level)) = 'TOTAL' as is_total_prize_level,
        cast(total_prizes_in_level as bigint) as total_prizes_in_level,
        try_cast(nullif(trim(prizes_claimed), '') as bigint) as prizes_claimed,
        try_strptime(nullif(trim(publisher_updated_at), ''), '%m/%d/%Y')::date as publisher_updated_at,
        cast(_row_id as varchar) as source_row_id,
        cast(_batch_id as varchar) as source_batch_id,
        cast(_load_id as varchar) as load_id,
        cast(_source_file as varchar) as source_file,
        cast(_file_row_num as bigint) as source_file_row_number,
        cast(_dt as date) as source_date,
        cast(_extract_started_at as timestamp with time zone) as extract_started_at,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as content_hash,
        row_number() over (
            partition by source, id, fetched_at
            order by _loaded_at desc, _source_file desc, _file_row_num desc, _row_id desc
        ) as observation_rank
    from {{ ref('base_texas_scratch_prizes') }}
)

select
    texas_scratch_prize_observation_key,
    source_system,
    prize_id,
    observed_at,
    game_number,
    game_name,
    game_close_date,
    ticket_price,
    prize_level_label,
    prize_level_number,
    is_total_prize_level,
    total_prizes_in_level,
    prizes_claimed,
    publisher_updated_at,
    source_row_id,
    source_batch_id,
    load_id,
    source_file,
    source_file_row_number,
    source_date,
    extract_started_at,
    source_loaded_at,
    content_hash
from ranked_observations
where observation_rank = 1
