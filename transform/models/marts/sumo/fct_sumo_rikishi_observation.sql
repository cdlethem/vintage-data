{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['daily']
) }}

with source_rows as (
    select
        md5(to_json(struct_pack(
            source_system := cast(source as varchar),
            rikishi_id := cast(id as varchar),
            observed_at := cast(fetched_at as timestamp with time zone)
        ))) as rikishi_observation_key,
        cast(source as varchar) as source_system,
        cast(id as varchar) as rikishi_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(updated_at as timestamp with time zone) as updated_at,
        cast(shikona_en as varchar) as shikona_en,
        cast(shikona_jp as varchar) as shikona_jp,
        cast(current_rank as varchar) as current_rank,
        case
            when current_rank is null then null
            when current_rank like 'Yokozuna%' then 'Makuuchi'
            when current_rank like 'Ozeki%' then 'Makuuchi'
            when current_rank like 'Sekiwake%' then 'Makuuchi'
            when current_rank like 'Komusubi%' then 'Makuuchi'
            when current_rank like 'Maegashira%' then 'Makuuchi'
            when current_rank like 'Juryo%' then 'Juryo'
            when current_rank like 'Makushita%' then 'Makushita'
            when current_rank like 'Sandanme%' then 'Sandanme'
            when current_rank like 'Jonidan%' then 'Jonidan'
            when current_rank like 'Jonokuchi%' then 'Jonokuchi'
            else 'Other / unknown'
        end as rank_division,
        cast(heya as varchar) as heya,
        cast(height_cm as double) as height_cm,
        cast(weight_kg as double) as weight_kg,
        cast(debut as varchar) as debut,
        cast(_row_id as varchar) as source_row_id,
        cast(_batch_id as varchar) as source_batch_id,
        cast(_source_file as varchar) as source_file,
        cast(_file_row_num as bigint) as source_file_row_number,
        cast(_dt as date) as source_date,
        cast(_extract_started_at as timestamp with time zone) as extract_started_at,
        cast(_load_id as varchar) as load_id,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as content_hash
    from {{ ref('base_sumo') }}
),

deduplicated as (
    select *
    from source_rows
    qualify row_number() over (
        partition by rikishi_observation_key
        order by source_loaded_at desc, source_file desc, source_file_row_number desc, source_row_id desc
    ) = 1
)

select
    rikishi_observation_key,
    source_system,
    rikishi_id,
    observed_at,
    updated_at,
    shikona_en,
    shikona_jp,
    current_rank,
    rank_division,
    heya,
    height_cm,
    weight_kg,
    debut,
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
