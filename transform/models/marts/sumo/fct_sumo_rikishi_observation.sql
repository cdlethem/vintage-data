{{ config(
    materialized='incremental',
    incremental_strategy='append',
    on_schema_change='fail',
    tags=['weekly']
) }}

with filtered_source as (
    select *
    from {{ ref('base_sumo') }}
    where cast(source as varchar) = 'sumo_rikishi'
    {% if flags.WHICH != 'compile' and is_incremental() %}
    and cast(_loaded_at as timestamp with time zone) >= (
        select coalesce(max(source_loaded_at), timestamp '1900-01-01')
        from {{ this }}
    )
    {% endif %}
),
typed_rows as (
    select
        cast(_source as varchar) as source_system,
        cast(id as varchar) as rikishi_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(shikona_en as varchar) as shikona_en,
        cast(shikona_jp as varchar) as shikona_jp,
        cast(current_rank as varchar) as current_rank,
        case
            when regexp_matches(cast(current_rank as varchar), '^(Yokozuna|Ozeki|Sekiwake|Komusubi|Maegashira)( |$)') then 'Makuuchi'
            when regexp_matches(cast(current_rank as varchar), '^Juryo( |$)') then 'Juryo'
            when regexp_matches(cast(current_rank as varchar), '^Makushita( |$)') then 'Makushita'
            when regexp_matches(cast(current_rank as varchar), '^Sandanme( |$)') then 'Sandanme'
            when regexp_matches(cast(current_rank as varchar), '^Jonidan( |$)') then 'Jonidan'
            when regexp_matches(cast(current_rank as varchar), '^Jonokuchi( |$)') then 'Jonokuchi'
        end as rank_division,
        cast(heya as varchar) as heya,
        cast(height_cm as double) as height_cm,
        cast(weight_kg as double) as weight_kg,
        cast(debut as varchar) as debut,
        cast(updated_at as timestamp with time zone) as updated_at,
        cast(_row_id as varchar) as source_row_id,
        cast(_batch_id as varchar) as source_batch_id,
        cast(_source_file as varchar) as source_file,
        cast(_file_row_num as bigint) as source_file_row_number,
        cast(_dt as date) as source_date,
        cast(_extract_started_at as timestamp with time zone) as extract_started_at,
        cast(_load_id as varchar) as load_id,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as content_hash
    from filtered_source
),
keyed_rows as (
    select
        cast(md5(to_json(struct_pack(
            source_system := source_system,
            rikishi_id := rikishi_id,
            observed_at := observed_at
        ))) as varchar) as rikishi_observation_key,
        *
    from typed_rows
),
deduplicated as (
    select
        *,
        row_number() over (
            partition by source_system, rikishi_id, observed_at
            order by source_loaded_at desc, source_file desc, source_file_row_number desc, source_row_id desc
        ) as _dedupe_rank
    from keyed_rows
)

select
    rikishi_observation_key,
    source_system,
    rikishi_id,
    observed_at,
    shikona_en,
    shikona_jp,
    current_rank,
    rank_division,
    heya,
    height_cm,
    weight_kg,
    debut,
    updated_at,
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
where _dedupe_rank = 1
{% if flags.WHICH != 'compile' and is_incremental() %}
and not exists (
    select 1
    from {{ this }} as existing
    where existing.source_system = deduplicated.source_system
      and existing.rikishi_id = deduplicated.rikishi_id
      and existing.observed_at = deduplicated.observed_at
)
{% endif %}
