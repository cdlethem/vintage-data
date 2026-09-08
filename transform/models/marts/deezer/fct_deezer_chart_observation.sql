{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='deezer_chart_observation_key',
    on_schema_change='fail',
    tags=['hourly']
) }}

with selected_source as (
    select
        cast(source as varchar) as source_name,
        cast(id as varchar) as track_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(genre_id as bigint) as genre_id,
        cast(position as bigint) as chart_position,
        cast(rank as bigint) as track_rank,
        cast(title as varchar) as track_title,
        cast(artist as varchar) as artist_name,
        cast(artist_id as bigint) as artist_id,
        cast(album as varchar) as album_title,
        cast(duration_s as bigint) as duration_seconds,
        cast(explicit_lyrics as boolean) as is_explicit,
        cast(url as varchar) as track_url,
        cast(_batch_id as varchar) as source_batch_id,
        cast(_load_id as varchar) as source_load_id,
        cast(_source_file as varchar) as source_file,
        cast(_file_row_num as bigint) as source_file_row_number,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as content_hash
    from {{ ref('base_deezer') }}
    {% if is_incremental() %}
    where cast(_loaded_at as timestamp with time zone) >= (
        select max(source_loaded_at)
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
                        source_name := source_name,
                        track_id := track_id,
                        observed_at := observed_at,
                        chart_position := chart_position
                    )
                )
            ) as varchar
        ) as deezer_chart_observation_key,
        selected_source.*
    from selected_source
),

deduplicated as (
    select *
    from keyed
    qualify row_number() over (
        partition by deezer_chart_observation_key
        order by source_loaded_at desc, source_file desc, source_file_row_number desc
    ) = 1
)

select
    cast(deezer_chart_observation_key as varchar) as deezer_chart_observation_key,
    cast(source_name as varchar) as source_name,
    cast(track_id as varchar) as track_id,
    cast(observed_at as timestamp with time zone) as observed_at,
    cast(genre_id as bigint) as genre_id,
    cast(chart_position as bigint) as chart_position,
    cast(track_rank as bigint) as track_rank,
    cast(track_title as varchar) as track_title,
    cast(artist_name as varchar) as artist_name,
    cast(artist_id as bigint) as artist_id,
    cast(album_title as varchar) as album_title,
    cast(duration_seconds as bigint) as duration_seconds,
    cast(is_explicit as boolean) as is_explicit,
    cast(track_url as varchar) as track_url,
    cast(source_batch_id as varchar) as source_batch_id,
    cast(source_load_id as varchar) as source_load_id,
    cast(source_file as varchar) as source_file,
    cast(source_file_row_number as bigint) as source_file_row_number,
    cast(source_loaded_at as timestamp with time zone) as source_loaded_at,
    cast(content_hash as varchar) as content_hash
from deduplicated
