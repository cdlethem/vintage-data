{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['hourly']
) }}

with source_rows as (
    select
        cast(source as varchar) as source_name,
        cast(id as varchar) as release_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(title as varchar) as release_title,
        cast(artists as json) as artists,
        cast(date as varchar) as release_date_text,
        cast(country as varchar) as country_code,
        cast(status as varchar) as release_status,
        cast(release_group_type as varchar) as release_group_type,
        cast(track_count as bigint) as track_count,
        cast(score as bigint) as search_score,
        cast(_batch_id as varchar) as source_batch_id,
        cast(_load_id as varchar) as source_load_id,
        cast(_source_file as varchar) as source_file,
        cast(_file_row_num as bigint) as source_file_row_number,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as content_hash
    from {{ ref('base_musicbrainz') }}
),

keyed as (
    select
        cast(
            md5(
                to_json(
                    struct_pack(
                        source_name := source_name,
                        release_id := release_id,
                        observed_at := observed_at
                    )
                )
            ) as varchar
        ) as release_observation_key,
        source_rows.*
    from source_rows
),

deduplicated as (
    select *
    from keyed
    qualify row_number() over (
        partition by release_observation_key
        order by source_loaded_at desc, source_file desc, source_file_row_number desc
    ) = 1
)

select
    cast(release_observation_key as varchar) as release_observation_key,
    cast(source_name as varchar) as source_name,
    cast(release_id as varchar) as release_id,
    cast(observed_at as timestamp with time zone) as observed_at,
    cast(release_title as varchar) as release_title,
    cast(artists as json) as artists,
    cast(release_date_text as varchar) as release_date_text,
    cast(country_code as varchar) as country_code,
    cast(release_status as varchar) as release_status,
    cast(release_group_type as varchar) as release_group_type,
    cast(track_count as bigint) as track_count,
    cast(search_score as bigint) as search_score,
    cast(source_batch_id as varchar) as source_batch_id,
    cast(source_load_id as varchar) as source_load_id,
    cast(source_file as varchar) as source_file,
    cast(source_file_row_number as bigint) as source_file_row_number,
    cast(source_loaded_at as timestamp with time zone) as source_loaded_at,
    cast(content_hash as varchar) as content_hash
from deduplicated
