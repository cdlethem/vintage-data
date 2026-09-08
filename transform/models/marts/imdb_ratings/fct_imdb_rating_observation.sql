{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='imdb_rating_observation_key',
    on_schema_change='fail',
    tags=['daily']
) }}

with source_rows as (
    select
        cast(source as varchar) as source_system,
        cast(tconst as varchar) as title_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        try_cast(averagerating as decimal(4, 1)) as average_rating,
        try_cast(numvotes as bigint) as vote_count,
        cast(publisher_updated_at as varchar) as publisher_updated_at,
        cast(change as varchar) as change_status,
        try_cast(previous_average_rating as decimal(4, 1)) as previous_average_rating,
        try_cast(previous_num_votes as bigint) as previous_vote_count,
        cast(_row_id as varchar) as source_row_id,
        cast(_batch_id as varchar) as source_batch_id,
        cast(_source_file as varchar) as source_file,
        cast(_file_row_num as bigint) as source_file_row_number,
        cast(_dt as date) as source_date,
        cast(_extract_started_at as timestamp with time zone) as extract_started_at,
        cast(_load_id as varchar) as load_id,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as content_hash
    from {{ ref('base_imdb_ratings') }}
    {% if is_incremental() %}
    where _loaded_at >= (
        select coalesce(max(source_loaded_at), timestamp '1900-01-01')
        from {{ this }}
    )
    {% endif %}
),
deduplicated as (
    select
        md5(to_json(struct_pack(
            title_id := title_id,
            observed_at := observed_at
        ))) as imdb_rating_observation_key,
        *,
        row_number() over (
            partition by title_id, observed_at
            order by source_loaded_at desc, source_file desc, source_file_row_number desc, source_row_id desc
        ) as _dedupe_rank
    from source_rows
)

select
    cast(imdb_rating_observation_key as varchar) as imdb_rating_observation_key,
    cast(source_system as varchar) as source_system,
    cast(title_id as varchar) as title_id,
    cast(observed_at as timestamp with time zone) as observed_at,
    cast(average_rating as decimal(4, 1)) as average_rating,
    cast(vote_count as bigint) as vote_count,
    cast(publisher_updated_at as varchar) as publisher_updated_at,
    cast(change_status as varchar) as change_status,
    cast(previous_average_rating as decimal(4, 1)) as previous_average_rating,
    cast(previous_vote_count as bigint) as previous_vote_count,
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
where _dedupe_rank = 1
