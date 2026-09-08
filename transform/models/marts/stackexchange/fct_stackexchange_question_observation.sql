{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='stackexchange_question_observation_key',
    on_schema_change='fail',
    tags=['twice_hourly']
) }}

with source_rows as (
    select
        cast(source as varchar) as source_name,
        cast(id as varchar) as question_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(title as varchar) as title,
        cast(tags as json) as tags,
        cast(to_timestamp(creation_date) as timestamp with time zone) as created_at,
        cast(owner_display_name as varchar) as owner_display_name,
        cast(owner_user_type as varchar) as owner_user_type,
        cast(score as bigint) as score,
        cast(view_count as bigint) as view_count,
        cast(answer_count as bigint) as answer_count,
        cast(is_answered as boolean) as is_answered,
        cast(link as varchar) as link,
        cast(quota_remaining as bigint) as quota_remaining,
        cast(_row_id as varchar) as source_row_id,
        cast(_batch_id as varchar) as source_batch_id,
        cast(_source_file as varchar) as source_file,
        cast(_file_row_num as bigint) as source_file_row_number,
        cast(_dt as date) as source_date,
        cast(_extract_started_at as timestamp with time zone) as extract_started_at,
        cast(_load_id as varchar) as load_id,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as content_hash
    from {{ ref('base_stackexchange') }}
    {% if is_incremental() %}
    where _loaded_at >= (
        select coalesce(max(source_loaded_at), timestamp '1900-01-01')
        from {{ this }}
    )
    {% endif %}
),
deduplicated as (
    select
        cast(md5(to_json(struct_pack(
            source_name := source_name,
            question_id := question_id,
            observed_at := observed_at
        ))) as varchar) as stackexchange_question_observation_key,
        *,
        row_number() over (
            partition by source_name, question_id, observed_at
            order by source_loaded_at desc, source_file desc, source_file_row_number desc, source_row_id desc
        ) as _dedupe_rank
    from source_rows
)

select
    cast(stackexchange_question_observation_key as varchar) as stackexchange_question_observation_key,
    cast(source_name as varchar) as source_name,
    cast(question_id as varchar) as question_id,
    cast(observed_at as timestamp with time zone) as observed_at,
    cast(title as varchar) as title,
    cast(tags as json) as tags,
    cast(created_at as timestamp with time zone) as created_at,
    cast(owner_display_name as varchar) as owner_display_name,
    cast(owner_user_type as varchar) as owner_user_type,
    cast(score as bigint) as score,
    cast(view_count as bigint) as view_count,
    cast(answer_count as bigint) as answer_count,
    cast(is_answered as boolean) as is_answered,
    cast(link as varchar) as link,
    cast(quota_remaining as bigint) as quota_remaining,
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
