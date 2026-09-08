{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='codeforces_submission_observation_key',
    on_schema_change='fail',
    tags=['hourly']
) }}

with source_rows as (
    select
        cast(
            md5(
                to_json(
                    struct_pack(
                        source := cast(source as varchar),
                        submission_id := cast(id as varchar),
                        observed_at := cast(fetched_at as timestamp with time zone)
                    )
                )
            ) as varchar
        ) as codeforces_submission_observation_key,
        cast(source as varchar) as source,
        cast(id as varchar) as submission_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(ts as timestamp with time zone) as submitted_at,
        cast("user" as varchar) as user_handle,
        cast(problem_rating as bigint) as problem_rating,
        cast(tags as json) as tags,
        cast(verdict as varchar) as verdict,
        cast(language as varchar) as language,
        cast(_dt as date) as source_date,
        cast(_extract_started_at as timestamp with time zone) as extract_started_at,
        cast(_batch_id as varchar) as source_batch_id,
        cast(_load_id as varchar) as load_id,
        cast(_source_file as varchar) as source_file,
        cast(_file_row_num as bigint) as source_file_row_number,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as content_hash
    from {{ ref('base_codeforces') }}
    {% if is_incremental() %}
    where cast(_loaded_at as timestamp with time zone) >= (
        select max(source_loaded_at)
        from {{ this }}
    )
    {% endif %}
),

deduplicated as (
    select *
    from source_rows
    qualify row_number() over (
        partition by codeforces_submission_observation_key
        order by source_loaded_at desc, source_file desc, source_file_row_number desc
    ) = 1
)

select
    codeforces_submission_observation_key,
    source,
    submission_id,
    observed_at,
    submitted_at,
    user_handle,
    problem_rating,
    tags,
    verdict,
    language,
    source_date,
    extract_started_at,
    source_batch_id,
    load_id,
    source_file,
    source_file_row_number,
    source_loaded_at,
    content_hash
from deduplicated
