{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['daily']
) }}

with source_rows as (
    select
        cast(source as varchar) as source,
        cast(id as varchar) as job_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(provider as varchar) as provider,
        cast(title as varchar) as title,
        cast(department as varchar) as department,
        cast(experience_level as json) as experience_level,
        cast(location as varchar) as location,
        cast(location_country as varchar) as location_country,
        cast(workplace_type as json) as workplace_type,
        cast(is_remote as boolean) as is_remote,
        cast(posted as timestamp with time zone) as posted_at,
        cast(url as varchar) as job_url,
        cast(salary_min as bigint) as salary_min,
        cast(salary_max as bigint) as salary_max,
        cast(description as varchar) as description,
        cast(_batch_id as varchar) as _batch_id,
        cast(_load_id as varchar) as _load_id,
        cast(_source_file as varchar) as _source_file,
        cast(_file_row_num as bigint) as _file_row_num,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as _content_hash
    from {{ ref('base_job_feed_getonboard') }}
),

keyed as (
    select
        cast(md5(to_json(struct_pack(
            source := source,
            job_id := job_id,
            observed_at := observed_at
        ))) as varchar) as job_feed_getonboard_key,
        *
    from source_rows
),

deduplicated as (
    select *
    from keyed
    qualify row_number() over (
        partition by job_feed_getonboard_key
        order by source_loaded_at desc, _source_file desc, _file_row_num desc
    ) = 1
)

select
    cast(job_feed_getonboard_key as varchar) as job_feed_getonboard_key,
    cast(source as varchar) as source,
    cast(job_id as varchar) as job_id,
    cast(observed_at as timestamp with time zone) as observed_at,
    cast(provider as varchar) as provider,
    cast(title as varchar) as title,
    cast(department as varchar) as department,
    cast(experience_level as json) as experience_level,
    cast(location as varchar) as location,
    cast(location_country as varchar) as location_country,
    cast(workplace_type as json) as workplace_type,
    cast(is_remote as boolean) as is_remote,
    cast(posted_at as timestamp with time zone) as posted_at,
    cast(job_url as varchar) as job_url,
    cast(salary_min as bigint) as salary_min,
    cast(salary_max as bigint) as salary_max,
    cast(description as varchar) as description,
    cast(_batch_id as varchar) as _batch_id,
    cast(_load_id as varchar) as _load_id,
    cast(_source_file as varchar) as _source_file,
    cast(_file_row_num as bigint) as _file_row_num,
    cast(source_loaded_at as timestamp with time zone) as source_loaded_at,
    cast(_content_hash as varchar) as _content_hash
from deduplicated
