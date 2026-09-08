{{ config(
    materialized='table',
    tags=['daily']
) }}

with normalized as (
    select
        cast(source as varchar) as source,
        cast(id as varchar) as job_id,
        cast(fetched_at as timestamp with time zone) as fetched_at,
        cast(provider as varchar) as provider,
        cast(company as varchar) as company,
        cast(title as varchar) as title,
        cast(department as varchar) as department,
        cast(employment_type as varchar) as employment_type,
        cast(location as varchar) as location,
        cast(workplace_type as varchar) as workplace_type,
        cast(is_remote as boolean) as is_remote,
        cast(posted as timestamp with time zone) as posted_at,
        cast(url as varchar) as url,
        cast(description as varchar) as description,
        cast(_source_file as varchar) as _source_file,
        cast(_file_row_num as bigint) as _file_row_num,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as _content_hash
    from {{ ref('base_job_feed_weworkremotely') }}
),

keyed as (
    select
        cast(
            md5(
                to_json(
                    struct_pack(
                        source := source,
                        job_id := job_id,
                        fetched_at := fetched_at
                    )
                )
            ) as varchar
        ) as job_observation_key,
        source,
        job_id,
        fetched_at,
        provider,
        company,
        title,
        department,
        employment_type,
        location,
        workplace_type,
        is_remote,
        posted_at,
        url,
        description,
        _source_file,
        _file_row_num,
        source_loaded_at,
        _content_hash
    from normalized
),

deduplicated as (
    select *
    from keyed
    qualify row_number() over (
        partition by job_observation_key
        order by source_loaded_at desc, _source_file desc, _file_row_num desc
    ) = 1
)

select
    cast(job_observation_key as varchar) as job_observation_key,
    cast(source as varchar) as source,
    cast(job_id as varchar) as job_id,
    cast(fetched_at as timestamp with time zone) as fetched_at,
    cast(provider as varchar) as provider,
    cast(company as varchar) as company,
    cast(title as varchar) as title,
    cast(department as varchar) as department,
    cast(employment_type as varchar) as employment_type,
    cast(location as varchar) as location,
    cast(workplace_type as varchar) as workplace_type,
    cast(is_remote as boolean) as is_remote,
    cast(posted_at as timestamp with time zone) as posted_at,
    cast(url as varchar) as url,
    cast(description as varchar) as description,
    cast(_source_file as varchar) as _source_file,
    cast(_file_row_num as bigint) as _file_row_num,
    cast(source_loaded_at as timestamp with time zone) as source_loaded_at,
    cast(_content_hash as varchar) as _content_hash
from deduplicated
