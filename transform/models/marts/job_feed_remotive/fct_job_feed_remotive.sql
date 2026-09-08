{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['daily']
) }}

with source_rows as (
    select
        md5(to_json(struct_pack(
            source_name := cast(source as varchar),
            job_id := cast(id as varchar),
            observed_at := cast(fetched_at as timestamp with time zone)
        ))) as job_observation_key,
        cast(source as varchar) as source_name,
        cast(id as varchar) as job_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(provider as varchar) as provider,
        cast(company as varchar) as company,
        cast(title as varchar) as title,
        cast(department as varchar) as department,
        cast(employment_type as varchar) as employment_type,
        cast(location as varchar) as location,
        cast(location_country as varchar) as location_country,
        cast(workplace_type as varchar) as workplace_type,
        cast(is_remote as boolean) as is_remote,
        cast(posted as timestamp with time zone) as posted,
        cast(url as varchar) as url,
        cast(salary_text as varchar) as salary_text,
        cast(description as varchar) as description,
        cast(_batch_id as varchar) as _batch_id,
        cast(_load_id as varchar) as _load_id,
        cast(_source_file as varchar) as _source_file,
        cast(_file_row_num as bigint) as _file_row_num,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as _content_hash,
        row_number() over (
            partition by source, id, fetched_at
            order by _loaded_at desc, _source_file desc, _file_row_num desc
        ) as _dedupe_rank
    from {{ ref('base_job_feed_remotive') }}
)

select
    job_observation_key,
    source_name,
    job_id,
    observed_at,
    provider,
    company,
    title,
    department,
    employment_type,
    location,
    location_country,
    workplace_type,
    is_remote,
    posted,
    url,
    salary_text,
    description,
    _batch_id,
    _load_id,
    _source_file,
    _file_row_num,
    source_loaded_at,
    _content_hash
from source_rows
where _dedupe_rank = 1
