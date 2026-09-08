{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['daily']
) }}

select
    md5(to_json(struct_pack(
        job_id := cast(id as varchar),
        observed_at := cast(fetched_at as timestamp with time zone)
    ))) as workingnomads_job_key,
    cast(id as varchar) as job_id,
    cast(fetched_at as timestamp with time zone) as observed_at,
    cast(source as varchar) as source_name,
    cast(provider as varchar) as provider,
    cast(company as varchar) as company,
    cast(title as varchar) as title,
    cast(department as varchar) as department,
    cast(location as varchar) as location,
    cast(location_country as varchar) as location_country,
    cast(workplace_type as varchar) as workplace_type,
    cast(is_remote as boolean) as is_remote,
    cast(posted as timestamp with time zone) as posted_at,
    cast(url as varchar) as job_url,
    cast(description as varchar) as description,
    cast(_source_file as varchar) as _source_file,
    cast(_file_row_num as bigint) as _file_row_num,
    cast(_loaded_at as timestamp with time zone) as source_loaded_at,
    cast(_content_hash as varchar) as _content_hash
from {{ ref('base_job_feed_workingnomads') }}
