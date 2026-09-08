{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='package_release_observation_key',
    on_schema_change='fail',
    tags=['twice_hourly']
) }}

with source_rows as (
    select
        cast(_source as varchar) as source_relation,
        cast(source as varchar) as source_name,
        cast(id as varchar) as release_id,
        cast(package as varchar) as package_name,
        cast(version as varchar) as package_version,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(summary as varchar) as summary,
        cast(author as varchar) as author,
        try_strptime(published, '%a, %d %b %Y %H:%M:%S GMT') at time zone 'UTC' as published_at,
        cast(url as varchar) as release_url,
        cast(_row_id as varchar) as source_row_id,
        cast(_batch_id as varchar) as source_batch_id,
        cast(_source_file as varchar) as source_file,
        cast(_file_row_num as bigint) as source_file_row_number,
        cast(_dt as date) as source_date,
        cast(_extract_started_at as timestamp with time zone) as extract_started_at,
        cast(_load_id as varchar) as load_id,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as content_hash
    from {{ ref('base_package_registries') }}
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
            source_name := source_name,
            package_name := package_name,
            package_version := package_version,
            observed_at := observed_at
        ))) as package_release_observation_key,
        *,
        row_number() over (
            partition by source_name, package_name, package_version, observed_at
            order by source_loaded_at desc, source_file desc, source_file_row_number desc, source_row_id desc
        ) as _dedupe_rank
    from source_rows
)

select
    cast(package_release_observation_key as varchar) as package_release_observation_key,
    cast(source_relation as varchar) as source_relation,
    cast(source_name as varchar) as source_name,
    cast(release_id as varchar) as release_id,
    cast(package_name as varchar) as package_name,
    cast(package_version as varchar) as package_version,
    cast(observed_at as timestamp with time zone) as observed_at,
    cast(summary as varchar) as summary,
    cast(author as varchar) as author,
    cast(published_at as timestamp with time zone) as published_at,
    cast(release_url as varchar) as release_url,
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
