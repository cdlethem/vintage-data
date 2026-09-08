{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='gdelt_document_observation_key',
    on_schema_change='fail',
    tags=['hourly']
) }}

with source_rows as (
    select
        cast(source as varchar) as source_system,
        cast(id as varchar) as document_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(try_strptime(cast(published as varchar), '%Y%m%dT%H%M%SZ') as timestamp with time zone) as published_at,
        cast(title as varchar) as title,
        cast(domain as varchar) as domain,
        cast(language as varchar) as language,
        cast(sourcecountry as varchar) as source_country,
        cast(url as varchar) as document_url,
        cast(_row_id as varchar) as source_row_id,
        cast(_batch_id as varchar) as source_batch_id,
        cast(_source_file as varchar) as source_file,
        cast(_file_row_num as bigint) as source_file_row_number,
        cast(_dt as date) as source_date,
        cast(_extract_started_at as timestamp with time zone) as extract_started_at,
        cast(_load_id as varchar) as load_id,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as content_hash
    from {{ ref('base_gdelt_doc') }}
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
            document_id := document_id,
            observed_at := observed_at
        ))) as gdelt_document_observation_key,
        *,
        row_number() over (
            partition by document_id, observed_at
            order by source_loaded_at desc, source_file desc, source_file_row_number desc, source_row_id desc
        ) as _dedupe_rank
    from source_rows
)

select
    cast(gdelt_document_observation_key as varchar) as gdelt_document_observation_key,
    cast(source_system as varchar) as source_system,
    cast(document_id as varchar) as document_id,
    cast(observed_at as timestamp with time zone) as observed_at,
    cast(published_at as timestamp with time zone) as published_at,
    cast(title as varchar) as title,
    cast(domain as varchar) as domain,
    cast(language as varchar) as language,
    cast(source_country as varchar) as source_country,
    cast(document_url as varchar) as document_url,
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
