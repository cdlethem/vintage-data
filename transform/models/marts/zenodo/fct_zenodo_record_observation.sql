{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['twice_hourly']
) }}

with source_rows as (
    select
        md5(to_json(struct_pack(
            source_name := cast(source as varchar),
            record_id := cast(id as varchar),
            observed_at := cast(fetched_at as timestamp with time zone)
        )))::varchar as zenodo_record_observation_key,
        cast(source as varchar) as source_name,
        cast(id as varchar) as record_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(doi as varchar) as doi,
        cast(concept_doi as varchar) as concept_doi,
        cast(title as varchar) as title,
        cast(publication_date as varchar) as publication_date,
        cast(created as timestamp with time zone) as created_at,
        cast(resource_type as varchar) as resource_type,
        cast(access_right as varchar) as access_right,
        cast(license as varchar) as license,
        cast(creators as json) as creators,
        cast(keywords as json) as keywords,
        cast(_batch_id as varchar) as _batch_id,
        cast(_load_id as varchar) as _load_id,
        cast(_source_file as varchar) as _source_file,
        cast(_file_row_num as bigint) as _file_row_num,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as _content_hash
    from {{ ref('base_zenodo') }}
),

deduplicated as (
    select *
    from source_rows
    qualify row_number() over (
        partition by zenodo_record_observation_key
        order by source_loaded_at desc, _source_file desc, _file_row_num desc
    ) = 1
)

select
    zenodo_record_observation_key,
    source_name,
    record_id,
    observed_at,
    doi,
    concept_doi,
    title,
    publication_date,
    created_at,
    resource_type,
    access_right,
    license,
    creators,
    keywords,
    _batch_id,
    _load_id,
    _source_file,
    _file_row_num,
    source_loaded_at,
    _content_hash
from deduplicated
