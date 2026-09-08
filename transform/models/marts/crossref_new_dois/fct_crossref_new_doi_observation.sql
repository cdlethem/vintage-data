{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['hourly']
) }}

with source_rows as (
    select
        md5(to_json(struct_pack(
            source_name := cast(source as varchar),
            doi_id := cast(id as varchar),
            observed_at := cast(fetched_at as timestamp with time zone)
        )))::varchar as crossref_new_doi_observation_key,
        cast(source as varchar) as source_name,
        cast(id as varchar) as doi_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(created as timestamp with time zone) as created_at,
        cast(type as varchar) as work_type,
        cast(title as varchar) as title,
        cast(journal as varchar) as journal,
        cast(publisher as varchar) as publisher,
        cast(subjects as json) as subjects,
        cast(n_authors as bigint) as author_count,
        cast(has_abstract as boolean) as has_abstract,
        cast(abstract as varchar) as abstract,
        cast(_batch_id as varchar) as _batch_id,
        cast(_load_id as varchar) as _load_id,
        cast(_source_file as varchar) as _source_file,
        cast(_file_row_num as bigint) as _file_row_num,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as _content_hash
    from {{ ref('base_crossref_new_dois') }}
),

deduplicated as (
    select *
    from source_rows
    qualify row_number() over (
        partition by crossref_new_doi_observation_key
        order by source_loaded_at desc, _source_file desc, _file_row_num desc
    ) = 1
)

select
    crossref_new_doi_observation_key,
    source_name,
    doi_id,
    observed_at,
    created_at,
    work_type,
    title,
    journal,
    publisher,
    subjects,
    author_count,
    has_abstract,
    abstract,
    _batch_id,
    _load_id,
    _source_file,
    _file_row_num,
    source_loaded_at,
    _content_hash
from deduplicated
