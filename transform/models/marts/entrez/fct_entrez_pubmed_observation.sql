{{ config(
    enabled=var('enable_entrez_models', false),
    materialized='table',
    on_schema_change='fail',
    tags=['daily']
) }}

with source_rows as (
    select
        md5(to_json(struct_pack(
            source_name := cast(source as varchar),
            pmid := cast(id as varchar),
            observed_at := cast(fetched_at as timestamp with time zone)
        ))) as pubmed_observation_key,
        cast(source as varchar) as source_name,
        cast(id as varchar) as pmid,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(db as varchar) as database_name,
        cast(term as varchar) as search_term,
        cast(title as varchar) as title,
        cast(abstract as varchar) as abstract,
        cast(journal as varchar) as journal,
        cast(publication_date as varchar) as publication_date,
        cast(doi as varchar) as doi,
        cast(authors as json) as authors,
        cast(publication_types as json) as publication_types,
        cast(raw_payload as varchar) as raw_payload,
        cast(_batch_id as varchar) as _batch_id,
        cast(_load_id as varchar) as _load_id,
        cast(_source_file as varchar) as _source_file,
        cast(_file_row_num as bigint) as _file_row_num,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as _content_hash
    from {{ ref('base_entrez') }}
),

deduplicated as (
    select *
    from source_rows
    qualify row_number() over (
        partition by pubmed_observation_key
        order by source_loaded_at desc, _source_file desc, _file_row_num desc
    ) = 1
)

select
    pubmed_observation_key,
    source_name,
    pmid,
    observed_at,
    database_name,
    search_term,
    title,
    abstract,
    journal,
    publication_date,
    doi,
    authors,
    publication_types,
    raw_payload,
    _batch_id,
    _load_id,
    _source_file,
    _file_row_num,
    source_loaded_at,
    _content_hash
from deduplicated
