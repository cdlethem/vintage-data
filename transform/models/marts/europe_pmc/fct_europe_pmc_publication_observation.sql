{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['daily']
) }}

with source_rows as (
    select
        md5(to_json(struct_pack(
            source_name := cast(source as varchar),
            article_id := cast(id as varchar),
            observed_at := cast(fetched_at as timestamp with time zone)
        )))::varchar as europe_pmc_publication_observation_key,
        cast(source as varchar) as source_name,
        cast(id as varchar) as article_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(publication_source as varchar) as publication_source,
        cast(publication_id as varchar) as publication_id,
        cast(pmid as varchar) as pmid,
        cast(pmcid as varchar) as pmcid,
        cast(doi as varchar) as doi,
        cast(title as varchar) as title,
        cast(abstract as varchar) as abstract,
        cast(author_string as varchar) as author_string,
        cast(authors as json) as authors,
        cast(journal as json) as journal,
        cast(publication_year as varchar) as publication_year,
        cast(first_publication_date as date) as first_publication_date,
        cast(first_index_date as date) as first_index_date,
        cast(publication_status as varchar) as publication_status,
        cast(publication_types as json) as publication_types,
        cast(language as varchar) as language,
        cast(keywords as json) as keywords,
        cast(mesh_headings as json) as mesh_headings,
        cast(grants as json) as grants,
        cast(cited_by_count as bigint) as cited_by_count,
        cast(is_open_access as boolean) as is_open_access,
        cast(has_pdf as boolean) as has_pdf,
        cast(in_epmc as boolean) as in_epmc,
        cast(in_pmc as boolean) as in_pmc,
        cast(source_url as varchar) as source_url,
        cast(full_text_links as json) as full_text_links,
        cast(annotations as json) as annotations,
        cast(retrieval as json) as retrieval,
        cast(_batch_id as varchar) as _batch_id,
        cast(_load_id as varchar) as _load_id,
        cast(_source_file as varchar) as _source_file,
        cast(_file_row_num as bigint) as _file_row_num,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as _content_hash
    from {{ ref('base_europe_pmc') }}
),

deduplicated as (
    select *
    from source_rows
    qualify row_number() over (
        partition by europe_pmc_publication_observation_key
        order by source_loaded_at desc, _source_file desc, _file_row_num desc
    ) = 1
)

select
    europe_pmc_publication_observation_key,
    source_name,
    article_id,
    observed_at,
    publication_source,
    publication_id,
    pmid,
    pmcid,
    doi,
    title,
    abstract,
    author_string,
    authors,
    journal,
    publication_year,
    first_publication_date,
    first_index_date,
    publication_status,
    publication_types,
    language,
    keywords,
    mesh_headings,
    grants,
    cited_by_count,
    is_open_access,
    has_pdf,
    in_epmc,
    in_pmc,
    source_url,
    full_text_links,
    annotations,
    retrieval,
    _batch_id,
    _load_id,
    _source_file,
    _file_row_num,
    source_loaded_at,
    _content_hash
from deduplicated
