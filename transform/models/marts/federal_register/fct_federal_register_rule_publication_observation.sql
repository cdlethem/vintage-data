{{ config(
    materialized='table',
    tags=['daily']
) }}

with source_rows as (
    select
        md5(
            to_json(
                struct_pack(
                    source_name := cast(source as varchar),
                    document_id := cast(id as varchar),
                    observed_at := cast(fetched_at as timestamp with time zone)
                )
            )
        )::varchar as federal_register_rule_publication_observation_key,
        cast(source as varchar) as source_name,
        cast(id as varchar) as document_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(published as date) as publication_date,
        cast(doc_type as varchar) as document_type,
        cast(title as varchar) as title,
        cast(abstract as varchar) as abstract,
        cast(agencies as json) as agencies,
        cast(url as varchar) as document_url,
        cast(text_url as varchar) as text_url,
        cast(public_inspection as boolean) as public_inspection,
        cast(_batch_id as varchar) as _batch_id,
        cast(_load_id as varchar) as _load_id,
        cast(_source_file as varchar) as _source_file,
        cast(_file_row_num as bigint) as _file_row_num,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as _content_hash
    from {{ ref('base_federal_register') }}
),

deduplicated as (
    select *
    from source_rows
    qualify row_number() over (
        partition by federal_register_rule_publication_observation_key
        order by source_loaded_at desc, _source_file desc, _file_row_num desc
    ) = 1
)

select
    federal_register_rule_publication_observation_key,
    source_name,
    document_id,
    observed_at,
    publication_date,
    document_type,
    title,
    abstract,
    agencies,
    document_url,
    text_url,
    public_inspection,
    _batch_id,
    _load_id,
    _source_file,
    _file_row_num,
    source_loaded_at,
    _content_hash
from deduplicated
