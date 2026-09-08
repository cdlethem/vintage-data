{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['daily']
) }}

with ranked as (
    select
        md5(to_json(struct_pack(
            notice_id := cast(id as varchar),
            observed_at := cast(fetched_at as timestamp with time zone)
        ))) as ted_procurement_key,
        cast(id as varchar) as notice_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(source as varchar) as source_name,
        cast(publication_number as varchar) as publication_number,
        cast(publication_date as date) as publication_date,
        cast(publication_utc_offset as varchar) as publication_utc_offset,
        cast(deadline as json) as deadline,
        cast(notice_type as varchar) as notice_type,
        cast(title_en as varchar) as title_en,
        cast(n_title_languages as bigint) as n_title_languages,
        cast(buyer_name as varchar) as buyer_name,
        cast(buyer_name_lang as varchar) as buyer_name_lang,
        cast(buyer_country as json) as buyer_country,
        cast(contract_nature as json) as contract_nature,
        cast(cpv_codes as json) as cpv_codes,
        cast(total_value as double) as total_value,
        cast(place_of_performance as json) as place_of_performance,
        cast(xml_url as varchar) as xml_url,
        cast(_batch_id as varchar) as _batch_id,
        cast(_load_id as varchar) as _load_id,
        cast(_source_file as varchar) as _source_file,
        cast(_file_row_num as bigint) as _file_row_num,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as _content_hash,
        row_number() over (
            partition by id, fetched_at
            order by _loaded_at desc, _source_file desc, _file_row_num desc
        ) as _dedupe_rank
    from {{ ref('base_ted_procurement') }}
)

select
    ted_procurement_key,
    notice_id,
    observed_at,
    source_name,
    publication_number,
    publication_date,
    publication_utc_offset,
    deadline,
    notice_type,
    title_en,
    n_title_languages,
    buyer_name,
    buyer_name_lang,
    buyer_country,
    contract_nature,
    cpv_codes,
    total_value,
    place_of_performance,
    xml_url,
    _batch_id,
    _load_id,
    _source_file,
    _file_row_num,
    source_loaded_at,
    _content_hash
from ranked
where _dedupe_rank = 1
