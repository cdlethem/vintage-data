{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['hourly']
) }}

-- SEC EDGAR is an append-only series of filing-party snapshots. Keep one row per
-- accession, party role, CIK, and extractor observation, retaining the latest
-- loaded copy when a raw batch repeats an observation.
with source_rows as (
    select
        cast(_source as varchar) as source_relation,
        cast(source as varchar) as source_name,
        cast(_row_id as varchar) as source_row_id,
        cast(id as varchar) as filing_party_id,
        cast(accession_number as varchar) as accession_number,
        cast(role as varchar) as party_role,
        cast(cik as varchar) as cik,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(form_type as varchar) as form_type,
        cast(party_name as varchar) as party_name,
        cast(filed_date as date) as filed_date,
        cast(size as varchar) as filing_size,
        cast(updated as timestamp with time zone) as updated_at,
        cast(url as varchar) as filing_url,
        cast(_batch_id as varchar) as _batch_id,
        cast(_load_id as varchar) as _load_id,
        cast(_source_file as varchar) as _source_file,
        cast(_file_row_num as bigint) as _file_row_num,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as _content_hash,
        cast(md5(to_json(struct_pack(
            accession_number := cast(accession_number as varchar),
            party_role := cast(role as varchar),
            cik := cast(cik as varchar),
            observed_at := cast(fetched_at as timestamp with time zone)
        ))) as varchar) as sec_edgar_filing_observation_key
    from {{ ref('base_sec_edgar') }}
),

deduplicated as (
    select *
    from source_rows
    qualify row_number() over (
        partition by accession_number, party_role, cik, observed_at
        order by source_loaded_at desc, _source_file desc, _file_row_num desc, source_row_id desc
    ) = 1
)

select
    cast(sec_edgar_filing_observation_key as varchar) as sec_edgar_filing_observation_key,
    cast(source_relation as varchar) as source_relation,
    cast(source_name as varchar) as source_name,
    cast(source_row_id as varchar) as source_row_id,
    cast(filing_party_id as varchar) as filing_party_id,
    cast(accession_number as varchar) as accession_number,
    cast(party_role as varchar) as party_role,
    cast(cik as varchar) as cik,
    cast(observed_at as timestamp with time zone) as observed_at,
    cast(form_type as varchar) as form_type,
    cast(party_name as varchar) as party_name,
    cast(filed_date as date) as filed_date,
    cast(filing_size as varchar) as filing_size,
    cast(updated_at as timestamp with time zone) as updated_at,
    cast(filing_url as varchar) as filing_url,
    cast(_batch_id as varchar) as _batch_id,
    cast(_load_id as varchar) as _load_id,
    cast(_source_file as varchar) as _source_file,
    cast(_file_row_num as bigint) as _file_row_num,
    cast(source_loaded_at as timestamp with time zone) as source_loaded_at,
    cast(_content_hash as varchar) as _content_hash
from deduplicated
