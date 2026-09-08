{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['daily']
) }}

with source_rows as (
    select
        md5(to_json(struct_pack(
            source := cast(source as varchar),
            compound_id := cast(id as varchar),
            fetched_at := cast(fetched_at as timestamp with time zone),
            query := cast(query as varchar)
        ))) as pubchem_compound_snapshot_key,
        cast(source as varchar) as source,
        cast(id as varchar) as compound_id,
        cast(fetched_at as timestamp with time zone) as fetched_at,
        cast(query as varchar) as query,
        cast(found as boolean) as found,
        cast(molecular_formula as varchar) as molecular_formula,
        try_cast(molecular_weight as decimal(18, 6)) as molecular_weight,
        cast(iupac_name as varchar) as iupac_name,
        cast(smiles as varchar) as smiles,
        cast(_batch_id as varchar) as _batch_id,
        cast(_load_id as varchar) as _load_id,
        cast(_source_file as varchar) as _source_file,
        cast(_file_row_num as bigint) as _file_row_num,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as _content_hash,
        row_number() over (
            partition by source, id, fetched_at, query
            order by _loaded_at desc, _source_file desc, _file_row_num desc
        ) as _dedupe_rank
    from {{ ref('base_pubchem') }}
)

select
    pubchem_compound_snapshot_key,
    source,
    compound_id,
    fetched_at,
    query,
    found,
    molecular_formula,
    molecular_weight,
    iupac_name,
    smiles,
    _batch_id,
    _load_id,
    _source_file,
    _file_row_num,
    source_loaded_at,
    _content_hash
from source_rows
where _dedupe_rank = 1
