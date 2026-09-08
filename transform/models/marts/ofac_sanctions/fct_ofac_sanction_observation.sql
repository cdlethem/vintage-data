{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['daily']
) }}

-- The daily source is a complete snapshot; a physical table keeps the searchable
-- observation history available without rescanning the raw snapshot on every query.
with source_rows as (
    select
        cast(_source as varchar) as source_relation,
        cast(source as varchar) as source_name,
        cast(_row_id as varchar) as source_row_id,
        cast(id as varchar) as sanction_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(name as varchar) as entity_name,
        cast(entity_type as varchar) as entity_type,
        cast(title as varchar) as title,
        cast(remarks as varchar) as remarks,
        cast(programs as json) as programs,
        cast(aliases as json) as aliases,
        cast(addresses as json) as addresses,
        cast(ids as json) as identifiers,
        cast(_batch_id as varchar) as _batch_id,
        cast(_load_id as varchar) as _load_id,
        cast(_source_file as varchar) as _source_file,
        cast(_file_row_num as bigint) as _file_row_num,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as _content_hash,
        cast(md5(to_json(struct_pack(
            source_name := cast(source as varchar),
            sanction_id := cast(id as varchar),
            observed_at := cast(fetched_at as timestamp with time zone)
        ))) as varchar) as ofac_sanction_observation_key
    from {{ ref('base_ofac_sanctions') }}
),

deduplicated as (
    select *
    from source_rows
    qualify row_number() over (
        partition by source_name, sanction_id, observed_at
        order by source_loaded_at desc, _source_file desc, _file_row_num desc, source_row_id desc
    ) = 1
)

select
    cast(ofac_sanction_observation_key as varchar) as ofac_sanction_observation_key,
    cast(source_relation as varchar) as source_relation,
    cast(source_name as varchar) as source_name,
    cast(source_row_id as varchar) as source_row_id,
    cast(sanction_id as varchar) as sanction_id,
    cast(observed_at as timestamp with time zone) as observed_at,
    cast(entity_name as varchar) as entity_name,
    cast(entity_type as varchar) as entity_type,
    cast(title as varchar) as title,
    cast(remarks as varchar) as remarks,
    cast(programs as json) as programs,
    cast(aliases as json) as aliases,
    cast(addresses as json) as addresses,
    cast(identifiers as json) as identifiers,
    cast(_batch_id as varchar) as _batch_id,
    cast(_load_id as varchar) as _load_id,
    cast(_source_file as varchar) as _source_file,
    cast(_file_row_num as bigint) as _file_row_num,
    cast(source_loaded_at as timestamp with time zone) as source_loaded_at,
    cast(_content_hash as varchar) as _content_hash
from deduplicated
