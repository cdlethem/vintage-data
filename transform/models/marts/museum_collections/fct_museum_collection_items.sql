{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['daily']
) }}

with ranked as (
    select
        md5(to_json(struct_pack(
            source := source,
            id := id,
            fetched_at := fetched_at
        ))) as museum_collection_item_key,
        cast(source as varchar) as source,
        cast(id as varchar) as id,
        cast(fetched_at as timestamp with time zone) as fetched_at,
        cast(title as varchar) as title,
        cast(artist as varchar) as artist,
        cast(origin as varchar) as origin,
        cast(date_display as varchar) as date_display,
        cast(date_start as bigint) as date_start,
        cast(date_end as bigint) as date_end,
        cast(medium as varchar) as medium,
        cast(classification as varchar) as classification,
        cast(department as varchar) as department,
        cast(public_domain as boolean) as public_domain,
        cast(on_view as boolean) as on_view,
        cast(gallery as varchar) as gallery,
        cast(updated_at as timestamp with time zone) as updated_at,
        cast(_batch_id as varchar) as _batch_id,
        cast(_load_id as varchar) as _load_id,
        cast(_source_file as varchar) as _source_file,
        cast(_file_row_num as bigint) as _file_row_num,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as _content_hash,
        row_number() over (
            partition by source, id, fetched_at
            order by _loaded_at desc, _source_file desc, _file_row_num desc
        ) as _dedupe_rank
    from {{ ref('base_museum_collections') }}
)

select
    museum_collection_item_key,
    source,
    id,
    fetched_at,
    title,
    artist,
    origin,
    date_display,
    date_start,
    date_end,
    medium,
    classification,
    department,
    public_domain,
    on_view,
    gallery,
    updated_at,
    _batch_id,
    _load_id,
    _source_file,
    _file_row_num,
    source_loaded_at,
    _content_hash
from ranked
where _dedupe_rank = 1
