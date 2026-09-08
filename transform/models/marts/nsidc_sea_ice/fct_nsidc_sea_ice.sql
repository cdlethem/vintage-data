{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['daily']
) }}

with ranked_observations as (
    select
        md5(to_json(struct_pack(
            source_name := cast(source as varchar),
            sea_ice_id := cast(id as varchar),
            observation_date := cast(date as date)
        ))) as nsidc_sea_ice_observation_key,
        cast(source as varchar) as source_name,
        cast(id as varchar) as sea_ice_id,
        cast(fetched_at as timestamp with time zone) as fetched_at,
        cast(date as date) as observation_date,
        cast(extent_million_km2 as double) as extent_million_km2,
        cast(missing_million_km2 as double) as missing_million_km2,
        cast(_row_id as varchar) as _row_id,
        cast(_batch_id as varchar) as _batch_id,
        cast(_source_file as varchar) as _source_file,
        cast(_file_row_num as bigint) as _file_row_num,
        cast(_dt as date) as _dt,
        cast(_extract_started_at as timestamp with time zone) as _extract_started_at,
        cast(_load_id as varchar) as _load_id,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as _content_hash,
        row_number() over (
            partition by source, id, date
            order by _loaded_at desc, _source_file desc, _file_row_num desc, _row_id desc
        ) as observation_rank
    from {{ ref('base_nsidc_sea_ice') }}
)

select
    nsidc_sea_ice_observation_key,
    source_name,
    sea_ice_id,
    fetched_at,
    observation_date,
    extent_million_km2,
    missing_million_km2,
    _row_id,
    _batch_id,
    _source_file,
    _file_row_num,
    _dt,
    _extract_started_at,
    _load_id,
    source_loaded_at,
    _content_hash
from ranked_observations
where observation_rank = 1
