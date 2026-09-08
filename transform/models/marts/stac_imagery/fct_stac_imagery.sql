{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['hourly']
) }}

with typed_rows as (
    select
        cast(source as varchar) as source_name,
        cast(id as varchar) as item_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(collection as varchar) as collection,
        cast(captured as timestamp with time zone) as captured_at,
        cast(age_hours as double) as age_hours,
        cast(platform as varchar) as platform,
        cast(constellation as varchar) as constellation,
        cast(instruments as json) as instruments,
        cast(cloud_cover as double) as cloud_cover,
        cast(epsg as bigint) as epsg,
        cast(grid as varchar) as grid,
        cast(bbox as json) as bbox,
        cast(asset_keys as json) as asset_keys,
        cast(visual_href as varchar) as visual_href,
        cast(thumbnail_href as varchar) as thumbnail_href,
        cast(requester_pays as boolean) as requester_pays,
        cast(_row_id as varchar) as _row_id,
        cast(_batch_id as varchar) as _batch_id,
        cast(_source_file as varchar) as _source_file,
        cast(_file_row_num as bigint) as _file_row_num,
        cast(_dt as date) as _dt,
        cast(_extract_started_at as timestamp with time zone) as _extract_started_at,
        cast(_load_id as varchar) as _load_id,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as _content_hash
    from {{ ref('base_stac_imagery') }}
), keyed_rows as (
    select
        md5(to_json(struct_pack(
            source_name := source_name,
            item_id := item_id,
            observed_at := observed_at
        ))) as stac_imagery_snapshot_key,
        *,
        row_number() over (
            partition by source_name, item_id, observed_at
            order by source_loaded_at desc, _source_file desc, _file_row_num desc, _row_id desc
        ) as _dedupe_rank
    from typed_rows
)

select
    stac_imagery_snapshot_key,
    source_name,
    item_id,
    observed_at,
    collection,
    captured_at,
    age_hours,
    platform,
    constellation,
    instruments,
    cloud_cover,
    epsg,
    grid,
    bbox,
    asset_keys,
    visual_href,
    thumbnail_href,
    requester_pays,
    _row_id,
    _batch_id,
    _source_file,
    _file_row_num,
    _dt,
    _extract_started_at,
    _load_id,
    source_loaded_at,
    _content_hash
from keyed_rows
where _dedupe_rank = 1
