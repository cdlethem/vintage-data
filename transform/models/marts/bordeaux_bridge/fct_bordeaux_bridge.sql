{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['twice_hourly']
) }}

with source_rows as (
    select
        cast(source as varchar) as source,
        cast(id as varchar) as bridge_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(geo_point_2d as json) as geo_point_2d,
        cast(geo_shape as json) as geo_shape,
        cast(gid as varchar) as gid,
        cast(geom_o as varchar) as geom_o,
        cast(ident as varchar) as bridge_identifier,
        cast(nom as varchar) as bridge_name,
        cast(etat as varchar) as status_code,
        cast(ferme as boolean) as is_closed,
        cast(cdate as timestamp with time zone) as created_at,
        cast(mdate as timestamp with time zone) as updated_at,
        cast(_row_id as varchar) as _row_id,
        cast(_batch_id as varchar) as _batch_id,
        cast(_source_file as varchar) as _source_file,
        cast(_file_row_num as bigint) as _file_row_num,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as _content_hash
    from {{ ref('base_bordeaux_bridge') }}
),

keyed as (
    select
        cast(md5(to_json(struct_pack(
            source := source,
            bridge_id := bridge_id,
            observed_at := observed_at
        ))) as varchar) as bridge_status_observation_key,
        source,
        bridge_id,
        observed_at,
        geo_point_2d,
        geo_shape,
        gid,
        geom_o,
        bridge_identifier,
        bridge_name,
        status_code,
        is_closed,
        created_at,
        updated_at,
        _row_id,
        _batch_id,
        _source_file,
        _file_row_num,
        source_loaded_at,
        _content_hash
    from source_rows
),

deduplicated as (
    select *
    from keyed
    qualify row_number() over (
        partition by bridge_status_observation_key
        order by source_loaded_at desc, _source_file desc, _file_row_num desc
    ) = 1
)

select
    cast(bridge_status_observation_key as varchar) as bridge_status_observation_key,
    cast(source as varchar) as source,
    cast(bridge_id as varchar) as bridge_id,
    cast(observed_at as timestamp with time zone) as observed_at,
    cast(geo_point_2d as json) as geo_point_2d,
    cast(geo_shape as json) as geo_shape,
    cast(gid as varchar) as gid,
    cast(geom_o as varchar) as geom_o,
    cast(bridge_identifier as varchar) as bridge_identifier,
    cast(bridge_name as varchar) as bridge_name,
    cast(status_code as varchar) as status_code,
    cast(is_closed as boolean) as is_closed,
    cast(created_at as timestamp with time zone) as created_at,
    cast(updated_at as timestamp with time zone) as updated_at,
    cast(_row_id as varchar) as _row_id,
    cast(_batch_id as varchar) as _batch_id,
    cast(_source_file as varchar) as _source_file,
    cast(_file_row_num as bigint) as _file_row_num,
    cast(source_loaded_at as timestamp with time zone) as source_loaded_at,
    cast(_content_hash as varchar) as _content_hash
from deduplicated
