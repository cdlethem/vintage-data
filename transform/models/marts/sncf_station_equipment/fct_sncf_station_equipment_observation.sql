{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['twice_hourly']
) }}

with ranked_equipment as (
    select
        md5(to_json(struct_pack(
            source_name := cast(source as varchar),
            equipment_id := cast(id as varchar),
            observed_at := cast(fetched_at as timestamp with time zone)
        ))) as sncf_station_equipment_key,
        cast(source as varchar) as source_name,
        cast(id as varchar) as equipment_id,
        cast(gareid as varchar) as station_id,
        cast(libelle as varchar) as equipment_name,
        cast(position_geographique as varchar) as position_geographique,
        try_cast(trim(split_part(position_geographique, ',', 1)) as double) as latitude,
        try_cast(trim(split_part(position_geographique, ',', 2)) as double) as longitude,
        cast(localisationdescriptive as varchar) as location_description,
        cast(etat_de_fonctionnement as varchar) as operating_status,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(_row_id as varchar) as source_row_id,
        cast(_batch_id as varchar) as source_batch_id,
        cast(_source_file as varchar) as source_file,
        cast(_file_row_num as bigint) as source_file_row_number,
        cast(_dt as date) as source_date,
        cast(_extract_started_at as timestamp with time zone) as extract_started_at,
        cast(_load_id as varchar) as load_id,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as content_hash,
        row_number() over (
            partition by source, id, fetched_at
            order by _loaded_at desc, _source_file desc, _file_row_num desc, _row_id desc
        ) as equipment_rank
    from {{ ref('base_sncf_station_equipment') }}
)

select
    sncf_station_equipment_key,
    source_name,
    equipment_id,
    station_id,
    equipment_name,
    position_geographique,
    latitude,
    longitude,
    location_description,
    operating_status,
    observed_at,
    source_row_id,
    source_batch_id,
    source_file,
    source_file_row_number,
    source_date,
    extract_started_at,
    load_id,
    source_loaded_at,
    content_hash
from ranked_equipment
where equipment_rank = 1
