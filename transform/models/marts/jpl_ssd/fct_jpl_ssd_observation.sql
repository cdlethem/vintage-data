{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['hourly']
) }}

with source_rows as (
    select
        md5(to_json(struct_pack(
            source_name := cast(source as varchar),
            observation_id := cast(id as varchar),
            observed_at := cast(fetched_at as timestamp with time zone)
        )))::varchar as observation_key,
        cast(source as varchar) as source_name,
        cast(id as varchar) as observation_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(energy_e10j as double) as energy_e10j,
        cast(impact_energy_kt as double) as impact_energy_kt,
        cast(lat as double) as latitude_deg,
        cast(lon as double) as longitude_deg,
        cast(alt_km as double) as altitude_km,
        cast(vel_kms as double) as velocity_km_s,
        cast(_row_id as varchar) as _row_id,
        cast(_batch_id as varchar) as _batch_id,
        cast(_source_file as varchar) as _source_file,
        cast(_file_row_num as bigint) as _file_row_num,
        cast(_dt as date) as source_date,
        cast(_extract_started_at as timestamp with time zone) as extract_started_at,
        cast(_load_id as varchar) as load_id,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as _content_hash
    from {{ ref('base_jpl_ssd') }}
),

deduplicated as (
    select *
    from source_rows
    qualify row_number() over (
        partition by observation_key
        order by source_loaded_at desc, _source_file desc, _file_row_num desc, _row_id desc
    ) = 1
)

select
    observation_key,
    source_name,
    observation_id,
    observed_at,
    energy_e10j,
    impact_energy_kt,
    latitude_deg,
    longitude_deg,
    altitude_km,
    velocity_km_s,
    _row_id,
    _batch_id,
    _source_file,
    _file_row_num,
    source_date,
    extract_started_at,
    load_id,
    source_loaded_at,
    _content_hash
from deduplicated
