{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['daily']
) }}

with ranked_observations as (
    select
        cast(md5(to_json(struct_pack(
            source_name := cast(source as varchar),
            reservoir_level_id := cast(id as varchar),
            observed_at := cast(fetched_at as timestamp with time zone)
        ))) as varchar) as reservoir_level_observation_key,
        cast(source as varchar) as source_name,
        cast(id as varchar) as reservoir_level_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(station as varchar) as station,
        cast(sensor_num as bigint) as sensor_num,
        cast(measure as varchar) as measure,
        cast(sensor_type as varchar) as sensor_type,
        cast(dur_code as varchar) as duration_code,
        cast(obs_date as timestamp with time zone) as observation_start_at,
        cast(obs_end as timestamp with time zone) as observation_end_at,
        cast(value as bigint) as measurement_value,
        cast(units as varchar) as units,
        cast(is_missing as boolean) as is_missing,
        cast(is_estimated as boolean) as is_estimated,
        cast(is_revised as boolean) as is_revised,
        cast(capacity_af as bigint) as capacity_af,
        cast(pct_of_capacity as double) as pct_of_capacity,
        cast(_row_id as varchar) as _row_id,
        cast(_batch_id as varchar) as _batch_id,
        cast(_load_id as varchar) as _load_id,
        cast(_source_file as varchar) as _source_file,
        cast(_file_row_num as bigint) as _file_row_num,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as _content_hash,
        row_number() over (
            partition by source, id, fetched_at
            order by _loaded_at desc, _source_file desc, _file_row_num desc, _row_id desc
        ) as observation_rank
    from {{ ref('base_reservoir_levels') }}
)

select
    reservoir_level_observation_key,
    source_name,
    reservoir_level_id,
    observed_at,
    station,
    sensor_num,
    measure,
    sensor_type,
    duration_code,
    observation_start_at,
    observation_end_at,
    measurement_value,
    units,
    is_missing,
    is_estimated,
    is_revised,
    capacity_af,
    pct_of_capacity,
    _row_id,
    _batch_id,
    _load_id,
    _source_file,
    _file_row_num,
    source_loaded_at,
    _content_hash
from ranked_observations
where observation_rank = 1
