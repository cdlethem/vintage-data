{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['twice_hourly']
) }}

with ranked_observations as (
    select
        md5(to_json(struct_pack(
            source := cast(source as varchar),
            observation_id := cast(id as varchar),
            fetched_at := cast(fetched_at as timestamp with time zone)
        ))) as amsat_status_observation_key,
        cast(source as varchar) as source,
        cast(id as varchar) as observation_id,
        cast(fetched_at as timestamp with time zone) as fetched_at,
        cast(name as varchar) as satellite_name,
        cast(satellite_display_name as varchar) as satellite_display_name,
        cast(reported_time as timestamp with time zone) as reported_time,
        cast(callsign as varchar) as callsign,
        cast(report as varchar) as report,
        cast(grid_square as varchar) as grid_square,
        cast(period as bigint) as period,
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
            partition by source, id, fetched_at
            order by _loaded_at desc, _source_file desc, _file_row_num desc, _row_id desc
        ) as observation_rank
    from {{ ref('base_amsat_status') }}
)

select
    amsat_status_observation_key,
    source,
    observation_id,
    fetched_at,
    satellite_name,
    satellite_display_name,
    reported_time,
    callsign,
    report,
    grid_square,
    period,
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
