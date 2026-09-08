{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['twice_hourly']
) }}

with ranked_observations as (
    select
        md5(to_json(struct_pack(
            source_name := cast(source as varchar),
            flood_id := cast(id as varchar),
            fetched_at := cast(fetched_at as timestamp with time zone)
        ))) as environment_agency_flood_key,
        cast(source as varchar) as source_name,
        cast(id as varchar) as flood_id,
        cast(fetched_at as timestamp with time zone) as fetched_at,
        cast(description as varchar) as description,
        cast(eaareaname as varchar) as area_name,
        cast(earegionname as varchar) as region_name,
        cast(floodareaid as varchar) as flood_area_id,
        cast(floodarea as json) as flood_area,
        cast(istidal as boolean) as is_tidal,
        cast(message as varchar) as message,
        cast(severity as varchar) as severity,
        cast(severitylevel as bigint) as severity_level,
        cast(timemessagechanged as timestamp with time zone) as message_changed_at,
        cast(timeraised as timestamp with time zone) as raised_at,
        cast(timeseveritychanged as timestamp with time zone) as severity_changed_at,
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
    from {{ ref('base_environment_agency_floods') }}
)

select
    environment_agency_flood_key,
    source_name,
    flood_id,
    fetched_at,
    description,
    area_name,
    region_name,
    flood_area_id,
    flood_area,
    is_tidal,
    message,
    severity,
    severity_level,
    message_changed_at,
    raised_at,
    severity_changed_at,
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
