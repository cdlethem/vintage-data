{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['twice_hourly']
) }}

with source_rows as (
    select
        cast(_row_id as varchar) as _row_id,
        cast(_source as varchar) as _source,
        cast(_batch_id as varchar) as _batch_id,
        cast(_source_file as varchar) as _source_file,
        cast(_file_row_num as bigint) as _file_row_num,
        cast(_dt as date) as _dt,
        cast(_extract_started_at as timestamp with time zone) as _extract_started_at,
        cast(_load_id as varchar) as _load_id,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as _content_hash,
        cast(source as varchar) as source,
        cast(id as varchar) as alert_id,
        cast(fetched_at as timestamp with time zone) as fetched_at,
        cast(incident_id as bigint) as incident_id,
        cast(bathing_water_incident_id as varchar) as bathing_water_incident_id,
        cast(beach_id as varchar) as beach_id,
        cast(beach_name as varchar) as beach_name,
        cast(county_name as varchar) as county_name,
        cast(local_authority_name as varchar) as local_authority_name,
        cast(has_bathing_restriction_in_place as varchar) as restriction_in_place,
        cast(incident_start_date as timestamp with time zone) as incident_start_date,
        cast(incident_expected_duration as bigint) as incident_expected_duration_days,
        cast(bathing_restriction_type as varchar) as restriction_type,
        cast(incident_description as varchar) as incident_description,
        cast(bathing_notice_pdf as varchar) as bathing_notice_url,
        cast(last_updated as timestamp with time zone) as last_updated,
        md5(to_json(struct_pack(
            source := cast(source as varchar),
            alert_id := cast(id as varchar),
            fetched_at := cast(fetched_at as timestamp with time zone)
        ))) as ireland_bathing_alert_key
    from {{ ref('base_ireland_bathing_alerts') }}
),

deduplicated as (
    select *
    from source_rows
    qualify row_number() over (
        partition by ireland_bathing_alert_key
        order by source_loaded_at desc, _source_file desc, _file_row_num desc, _row_id desc
    ) = 1
)

select
    cast(ireland_bathing_alert_key as varchar) as ireland_bathing_alert_key,
    cast(source as varchar) as source,
    cast(alert_id as varchar) as alert_id,
    cast(fetched_at as timestamp with time zone) as fetched_at,
    cast(incident_id as bigint) as incident_id,
    cast(bathing_water_incident_id as varchar) as bathing_water_incident_id,
    cast(beach_id as varchar) as beach_id,
    cast(beach_name as varchar) as beach_name,
    cast(county_name as varchar) as county_name,
    cast(local_authority_name as varchar) as local_authority_name,
    cast(restriction_in_place as varchar) as restriction_in_place,
    cast(incident_start_date as timestamp with time zone) as incident_start_date,
    cast(incident_expected_duration_days as bigint) as incident_expected_duration_days,
    cast(restriction_type as varchar) as restriction_type,
    cast(incident_description as varchar) as incident_description,
    cast(bathing_notice_url as varchar) as bathing_notice_url,
    cast(last_updated as timestamp with time zone) as last_updated,
    cast(_row_id as varchar) as _row_id,
    cast(_source as varchar) as _source,
    cast(_batch_id as varchar) as _batch_id,
    cast(_source_file as varchar) as _source_file,
    cast(_file_row_num as bigint) as _file_row_num,
    cast(_dt as date) as _dt,
    cast(_extract_started_at as timestamp with time zone) as _extract_started_at,
    cast(_load_id as varchar) as _load_id,
    cast(source_loaded_at as timestamp with time zone) as source_loaded_at,
    cast(_content_hash as varchar) as _content_hash
from deduplicated
