{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['twice_hourly']
) }}

with normalized as (
    select
        cast(source as varchar) as source,
        cast(id as varchar) as hospital_id,
        cast(facility_slug as varchar) as facility_slug,
        cast(facility_name as varchar) as facility_name,
        cast(facility_address as varchar) as facility_address,
        cast(opening_hours as varchar) as opening_hours,
        cast(publisher_updated_at as varchar) as publisher_updated_text,
        try_strptime(
            nullif(regexp_replace(publisher_updated_at, '^Last Updated: ', ''), ''),
            '%d %B %Y %I:%M:%S %p'
        ) as publisher_updated_at,
        cast(currently_closed as boolean) as currently_closed,
        cast(live_data_availability as boolean) as live_data_available,
        cast(api_code as varchar) as api_code,
        cast(wait_time_all_patients as varchar) as wait_time_all_patients_text,
        case
            when regexp_matches(trim(wait_time_all_patients), '^[0-9]+ minutes?$') then
                try_cast(regexp_extract(trim(wait_time_all_patients), '([0-9]+)', 1) as bigint)
            else null
        end as wait_time_all_patients_minutes,
        cast(wait_time_non_critical as varchar) as wait_time_non_critical_text,
        case
            when regexp_matches(trim(wait_time_non_critical), '^[0-9]+ minutes?$') then
                try_cast(regexp_extract(trim(wait_time_non_critical), '([0-9]+)', 1) as bigint)
            else null
        end as wait_time_non_critical_minutes,
        case
            when nullif(trim(patients_waiting), '') is null or trim(patients_waiting) = '-' then null
            else try_cast(trim(patients_waiting) as bigint)
        end as patients_waiting,
        cast(treatment_spaces as bigint) as treatment_spaces,
        cast(treatment_spaces_updated_date as varchar) as treatment_spaces_updated_text,
        try_strptime(nullif(trim(treatment_spaces_updated_date), ''), '%d/%m/%Y')::date
            as treatment_spaces_updated_date,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(_row_id as varchar) as _row_id,
        cast(_source_file as varchar) as _source_file,
        cast(_file_row_num as bigint) as _file_row_num,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as _content_hash
    from {{ ref('base_queensland_hospital') }}
),

keyed as (
    select
        cast(
            md5(
                to_json(
                    struct_pack(
                        source := source,
                        hospital_id := hospital_id,
                        observed_at := observed_at
                    )
                )
            ) as varchar
        ) as hospital_observation_key,
        *
    from normalized
),

deduplicated as (
    select *
    from keyed
    qualify row_number() over (
        partition by hospital_observation_key
        order by source_loaded_at desc, _source_file desc, _file_row_num desc
    ) = 1
)

select
    cast(hospital_observation_key as varchar) as hospital_observation_key,
    cast(source as varchar) as source,
    cast(hospital_id as varchar) as hospital_id,
    cast(facility_slug as varchar) as facility_slug,
    cast(facility_name as varchar) as facility_name,
    cast(facility_address as varchar) as facility_address,
    cast(opening_hours as varchar) as opening_hours,
    cast(publisher_updated_text as varchar) as publisher_updated_text,
    cast(publisher_updated_at as timestamp) as publisher_updated_at,
    cast(currently_closed as boolean) as currently_closed,
    cast(live_data_available as boolean) as live_data_available,
    cast(api_code as varchar) as api_code,
    cast(wait_time_all_patients_text as varchar) as wait_time_all_patients_text,
    cast(wait_time_all_patients_minutes as bigint) as wait_time_all_patients_minutes,
    cast(wait_time_non_critical_text as varchar) as wait_time_non_critical_text,
    cast(wait_time_non_critical_minutes as bigint) as wait_time_non_critical_minutes,
    cast(patients_waiting as bigint) as patients_waiting,
    cast(treatment_spaces as bigint) as treatment_spaces,
    cast(treatment_spaces_updated_text as varchar) as treatment_spaces_updated_text,
    cast(treatment_spaces_updated_date as date) as treatment_spaces_updated_date,
    cast(observed_at as timestamp with time zone) as observed_at,
    cast(_row_id as varchar) as _row_id,
    cast(_source_file as varchar) as _source_file,
    cast(_file_row_num as bigint) as _file_row_num,
    cast(source_loaded_at as timestamp with time zone) as source_loaded_at,
    cast(_content_hash as varchar) as _content_hash
from deduplicated
