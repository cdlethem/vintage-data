{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='quebec_emergency_department_observation_key',
    on_schema_change='fail',
    tags=['hourly']
) }}

with source_rows as (
    select
        cast(_source as varchar) as source_relation,
        cast(source as varchar) as source,
        cast(id as varchar) as department_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(rss as varchar) as rss_url,
        cast(region as varchar) as health_region,
        cast(nom_etablissement as varchar) as establishment_name,
        cast(nom_installation as varchar) as facility_name,
        cast(no_permis_installation as varchar) as facility_permit_number,
        cast(
            case
                when id like 'region-%|Total régional' then 'regional_total'
                else 'facility'
            end as varchar
        ) as reporting_level,
        try_cast(nombre_de_civieres_fonctionnelles as bigint) as functional_stretcher_count,
        try_cast(nombre_de_civieres_occupees as bigint) as occupied_stretcher_count,
        try_cast(nombre_de_patients_sur_civiere_plus_de_24_heures as bigint) as patients_over_24_hours,
        try_cast(nombre_de_patients_sur_civiere_plus_de_48_heures as bigint) as patients_over_48_hours,
        try_cast(nombre_total_de_patients_presents_a_lurgence as bigint) as patients_present_count,
        try_cast(nombre_total_de_patients_en_attente_de_pec as bigint) as patients_waiting_for_care_count,
        try_cast(dms_sur_civiere as double) as stretcher_length_of_stay_hours,
        try_cast(dms_ambulatoire as double) as ambulatory_length_of_stay_hours,
        cast(dms_sur_civiere_horaire as varchar) as stretcher_length_of_stay_hourly_text,
        cast(dms_ambulatoire_horaire as varchar) as ambulatory_length_of_stay_hourly_text,
        cast(
            case
                when regexp_matches(trim(dms_sur_civiere_horaire), '^[0-9]+:[0-9]{2}$') then
                    try_cast(split_part(trim(dms_sur_civiere_horaire), ':', 1) as bigint) * 60
                    + try_cast(split_part(trim(dms_sur_civiere_horaire), ':', 2) as bigint)
                else null
            end as bigint
        ) as stretcher_length_of_stay_hourly_minutes,
        cast(
            case
                when regexp_matches(trim(dms_ambulatoire_horaire), '^[0-9]+:[0-9]{2}$') then
                    try_cast(split_part(trim(dms_ambulatoire_horaire), ':', 1) as bigint) * 60
                    + try_cast(split_part(trim(dms_ambulatoire_horaire), ':', 2) as bigint)
                else null
            end as bigint
        ) as ambulatory_length_of_stay_hourly_minutes,
        cast(heure_de_l_extraction__image as varchar) as extraction_image_time_text,
        cast(mise_a_jour as varchar) as published_update_text,
        cast(_row_id as varchar) as _row_id,
        cast(_batch_id as varchar) as _batch_id,
        cast(_source_file as varchar) as _source_file,
        cast(_file_row_num as bigint) as _file_row_num,
        cast(_dt as date) as _dt,
        cast(_extract_started_at as timestamp with time zone) as _extract_started_at,
        cast(_load_id as varchar) as _load_id,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as _content_hash
    from {{ ref('base_quebec_emergency_departments') }}
    {% if is_incremental() %}
    where _loaded_at >= (
        select coalesce(max(source_loaded_at), timestamp '1900-01-01')
        from {{ this }}
    )
    {% endif %}
),

ranked as (
    select
        cast(md5(to_json(struct_pack(
            source := source,
            department_id := department_id,
            observed_at := observed_at
        ))) as varchar) as quebec_emergency_department_observation_key,
        *,
        row_number() over (
            partition by source, department_id, observed_at
            order by source_loaded_at desc, _source_file desc, _file_row_num desc, _row_id desc
        ) as observation_rank
    from source_rows
)

select
    cast(quebec_emergency_department_observation_key as varchar) as quebec_emergency_department_observation_key,
    cast(source_relation as varchar) as source_relation,
    cast(source as varchar) as source,
    cast(department_id as varchar) as department_id,
    cast(observed_at as timestamp with time zone) as observed_at,
    cast(rss_url as varchar) as rss_url,
    cast(health_region as varchar) as health_region,
    cast(establishment_name as varchar) as establishment_name,
    cast(facility_name as varchar) as facility_name,
    cast(facility_permit_number as varchar) as facility_permit_number,
    cast(reporting_level as varchar) as reporting_level,
    cast(functional_stretcher_count as bigint) as functional_stretcher_count,
    cast(occupied_stretcher_count as bigint) as occupied_stretcher_count,
    cast(patients_over_24_hours as bigint) as patients_over_24_hours,
    cast(patients_over_48_hours as bigint) as patients_over_48_hours,
    cast(patients_present_count as bigint) as patients_present_count,
    cast(patients_waiting_for_care_count as bigint) as patients_waiting_for_care_count,
    cast(stretcher_length_of_stay_hours as double) as stretcher_length_of_stay_hours,
    cast(ambulatory_length_of_stay_hours as double) as ambulatory_length_of_stay_hours,
    cast(stretcher_length_of_stay_hourly_text as varchar) as stretcher_length_of_stay_hourly_text,
    cast(ambulatory_length_of_stay_hourly_text as varchar) as ambulatory_length_of_stay_hourly_text,
    cast(stretcher_length_of_stay_hourly_minutes as bigint) as stretcher_length_of_stay_hourly_minutes,
    cast(ambulatory_length_of_stay_hourly_minutes as bigint) as ambulatory_length_of_stay_hourly_minutes,
    cast(extraction_image_time_text as varchar) as extraction_image_time_text,
    cast(published_update_text as varchar) as published_update_text,
    cast(_row_id as varchar) as _row_id,
    cast(_batch_id as varchar) as _batch_id,
    cast(_source_file as varchar) as _source_file,
    cast(_file_row_num as bigint) as _file_row_num,
    cast(_dt as date) as _dt,
    cast(_extract_started_at as timestamp with time zone) as _extract_started_at,
    cast(_load_id as varchar) as _load_id,
    cast(source_loaded_at as timestamp with time zone) as source_loaded_at,
    cast(_content_hash as varchar) as _content_hash
from ranked
where observation_rank = 1
