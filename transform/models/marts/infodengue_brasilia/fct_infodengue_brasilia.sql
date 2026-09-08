{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='infodengue_brasilia_observation_key',
    on_schema_change='fail',
    tags=['daily']
) }}

with source_rows as (
    select
        cast(_source as varchar) as source_relation,
        cast(source as varchar) as source_endpoint,
        cast(id as varchar) as observation_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(data_inise as date) as week_start_date,
        cast(se as varchar) as epidemiological_week,
        cast(disease as varchar) as disease,
        try_cast(casos_est as double) as cases_estimate,
        try_cast(casos_est_min as double) as cases_estimate_min,
        try_cast(casos_est_max as double) as cases_estimate_max,
        try_cast(casos as double) as cases_reported,
        try_cast(p_rt1 as double) as rt_probability,
        try_cast(p_inc100k as double) as incidence_per_100k,
        cast(localidade_id as varchar) as locality_id,
        cast(nivel as varchar) as alert_level,
        cast(versao_modelo as date) as model_version_date,
        cast(municipio_nome as varchar) as municipality_name,
        try_cast(rt as double) as reproduction_number,
        try_cast(pop as double) as population,
        try_cast(tempmin as double) as temperature_min,
        try_cast(umidmax as double) as humidity_max,
        cast(receptivo as varchar) as receptive_status,
        cast(transmissao as varchar) as transmission_status,
        cast(nivel_inc as varchar) as incidence_level,
        try_cast(umidmed as double) as humidity_mean,
        try_cast(umidmin as double) as humidity_min,
        try_cast(tempmed as double) as temperature_mean,
        try_cast(tempmax as double) as temperature_max,
        try_cast(casprov as double) as probable_cases,
        try_cast(casprov_est as double) as probable_cases_estimate,
        try_cast(casprov_est_min as double) as probable_cases_estimate_min,
        try_cast(casprov_est_max as double) as probable_cases_estimate_max,
        try_cast(casconf as double) as confirmed_cases,
        try_cast(notif_accum_year as double) as notifications_accumulated_year,
        cast(geocode as varchar) as geocode,
        cast(_row_id as varchar) as source_row_id,
        cast(_batch_id as varchar) as source_batch_id,
        cast(_source_file as varchar) as source_file,
        cast(_file_row_num as bigint) as source_file_row_number,
        cast(_dt as date) as source_partition_date,
        cast(_extract_started_at as timestamp with time zone) as extract_started_at,
        cast(_load_id as varchar) as load_id,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as content_hash
    from {{ ref('base_infodengue_brasilia') }}
    {% if is_incremental() %}
    where _loaded_at >= (
        select coalesce(
            max(source_loaded_at),
            timestamp with time zone '1900-01-01 00:00:00+00'
        )
        from {{ this }}
    )
    {% endif %}
),

keyed as (
    select
        cast(md5(to_json(struct_pack(
            source_relation := source_relation,
            observation_id := observation_id,
            observed_at := observed_at
        ))) as varchar) as infodengue_brasilia_observation_key,
        *
    from source_rows
),

deduplicated as (
    select *
    from keyed
    qualify row_number() over (
        partition by infodengue_brasilia_observation_key
        order by source_loaded_at desc, source_file desc, source_file_row_number desc, source_row_id desc
    ) = 1
)

select
    cast(infodengue_brasilia_observation_key as varchar) as infodengue_brasilia_observation_key,
    cast(source_relation as varchar) as source_relation,
    cast(source_endpoint as varchar) as source_endpoint,
    cast(observation_id as varchar) as observation_id,
    cast(observed_at as timestamp with time zone) as observed_at,
    cast(week_start_date as date) as week_start_date,
    cast(epidemiological_week as varchar) as epidemiological_week,
    cast(disease as varchar) as disease,
    cast(cases_estimate as double) as cases_estimate,
    cast(cases_estimate_min as double) as cases_estimate_min,
    cast(cases_estimate_max as double) as cases_estimate_max,
    cast(cases_reported as double) as cases_reported,
    cast(rt_probability as double) as rt_probability,
    cast(incidence_per_100k as double) as incidence_per_100k,
    cast(locality_id as varchar) as locality_id,
    cast(alert_level as varchar) as alert_level,
    cast(model_version_date as date) as model_version_date,
    cast(municipality_name as varchar) as municipality_name,
    cast(reproduction_number as double) as reproduction_number,
    cast(population as double) as population,
    cast(temperature_min as double) as temperature_min,
    cast(humidity_max as double) as humidity_max,
    cast(receptive_status as varchar) as receptive_status,
    cast(transmission_status as varchar) as transmission_status,
    cast(incidence_level as varchar) as incidence_level,
    cast(humidity_mean as double) as humidity_mean,
    cast(humidity_min as double) as humidity_min,
    cast(temperature_mean as double) as temperature_mean,
    cast(temperature_max as double) as temperature_max,
    cast(probable_cases as double) as probable_cases,
    cast(probable_cases_estimate as double) as probable_cases_estimate,
    cast(probable_cases_estimate_min as double) as probable_cases_estimate_min,
    cast(probable_cases_estimate_max as double) as probable_cases_estimate_max,
    cast(confirmed_cases as double) as confirmed_cases,
    cast(notifications_accumulated_year as double) as notifications_accumulated_year,
    cast(geocode as varchar) as geocode,
    cast(source_row_id as varchar) as source_row_id,
    cast(source_batch_id as varchar) as source_batch_id,
    cast(source_file as varchar) as source_file,
    cast(source_file_row_number as bigint) as source_file_row_number,
    cast(source_partition_date as date) as source_partition_date,
    cast(extract_started_at as timestamp with time zone) as extract_started_at,
    cast(load_id as varchar) as load_id,
    cast(source_loaded_at as timestamp with time zone) as source_loaded_at,
    cast(content_hash as varchar) as content_hash
from deduplicated
