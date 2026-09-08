{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['hourly']
) }}

with source_rows as (
    select
        md5(to_json(struct_pack(
            study_id := cast(id as varchar),
            observed_at := cast(fetched_at as timestamp with time zone)
        ))) as clinicaltrials_study_observation_key,
        cast(source as varchar) as source_name,
        cast(id as varchar) as study_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(title as varchar) as title,
        cast(status as varchar) as status,
        cast(start_date as varchar) as start_date,
        cast(completion_date as varchar) as completion_date,
        cast(last_update as date) as last_update,
        cast(study_type as varchar) as study_type,
        cast(phases as json) as phases,
        cast(enrollment as bigint) as enrollment,
        cast(enrollment_type as varchar) as enrollment_type,
        cast(sponsor as varchar) as sponsor,
        cast(sponsor_class as varchar) as sponsor_class,
        cast(conditions as json) as conditions,
        cast(interventions as json) as interventions,
        cast(sex as varchar) as sex,
        cast(min_age as varchar) as min_age,
        cast(max_age as varchar) as max_age,
        cast(healthy_volunteers as boolean) as healthy_volunteers,
        cast(eligibility_criteria as varchar) as eligibility_criteria,
        cast(why_stopped as varchar) as why_stopped,
        cast(_row_id as varchar) as source_row_id,
        cast(_batch_id as varchar) as source_batch_id,
        cast(_source_file as varchar) as source_file,
        cast(_file_row_num as bigint) as source_file_row_number,
        cast(_dt as date) as source_date,
        cast(_extract_started_at as timestamp with time zone) as extract_started_at,
        cast(_load_id as varchar) as load_id,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as content_hash
    from {{ ref('base_clinicaltrials') }}
),

deduplicated as (
    select *
    from source_rows
    qualify row_number() over (
        partition by clinicaltrials_study_observation_key
        order by source_loaded_at desc, source_file desc, source_file_row_number desc, source_row_id desc
    ) = 1
)

select
    clinicaltrials_study_observation_key,
    source_name,
    study_id,
    observed_at,
    title,
    status,
    start_date,
    completion_date,
    last_update,
    study_type,
    phases,
    enrollment,
    enrollment_type,
    sponsor,
    sponsor_class,
    conditions,
    interventions,
    sex,
    min_age,
    max_age,
    healthy_volunteers,
    eligibility_criteria,
    why_stopped,
    source_row_id,
    source_batch_id,
    source_file,
    source_file_row_number,
    source_date,
    extract_started_at,
    load_id,
    source_loaded_at,
    content_hash
from deduplicated
