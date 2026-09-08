{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['daily']
) }}

with ranked_observations as (
    select
        md5(to_json(struct_pack(
            project_id := cast(id as varchar),
            observed_at := cast(fetched_at as timestamp with time zone)
        ))) as nih_reporter_project_observation_key,
        cast(source as varchar) as source_name,
        cast(id as varchar) as project_id,
        cast(project_num as varchar) as project_number,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(title as varchar) as project_title,
        cast(date_added as timestamp with time zone) as date_added,
        cast(fiscal_year as bigint) as fiscal_year,
        cast(award_amount as bigint) as award_amount,
        cast(org_name as varchar) as organization_name,
        cast(org_state as varchar) as organization_state,
        cast(principal_investigators as json) as principal_investigators,
        cast(project_start_date as timestamp with time zone) as project_start_at,
        cast(project_end_date as timestamp with time zone) as project_end_at,
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
            partition by id, fetched_at
            order by _loaded_at desc, _source_file desc, _file_row_num desc, _row_id desc
        ) as observation_rank
    from {{ ref('base_nih_reporter') }}
)

select
    nih_reporter_project_observation_key,
    source_name,
    project_id,
    project_number,
    observed_at,
    project_title,
    date_added,
    fiscal_year,
    award_amount,
    organization_name,
    organization_state,
    principal_investigators,
    project_start_at,
    project_end_at,
    source_row_id,
    source_batch_id,
    source_file,
    source_file_row_number,
    source_date,
    extract_started_at,
    load_id,
    source_loaded_at,
    content_hash
from ranked_observations
where observation_rank = 1
