{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='mpc_neocp_candidate_observation_key',
    on_schema_change='fail',
    tags=['twice_hourly']
) }}

with source_rows as (
    select
        cast(source as varchar) as source,
        cast(id as varchar) as candidate_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(temp_desig as varchar) as temporary_designation,
        try_cast(score as integer) as discovery_score,
        cast(discovery as varchar) as discovery,
        cast(r_a as varchar) as right_ascension,
        cast(decl as varchar) as declination,
        try_cast(v as double) as visual_magnitude,
        cast(updated as varchar) as update_description,
        cast(note as varchar) as note,
        try_cast(nobs as integer) as observation_count,
        try_cast(arc as double) as observation_arc_days,
        try_cast(h as double) as absolute_magnitude,
        try_cast(not_seen_dys as double) as days_since_last_seen,
        cast(_row_id as varchar) as source_row_id,
        cast(_batch_id as varchar) as source_batch_id,
        cast(_source_file as varchar) as source_file,
        cast(_file_row_num as bigint) as source_file_row_number,
        cast(_dt as date) as source_date,
        cast(_extract_started_at as timestamp with time zone) as extract_started_at,
        cast(_load_id as varchar) as load_id,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as content_hash
    from {{ ref('base_mpc_neocp') }}
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

keyed_rows as (
    select
        md5(to_json(struct_pack(
            source := source,
            candidate_id := candidate_id,
            observed_at := observed_at
        ))) as mpc_neocp_candidate_observation_key,
        *
    from source_rows
),

deduplicated as (
    select *
    from keyed_rows
    qualify row_number() over (
        partition by mpc_neocp_candidate_observation_key
        order by source_loaded_at desc, source_file desc, source_file_row_number desc, source_row_id desc
    ) = 1
)

select
    mpc_neocp_candidate_observation_key,
    source,
    candidate_id,
    observed_at,
    temporary_designation,
    discovery_score,
    discovery,
    right_ascension,
    declination,
    visual_magnitude,
    update_description,
    note,
    observation_count,
    observation_arc_days,
    absolute_magnitude,
    days_since_last_seen,
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
