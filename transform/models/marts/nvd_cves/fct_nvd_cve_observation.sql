{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='nvd_cve_observation_key',
    on_schema_change='fail',
    tags=['twice_hourly']
) }}

with source_rows as (
    select
        cast(_source as varchar) as source_system,
        cast(id as varchar) as cve_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(source as varchar) as source_name,
        cast(published as timestamp with time zone) as published_at,
        cast(last_modified as timestamp with time zone) as last_modified_at,
        cast(status as varchar) as status,
        cast(description as varchar) as description,
        cast(cvss_version as varchar) as cvss_version,
        cast(cvss_score as double) as cvss_score,
        cast(cvss_severity as varchar) as cvss_severity,
        cast(cvss_vector as varchar) as cvss_vector,
        cast(cwes as json) as cwes,
        cast(n_references as bigint) as reference_count,
        cast(source_identifier as varchar) as source_identifier,
        cast(_row_id as varchar) as source_row_id,
        cast(_batch_id as varchar) as source_batch_id,
        cast(_source_file as varchar) as source_file,
        cast(_file_row_num as bigint) as source_file_row_number,
        cast(_dt as date) as source_date,
        cast(_extract_started_at as timestamp with time zone) as extract_started_at,
        cast(_load_id as varchar) as load_id,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as content_hash
    from {{ ref('base_nvd_cves') }}
    {% if is_incremental() %}
    -- Replay the latest load boundary so merge can upsert by observation key.
    where _loaded_at >= (
        select coalesce(max(source_loaded_at), timestamp '1900-01-01')
        from {{ this }}
    )
    {% endif %}
),

deduplicated as (
    select
        cast(md5(to_json(struct_pack(
            source_system := source_system,
            cve_id := cve_id,
            observed_at := observed_at
        ))) as varchar) as nvd_cve_observation_key,
        *,
        row_number() over (
            partition by source_system, cve_id, observed_at
            order by source_loaded_at desc, source_file desc, source_file_row_number desc, source_row_id desc
        ) as _dedupe_rank
    from source_rows
)

select
    cast(nvd_cve_observation_key as varchar) as nvd_cve_observation_key,
    cast(source_system as varchar) as source_system,
    cast(cve_id as varchar) as cve_id,
    cast(observed_at as timestamp with time zone) as observed_at,
    cast(source_name as varchar) as source_name,
    cast(published_at as timestamp with time zone) as published_at,
    cast(last_modified_at as timestamp with time zone) as last_modified_at,
    cast(status as varchar) as status,
    cast(description as varchar) as description,
    cast(cvss_version as varchar) as cvss_version,
    cast(cvss_score as double) as cvss_score,
    cast(cvss_severity as varchar) as cvss_severity,
    cast(cvss_vector as varchar) as cvss_vector,
    cast(cwes as json) as cwes,
    cast(reference_count as bigint) as reference_count,
    cast(source_identifier as varchar) as source_identifier,
    cast(source_row_id as varchar) as source_row_id,
    cast(source_batch_id as varchar) as source_batch_id,
    cast(source_file as varchar) as source_file,
    cast(source_file_row_number as bigint) as source_file_row_number,
    cast(source_date as date) as source_date,
    cast(extract_started_at as timestamp with time zone) as extract_started_at,
    cast(load_id as varchar) as load_id,
    cast(source_loaded_at as timestamp with time zone) as source_loaded_at,
    cast(content_hash as varchar) as content_hash
from deduplicated
where _dedupe_rank = 1
