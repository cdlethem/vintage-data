{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['hourly']
) }}

with source_rows as (
    select
        md5(to_json(struct_pack(
            source_name := cast(source as varchar),
            incident_id := cast(id as varchar),
            observed_at := cast(fetched_at as timestamp with time zone)
        ))) as statuspage_incident_key,
        cast(source as varchar) as source_name,
        cast(id as varchar) as incident_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(name as varchar) as incident_name,
        cast(status as varchar) as incident_status,
        cast(impact as varchar) as impact,
        cast(created_at as timestamp with time zone) as created_at,
        cast(updated_at as timestamp with time zone) as updated_at,
        cast(monitoring_at as timestamp with time zone) as monitoring_at,
        cast(resolved_at as timestamp with time zone) as resolved_at,
        cast(started_at as timestamp with time zone) as started_at,
        cast(shortlink as varchar) as shortlink,
        cast(page_id as varchar) as page_id,
        cast(incident_updates as json) as incident_updates,
        cast(components as json) as components,
        cast(page as json) as page,
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
            partition by source, id, fetched_at
            order by _loaded_at desc, _source_file desc, _file_row_num desc, _row_id desc
        ) as _dedupe_rank
    from {{ ref('base_statuspage_incidents') }}
)

select
    statuspage_incident_key,
    source_name,
    incident_id,
    observed_at,
    incident_name,
    incident_status,
    impact,
    created_at,
    updated_at,
    monitoring_at,
    resolved_at,
    started_at,
    shortlink,
    page_id,
    incident_updates,
    components,
    page,
    source_row_id,
    source_batch_id,
    source_file,
    source_file_row_number,
    source_date,
    extract_started_at,
    load_id,
    source_loaded_at,
    content_hash
from source_rows
where _dedupe_rank = 1
