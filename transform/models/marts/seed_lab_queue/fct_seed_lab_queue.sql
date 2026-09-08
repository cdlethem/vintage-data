{{ config(
    materialized='table',
    tags=['twice_hourly']
) }}

select
    md5(to_json(struct_pack(
        source := source,
        queue_id := id,
        observed_at := fetched_at
    )))::varchar as queue_observation_key,
    source::varchar as source,
    id::varchar as queue_id,
    case
        when left(id, 1) = '*' then 'estimated_completion'
        when id = 'total' then 'total'
        when id = 'current-rush-&-super-rush-queue' then 'rush'
        else 'dated_queue'
    end::varchar as queue_entry_type,
    received_date_or_queue::varchar as queue_label,
    fetched_at::timestamp with time zone as observed_at,
    sample_count::bigint as sample_count,
    case
        when left(id, 1) = '*' then sample_count
        else null
    end::bigint as estimated_days_to_complete,
    estimated_completion::varchar as estimated_completion_text,
    _row_id::varchar as _row_id,
    _source_file::varchar as _source_file,
    _file_row_num::bigint as _file_row_num,
    _loaded_at::timestamp with time zone as source_loaded_at,
    _content_hash::varchar as _content_hash
from {{ ref('base_seed_lab_queue') }}
