{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['hourly']
) }}

select
    md5(to_json(struct_pack(
        recall_id := cast(id as varchar),
        observed_at := cast(fetched_at as timestamp with time zone)
    ))) as nhtsa_recall_key,
    cast(id as varchar) as recall_id,
    cast(fetched_at as timestamp with time zone) as observed_at,
    cast(source as varchar) as source_name,
    cast(manufacturer as varchar) as manufacturer,
    cast(component as varchar) as component,
    cast(summary as varchar) as summary,
    cast(consequence as varchar) as consequence,
    cast(remedy as varchar) as remedy,
    cast(try_strptime(report_received_date, '%d/%m/%Y') as date) as report_received_date,
    cast(park_it as boolean) as park_it,
    cast(_source_file as varchar) as _source_file,
    cast(_file_row_num as bigint) as _file_row_num,
    cast(_loaded_at as timestamp with time zone) as source_loaded_at,
    cast(_content_hash as varchar) as _content_hash
from {{ ref('base_nhtsa') }}
