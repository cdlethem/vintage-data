{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['daily']
) }}

with source_rows as (
    select
        md5(to_json(struct_pack(
            source_name := cast(source as varchar),
            recall_id := cast(id as varchar),
            observed_at := cast(fetched_at as timestamp with time zone)
        ))) as recall_observation_key,
        cast(source as varchar) as source_name,
        cast(id as varchar) as recall_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        try_strptime(cast(report_date as varchar), '%Y%m%d')::date as report_date,
        try_strptime(cast(recall_initiation_date as varchar), '%Y%m%d')::date as recall_initiation_date,
        cast(status as varchar) as status,
        cast(classification as varchar) as classification,
        cast(firm as varchar) as firm,
        cast(state as varchar) as state,
        cast(country as varchar) as country,
        cast(product as varchar) as product,
        cast(reason as varchar) as reason,
        cast(quantity as varchar) as quantity,
        cast(distribution as varchar) as distribution,
        cast(_batch_id as varchar) as _batch_id,
        cast(_load_id as varchar) as _load_id,
        cast(_source_file as varchar) as _source_file,
        cast(_file_row_num as bigint) as _file_row_num,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as _content_hash,
        cast(_payload as json) as _payload
    from {{ ref('base_openfda') }}
),

deduplicated as (
    select *
    from source_rows
    qualify row_number() over (
        partition by recall_observation_key
        order by source_loaded_at desc, _file_row_num desc
    ) = 1
)

select
    recall_observation_key,
    source_name,
    recall_id,
    observed_at,
    report_date,
    recall_initiation_date,
    status,
    classification,
    firm,
    state,
    country,
    product,
    reason,
    quantity,
    distribution,
    _batch_id,
    _load_id,
    _source_file,
    _file_row_num,
    source_loaded_at,
    _content_hash,
    _payload
from deduplicated
