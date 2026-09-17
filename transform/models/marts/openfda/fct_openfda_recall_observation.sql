{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='openfda_recall_observation_key',
    on_schema_change='fail',
    tags=['daily']
) }}

with selected_source as (
    select
        cast(source as varchar) as source_name,
        cast(id as varchar) as recall_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(try_strptime(report_date, '%Y%m%d') as date) as report_date,
        cast(try_strptime(recall_initiation_date, '%Y%m%d') as date) as recall_initiation_date,
        cast(status as varchar) as status,
        cast(classification as varchar) as classification,
        cast(firm as varchar) as firm,
        cast(state as varchar) as state,
        cast(country as varchar) as country,
        cast(product as varchar) as product,
        cast(reason as varchar) as reason,
        cast(quantity as varchar) as quantity,
        cast(distribution as varchar) as distribution,
        cast(_row_id as varchar) as source_row_id,
        cast(_source as varchar) as raw_source_name,
        cast(_batch_id as varchar) as source_batch_id,
        cast(_source_file as varchar) as source_file,
        cast(_file_row_num as bigint) as source_file_row_number,
        cast(_dt as date) as source_date,
        cast(_extract_started_at as timestamp with time zone) as extract_started_at,
        cast(_load_id as varchar) as source_load_id,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as content_hash,
        cast(_payload as json) as payload
    from {{ ref('base_openfda') }}
    {% if is_incremental() %}
    where cast(_loaded_at as timestamp with time zone) >= (
        select coalesce(max(source_loaded_at), cast('1900-01-01' as timestamp with time zone))
        from {{ this }}
    )
    {% endif %}
),

keyed as (
    select
        cast(
            md5(
                to_json(
                    struct_pack(
                        source_name := source_name,
                        recall_id := recall_id,
                        observed_at := observed_at
                    )
                )
            ) as varchar
        ) as openfda_recall_observation_key,
        selected_source.*
    from selected_source
),

deduplicated as (
    select *
    from keyed
    qualify row_number() over (
        partition by openfda_recall_observation_key
        order by source_loaded_at desc, source_file_row_number desc, source_row_id desc
    ) = 1
)

select
    openfda_recall_observation_key,
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
    source_row_id,
    raw_source_name,
    source_batch_id,
    source_file,
    source_file_row_number,
    source_date,
    extract_started_at,
    source_load_id,
    source_loaded_at,
    content_hash,
    payload
from deduplicated
