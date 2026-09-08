{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='malaysia_pricecatcher_observation_key',
    on_schema_change='fail',
    tags=['daily']
) }}

with source_rows as (
    select
        cast(source as varchar) as source,
        cast(id as varchar) as price_observation_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(date as date) as price_date,
        cast(premise_code as varchar) as premise_code,
        cast(item_code as varchar) as item_code,
        try_cast(price as decimal(18, 4)) as price_amount,
        cast(publisher_updated_at as varchar) as publisher_updated_at,
        cast(_row_id as varchar) as source_row_id,
        cast(_batch_id as varchar) as source_batch_id,
        cast(_source_file as varchar) as source_file,
        cast(_file_row_num as bigint) as source_file_row_number,
        cast(_dt as date) as source_partition_date,
        cast(_extract_started_at as timestamp with time zone) as extract_started_at,
        cast(_load_id as varchar) as load_id,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as content_hash
    from {{ ref('base_malaysia_pricecatcher') }}
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
            source := source,
            price_observation_id := price_observation_id,
            observed_at := observed_at
        ))) as varchar) as malaysia_pricecatcher_observation_key,
        *
    from source_rows
),

deduplicated as (
    select *
    from keyed
    qualify row_number() over (
        partition by malaysia_pricecatcher_observation_key
        order by source_loaded_at desc, source_file desc, source_file_row_number desc, source_row_id desc
    ) = 1
)

select
    cast(malaysia_pricecatcher_observation_key as varchar) as malaysia_pricecatcher_observation_key,
    cast(source as varchar) as source,
    cast(price_observation_id as varchar) as price_observation_id,
    cast(observed_at as timestamp with time zone) as observed_at,
    cast(price_date as date) as price_date,
    cast(premise_code as varchar) as premise_code,
    cast(item_code as varchar) as item_code,
    cast(price_amount as decimal(18, 4)) as price_amount,
    cast(publisher_updated_at as varchar) as publisher_updated_at,
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
