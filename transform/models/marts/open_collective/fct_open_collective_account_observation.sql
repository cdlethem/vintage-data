{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['daily']
) }}

with source_rows as (
    select
        cast(source as varchar) as source_name,
        cast(id as varchar) as account_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(slug as varchar) as account_slug,
        cast(currency as varchar) as currency_code,
        cast(image as varchar) as image_url,
        cast(balance as double) as balance_amount,
        cast(yearlyincome as bigint) as legacy_yearly_income,
        cast(backerscount as bigint) as legacy_backer_count,
        cast(name as varchar) as account_name,
        cast(legacy_id as bigint) as legacy_account_id,
        cast(yearly_budget as double) as yearly_budget_amount,
        cast(backers_count as bigint) as backer_count,
        cast(total_amount_spent as double) as total_amount_spent,
        cast(total_amount_received as double) as total_amount_received,
        cast(_row_id as varchar) as source_row_id,
        cast(_batch_id as varchar) as source_batch_id,
        cast(_source_file as varchar) as source_file,
        cast(_file_row_num as bigint) as source_file_row_number,
        cast(_dt as date) as source_date,
        cast(_extract_started_at as timestamp with time zone) as extract_started_at,
        cast(_load_id as varchar) as source_load_id,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as content_hash
    from {{ ref('base_open_collective') }}
),

keyed as (
    select
        cast(
            md5(
                to_json(
                    struct_pack(
                        source_name := source_name,
                        account_id := account_id,
                        observed_at := observed_at
                    )
                )
            ) as varchar
        ) as open_collective_account_observation_key,
        source_rows.*
    from source_rows
),

deduplicated as (
    select *
    from keyed
    qualify row_number() over (
        partition by open_collective_account_observation_key
        order by source_loaded_at desc, source_file desc, source_file_row_number desc, source_row_id desc
    ) = 1
)

select
    open_collective_account_observation_key,
    source_name,
    account_id,
    observed_at,
    account_slug,
    currency_code,
    image_url,
    balance_amount,
    legacy_yearly_income,
    legacy_backer_count,
    account_name,
    legacy_account_id,
    yearly_budget_amount,
    backer_count,
    total_amount_spent,
    total_amount_received,
    source_row_id,
    source_batch_id,
    source_file,
    source_file_row_number,
    source_date,
    extract_started_at,
    source_load_id,
    source_loaded_at,
    content_hash
from deduplicated
