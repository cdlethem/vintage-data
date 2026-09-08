{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['daily']
) }}

with ranked as (
    select
        md5(
            to_json(
                struct_pack(
                    ein := ein,
                    fetched_at := fetched_at
                )
            )
        )::varchar as irs_exempt_organization_observation_key,
        source::varchar as source_name,
        id::varchar as organization_id,
        fetched_at::timestamp with time zone as fetched_at,
        ein::varchar as ein,
        name::varchar as organization_name,
        ico::varchar as officer_name,
        street::varchar as street_address,
        city::varchar as city,
        state::varchar as state,
        zip::varchar as postal_code,
        "group"::varchar as group_code,
        subsection::varchar as subsection_code,
        affiliation::varchar as affiliation_code,
        classification::varchar as classification_code,
        ruling::varchar as ruling_date,
        deductibility::varchar as deductibility_code,
        foundation::varchar as foundation_code,
        activity::varchar as activity_code,
        organization::varchar as organization_code,
        status::varchar as status_code,
        tax_period::varchar as tax_period,
        asset_cd::varchar as asset_code,
        income_cd::varchar as income_code,
        filing_req_cd::varchar as filing_requirement_code,
        pf_filing_req_cd::varchar as private_foundation_filing_requirement_code,
        acct_pd::varchar as accounting_period,
        cast(nullif(trim(asset_amt), '') as decimal(20, 2)) as asset_amount,
        cast(nullif(trim(income_amt), '') as decimal(20, 2)) as income_amount,
        cast(nullif(trim(revenue_amt), '') as decimal(20, 2)) as revenue_amount,
        ntee_cd::varchar as ntee_code,
        sort_name::varchar as sort_name,
        region::bigint as region_code,
        try_strptime(
            nullif(trim(publisher_updated_at), ''),
            '%a, %d %b %Y %H:%M:%S GMT'
        ) at time zone 'UTC' as publisher_updated_at,
        _batch_id::varchar as _batch_id,
        _load_id::varchar as _load_id,
        _source_file::varchar as _source_file,
        _file_row_num::bigint as _file_row_num,
        _loaded_at::timestamp with time zone as source_loaded_at,
        _content_hash::varchar as _content_hash,
        row_number() over (
            partition by ein, fetched_at
            order by _loaded_at desc, _source_file desc, _file_row_num desc, _row_id desc
        ) as observation_rank
    from {{ ref('base_irs_exempt_organizations') }}
)

select
    irs_exempt_organization_observation_key,
    source_name,
    organization_id,
    fetched_at,
    ein,
    organization_name,
    officer_name,
    street_address,
    city,
    state,
    postal_code,
    group_code,
    subsection_code,
    affiliation_code,
    classification_code,
    ruling_date,
    deductibility_code,
    foundation_code,
    activity_code,
    organization_code,
    status_code,
    tax_period,
    asset_code,
    income_code,
    filing_requirement_code,
    private_foundation_filing_requirement_code,
    accounting_period,
    asset_amount,
    income_amount,
    revenue_amount,
    ntee_code,
    sort_name,
    region_code,
    publisher_updated_at,
    _batch_id,
    _load_id,
    _source_file,
    _file_row_num,
    source_loaded_at,
    _content_hash
from ranked
where observation_rank = 1
