{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='openfema_declaration_observation_key',
    on_schema_change='fail',
    tags=['twice_hourly']
) }}

with selected_source as (
    select
        cast(source as varchar) as source_name,
        cast(id as varchar) as declaration_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(femadeclarationstring as varchar) as fema_declaration_string,
        cast(disasternumber as bigint) as disaster_number,
        cast(state as varchar) as state_code,
        cast(declarationtype as varchar) as declaration_type,
        cast(declarationdate as timestamp with time zone) as declaration_at,
        cast(fydeclared as bigint) as fiscal_year_declared,
        cast(incidenttype as varchar) as incident_type,
        cast(declarationtitle as varchar) as declaration_title,
        cast(ihprogramdeclared as boolean) as is_individual_household_program,
        cast(iaprogramdeclared as boolean) as is_individual_assistance_program,
        cast(paprogramdeclared as boolean) as is_public_assistance_program,
        cast(hmprogramdeclared as boolean) as is_hazard_mitigation_program,
        cast(incidentbegindate as timestamp with time zone) as incident_begin_at,
        cast(incidentenddate as timestamp with time zone) as incident_end_at,
        cast(tribalrequest as boolean) as is_tribal_request,
        cast(fipsstatecode as varchar) as fips_state_code,
        cast(fipscountycode as varchar) as fips_county_code,
        cast(placecode as varchar) as place_code,
        cast(designatedarea as varchar) as designated_area,
        cast(declarationrequestnumber as varchar) as declaration_request_number,
        cast(declarationrequestdate as timestamp with time zone) as declaration_requested_at,
        cast(lastiafilingdate as timestamp with time zone) as last_ia_filing_at,
        cast(incidentid as varchar) as incident_id,
        cast(region as bigint) as region_code,
        cast(designatedincidenttypes as varchar) as designated_incident_types,
        cast(lastrefresh as timestamp with time zone) as source_last_refreshed_at,
        cast(hash as varchar) as source_record_hash,
        cast(_row_id as varchar) as source_row_id,
        cast(_batch_id as varchar) as source_batch_id,
        cast(_source_file as varchar) as source_file,
        cast(_file_row_num as bigint) as source_file_row_number,
        cast(_dt as date) as source_date,
        cast(_extract_started_at as timestamp with time zone) as extract_started_at,
        cast(_load_id as varchar) as source_load_id,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as content_hash
    from {{ ref('base_openfema_declarations') }}
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
                        declaration_id := declaration_id,
                        observed_at := observed_at
                    )
                )
            ) as varchar
        ) as openfema_declaration_observation_key,
        selected_source.*
    from selected_source
),

deduplicated as (
    select *
    from keyed
    qualify row_number() over (
        partition by openfema_declaration_observation_key
        order by source_loaded_at desc, source_file desc, source_file_row_number desc, source_row_id desc
    ) = 1
)

select
    openfema_declaration_observation_key,
    source_name,
    declaration_id,
    observed_at,
    fema_declaration_string,
    disaster_number,
    state_code,
    declaration_type,
    declaration_at,
    fiscal_year_declared,
    incident_type,
    declaration_title,
    is_individual_household_program,
    is_individual_assistance_program,
    is_public_assistance_program,
    is_hazard_mitigation_program,
    incident_begin_at,
    incident_end_at,
    is_tribal_request,
    fips_state_code,
    fips_county_code,
    place_code,
    designated_area,
    declaration_request_number,
    declaration_requested_at,
    last_ia_filing_at,
    incident_id,
    region_code,
    designated_incident_types,
    source_last_refreshed_at,
    source_record_hash,
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
