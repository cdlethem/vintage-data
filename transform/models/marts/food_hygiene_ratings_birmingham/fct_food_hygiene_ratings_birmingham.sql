{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['daily']
) }}

with source_rows as (
    select
        cast(md5(to_json(struct_pack(
            source := cast(source as varchar),
            business_id := cast(id as varchar),
            observed_at := cast(fetched_at as timestamp with time zone)
        ))) as varchar) as food_hygiene_observation_key,
        cast(source as varchar) as source,
        cast(id as varchar) as business_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(addressline1 as varchar) as address_line_1,
        cast(addressline2 as varchar) as address_line_2,
        cast(addressline3 as varchar) as address_line_3,
        cast(addressline4 as varchar) as address_line_4,
        cast(businessname as varchar) as business_name,
        cast(businesstype as varchar) as business_type,
        cast(businesstypeid as bigint) as business_type_id,
        cast(changesbyserverid as bigint) as changes_by_server_id,
        cast(fhrsid as bigint) as fhrs_id,
        cast(localauthoritybusinessid as varchar) as local_authority_business_id,
        cast(localauthoritycode as varchar) as local_authority_code,
        cast(localauthorityemailaddress as varchar) as local_authority_email_address,
        cast(localauthorityname as varchar) as local_authority_name,
        cast(localauthoritywebsite as varchar) as local_authority_website,
        cast(newratingpending as boolean) as new_rating_pending,
        cast(phone as varchar) as phone,
        cast(postcode as varchar) as postcode,
        cast(ratingdate as timestamp with time zone) as rating_date,
        cast(ratingkey as varchar) as rating_key,
        try_cast(ratingvalue as bigint) as rating_value,
        cast(righttoreply as varchar) as right_to_reply,
        cast(schemetype as varchar) as scheme_type,
        try_cast(json_extract_string(geocode, '$.latitude') as double) as latitude,
        try_cast(json_extract_string(geocode, '$.longitude') as double) as longitude,
        try_cast(json_extract_string(scores, '$.Hygiene') as bigint) as hygiene_score,
        try_cast(json_extract_string(scores, '$.Structural') as bigint) as structural_score,
        try_cast(json_extract_string(scores, '$.ConfidenceInManagement') as bigint) as confidence_in_management_score,
        cast(publisher_updated_at as timestamp with time zone) as publisher_updated_at,
        cast(_batch_id as varchar) as _batch_id,
        cast(_load_id as varchar) as _load_id,
        cast(_row_id as varchar) as _row_id,
        cast(_source_file as varchar) as _source_file,
        cast(_file_row_num as bigint) as _file_row_num,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as _content_hash
    from {{ ref('base_food_hygiene_ratings_birmingham') }}
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by source, business_id, observed_at
            order by source_loaded_at desc, _source_file desc, _file_row_num desc
        ) as _dedupe_rank
    from source_rows
)

select
    food_hygiene_observation_key,
    source,
    business_id,
    observed_at,
    address_line_1,
    address_line_2,
    address_line_3,
    address_line_4,
    business_name,
    business_type,
    business_type_id,
    changes_by_server_id,
    fhrs_id,
    local_authority_business_id,
    local_authority_code,
    local_authority_email_address,
    local_authority_name,
    local_authority_website,
    new_rating_pending,
    phone,
    postcode,
    rating_date,
    rating_key,
    rating_value,
    right_to_reply,
    scheme_type,
    latitude,
    longitude,
    hygiene_score,
    structural_score,
    confidence_in_management_score,
    publisher_updated_at,
    _batch_id,
    _load_id,
    _row_id,
    _source_file,
    _file_row_num,
    source_loaded_at,
    _content_hash
from deduplicated
where _dedupe_rank = 1
