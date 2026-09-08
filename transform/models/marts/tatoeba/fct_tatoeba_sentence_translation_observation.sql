{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='tatoeba_sentence_translation_observation_key',
    on_schema_change='fail',
    tags=['hourly']
) }}

with selected_source as (
    select
        cast(source as varchar) as source_name,
        cast(id as varchar) as sentence_id,
        cast(fetched_at as timestamp with time zone) as observed_at,
        cast(text as varchar) as sentence_text,
        cast(lang as varchar) as sentence_language,
        cast(license as varchar) as license,
        cast(translations as json) as translations,
        cast(_batch_id as varchar) as source_batch_id,
        cast(_load_id as varchar) as source_load_id,
        cast(_source_file as varchar) as source_file,
        cast(_file_row_num as bigint) as source_file_row_number,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as content_hash
    from {{ ref('base_tatoeba') }}
    {% if is_incremental() %}
    where cast(_loaded_at as timestamp with time zone) >= (
        select max(source_loaded_at)
        from {{ this }}
    )
    {% endif %}
),

exploded as (
    select
        source_name,
        sentence_id,
        observed_at,
        sentence_text,
        sentence_language,
        license,
        cast(json_extract_string(translation.value, '$.id') as varchar) as translation_id,
        cast(json_extract_string(translation.value, '$.text') as varchar) as translation_text,
        cast(json_extract_string(translation.value, '$.lang') as varchar) as translation_language,
        source_batch_id,
        source_load_id,
        source_file,
        source_file_row_number,
        source_loaded_at,
        content_hash
    from selected_source
    cross join lateral json_each(translations) as translation
),

keyed as (
    select
        cast(
            md5(
                to_json(
                    struct_pack(
                        source_name := source_name,
                        sentence_id := sentence_id,
                        translation_id := translation_id,
                        observed_at := observed_at
                    )
                )
            ) as varchar
        ) as tatoeba_sentence_translation_observation_key,
        exploded.*
    from exploded
),

deduplicated as (
    select *
    from keyed
    qualify row_number() over (
        partition by tatoeba_sentence_translation_observation_key
        order by source_loaded_at desc, source_file desc, source_file_row_number desc
    ) = 1
)

select
    tatoeba_sentence_translation_observation_key,
    source_name,
    sentence_id,
    sentence_text,
    sentence_language,
    translation_id,
    translation_text,
    translation_language,
    observed_at,
    license,
    source_batch_id,
    source_load_id,
    source_file,
    source_file_row_number,
    source_loaded_at,
    content_hash
from deduplicated
