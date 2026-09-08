{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['daily']
) }}

with ranked as (
    select
        md5(to_json(struct_pack(
            source := cast(source as varchar),
            storefront := cast(storefront as varchar),
            chart := cast(chart as varchar),
            observed_at := cast(fetched_at as timestamp with time zone),
            chart_rank := cast(rank as bigint)
        ))) as itunes_chart_observation_key,
        cast(source as varchar) as source,
        cast(id as varchar) as id,
        cast(fetched_at as timestamp with time zone) as fetched_at,
        cast(storefront as varchar) as storefront,
        cast(chart as varchar) as chart,
        cast(rank as bigint) as rank,
        cast(name as varchar) as name,
        cast(artist as varchar) as artist,
        cast(collection as varchar) as collection,
        cast(release_date as timestamp with time zone) as release_date,
        cast(genre as varchar) as genre,
        cast(price_amount as varchar) as price_amount,
        cast(price_currency as varchar) as price_currency,
        cast(url as varchar) as url,
        cast(_batch_id as varchar) as _batch_id,
        cast(_load_id as varchar) as _load_id,
        cast(_source_file as varchar) as _source_file,
        cast(_file_row_num as bigint) as _file_row_num,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as _content_hash,
        row_number() over (
            partition by source, storefront, chart, fetched_at, rank
            order by _loaded_at desc, _source_file desc, _file_row_num desc
        ) as _dedupe_rank
    from {{ ref('base_itunes_charts') }}
)

select
    itunes_chart_observation_key,
    source,
    id,
    fetched_at,
    storefront,
    chart,
    rank,
    name,
    artist,
    collection,
    release_date,
    genre,
    price_amount,
    price_currency,
    url,
    _batch_id,
    _load_id,
    _source_file,
    _file_row_num,
    source_loaded_at,
    _content_hash
from ranked
where _dedupe_rank = 1
