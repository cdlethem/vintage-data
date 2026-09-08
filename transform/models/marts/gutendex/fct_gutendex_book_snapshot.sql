{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['daily']
) }}

with source_rows as (
    select
        md5(to_json(struct_pack(
            source := cast(source as varchar),
            book_id := cast(id as varchar),
            fetched_at := cast(fetched_at as timestamp with time zone)
        ))) as gutendex_book_snapshot_key,
        cast(source as varchar) as source,
        cast(id as varchar) as book_id,
        cast(fetched_at as timestamp with time zone) as fetched_at,
        cast(title as varchar) as title,
        cast(authors as json) as authors,
        cast(author_years as json) as author_years,
        cast(languages as json) as languages,
        cast(subjects as json) as subjects,
        cast(bookshelves as json) as bookshelves,
        cast(download_count as bigint) as download_count,
        cast(copyright as boolean) as copyright,
        cast(text_url as varchar) as text_url,
        cast(_batch_id as varchar) as _batch_id,
        cast(_load_id as varchar) as _load_id,
        cast(_source_file as varchar) as _source_file,
        cast(_file_row_num as bigint) as _file_row_num,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as _content_hash
    from {{ ref('base_gutendex') }}
),

deduplicated as (
    select *
    from source_rows
    qualify row_number() over (
        partition by gutendex_book_snapshot_key
        order by source_loaded_at desc, _source_file desc, _file_row_num desc
    ) = 1
)

select
    gutendex_book_snapshot_key,
    source,
    book_id,
    fetched_at,
    title,
    authors,
    author_years,
    languages,
    subjects,
    bookshelves,
    download_count,
    copyright,
    text_url,
    _batch_id,
    _load_id,
    _source_file,
    _file_row_num,
    source_loaded_at,
    _content_hash
from deduplicated
