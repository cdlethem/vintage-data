{{ config(enabled=var('enable_entrez_models', false)) }}

select
    "_row_id",
    "_source",
    "_batch_id",
    "_source_file",
    "_file_row_num",
    "_dt",
    "_extract_started_at",
    "_load_id",
    "_loaded_at",
    "_content_hash",
    "_payload",
    "source",
    "id",
    "fetched_at",
    "db",
    "term",
    "title",
    "abstract",
    "journal",
    "publication_date",
    "doi",
    "authors",
    "publication_types",
    "raw_payload"
from {{ source('raw', 'entrez') }}
