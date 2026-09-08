{{ config(
    materialized='table',
    on_schema_change='fail',
    tags=['daily']
) }}

with source_rows as (
    select
        md5(to_json(struct_pack(
            source := cast(source as varchar),
            sequence_id := cast(id as varchar),
            fetched_at := cast(fetched_at as timestamp with time zone)
        ))) as nextstrain_sequence_observation_key,
        cast(source as varchar) as source,
        cast(id as varchar) as sequence_id,
        cast(fetched_at as timestamp with time zone) as fetched_at,
        cast(dataset as varchar) as dataset,
        cast(built_at as date) as built_at,
        cast(strain as varchar) as strain,
        cast(is_tip as boolean) as is_tip,
        cast(parent as varchar) as parent,
        cast(depth as bigint) as depth,
        cast(num_date as double) as num_date,
        cast(date as date) as sample_date,
        cast(date_confidence as json) as date_confidence,
        cast(date_inferred as boolean) as date_inferred,
        cast(divergence as double) as divergence,
        cast(clade as varchar) as clade,
        cast(subclade as varchar) as subclade,
        cast(country as varchar) as country,
        cast(region as varchar) as region,
        cast(division as varchar) as division,
        cast(accession as varchar) as accession,
        cast(submitting_lab as varchar) as submitting_lab,
        cast(n_aa_mutations as bigint) as amino_acid_mutation_count,
        cast(n_nuc_mutations as bigint) as nucleotide_mutation_count,
        cast(provenance as json) as provenance,
        cast(_row_id as varchar) as _row_id,
        cast(_batch_id as varchar) as _batch_id,
        cast(_source_file as varchar) as _source_file,
        cast(_file_row_num as bigint) as _file_row_num,
        cast(_dt as date) as _dt,
        cast(_extract_started_at as timestamp with time zone) as _extract_started_at,
        cast(_load_id as varchar) as _load_id,
        cast(_loaded_at as timestamp with time zone) as source_loaded_at,
        cast(_content_hash as varchar) as _content_hash,
        row_number() over (
            partition by source, id, fetched_at
            order by _loaded_at desc, _source_file desc, _file_row_num desc
        ) as _dedupe_rank
    from {{ ref('base_nextstrain') }}
)

select
    nextstrain_sequence_observation_key,
    source,
    sequence_id,
    fetched_at,
    dataset,
    built_at,
    strain,
    is_tip,
    parent,
    depth,
    num_date,
    sample_date,
    date_confidence,
    date_inferred,
    divergence,
    clade,
    subclade,
    country,
    region,
    division,
    accession,
    submitting_lab,
    amino_acid_mutation_count,
    nucleotide_mutation_count,
    provenance,
    _row_id,
    _batch_id,
    _source_file,
    _file_row_num,
    _dt,
    _extract_started_at,
    _load_id,
    source_loaded_at,
    _content_hash
from source_rows
where _dedupe_rank = 1
