"""Destination registry. Adding a warehouse means adding one entry here."""
from .base import Destination, LoadRequest, LoadResult
from .duckdb_dest import DuckDBDestination

REGISTRY: dict[str, type[Destination]] = {
    DuckDBDestination.type: DuckDBDestination,
}

__all__ = ["REGISTRY", "Destination", "LoadRequest", "LoadResult", "get_destination"]


def get_destination(config: dict, raw_schema: str, meta_schema: str) -> Destination:
    type_ = config.get("type")
    if type_ not in REGISTRY:
        raise ValueError(f"unknown destination type {type_!r} "
                         f"(available: {sorted(REGISTRY)})")
    return REGISTRY[type_](config, raw_schema, meta_schema)
