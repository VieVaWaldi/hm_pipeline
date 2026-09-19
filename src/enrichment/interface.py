"""
Enricher interface — the contract every src/enrichment/* module implements.

Enrichers are pure: given input records (plain values, not duckdb rows or ORM
model instances), they return output records. They never import a pipeline's
models or know about table names — that's the job of the duckdb-aware glue in
src/pipelines/core_v4/enrichment/, which reads rows into InputT, calls an
Enricher, and writes OutputT back.
"""

from typing import Iterable, Protocol, TypeVar

InputT = TypeVar("InputT")
OutputT = TypeVar("OutputT")


class Enricher(Protocol[InputT, OutputT]):
    def enrich(self, items: Iterable[InputT]) -> Iterable[OutputT]:
        """Enrich a batch of items. Implementations may buffer/batch internally
        (e.g. GPU batch inference, rate-limited API calls) however they like.
        OutputT should carry its own identifying key (e.g. the source row id)
        rather than relying on positional order against `items` — an
        implementation is free to skip items it can't enrich."""
        ...
