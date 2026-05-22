"""Example source skeleton. Implement the 3 required methods.

Required: authenticate, build_filter, fetch.
Optional: transform_record, list_partitions, resolve_delta_bound.
Expose your class as `SOURCE_CLASS` at module level.
"""
from __future__ import annotations

from typing import Any, Iterable

from common.config import ObjectSpec
from common.source import Source


class ExampleSource(Source):
    def authenticate(self) -> None:
        # Open a requests.Session / DB connection / etc. Store on self.
        raise NotImplementedError

    def build_filter(self, obj: ObjectSpec, partition: Any | None,
                     lower: str | None, upper: str | None) -> Any:
        # Return a source-native filter (e.g. OData $filter string, SQL WHERE).
        raise NotImplementedError

    def fetch(self, obj: ObjectSpec, partition: Any | None, filter_: Any) -> Iterable[dict]:
        # Yield record dicts. Handle pagination here.
        raise NotImplementedError


SOURCE_CLASS = ExampleSource
