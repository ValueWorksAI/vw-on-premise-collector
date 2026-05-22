"""Per-source configuration schema.

Each source folder must contain a `config.yaml` matching `SourceConfig`. The
`connection` block is free-form and parsed by the source's `push.py`.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from .secrets import expand


@dataclass
class AzureTarget:
    container: str
    prefix: str  # e.g. "raw/diamant"
    storage_url: str  # may use ${env:AZURE_STORAGE_URL}
    sas_token: str  # may use ${env:AZURE_STORAGE_SAS_TOKEN}

    @property
    def base_url(self) -> str:
        return f"{self.storage_url.rstrip('/')}/{self.container}/{self.prefix.strip('/')}"


@dataclass
class ObjectSpec:
    name: str
    endpoint: str | None = None  # source-specific, e.g. OData path or SQL table
    timestamp_field: str | None = None  # None => always full refresh
    partition_scoped: bool = True  # if False, fetched once (not per partition)
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass
class SourceConfig:
    name: str
    output_dir: Path
    azure: AzureTarget
    partitions: list[Any]  # partition keys (e.g. companyIds). May be empty.
    partition_field: str | None  # name of the partition field in the source (e.g. "companyId")
    objects: list[ObjectSpec]
    connection: dict[str, Any]  # free-form, source-specific
    max_workers: int = 5

    @classmethod
    def load(cls, config_path: Path) -> "SourceConfig":
        raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
        raw = expand(raw)

        azure = AzureTarget(**raw["azure"])
        objects = [ObjectSpec(**o) for o in raw["objects"]]
        return cls(
            name=raw["name"],
            output_dir=Path(raw["output_dir"]),
            azure=azure,
            partitions=raw.get("partitions", []) or [],
            partition_field=raw.get("partition_field"),
            objects=objects,
            connection=raw.get("connection", {}) or {},
            max_workers=int(raw.get("max_workers", 5)),
        )
