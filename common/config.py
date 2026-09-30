"""Per-source configuration schema.

Each source folder must contain a `config.yaml` matching `SourceConfig`. The
`connection` block is free-form and parsed by the source's `push.py`.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import Any

import yaml

from .secrets import expand

log = logging.getLogger(__name__)

_TRUE = {"true", "yes", "on", "1"}
_FALSE = {"false", "no", "off", "0", ""}


def _as_bool(value: Any, key: str) -> bool:
    """Coerce a YAML value to a bool, strictly.

    `bool("false")` is True in Python, so a quoted `key: "false"` would otherwise
    mean the opposite of what it says — and for `keep_local_copy` that is the
    difference between reclaiming the local disk and doubling what it holds.
    """
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return bool(value)
    if isinstance(value, str):
        v = value.strip().lower()
        if v in _TRUE:
            return True
        if v in _FALSE:
            return False
    raise ValueError(f"{key}: expected a boolean, got {value!r}")


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
    # Records buffered before a batch is flushed to disk. Peak memory is roughly
    # batch_size x column count, so this is the knob that decides whether a wide
    # object fits in RAM at all.
    batch_size: int = 100_000
    # Keep an object's parquet on the local disk after it has been uploaded.
    # Off by default: nothing in the run path ever reads it back — the delta
    # watermark comes from the META blob in Azure, and wipe_meta_dirs clears the
    # local META before a run starts — so it is a second full copy of the dataset
    # earning nothing. Turn it on only to inspect output on the machine itself.
    keep_local_copy: bool = False

    @classmethod
    def load(cls, config_path: Path) -> "SourceConfig":
        raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
        raw = expand(raw)

        # A misspelled key is silently ignored by the explicit gets below, which for
        # a flag means it quietly takes the default rather than what was intended.
        unknown = sorted(set(raw) - {f.name for f in fields(cls)})
        if unknown:
            log.warning(f"{config_path}: ignoring unrecognised key(s) "
                        f"{', '.join(unknown)} — check the spelling, they do nothing")

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
            batch_size=int(raw.get("batch_size", 100_000)),
            keep_local_copy=_as_bool(raw.get("keep_local_copy"), "keep_local_copy"),
        )
