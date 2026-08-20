"""Source base class. A new source = subclass implementing 3 required methods.

Required:
  - authenticate(self) -> None
  - build_filter(self, obj, partition, lower, upper) -> Any
  - fetch(self, obj, partition, filter_) -> Iterable[dict]

Optional overrides:
  - transform_record(self, obj, record) -> dict
  - list_partitions(self, obj) -> list[Any]
  - resolve_delta_bound(self, obj, mode) -> tuple[lower, upper, refresh_type, is_delta]

The base class owns the run loop: per object, resolve delta bound, fetch each
partition, write per-partition parquet shards, merge, write META, upload to
Azure, clean up local files.
"""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

from . import azure, parquet
from .config import ObjectSpec, SourceConfig

log = logging.getLogger(__name__)


class Source(ABC):
    """Base class. Subclass per source under sources/<name>/push.py."""

    def __init__(self, config: SourceConfig, mode: str):
        self.config = config
        self.mode = mode  # "full" | "delta"
        self.collection_start = datetime.now().isoformat()

    # ----- required -----
    @abstractmethod
    def authenticate(self) -> None: ...

    @abstractmethod
    def build_filter(self, obj: ObjectSpec, partition: Any | None,
                     lower: str | None, upper: str | None) -> Any:
        """Return source-native filter (OData string, SQL WHERE, etc.)."""

    @abstractmethod
    def fetch(self, obj: ObjectSpec, partition: Any | None, filter_: Any) -> Iterable[dict]:
        """Yield records for one (object, partition)."""

    # ----- optional -----
    def transform_record(self, obj: ObjectSpec, record: dict) -> dict:
        """Per-record normalization. Default: tag with collection timestamp."""
        record.setdefault("changeDate", self.collection_start)
        return record

    def list_partitions(self, obj: ObjectSpec) -> list[Any]:
        """Default: use partitions from config; [] if object isn't partition-scoped."""
        if not obj.partition_scoped:
            return [None]
        return list(self.config.partitions) if self.config.partitions else [None]

    def resolve_delta_bound(self, obj: ObjectSpec) -> tuple[str | None, str | None, str, bool]:
        """Return (lower, upper, refresh_type, is_delta) for an object.

        Default: full-refresh always sets lower=None. Delta mode reads our own
        latest raw META from Azure and uses its `timestamp_upper_bound` as the
        new lower bound. Objects with no `timestamp_field` always full-refresh.
        """
        upper = self.collection_start
        if self.mode != "delta" or not obj.timestamp_field:
            reason = "full-refresh" if self.mode != "delta" else "full-refresh / no timestamp field"
            return None, upper, reason, False

        meta = azure.get_latest_raw_meta(self.config.azure, obj.name)
        if not meta:
            return None, upper, "full-refresh / no raw META found", False
        lower = meta.get("timestamp_upper_bound")
        if not lower:
            return None, upper, "full-refresh / no timestamp_upper_bound in raw META", False
        return lower, upper, "delta-refresh", True

    # ----- run loop -----
    def run(self) -> int:
        """Returns process exit code. 0 on success, 1 if any object failed."""
        out_dir = self.config.output_dir
        out_dir.mkdir(parents=True, exist_ok=True)
        log.info(f"[{self.config.name}] wiping META dirs in {out_dir}")
        parquet.wipe_meta_dirs(out_dir)

        log.info(f"[{self.config.name}] authenticating")
        self.authenticate()

        # One bad object must not discard the whole run: without this a failure on the
        # last of N objects means nothing at all is uploaded, including the N-1 that
        # succeeded.
        failed: list[tuple[str, str]] = []
        for i, obj in enumerate(self.config.objects, start=1):
            log.info(f"[{self.config.name}] object {i}/{len(self.config.objects)}: {obj.name}")
            try:
                self._run_object(obj)
            except Exception as e:  # noqa: BLE001
                log.exception(f"[{obj.name}] FAILED, continuing with the next object: {e}")
                failed.append((obj.name, f"{type(e).__name__}: {e}"))
                parquet.discard_parts(out_dir, obj.name)

        log.info(f"[{self.config.name}] uploading to Azure")
        self._upload_all()

        log.info(f"[{self.config.name}] local cleanup")
        parquet.cleanup_run(out_dir, [o.name for o in self.config.objects])

        ok = len(self.config.objects) - len(failed)
        if failed:
            log.error(f"[{self.config.name}] {ok}/{len(self.config.objects)} objects "
                      f"collected; {len(failed)} failed:")
            for name, err in failed:
                log.error(f"    {name}: {err}")
            return 1
        log.info(f"[{self.config.name}] done, {ok} object(s)")
        return 0

    def _run_object(self, obj: ObjectSpec) -> None:
        lower, upper, refresh_type, is_delta = self.resolve_delta_bound(obj)
        log.info(f"[{obj.name}] mode={refresh_type} lower={lower} upper={upper}")

        out_dir = self.config.output_dir
        parquet.discard_parts(out_dir, obj.name)   # any leftovers from an earlier run
        partitions = self.list_partitions(obj)
        batch_size = self.config.batch_size

        def _do_partition(p: Any | None) -> int:
            """Fetch one partition, flushing to disk every batch_size records."""
            f = self.build_filter(obj, p, lower, upper)
            buf: list[dict] = []
            written = index = 0
            for r in self.fetch(obj, p, f):
                buf.append(self.transform_record(obj, r))
                if len(buf) >= batch_size:
                    parquet.write_part(buf, out_dir, obj.name, p, index)
                    written += len(buf)
                    index += 1
                    buf.clear()
                    log.info(f"[{obj.name}] partition={p} {written:,} records so far")
            if buf:
                parquet.write_part(buf, out_dir, obj.name, p, index)
                written += len(buf)
            return written

        if len(partitions) > 1:
            with ThreadPoolExecutor(max_workers=self.config.max_workers) as ex:
                futures = {ex.submit(_do_partition, p): p for p in partitions}
                for fut in as_completed(futures):
                    p = futures[fut]
                    n = fut.result()   # a partition failure fails the object
                    log.info(f"[{obj.name}] partition={p} records={n}")
        else:
            n = _do_partition(partitions[0])
            log.info(f"[{obj.name}] records={n}")

        # Row count comes from the combine, so nothing is read back to count it.
        _fp, total = parquet.combine_parts(out_dir, obj.name, self.collection_start)
        parquet.discard_parts(out_dir, obj.name)

        parquet.write_meta(
            out_dir, obj.name, self.collection_start,
            is_delta=is_delta, refresh_type=refresh_type,
            lower_bound=lower, upper_bound=upper, total_records=total,
        )

    def _upload_all(self) -> None:
        out_dir = self.config.output_dir
        target = self.config.azure
        ts = parquet.sanitize_timestamp(self.collection_start)
        for obj in self.config.objects:
            obj_dir = out_dir / obj.name
            if not obj_dir.exists():
                continue
            meta_fp = obj_dir / "META" / f"_{ts}.json"
            if not meta_fp.exists():
                log.warning(f"[{obj.name}] no META for this run, skipping upload")
                continue
            parquet_fp = obj_dir / f"_{ts}.parquet"
            if parquet_fp.exists():
                azure.upload_file(
                    parquet_fp,
                    f"{target.base_url}/{obj.name}/{parquet_fp.name}",
                    target.sas_token,
                )
            else:
                log.info(f"[{obj.name}] no new data, parquet skipped")
            azure.upload_file(
                meta_fp,
                f"{target.base_url}/{obj.name}/META/{meta_fp.name}",
                target.sas_token,
            )


def load_source(source_dir: Path, mode: str) -> Source:
    """Import `push.py` from a source folder and return its Source instance."""
    import importlib.util

    cfg = SourceConfig.load(source_dir / "config.yaml")
    spec = importlib.util.spec_from_file_location(f"sources.{source_dir.name}.push", source_dir / "push.py")
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load {source_dir / 'push.py'}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    if not hasattr(module, "SOURCE_CLASS"):
        raise AttributeError(f"{source_dir / 'push.py'} must define SOURCE_CLASS = <YourSource>")
    cls = module.SOURCE_CLASS
    return cls(cfg, mode)
