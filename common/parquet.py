"""Parquet sharding, merging, META file writing, and local cleanup."""
from __future__ import annotations

import json
import logging
import shutil
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

log = logging.getLogger(__name__)


def sanitize_timestamp(iso: str) -> str:
    """Make an ISO timestamp safe for a filename (matches legacy behaviour)."""
    return iso.replace(":", "+").split(".")[0]


def write_shard(records: list[dict], output_dir: Path, object_name: str, partition: Any | None,
                timestamp: str) -> Path | None:
    """Write a per-partition shard parquet. Returns path or None if no records."""
    if not records:
        return None
    folder_name = f"{object_name}_Partition{partition}" if partition is not None else object_name
    out = output_dir / folder_name
    out.mkdir(parents=True, exist_ok=True)
    fp = out / f"_{sanitize_timestamp(timestamp)}.parquet"
    pd.DataFrame(records).to_parquet(fp, index=False, engine="pyarrow")
    log.info(f"  Wrote {len(records):,} records -> {fp}")
    return fp


def merge_shards(output_dir: Path, object_name: str, timestamp: str) -> tuple[Path | None, int]:
    """Merge all `{object_name}_Partition*` shards from this run into one parquet.

    Returns (merged_path, total_records). (None, 0) if nothing to merge.
    """
    ts = sanitize_timestamp(timestamp)
    shard_dirs = list(output_dir.glob(f"{object_name}_Partition*"))
    dfs: list[pd.DataFrame] = []
    for d in shard_dirs:
        for pq in d.glob(f"_{ts}.parquet"):
            try:
                dfs.append(pd.read_parquet(pq))
            except Exception as e:  # noqa: BLE001
                log.error(f"Failed to read {pq}: {e}")
    if not dfs:
        return None, 0
    merged = pd.concat(dfs, ignore_index=True)
    merged_dir = output_dir / object_name
    merged_dir.mkdir(parents=True, exist_ok=True)
    fp = merged_dir / f"_{ts}.parquet"
    merged.to_parquet(fp, index=False, engine="pyarrow")
    log.info(f"  Merged {len(dfs)} shards, {len(merged):,} records -> {fp}")
    return fp, len(merged)


def write_meta(output_dir: Path, object_name: str, timestamp: str, *,
               is_delta: bool, refresh_type: str,
               lower_bound: str | None, upper_bound: str | None,
               total_records: int) -> Path:
    ts = sanitize_timestamp(timestamp)
    meta_dir = output_dir / object_name / "META"
    meta_dir.mkdir(parents=True, exist_ok=True)
    fp = meta_dir / f"_{ts}.json"
    fp.write_text(json.dumps({
        "delta": is_delta,
        "type": refresh_type,
        "_collection_run": timestamp,
        "timestamp_lower_bound": lower_bound,
        "timestamp_upper_bound": upper_bound,
        "total_records": total_records,
        "total_files": 1 if total_records > 0 else 0,
    }, indent=2), encoding="utf-8")
    log.info(f"  META -> {fp}")
    return fp


def cleanup_run(output_dir: Path, object_names: Iterable[str]) -> None:
    """Keep only latest parquet per object dir; delete all `_Partition*` shard dirs."""
    for obj in object_names:
        obj_dir = output_dir / obj
        if obj_dir.exists():
            pqs = sorted(obj_dir.glob("*.parquet"), key=lambda p: p.stat().st_mtime, reverse=True)
            for old in pqs[1:]:
                log.info(f"  Removed old parquet: {old.name}")
                old.unlink(missing_ok=True)
    for shard_dir in output_dir.glob("*_Partition*"):
        if shard_dir.is_dir():
            log.info(f"  Removed shard dir: {shard_dir.name}")
            shutil.rmtree(shard_dir, ignore_errors=True)


def wipe_meta_dirs(output_dir: Path) -> None:
    """Delete all META subfolders so a fresh run starts clean (matches legacy PS)."""
    if not output_dir.exists():
        return
    for meta_dir in output_dir.rglob("META"):
        if meta_dir.is_dir():
            shutil.rmtree(meta_dir, ignore_errors=True)
