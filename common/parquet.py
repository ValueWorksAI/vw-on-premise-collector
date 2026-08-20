"""Parquet batch writing, streaming combine, META file writing, and local cleanup.

Objects are written in batches and combined by streaming row groups, so peak memory
is bounded by the batch size rather than by the size of the object. Holding a whole
object in memory is what made the widest tables impossible to collect.
"""
from __future__ import annotations

import json
import logging
import shutil
from pathlib import Path
from typing import Any, Iterable

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

log = logging.getLogger(__name__)


def sanitize_timestamp(iso: str) -> str:
    """Make an ISO timestamp safe for a filename (matches legacy behaviour)."""
    return iso.replace(":", "+").split(".")[0]


def parts_dir(output_dir: Path, object_name: str) -> Path:
    """Scratch directory holding one run's batch files for an object."""
    return output_dir / f"_parts_{object_name}"


def _timestamps_to_us(table: pa.Table) -> pa.Table:
    """Normalise every timestamp column to microseconds.

    pandas types a batch of in-range datetimes as datetime64[ns] but keeps a batch
    containing an out-of-range one as object, which pyarrow reads as timestamp[us].
    Unifying those two picks ns, and casting the us values into it overflows — abas
    uses sentinel dates such as 9999-12-31, while ns stops at 2262. us reaches roughly
    +/-290,000 years, and ns -> us is always safe, so standardise on us up front and
    the batches can never disagree.
    """
    fields, changed = [], False
    for f in table.schema:
        if pa.types.is_timestamp(f.type) and f.type.unit != "us":
            fields.append(f.with_type(pa.timestamp("us", tz=f.type.tz)))
            changed = True
        else:
            fields.append(f)
    return table.cast(pa.schema(fields)) if changed else table


def write_part(records: list[dict], output_dir: Path, object_name: str,
               partition: Any | None, index: int) -> Path | None:
    """Write one batch of records. Returns the path, or None if there was nothing."""
    if not records:
        return None
    d = parts_dir(output_dir, object_name)
    d.mkdir(parents=True, exist_ok=True)
    tag = "all" if partition is None else str(partition)
    fp = d / f"p{tag}_{index:05d}.parquet"
    table = _timestamps_to_us(pa.Table.from_pandas(pd.DataFrame(records), preserve_index=False))
    pq.write_table(table, fp)
    return fp


def _align(table: pa.Table, schema: pa.Schema) -> pa.Table:
    """Reshape a batch to the unified schema, filling absent columns with nulls.

    Batches are typed independently, so a column that is all-NULL in one batch and a
    string in the next arrives with a different type. Without this the combine fails
    partway through, after the source has already been read.
    """
    if table.schema.equals(schema):
        return table
    arrays = []
    for field in schema:
        if field.name in table.schema.names:
            col = table.column(field.name)
            if col.type.equals(field.type):
                arrays.append(col)
            else:
                try:
                    arrays.append(col.cast(field.type))
                except Exception as e:  # noqa: BLE001
                    raise ValueError(
                        f"column {field.name!r}: cannot reconcile {col.type} across "
                        f"batches with unified type {field.type} ({e})") from e
        else:
            arrays.append(pa.nulls(table.num_rows, field.type))
    return pa.Table.from_arrays(arrays, schema=schema)


def combine_parts(output_dir: Path, object_name: str, timestamp: str) -> tuple[Path | None, int]:
    """Stream every batch of this object into one parquet. Returns (path, row count).

    Row groups are copied one at a time, so memory stays at one row group rather than
    one object — unlike a pandas concat, which needs the whole thing twice over.
    """
    parts = sorted(parts_dir(output_dir, object_name).glob("*.parquet"))
    if not parts:
        return None, 0
    schema = pa.unify_schemas([pq.ParquetFile(p).schema_arrow for p in parts],
                              promote_options="permissive")
    out = output_dir / object_name
    out.mkdir(parents=True, exist_ok=True)
    fp = out / f"_{sanitize_timestamp(timestamp)}.parquet"
    total = 0
    writer = pq.ParquetWriter(fp, schema)
    try:
        for part in parts:
            pf = pq.ParquetFile(part)
            for i in range(pf.num_row_groups):
                table = _align(pf.read_row_group(i), schema)
                writer.write_table(table)
                total += table.num_rows
    finally:
        writer.close()
    log.info(f"  Combined {len(parts)} batch file(s), {total:,} records -> {fp}")
    return fp, total


def discard_parts(output_dir: Path, object_name: str) -> None:
    """Drop an object's batch files (after combining, or after a failure)."""
    shutil.rmtree(parts_dir(output_dir, object_name), ignore_errors=True)


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
    """Keep only the latest parquet per object dir; delete all batch directories."""
    for obj in object_names:
        obj_dir = output_dir / obj
        if obj_dir.exists():
            pqs = sorted(obj_dir.glob("*.parquet"), key=lambda p: p.stat().st_mtime, reverse=True)
            for old in pqs[1:]:
                log.info(f"  Removed old parquet: {old.name}")
                old.unlink(missing_ok=True)
    for d in list(output_dir.glob("_parts_*")) + list(output_dir.glob("*_Partition*")):
        if d.is_dir():
            log.info(f"  Removed batch dir: {d.name}")
            shutil.rmtree(d, ignore_errors=True)


def wipe_meta_dirs(output_dir: Path) -> None:
    """Delete all META subfolders so a fresh run starts clean (matches legacy PS)."""
    if not output_dir.exists():
        return
    for meta_dir in output_dir.rglob("META"):
        if meta_dir.is_dir():
            shutil.rmtree(meta_dir, ignore_errors=True)
