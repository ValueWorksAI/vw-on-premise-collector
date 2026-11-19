"""Copy the files that collected rows point to (scanned invoices next to a document
table, say) from the collector host to Blob Storage.

Opt-in per object under `extra.files` (README, "File attachments"):

    extra:
      files:
        base_dir: 'D:\\Documents'          # folder the paths in the rows are relative to
        path_columns: [FOLDER, FILE_NAME]  # joined in this order; the last one is the file name
        prefix: files/my-source            # optional; default <azure.prefix>/_files/<object>
        max_file_mb: 100                   # optional; larger files are skipped and counted

The object's rows are collected and uploaded as usual. The paths are read back from
its parquet before the local copy is dropped, and the files themselves are copied at
the end of the source's run, after every table, so a large first copy cannot hold up
the tables behind it.

Only files not yet in storage are sent: one listing of the target prefix per run,
then one PUT per missing file, guarded so nothing that exists is ever overwritten.
Nothing is deleted either; a file removed on the host stays in storage. A file the
rows name but the host does not have is counted, not fatal. An upload that still fails
after retries fails the object, and the next run sends the file again because it is
still missing from the listing.
"""
from __future__ import annotations

import logging
import mimetypes
import threading
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import requests

from . import azure
from .config import AzureTarget, ObjectSpec
from .retry import with_retries

log = logging.getLogger(__name__)

UPLOAD_WORKERS = 4
# Stop sending once this many files have failed for good: by then the target is
# unreachable or refusing, and retrying thousands of files one by one only delays the
# run report that says so.
MAX_FAILURES = 20
MISSING_EXAMPLES = 10
_KEYS = {"base_dir", "path_columns", "prefix", "max_file_mb"}


@dataclass(frozen=True)
class FileSpec:
    base_dir: Path
    path_columns: tuple[str, ...]
    prefix: str
    max_bytes: int

    @classmethod
    def parse(cls, obj: ObjectSpec, azure_prefix: str) -> "FileSpec | None":
        """Read `extra.files` of one object; None when the object has none.

        Raises on anything malformed, so a broken block stops the source at load time
        instead of after every table has been collected.
        """
        raw = (obj.extra or {}).get("files")
        if raw is None:
            return None
        where = f"{obj.name}: extra.files"
        if not isinstance(raw, dict):
            raise ValueError(f"{where} must be a mapping")
        unknown = sorted(set(raw) - _KEYS)
        if unknown:
            raise ValueError(f"{where}: unknown key(s) {', '.join(unknown)}")
        base_dir = str(raw.get("base_dir") or "").strip()
        if not base_dir:
            raise ValueError(f"{where}.base_dir is required")
        cols = raw.get("path_columns")
        if (not isinstance(cols, list) or not cols
                or not all(isinstance(c, str) and c.strip() for c in cols)):
            raise ValueError(f"{where}.path_columns must be a non-empty list of column names")
        selected = (obj.extra or {}).get("columns")
        if selected:
            absent = [c for c in cols if c not in selected]
            if absent:
                raise ValueError(f"{where}.path_columns {absent} are not in extra.columns")
        prefix = str(raw.get("prefix") or f"{azure_prefix.strip('/')}/_files/{obj.name}").strip("/")
        try:
            max_mb = float(raw.get("max_file_mb", 100))
        except (TypeError, ValueError):
            raise ValueError(f"{where}.max_file_mb must be a number") from None
        if max_mb <= 0:
            raise ValueError(f"{where}.max_file_mb must be positive")
        return cls(Path(base_dir), tuple(cols), prefix, int(max_mb * 1024 * 1024))


def _relative_path(parts: tuple[Any, ...]) -> tuple[str | None, str | None]:
    """Join one row's path columns. Returns (path, None) or (None, skip reason)."""
    if parts[-1] is None or not str(parts[-1]).strip():
        return None, "no_file_name"
    raw = [str(p) for p in parts if p is not None]
    joined = "/".join(raw).replace("\\", "/")
    # Absolute paths (C:\..., \\server\...) and parent references would read outside
    # base_dir, which is the only folder the configuration has agreed to send. A
    # single leading separator is common in "relative" columns and stays relative.
    if joined.startswith("//") or ":" in joined.lstrip("/").split("/", 1)[0]:
        return None, "outside_base_dir"
    segments = [s for s in joined.split("/") if s not in ("", ".")]
    if ".." in segments:
        return None, "outside_base_dir"
    return "/".join(segments), None


def relative_paths(parquet_fp: Path | None, spec: FileSpec) -> tuple[list[str], dict[str, int]]:
    """Distinct relative file paths named by an object's rows, plus skip counts."""
    if parquet_fp is None or not parquet_fp.exists():
        return [], {}
    table = pq.read_table(parquet_fp, columns=list(spec.path_columns))
    columns = [table.column(c).to_pylist() for c in spec.path_columns]
    paths: set[str] = set()
    skipped: Counter[str] = Counter()
    for parts in zip(*columns):
        rel, reason = _relative_path(parts)
        if rel is None:
            skipped[reason] += 1
        else:
            paths.add(rel)
    return sorted(paths), dict(skipped)


def copy_missing(spec: FileSpec, target: AzureTarget, paths: list[str],
                 skipped: dict[str, int], label: str) -> dict[str, Any]:
    """Upload the files in `paths` that are not under `spec.prefix` yet.

    Returns the counts for the run report. Raises only when the copy cannot start at
    all (base_dir missing, listing refused); single-file failures are counted.
    """
    if not spec.base_dir.is_dir():
        raise FileNotFoundError(f"extra.files.base_dir {spec.base_dir} is not a folder on this host")

    local = threading.local()

    def session() -> requests.Session:
        # One keep-alive connection per worker: thousands of small files would
        # otherwise pay a TLS handshake each.
        if not hasattr(local, "s"):
            local.s = requests.Session()
        return local.s

    cut = len(spec.prefix) + 1
    present = {n[cut:] for n in azure.list_blob_names(target, spec.prefix, session())}
    in_storage = sum(1 for p in paths if p in present)

    todo: list[tuple[str, Path]] = []
    missing: list[str] = []
    too_large = 0
    for rel in paths:
        if rel in present:
            continue
        fp = spec.base_dir.joinpath(*rel.split("/"))
        try:
            size = fp.stat().st_size if fp.is_file() else None
        except OSError:
            size = None
        if size is None:
            missing.append(rel)
        elif size > spec.max_bytes:
            too_large += 1
            log.warning(f"[{label}] file skipped, {size / 1048576:.0f} MB is over max_file_mb: {rel}")
        else:
            todo.append((rel, fp))
    if missing:
        log.warning(f"[{label}] {len(missing):,} file(s) named in the rows are not on this host, "
                    f"e.g. {', '.join(missing[:MISSING_EXAMPLES])}")
    log.info(f"[{label}] files: {len(paths):,} named, {in_storage:,} already in storage, "
             f"{len(todo):,} to upload to {target.container}/{spec.prefix}/")

    def upload(rel: str, fp: Path) -> tuple[bool, int]:
        def attempt() -> tuple[bool, int]:
            data = fp.read_bytes()
            ctype = mimetypes.guess_type(fp.name)[0] or "application/octet-stream"
            return azure.put_blob_if_absent(target, f"{spec.prefix}/{rel}", data, ctype,
                                            session()), len(data)
        return with_retries(attempt, attempts=3, delay=10, backoff=2)

    uploaded = raced = failed = done = 0
    sent_bytes = 0
    with ThreadPoolExecutor(max_workers=UPLOAD_WORKERS) as ex:
        futures = {ex.submit(upload, rel, fp): rel for rel, fp in todo}
        for fut in as_completed(futures):
            done += 1
            try:
                ok, size = fut.result()
            except Exception as e:  # noqa: BLE001
                failed += 1
                log.error(f"[{label}] {e}")
                if failed >= MAX_FAILURES:
                    log.error(f"[{label}] {failed} uploads failed, not sending the rest this run")
                    ex.shutdown(wait=True, cancel_futures=True)
                    break
                continue
            if ok:
                uploaded += 1
                sent_bytes += size
            else:
                raced += 1   # appeared after the listing, e.g. a parallel run
            if done % 500 == 0:
                log.info(f"[{label}] files: {done:,}/{len(todo):,} done")

    not_attempted = len(todo) - uploaded - raced - failed
    summary = {
        "named": len(paths),
        "already_in_storage": in_storage + raced,
        "uploaded": uploaded,
        "uploaded_mb": round(sent_bytes / 1048576, 1),
        "missing_on_host": len(missing),
        "too_large": too_large,
        "failed": failed,
        "not_attempted": not_attempted,
        **{f"skipped_{k}": v for k, v in sorted(skipped.items())},
    }
    log.info(f"[{label}] files: {uploaded:,} uploaded ({summary['uploaded_mb']:,} MB), "
             f"{len(missing):,} missing on host, {failed:,} failed")
    return summary
