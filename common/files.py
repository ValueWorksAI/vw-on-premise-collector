"""Copy the files that collected rows point to (scanned invoices next to a document
table, say) from the collector host to Blob Storage.

Opt-in per object under `extra.files` (README, "File attachments"):

    extra:
      files:
        base_dir: 'D:\\Documents'          # folder the paths in the rows are relative to
        path_columns: [FOLDER, FILE_NAME]  # joined in this order; the last one is the file name
        prefix: files/my-source            # optional; default <azure.prefix>/_files/<object>
        max_file_mb: 100                   # optional; larger files are skipped and counted
        min_age_minutes: 10                # optional; younger files wait for the next run
        max_minutes: 60                    # optional; time budget for the copy per run

The object's rows are collected and uploaded as usual. The paths are read back from
its parquet before the local copy is dropped; the files themselves are copied after
every table of the source has landed and the run report has been published.

Only files not yet in storage are sent: one listing of the target prefix per run, then
one streamed PUT per missing file, guarded so nothing that exists is ever overwritten.
Nothing is deleted either; a file removed on the host stays in storage. Because the
listing decides what is missing, any file that did not make it this run (failed,
unreadable, too young, over the time budget) is simply sent by a later run. That only
holds while every run names every file, so `extra.files` requires a full-refresh
object (`timestamp_field: null`).
"""
from __future__ import annotations

import base64
import hashlib
import logging
import mimetypes
import os
import threading
import time
from collections import Counter
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import requests

from . import azure
from .config import AzureTarget, ObjectSpec

log = logging.getLogger(__name__)

UPLOAD_WORKERS = 4
# Stop for this run after this many service-side failures in a row: the target is
# then unreachable or refusing, and trying thousands of files one by one only delays
# the run report that says so. Local read errors never count toward it.
MAX_CONSECUTIVE_FAILURES = 20
EXAMPLES = 10
RETRY_DELAY = 10.0
_KEYS = {"base_dir", "path_columns", "prefix", "max_file_mb", "min_age_minutes", "max_minutes"}
# Windows refuses paths of 260+ characters unless LongPathsEnabled is set; the
# extended-length form \\?\ works regardless. Switch a little early.
_LONG_PATH = 248


class _LocalError(Exception):
    """The file could not be read on this host. Counted, never a service failure."""


@dataclass(frozen=True)
class FileSpec:
    base_dir: Path
    path_columns: tuple[str, ...]
    prefix: str
    max_bytes: int
    min_age_s: float
    max_seconds: float

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
        if obj.timestamp_field:
            # A delta run names only the rows changed since the last one, so a file
            # that missed a run would never be named again.
            raise ValueError(f"{where} needs a full-refresh object (timestamp_field: null), "
                             f"so every run names every file and the ones a run missed "
                             f"are picked up by the next")
        base_dir = str(raw.get("base_dir") or "").strip()
        if not base_dir:
            raise ValueError(f"{where}.base_dir is required")
        cols = raw.get("path_columns")
        if (not isinstance(cols, list) or not cols
                or not all(isinstance(c, str) and c.strip() for c in cols)):
            raise ValueError(f"{where}.path_columns must be a non-empty list of column names")
        selected = (obj.extra or {}).get("columns")
        if selected:
            chosen = {str(c).upper() for c in selected}
            absent = [c for c in cols if c.upper() not in chosen]
            if absent:
                raise ValueError(f"{where}.path_columns {absent} are not in extra.columns")
        prefix = str(raw.get("prefix") or f"{azure_prefix.strip('/')}/_files/{obj.name}").strip("/")
        if not prefix:
            raise ValueError(f"{where}.prefix must not be empty")

        def number(key: str, default: float, allow_zero: bool) -> float:
            try:
                v = float(raw.get(key, default))
            except (TypeError, ValueError):
                raise ValueError(f"{where}.{key} must be a number") from None
            if v < 0 or (v == 0 and not allow_zero):
                raise ValueError(f"{where}.{key} must be {'0 or more' if allow_zero else 'positive'}")
            return v

        return cls(
            base_dir=Path(base_dir),
            path_columns=tuple(cols),
            prefix=prefix,
            max_bytes=int(number("max_file_mb", 100, False) * 1024 * 1024),
            min_age_s=number("min_age_minutes", 10, True) * 60,
            max_seconds=number("max_minutes", 60, False) * 60,
        )


def check_prefixes(specs: dict[str, FileSpec], table_prefix: str) -> None:
    """Refuse file prefixes that overlap: a listing of one would count the other's
    files as present, and two objects naming the same relative path would share a
    blob, so one object's rows would resolve to the other's document."""
    table = table_prefix.strip("/")
    items = list(specs.items())
    for name, spec in items:
        if spec.prefix == table:
            raise ValueError(f"{name}: extra.files.prefix must not be the table prefix {table}")
    for i, (n1, s1) in enumerate(items):
        for n2, s2 in items[i + 1:]:
            a, b = s1.prefix + "/", s2.prefix + "/"
            if a.startswith(b) or b.startswith(a):
                raise ValueError(f"extra.files prefixes overlap: {n1} ({s1.prefix}) and "
                                 f"{n2} ({s2.prefix}); give each object its own")


def _relative_path(parts: tuple[Any, ...]) -> tuple[str | None, str | None]:
    """Join one row's path columns. Returns (path, None) or (None, skip reason).

    Each part is right-stripped (CHAR columns arrive space-padded, and Windows drops
    trailing spaces anyway). A single leading separator stays relative. UNC parts,
    any ':' (drive letters, also in later segments, which would restart the path on
    another drive) and '..'-like segments make the row outside base_dir.
    """
    last = parts[-1]
    if last is None or not str(last).strip():
        return None, "no_file_name"
    segments: list[str] = []
    for i, part in enumerate(parts):
        if part is None:
            continue
        text = str(part).rstrip(" ").replace("\\", "/")
        if text.startswith("//"):
            return None, "outside_base_dir"
        before = len(segments)
        for seg in text.split("/"):
            if seg in ("", "."):
                continue
            # Win32 drops trailing dots and spaces, so '.. ' or '...' can act as '..'.
            if ":" in seg or not seg.rstrip(" ."):
                return None, "outside_base_dir"
            segments.append(seg)
        if i == len(parts) - 1 and len(segments) == before:
            return None, "no_file_name"   # the file-name column held only separators
    return "/".join(segments), None


def relative_paths(parquet_fp: Path | None, spec: FileSpec) -> tuple[list[str], dict[str, int]]:
    """Distinct relative file paths named by an object's rows, plus skip counts.

    Path columns are matched case-insensitively: drivers differ in how they report
    column names, and a SELECT * object has no `columns` list to check them against
    at load time. A column that is not there raises ValueError.
    """
    if parquet_fp is None or not parquet_fp.exists():
        return [], {}
    present = pq.ParquetFile(parquet_fp).schema_arrow.names
    by_upper = {n.upper(): n for n in present}
    missing = [c for c in spec.path_columns if c.upper() not in by_upper]
    if missing:
        raise ValueError(f"path column(s) {missing} not in the table "
                         f"(it has: {', '.join(present[:30])})")
    cols = [by_upper[c.upper()] for c in spec.path_columns]
    table = pq.read_table(parquet_fp, columns=cols)
    columns = [table.column(c).to_pylist() for c in cols]
    paths: set[str] = set()
    skipped: Counter[str] = Counter()
    for parts in zip(*columns):
        rel, reason = _relative_path(parts)
        if rel is None:
            skipped[reason] += 1
        else:
            paths.add(rel)
    return sorted(paths), dict(skipped)


def _native(fp: Path) -> str:
    """Absolute path for stat/open; extended-length form for long paths on Windows."""
    p = os.path.abspath(fp)
    if os.name == "nt" and len(p) >= _LONG_PATH and not p.startswith("\\\\?\\"):
        p = "\\\\?\\UNC\\" + p[2:] if p.startswith("\\\\") else "\\\\?\\" + p
    return p


def _inside(base_abs: str, path_abs: str) -> bool:
    base = os.path.normcase(base_abs).rstrip("\\/")
    return os.path.normcase(path_abs).startswith(base + os.sep)


def _summary(named: int, skipped: dict[str, int], **counts: Any) -> dict[str, Any]:
    keys = ("already_in_storage", "uploaded", "uploaded_mb", "missing_on_host", "unreadable",
            "too_large", "too_recent", "failed", "not_attempted")
    out: dict[str, Any] = {"status": counts.pop("status", "ok"), "named": named}
    out.update({k: counts.get(k, 0) for k in keys})
    out.update({f"skipped_{k}": v for k, v in sorted(skipped.items())})
    return out


def copy_missing(spec: FileSpec, target: AzureTarget, paths: list[str],
                 skipped: dict[str, int], label: str) -> dict[str, Any]:
    """Upload the files in `paths` that are not under `spec.prefix` yet.

    Returns the counts for the run report, with `status` ok, incomplete (time budget
    used up; the rest follows next run) or failed (service failures). Raises only when
    the copy cannot start at all (base_dir missing, listing refused).
    """
    if not paths:
        return _summary(0, skipped)
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
    stored = {n[cut:] for n in azure.list_blob_names(target, spec.prefix, session())}
    in_storage = sum(1 for p in paths if p in stored)

    base_abs = os.path.abspath(spec.base_dir)
    now = time.time()
    todo: list[tuple[str, str, int, float]] = []
    missing: list[str] = []
    too_large = too_recent = 0
    outside = 0
    for rel in paths:
        if rel in stored:
            continue
        path_abs = os.path.abspath(spec.base_dir.joinpath(*rel.split("/")))
        if not _inside(base_abs, path_abs):
            outside += 1      # belt and braces after _relative_path's checks
            continue
        native = _native(Path(path_abs))
        try:
            st = os.stat(native)
            ok = os.path.isfile(native)
        except OSError:
            ok = False
        if not ok:
            missing.append(rel)
        elif st.st_size > spec.max_bytes:
            too_large += 1
            log.warning(f"[{label}] file skipped, {st.st_size / 1048576:.0f} MB is over max_file_mb: {rel}")
        elif now - st.st_mtime < spec.min_age_s:
            too_recent += 1   # may still be being written; the next run takes it
        else:
            todo.append((rel, native, st.st_size, st.st_mtime))
    if outside:
        skipped = dict(skipped, outside_base_dir=skipped.get("outside_base_dir", 0) + outside)
    if missing:
        log.warning(f"[{label}] {len(missing):,} file(s) named in the rows are not on this host, "
                    f"e.g. {', '.join(missing[:EXAMPLES])}")
    log.info(f"[{label}] files: {len(paths):,} named, {in_storage:,} already in storage, "
             f"{len(todo):,} to upload to {target.container}/{spec.prefix}/")

    def upload(rel: str, native: str, size: int, mtime: float) -> tuple[bool, int]:
        def attempt() -> tuple[bool, int]:
            try:
                md5 = hashlib.md5(usedforsecurity=False)
                with open(native, "rb") as fh:
                    for chunk in iter(lambda: fh.read(1 << 20), b""):
                        md5.update(chunk)
                st = os.stat(native)
            except OSError as e:
                raise _LocalError(f"{rel}: {type(e).__name__}: {e.strerror or e}") from None
            if st.st_size != size or st.st_mtime != mtime:
                raise _LocalError(f"{rel}: changed while being read, left for the next run")
            ctype = mimetypes.guess_type(rel)[0] or "application/octet-stream"
            try:
                with open(native, "rb") as fh:
                    # Streamed from the open file: each block gets the socket timeout,
                    # instead of one timeout for the whole body.
                    sent = azure.put_blob_if_absent(
                        target, f"{spec.prefix}/{rel}", fh, ctype, session(),
                        content_md5=base64.b64encode(md5.digest()).decode(), length=size)
            except OSError as e:
                raise _LocalError(f"{rel}: {type(e).__name__}: {e.strerror or e}") from None
            return sent, size

        # Service errors get 3 attempts; a local read error is final for this run
        # (retrying a locked or unreadable file seconds later rarely helps).
        for i in range(3):
            try:
                return attempt()
            except _LocalError:
                raise
            except Exception as e:  # noqa: BLE001
                if i == 2:
                    raise
                log.warning(f"[{label}] attempt {i + 1}/3 failed, retrying: {e}")
                time.sleep(RETRY_DELAY * (2 ** i))
        raise AssertionError("unreachable")

    uploaded = raced = failed = unreadable = 0
    sent_bytes = 0
    consecutive = 0
    stopped = ""
    deadline = time.monotonic() + spec.max_seconds
    queue = iter(todo)
    in_flight: dict[Future, str] = {}
    unreadable_examples: list[str] = []
    ex = ThreadPoolExecutor(max_workers=UPLOAD_WORKERS)
    try:
        while True:
            # Bounded submission: a stop (time budget, failure run, Ctrl-C) leaves at
            # most a few uploads in flight instead of a queue of thousands.
            while not stopped and len(in_flight) < UPLOAD_WORKERS * 2:
                if time.monotonic() >= deadline:
                    stopped = "time"
                    break
                item = next(queue, None)
                if item is None:
                    break
                in_flight[ex.submit(upload, *item)] = item[0]
            if not in_flight:
                break
            done, _ = wait(in_flight, return_when=FIRST_COMPLETED)
            for fut in done:
                rel = in_flight.pop(fut)
                try:
                    ok, size = fut.result()
                except _LocalError as e:
                    unreadable += 1
                    if len(unreadable_examples) < EXAMPLES:
                        unreadable_examples.append(str(e))
                    continue
                except Exception as e:  # noqa: BLE001
                    failed += 1
                    consecutive += 1
                    log.error(f"[{label}] {e}")
                    if consecutive >= MAX_CONSECUTIVE_FAILURES and not stopped:
                        stopped = "failures"
                        log.error(f"[{label}] {consecutive} uploads in a row failed, "
                                  f"not sending the rest this run")
                    continue
                consecutive = 0
                if ok:
                    uploaded += 1
                    sent_bytes += size
                else:
                    raced += 1   # appeared after the listing, e.g. a parallel run
                if (uploaded + raced) % 500 == 0:
                    log.info(f"[{label}] files: {uploaded + raced:,}/{len(todo):,} done")
    except BaseException:
        ex.shutdown(wait=False, cancel_futures=True)
        raise
    ex.shutdown(wait=True)

    if unreadable_examples:
        log.warning(f"[{label}] {unreadable:,} file(s) could not be read, e.g. "
                    + "; ".join(unreadable_examples))
    not_attempted = len(todo) - uploaded - raced - failed - unreadable
    if stopped == "time":
        log.warning(f"[{label}] time budget of {spec.max_seconds / 60:.0f} min used up, "
                    f"{not_attempted:,} file(s) follow in the next run")
    status = "failed" if failed else ("incomplete" if not_attempted else "ok")
    summary = _summary(len(paths), skipped, status=status,
                       already_in_storage=in_storage + raced, uploaded=uploaded,
                       uploaded_mb=round(sent_bytes / 1048576, 1), missing_on_host=len(missing),
                       unreadable=unreadable, too_large=too_large, too_recent=too_recent,
                       failed=failed, not_attempted=not_attempted)
    log.info(f"[{label}] files {status}: {uploaded:,} uploaded ({summary['uploaded_mb']:,} MB), "
             f"{len(missing):,} missing on host, {unreadable:,} unreadable, {too_recent:,} too recent, "
             f"{failed:,} failed, {not_attempted:,} not attempted")
    return summary
