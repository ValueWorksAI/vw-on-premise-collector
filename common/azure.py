"""Azure Blob Storage helpers: AzCopy upload, blob list/download via REST."""
from __future__ import annotations

import logging
import os
import subprocess
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

import requests

from .config import AzureTarget

log = logging.getLogger(__name__)


def _azcopy() -> str:
    return os.getenv("AZCOPY_PATH", r"C:\AzCopy\azcopy.exe")


def ensure_azcopy() -> None:
    path = _azcopy()
    if not Path(path).exists():
        raise FileNotFoundError(f"AzCopy not found at {path} (set AZCOPY_PATH)")


def _with_sas(url: str, sas: str) -> str:
    return f"{url}?{sas.lstrip('?')}"


def upload_file(local: Path, blob_url: str, sas: str) -> None:
    """Upload a single file via AzCopy."""
    dest = _with_sas(blob_url, sas)
    log.info(f"AzCopy upload: {local.name} -> {blob_url}")
    result = subprocess.run(
        [_azcopy(), "copy", str(local), dest, "--overwrite=true"],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        log.error(f"AzCopy stdout: {result.stdout}")
        log.error(f"AzCopy stderr: {result.stderr}")
        raise RuntimeError(f"AzCopy failed for {local} (exit {result.returncode})")


def list_blobs(target: AzureTarget, prefix: str) -> list[str]:
    """List blob names under <container>/<prefix>."""
    full_prefix = f"{target.prefix.strip('/')}/{prefix.strip('/')}/"
    url = f"{target.storage_url.rstrip('/')}/{target.container}?restype=container&comp=list&prefix={full_prefix}"
    url += f"&{target.sas_token.lstrip('?')}"
    resp = requests.get(url, timeout=30)
    if resp.status_code != 200:
        log.warning(f"List blobs failed ({resp.status_code}) for prefix {full_prefix}")
        return []
    root = ET.fromstring(resp.content)
    return [b.text for b in root.findall(".//Blobs/Blob/Name") if b.text]


def download_blob_json(target: AzureTarget, blob_name: str) -> dict[str, Any] | None:
    """Download a blob as JSON (returns None on 404)."""
    url = f"{target.storage_url.rstrip('/')}/{target.container}/{blob_name}"
    url = _with_sas(url, target.sas_token)
    resp = requests.get(url, timeout=30)
    if resp.status_code == 404:
        return None
    if resp.status_code != 200:
        log.warning(f"Download blob failed ({resp.status_code}) for {blob_name}")
        return None
    return resp.json()


def get_latest_raw_meta(target: AzureTarget, object_name: str) -> dict[str, Any] | None:
    """Latest META JSON from <prefix>/<object_name>/META/."""
    blobs = list_blobs(target, f"{object_name}/META")
    json_blobs = sorted(b for b in blobs if b.endswith(".json"))
    if not json_blobs:
        return None
    return download_blob_json(target, json_blobs[-1])


def get_pretransformed_meta(storage_url: str, sas: str, blob_path: str, container: str = "warehouse") -> dict[str, Any] | None:
    """Free-form pretransformed META lookup (path is source-specific)."""
    url = f"{storage_url.rstrip('/')}/{container}/{blob_path.lstrip('/')}"
    url = _with_sas(url, sas)
    resp = requests.get(url, timeout=30)
    if resp.status_code == 404:
        return None
    if resp.status_code != 200:
        log.warning(f"Pretransformed META fetch failed ({resp.status_code}) for {blob_path}")
        return None
    return resp.json()
