"""Azure Blob Storage helpers: AzCopy upload, blob list/download via REST."""
from __future__ import annotations

import base64
import hashlib
import logging
import os
import shutil
import subprocess
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlencode

import requests

from .config import AzureTarget

log = logging.getLogger(__name__)

# Pinned so single-shot PUTs above 256 MiB are accepted (2019-12-12+) and the
# behaviour does not shift with whatever the SAS's own `sv` happens to be.
BLOB_API_VERSION = "2021-08-06"


def _azcopy() -> str:
    default = r"C:\AzCopy\azcopy.exe" if os.name == "nt" else "azcopy"
    return os.getenv("AZCOPY_PATH", default)


def ensure_azcopy() -> None:
    path = _azcopy()
    if not Path(path).exists() and shutil.which(path) is None:
        raise FileNotFoundError(f"AzCopy not found at {path} (set AZCOPY_PATH)")


def _with_sas(url: str, sas: str) -> str:
    return f"{url}?{sas.lstrip('?')}"


def upload_file(local: Path, blob_url: str, sas: str) -> None:
    """Upload a single file via AzCopy."""
    dest = _with_sas(blob_url, sas)
    log.info(f"AzCopy upload: {local.name} -> {blob_url}")
    result = subprocess.run(
        # --from-to is explicit rather than inferred: AzCopy derives the destination
        # type from the hostname, which fails for anything that is not
        # *.blob.core.windows.net (Azure Stack, private endpoints, the local emulator).
        [_azcopy(), "copy", str(local), dest, "--overwrite=true", "--from-to=LocalBlob"],
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


def list_blob_names(target: AzureTarget, prefix: str,
                    session: requests.Session | None = None) -> list[str]:
    """Every blob name under <container>/<prefix>/, following continuation markers.

    Unlike `list_blobs` this raises on any failure: callers use the answer to decide
    what still has to be uploaded, so a failed call must not look like an empty
    folder. Errors never carry the request URL, which holds the SAS.
    """
    http = session or requests
    full_prefix = f"{prefix.strip('/')}/"
    base = f"{target.storage_url.rstrip('/')}/{target.container}"
    names: list[str] = []
    marker = ""
    while True:
        params = {"restype": "container", "comp": "list", "prefix": full_prefix,
                  "maxresults": "5000"}
        if marker:
            params["marker"] = marker
        url = f"{base}?{urlencode(params, quote_via=quote)}&{target.sas_token.lstrip('?')}"
        try:
            resp = http.get(url, headers={"x-ms-version": BLOB_API_VERSION}, timeout=60)
        except requests.RequestException as e:
            raise RuntimeError(f"listing {target.container}/{full_prefix} failed: "
                               f"{type(e).__name__}") from None
        if resp.status_code != 200:
            raise RuntimeError(f"listing {target.container}/{full_prefix} failed: HTTP "
                               f"{resp.status_code} {resp.headers.get('x-ms-error-code', '')}".rstrip())
        root = ET.fromstring(resp.content)
        names += [b.text for b in root.findall("./Blobs/Blob/Name") if b.text]
        marker = (root.findtext("NextMarker") or "").strip()
        if not marker:
            return names


def put_blob_if_absent(target: AzureTarget, blob_name: str, data: bytes, content_type: str,
                       session: requests.Session | None = None) -> bool:
    """Upload one block blob unless a blob of that name already exists.

    Returns True when uploaded, False when the name was taken (HTTP 409), and raises
    on anything else. `If-None-Match: *` makes the service refuse to overwrite, and
    Content-MD5 makes it reject a body that arrived damaged. Errors never carry the
    request URL, which holds the SAS.
    """
    http = session or requests
    url = (f"{target.storage_url.rstrip('/')}/{target.container}/{quote(blob_name, safe='/')}"
           f"?{target.sas_token.lstrip('?')}")
    headers = {
        "x-ms-blob-type": "BlockBlob",
        "x-ms-version": BLOB_API_VERSION,
        "If-None-Match": "*",
        "Content-Type": content_type,
        "Content-MD5": base64.b64encode(hashlib.md5(data, usedforsecurity=False).digest()).decode(),
    }
    try:
        resp = http.put(url, data=data, headers=headers, timeout=300)
    except requests.RequestException as e:
        raise RuntimeError(f"upload of {blob_name} failed: {type(e).__name__}") from None
    if resp.status_code == 201:
        return True
    code = resp.headers.get("x-ms-error-code", "")
    # The service answers a taken name with 409 BlobAlreadyExists; 412 is the generic
    # failed-precondition answer some versions and emulators give instead.
    if (resp.status_code, code) in {(409, "BlobAlreadyExists"), (412, "ConditionNotMet")}:
        return False
    raise RuntimeError(f"upload of {blob_name} failed: HTTP {resp.status_code} {code}".rstrip())


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
