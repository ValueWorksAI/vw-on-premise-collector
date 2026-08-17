"""Validate the Azure Blob SAS token end to end, before the first collector run.

Runs exactly the operations the collector needs — List, Create/Write, Read, and a
real AzCopy upload — and names the missing SAS permission when one fails. Also
inspects the token itself (scope, permissions, expiry) without touching the network.

Safe to run against a live target: everything happens under
<prefix>/_vw_collector_selftest/.

Usage:
  poetry run python tools/check_azure.py --container warehouse --prefix raw/abas
  poetry run python tools/check_azure.py --source abas    # reads sources/abas/config.yaml
"""
from __future__ import annotations

import argparse
import os
import sys
import tempfile
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qs

import requests

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from common.azure import ensure_azcopy, upload_file, _azcopy  # noqa: E402
from common.config import AzureTarget, SourceConfig  # noqa: E402
from common.secrets import load_env  # noqa: E402

SELFTEST_DIR = "_vw_collector_selftest"
TIMEOUT = 30

_results: list[tuple[str, bool, str]] = []


def _record(name: str, ok: bool, detail: str = "") -> bool:
    _results.append((name, ok, detail))
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f" — {detail}" if detail else ""))
    return ok


def _mask(url: str) -> str:
    """Hide the SAS signature so output can be pasted into a ticket."""
    import re

    return re.sub(r"(sig=)[^&]+", r"\1***", url)


def inspect_token(sas: str) -> None:
    """Static checks on the SAS itself — no network calls."""
    print("\n=== SAS token ===")
    q = {k: v[0] for k, v in parse_qs(sas.lstrip("?")).items()}
    if not q.get("sig"):
        _record("token is a SAS", False, "no sig= found; is this really a SAS token?")
        return
    _record("token is a SAS", True, f"service version sv={q.get('sv', '?')}")

    resource = q.get("sr") or q.get("signedresource")
    if resource == "c":
        _record("container-scoped (sr=c)", True)
    elif resource == "b":
        _record("container-scoped (sr=c)", False,
                "sr=b is blob-scoped — List will fail, delta refresh breaks silently")
    elif resource == "d":
        _record("container-scoped (sr=c)", False,
                "sr=d is directory-scoped (ADLS Gen2) — container List may be denied")
    elif resource is None and q.get("ss"):
        _record("container-scoped (sr=c)", True, "account SAS; verify ss=b and srt includes c+o")
    else:
        _record("container-scoped (sr=c)", False, f"unexpected sr={resource!r}")

    perms = q.get("sp", "")
    if not perms and q.get("si"):
        print(f"  [INFO] permissions come from stored access policy si={q['si']} "
              "— cannot validate locally, relying on the live checks below")
    else:
        missing = [p for p in "rcwl" if p not in perms]
        _record("permissions include r, c, w, l", not missing,
                f"sp={perms}" + (f", missing {''.join(missing)}" if missing else ""))
        if "d" in perms:
            print("  [WARN] Delete (d) is granted but not needed — drop it if you can")

    protocol = q.get("spr")
    if protocol and "http," in protocol + ",":
        print(f"  [WARN] spr={protocol} allows plain HTTP — regenerate as HTTPS-only")

    expiry_raw = q.get("se")
    if expiry_raw:
        try:
            expiry = datetime.fromisoformat(expiry_raw.replace("Z", "+00:00"))
            days = (expiry - datetime.now(timezone.utc)).days
            if days < 0:
                _record("token not expired", False, f"expired {-days} days ago ({expiry_raw})")
            else:
                _record("token not expired", True, f"{days} days left (se={expiry_raw})")
                if days < 30:
                    print("  [WARN] expires within 30 days — renew before scheduling")
        except ValueError:
            print(f"  [WARN] could not parse expiry se={expiry_raw}")
    if q.get("sip"):
        print(f"  [INFO] IP-restricted to {q['sip']} — must match this host's public egress IP")


def check_list(target: AzureTarget) -> bool:
    """List Blobs under the prefix — needs `l`. Delta refresh depends on this."""
    url = (f"{target.storage_url.rstrip('/')}/{target.container}"
           f"?restype=container&comp=list&maxresults=1"
           f"&prefix={target.prefix.strip('/')}/&{target.sas_token.lstrip('?')}")
    try:
        resp = requests.get(url, timeout=TIMEOUT)
    except requests.RequestException as e:
        return _record("List (SAS permission `l`)", False, f"network error: {type(e).__name__}: {e}")
    if resp.status_code == 200:
        n = len(ET.fromstring(resp.content).findall(".//Blobs/Blob/Name"))
        return _record("List (SAS permission `l`)", True,
                       "prefix already has blobs" if n else "prefix is empty (expected on a new target)")
    hint = {403: "missing `l` permission, or IP/protocol restriction, or expired",
            404: "container does not exist"}.get(resp.status_code, "")
    return _record("List (SAS permission `l`)", False, f"HTTP {resp.status_code} {hint}".strip())


def check_write_read(target: AzureTarget) -> tuple[bool, str]:
    """PUT then GET a probe blob — needs `c`+`w` then `r`. Returns (ok, blob_url)."""
    blob_url = f"{target.base_url}/{SELFTEST_DIR}/probe.txt"
    body = f"vw-on-prem-collector selftest {datetime.now(timezone.utc).isoformat()}\n".encode()
    try:
        put = requests.put(f"{blob_url}?{target.sas_token.lstrip('?')}", data=body,
                           headers={"x-ms-blob-type": "BlockBlob", "Content-Type": "text/plain"},
                           timeout=TIMEOUT)
    except requests.RequestException as e:
        _record("Create + Write (`c`,`w`)", False, f"network error: {type(e).__name__}: {e}")
        return False, blob_url
    if put.status_code not in (200, 201):
        _record("Create + Write (`c`,`w`)", False,
                f"HTTP {put.status_code}" + (" — missing `c` or `w`" if put.status_code == 403 else ""))
        return False, blob_url
    _record("Create + Write (`c`,`w`)", True)

    try:
        get = requests.get(f"{blob_url}?{target.sas_token.lstrip('?')}", timeout=TIMEOUT)
    except requests.RequestException as e:
        _record("Read (`r`)", False, f"network error: {type(e).__name__}: {e}")
        return False, blob_url
    ok = get.status_code == 200 and get.content == body
    _record("Read (`r`)", ok,
            "" if ok else f"HTTP {get.status_code} — missing `r`; delta refresh would silently full-refresh")
    return ok, blob_url


def check_azcopy(target: AzureTarget) -> bool:
    """Upload via AzCopy — the real production path (catches proxy/TLS issues)."""
    try:
        ensure_azcopy()
    except FileNotFoundError as e:
        return _record("AzCopy upload", False, str(e))
    print(f"  [INFO] azcopy: {_azcopy()}")
    with tempfile.TemporaryDirectory() as tmp:
        local = Path(tmp) / "azcopy-probe.txt"
        local.write_text("vw-on-prem-collector azcopy selftest\n", encoding="utf-8")
        try:
            upload_file(local, f"{target.base_url}/{SELFTEST_DIR}/azcopy-probe.txt", target.sas_token)
        except Exception as e:  # noqa: BLE001
            return _record("AzCopy upload", False,
                           f"{e} — check outbound 443, HTTPS_PROXY, and TLS interception")
    return _record("AzCopy upload", True)


def check_delete_denied(target: AzureTarget, blob_url: str) -> None:
    """The collector never deletes; `d` should be absent. Cleans up on the way."""
    try:
        resp = requests.delete(f"{blob_url}?{target.sas_token.lstrip('?')}", timeout=TIMEOUT)
    except requests.RequestException:
        return
    if resp.status_code in (200, 202):
        print("  [WARN] Delete succeeded — SAS is over-privileged; regenerate without `d` "
              "(probe blob was cleaned up)")
    elif resp.status_code == 403:
        print(f"  [INFO] Delete correctly denied. Remove {SELFTEST_DIR}/ manually when done.")


def resolve_target(args: argparse.Namespace) -> AzureTarget:
    if args.source:
        cfg_path = REPO_ROOT / "sources" / args.source / "config.yaml"
        if not cfg_path.exists():
            raise SystemExit(f"No config at {cfg_path}")
        return SourceConfig.load(cfg_path).azure
    storage_url = os.getenv("AZURE_STORAGE_URL")
    sas = os.getenv("AZURE_STORAGE_SAS_TOKEN")
    if not storage_url or not sas:
        raise SystemExit("AZURE_STORAGE_URL and AZURE_STORAGE_SAS_TOKEN must be set in .env")
    return AzureTarget(container=args.container, prefix=args.prefix,
                       storage_url=storage_url, sas_token=sas)


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate the Azure SAS token and upload path")
    parser.add_argument("--source", help="Read the azure block from sources/<name>/config.yaml")
    parser.add_argument("--container", default="warehouse")
    parser.add_argument("--prefix", default="raw/selftest")
    args = parser.parse_args()

    load_env(REPO_ROOT)
    target = resolve_target(args)

    print(f"Target: {_mask(target.base_url)}")
    for var in ("HTTPS_PROXY", "https_proxy", "HTTP_PROXY", "http_proxy"):
        if os.getenv(var):
            print(f"  [INFO] {var}={os.getenv(var)}")

    inspect_token(target.sas_token)

    print("\n=== Live checks ===")
    check_list(target)
    _, blob_url = check_write_read(target)
    check_azcopy(target)
    check_delete_denied(target, blob_url)

    failed = [name for name, ok, _ in _results if not ok]
    print("\n" + "=" * 60)
    if failed:
        print(f"FAILED: {len(failed)} check(s) — {', '.join(failed)}")
        print("The collector will not work until these pass.")
        return 1
    print(f"All {len(_results)} checks passed. SAS token is good for collector runs.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
