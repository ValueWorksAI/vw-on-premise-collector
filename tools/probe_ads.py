"""Dump an Advantage Database Server (ADS) data dictionary so a source's config.yaml can
be written, and check that the collector can read it.

Read-only. Lists what the login can see from the dictionary's system tables
(system.tables / columns / indexes / relations / views), the ODBC column types, and —
unless --no-counts — a COUNT(*) per table. Nothing but names, types and counts leaves
the machine; no row values are read (except MIN/MAX of delta candidates with --sample).

Usage:
  # Which ODBC drivers can this (64-bit) Python see? No connection needed.
  poetry run python tools/probe_ads.py --drivers

  # Connection from an existing source folder (config.yaml + .env)
  poetry run python tools/probe_ads.py --source x2-main --out x2-main-schema.json

  # Ad hoc, plus a generated config
  poetry run python tools/probe_ads.py --data-directory "\\\\SERVER\\X2\\Daten\\x.add" \
      --user VW-DataCollector --emit-config sources/x2-main/config.yaml --source-name x2-main

Password: --password, else ADS_PASSWORD from the environment/.env, else a prompt.
"""
from __future__ import annotations

import argparse
import getpass
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from common import ads  # noqa: E402
from common.secrets import expand, load_env  # noqa: E402

# Column-name fragments that suggest a last-changed column, best first.
NAME_HINTS = ("geaendert", "geändert", "geandert", "aenderung", "änderung", "modified",
              "changed", "lastchange", "updated", "mutation", "edit")
TIMESTAMP_TYPES = {"timestamp", "modtime", "datetime"}
SYSTEM_TABLES = ("tables", "columns", "indexes", "relations", "views", "users",
                 "usergroups", "permissions", "dictionary")


def list_drivers() -> int:
    import pyodbc
    bits = 64 if sys.maxsize > 2**32 else 32
    print(f"ODBC drivers visible to this {bits}-bit Python:")
    for d in pyodbc.drivers():
        print(f"  {d}")
    if not any("advantage" in d.lower() for d in pyodbc.drivers()):
        print("\n[WARN] no Advantage driver. Install the 64-bit 'Advantage ODBC Driver' "
              "(adsodbc_x86_64.exe) on this machine.")
        return 1
    return 0


def _rows(cur, sql: str) -> list[dict[str, Any]]:
    cur.execute(sql)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def _try_rows(cur, sql: str, problems: list[str]) -> list[dict[str, Any]]:
    try:
        return _rows(cur, sql)
    except Exception as e:  # noqa: BLE001
        problems.append(f"{sql}: {type(e).__name__}: {str(e)[:200]}")
        return []


def _clean(v: Any) -> Any:
    if isinstance(v, str):
        return v.strip()
    if isinstance(v, (bytes, bytearray)):
        return None
    return v


def _delta_candidates(columns: list[dict[str, Any]]) -> list[str]:
    def score(c: dict[str, Any]) -> tuple[int, int] | None:
        t = (c.get("type") or "").lower()
        n = (c.get("name") or "").lower().replace(" ", "").replace("_", "")
        hint = next((i for i, h in enumerate(NAME_HINTS) if h in n), None)
        if t == "modtime":
            return (0, 0)                      # maintained by the engine on every change
        if t in TIMESTAMP_TYPES and hint is not None:
            return (1, hint)
        if t == "date" and hint is not None:
            return (2, hint)                   # day precision: needs an overlap window
        return None

    ranked = [(score(c), c["name"]) for c in columns]
    return [name for s, name in sorted((x for x in ranked if x[0] is not None))]


def _columns(cur, table: str, sys_cols: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    """Column list via the ODBC catalog (SQLColumns).

    The ADS driver answers with ODBC 2.x result names (PRECISION/SCALE, upper case,
    no ORDINAL_POSITION), so rows are read by lower-cased description name rather than
    pyodbc's ODBC 3 attribute names. Falls back to an empty SELECT's description.
    """
    out: list[dict[str, Any]] = []
    try:
        cur.columns(table=table)
        names = [d[0].lower() for d in cur.description]
        rows = [dict(zip(names, r)) for r in cur.fetchall()]
    except Exception:  # noqa: BLE001
        rows = []
    for pos, r in enumerate(rows, 1):
        col = str(r.get("column_name") or "").strip()
        sc = sys_cols.get(col.lower(), {})
        out.append({
            "name": col,
            "type": str(r.get("type_name") or "").strip(),
            "size": r.get("column_size", r.get("precision")),
            "decimals": r.get("decimal_digits", r.get("scale")),
            "nullable": bool(r.get("nullable")),
            "position": r.get("ordinal_position") or pos,
            "comment": _clean(sc.get("Comment")) or None,
        })
    if out:
        return out
    cur.execute(f"SELECT * FROM {ads.quote_ident(table)} WHERE 1 = 0")
    return [{"name": d[0], "type": getattr(d[1], "__name__", str(d[1])), "size": d[3],
             "decimals": d[5], "nullable": bool(d[6]), "position": pos, "comment": None}
            for pos, d in enumerate(cur.description, 1)]


def probe(conn, args: argparse.Namespace) -> dict[str, Any]:
    import pyodbc

    cur = conn.cursor()
    problems: list[str] = []
    info = {}
    for key, const in (("driver", pyodbc.SQL_DRIVER_NAME), ("driver_version", pyodbc.SQL_DRIVER_VER),
                       ("dbms", pyodbc.SQL_DBMS_NAME), ("dbms_version", pyodbc.SQL_DBMS_VER)):
        try:
            info[key] = conn.getinfo(const)
        except Exception:  # noqa: BLE001
            info[key] = None

    clock: dict[str, Any] = {}
    try:
        cur.execute("SELECT NOW() FROM system.iota")
        server_now = cur.fetchone()[0]
        local_now = datetime.now()
        clock = {"server_local": str(server_now), "collector_local": local_now.isoformat(),
                 "skew_seconds": int((local_now - server_now).total_seconds())}
    except Exception as e:  # noqa: BLE001
        problems.append(f"clock: {e}")

    sys_tables = {t: _try_rows(cur, f"SELECT * FROM system.{t}", problems)
                  for t in ("tables", "columns", "indexes", "relations", "views")}

    names = [str(r.get("Name") or "").strip() for r in sys_tables["tables"]]
    if not names:  # fall back to the ODBC catalog
        names = [r.table_name for r in cur.tables(tableType="TABLE")]
    if args.include:
        names = [n for n in names if args.include.lower() in n.lower()]

    sys_cols: dict[str, dict[str, dict[str, Any]]] = {}
    for c in sys_tables["columns"]:
        parent = str(c.get("Parent") or "").strip()
        sys_cols.setdefault(parent.lower(), {})[str(c.get("Name") or "").strip().lower()] = c

    tmeta = {str(r.get("Name") or "").strip().lower(): r for r in sys_tables["tables"]}
    objects = []
    for i, name in enumerate(sorted(names, key=str.lower), 1):
        try:
            cols = _columns(cur, name, sys_cols.get(name.lower(), {}))
        except Exception as e:  # noqa: BLE001
            cols = []
            problems.append(f"columns({name}): {e}")
        row_count = None
        if not args.no_counts:
            try:
                cur.execute(f"SELECT COUNT(*) FROM {ads.quote_ident(name)}")
                row_count = int(cur.fetchone()[0])
            except Exception as e:  # noqa: BLE001
                problems.append(f"count({name}): {str(e)[:160]}")
        t = tmeta.get(name.lower(), {})
        obj = {"name": name, "type": "TABLE", "row_count": row_count, "columns": cols,
               "primary_key_index": _clean(t.get("Table_Primary_Key")) or None,
               "comment": _clean(t.get("Comment")) or None,
               "delta_candidates": _delta_candidates(cols)}
        if args.sample and obj["delta_candidates"] and row_count:
            col = ads.quote_ident(obj["delta_candidates"][0])
            try:
                cur.execute(f"SELECT MIN({col}), MAX({col}) FROM {ads.quote_ident(name)}")
                lo, hi = cur.fetchone()
                obj["delta_sample"] = {"min": str(lo), "max": str(hi)}
            except Exception as e:  # noqa: BLE001
                problems.append(f"sample({name}): {e}")
        objects.append(obj)
        if i % 50 == 0:
            print(f"  ... {i}/{len(names)} tables", flush=True)

    def strip_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [{k: _clean(v) for k, v in r.items()} for r in rows]

    return {
        "probed_at": datetime.now().isoformat(timespec="seconds"),
        "data_directory": args.data_directory_display,
        "login": args.user_display,
        "server": info,
        "clock": clock,
        "objects": objects,
        "indexes": strip_rows(sys_tables["indexes"]),
        "relations": strip_rows(sys_tables["relations"]),
        "views": strip_rows(sys_tables["views"]),
        "problems": problems,
    }


def print_summary(data: dict[str, Any]) -> None:
    objs = data["objects"]
    s = data["server"]
    print(f"\nDriver:    {s.get('driver')} {s.get('driver_version')}")
    print(f"Server:    {s.get('dbms')} {s.get('dbms_version')}")
    print(f"Dictionary {data['data_directory']}  (login: {data['login']})")
    counted = [o for o in objs if o["row_count"] is not None]
    empty = sum(1 for o in counted if o["row_count"] == 0)
    total = sum(o["row_count"] for o in counted)
    print(f"Visible:   {len(objs)} tables ({len(counted)} counted, {empty} empty, "
          f"{total:,} rows), {len(data['views'])} views, {len(data['relations'])} relations, "
          f"{len(data['indexes'])} indexes")
    skew = (data.get("clock") or {}).get("skew_seconds")
    if skew is not None:
        print(f"Clock:     server {data['clock']['server_local']}, collector offset {skew:+}s"
              + ("  [WARN] > 60 s: fix before enabling delta" if abs(skew) > 60 else ""))
    if data["problems"]:
        print(f"Problems:  {len(data['problems'])} (first 5)")
        for p in data["problems"][:5]:
            print(f"  - {p}")
    print(f"\n{'TABLE':<40} {'ROWS':>12} {'COLS':>5}  DELTA CANDIDATE")
    print("-" * 90)
    for o in sorted(objs, key=lambda x: -(x["row_count"] or 0))[:60]:
        rows = f"{o['row_count']:,}" if o["row_count"] is not None else "?"
        cand = o["delta_candidates"][0] if o["delta_candidates"] else "-- none, full refresh"
        print(f"{o['name']:<40} {rows:>12} {len(o['columns']):>5}  {cand}")
    if len(objs) > 60:
        print(f"... {len(objs) - 60} more (see --out)")


def build_config(data: dict[str, Any], args: argparse.Namespace) -> str:
    name = args.source_name
    envp = (args.env_prefix or f"{name}_ADS").upper().replace("-", "_")
    prefix = args.prefix or f"raw/{name}"
    lines = [
        f"# Generated by tools/probe_ads.py on {data['probed_at']} from {data['data_directory']}.",
        "# Review the table selection before use. Every object starts on full refresh;",
        "# delta is opt-in per object once volumes need it and the clock offset is checked.",
        f"name: {name}",
        f"output_dir: C:\\output\\{name}",
        "max_workers: 1",
        "keep_local_copy: false",
        "",
        "azure:",
        "  storage_url: ${env:AZURE_STORAGE_URL}",
        "  sas_token:   ${env:AZURE_STORAGE_SAS_TOKEN}",
        "  container:   warehouse",
        f"  prefix:      {prefix}",
        "",
        "partition_field: null",
        "partitions: []",
        "",
        "connection:",
        f"  data_directory: '{data['data_directory']}'",
        f"  username: ${{env:{envp}_USERNAME}}",
        f"  password: ${{env:{envp}_PASSWORD}}",
        f"  driver: {data['server'].get('driver_name_used') or ads.DEFAULT_DRIVER}",
        "  server_types: 2",
        "",
        "objects:",
    ]
    skipped = 0
    for o in data["objects"]:
        if o["row_count"] is not None and o["row_count"] < args.min_rows:
            skipped += 1
            continue
        ts = o["delta_candidates"][0] if o["delta_candidates"] else None
        rows = f"{o['row_count']:,} rows" if o["row_count"] is not None else "rows unknown"
        lines.append(f"  # {rows}")
        lines.append(f"  - name: {o['name']}")
        lines.append(f"    endpoint: {json.dumps(o['name'], ensure_ascii=False)}")
        lines.append(f"    timestamp_field: null   # delta candidate: {ts}" if ts
                     else "    timestamp_field: null")
        lines.append("    partition_scoped: false")
    if skipped:
        lines.insert(3, f"# {skipped} table(s) below {args.min_rows} row(s) were omitted.")
    return "\n".join(lines) + "\n"


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(errors="replace")
    p = argparse.ArgumentParser(description="Dump an ADS data dictionary for config.yaml authoring")
    p.add_argument("--drivers", action="store_true", help="List ODBC drivers and exit")
    p.add_argument("--source", help="Read the connection block from sources/<name>/config.yaml")
    p.add_argument("--data-directory", help="Path to the .add dictionary")
    p.add_argument("--user")
    p.add_argument("--password", help="Omit to read ADS_PASSWORD or prompt")
    p.add_argument("--driver", default=ads.DEFAULT_DRIVER)
    p.add_argument("--include", help="Only tables whose name contains this substring")
    p.add_argument("--no-counts", action="store_true", help="Skip COUNT(*) per table")
    p.add_argument("--sample", action="store_true", help="MIN/MAX of each table's delta candidate")
    p.add_argument("--out", help="Write the full schema JSON here")
    p.add_argument("--emit-config", metavar="PATH", help="Write a complete config.yaml here")
    p.add_argument("--source-name", help="Source name for the generated config")
    p.add_argument("--env-prefix", help="Env var prefix for username/password (default <SOURCE>_ADS)")
    p.add_argument("--prefix", help="Blob prefix for the generated config (default raw/<source>)")
    p.add_argument("--min-rows", type=int, default=1, help="With --emit-config: omit smaller tables")
    args = p.parse_args()

    if args.drivers:
        return list_drivers()

    load_env(REPO_ROOT)
    if args.source:
        import yaml
        cfg_path = REPO_ROOT / "sources" / args.source / "config.yaml"
        conn_cfg = expand(yaml.safe_load(cfg_path.read_text(encoding="utf-8"))["connection"])
        args.source_name = args.source_name or args.source
    else:
        if not args.data_directory or not args.user:
            p.error("--data-directory and --user are required (or use --source / --drivers)")
        password = args.password or os.getenv("ADS_PASSWORD") or getpass.getpass("ADS password: ")
        conn_cfg = {"data_directory": args.data_directory, "username": args.user,
                    "password": password, "driver": args.driver}
    args.data_directory_display = conn_cfg["data_directory"]
    args.user_display = conn_cfg["username"]
    if args.emit_config and not args.source_name:
        p.error("--emit-config needs --source-name (or --source)")

    print(f"Connecting to {conn_cfg['data_directory']} as {conn_cfg['username']} "
          f"via {conn_cfg.get('driver', ads.DEFAULT_DRIVER)} (remote server)...")
    try:
        conn = ads.connect(conn_cfg)
    except Exception as e:  # noqa: BLE001
        print(f"\nCONNECTION FAILED: {type(e).__name__}: {e}")
        print("Check: 64-bit Advantage ODBC driver installed (--drivers), ADS server reachable")
        print("       (TCP 6262), dictionary path as the server sees it, user/password.")
        return 1
    try:
        data = probe(conn, args)
    finally:
        conn.close()
    data["server"]["driver_name_used"] = conn_cfg.get("driver", ads.DEFAULT_DRIVER)

    print_summary(data)
    if args.emit_config:
        Path(args.emit_config).parent.mkdir(parents=True, exist_ok=True)
        Path(args.emit_config).write_text(build_config(data, args), encoding="utf-8")
        print(f"\nConfig written to {args.emit_config} — review the table selection before use.")
    if args.out:
        Path(args.out).write_text(json.dumps(data, indent=2, default=str, ensure_ascii=False),
                                  encoding="utf-8")
        print(f"Full schema written to {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
