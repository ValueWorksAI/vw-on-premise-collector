"""Dump an Advantage Database Server (ADS) data dictionary so a source's config.yaml can
be written, and check that the collector can read it.

Read-only. Lists what the login can see from the dictionary's system tables
(system.tables / columns / indexes / relations / views), the ODBC column types, and —
unless --no-counts — a COUNT(*) per table. Nothing but names, types and counts leaves
the machine; no row values are read (except MIN/MAX of delta candidates with --sample).

--fill PATH adds fill rates: per column, how many rows are not NULL and how many carry
a value (text not blank, number not zero, date after 1900, logical true). The database
only returns counts (COUNT/SUM aggregates); no row values are read, so the result can
be shared before any data transfer is agreed. --tables limits the probe to a list.

Usage:
  # Which ODBC drivers can this (64-bit) Python see? No connection needed.
  poetry run python tools/probe_ads.py --drivers

  # Connection from an existing source folder (config.yaml + .env)
  poetry run python tools/probe_ads.py --source x2-main --out x2-main-schema.json

  # Ad hoc, plus a generated config
  poetry run python tools/probe_ads.py --data-directory "\\\\SERVER\\X2\\Daten\\x.add" \
      --user VW-DataCollector --emit-config sources/x2-main/config.yaml --source-name x2-main

  # Row counts and fill rates for selected tables only, uploaded for review
  poetry run python tools/probe_ads.py --source x2-main --tables @tables.txt \
      --fill x2-main-fill.csv --out x2-main-schema.json --upload

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
NAME_HINTS = ("modifydt", "modified", "modify", "geaendert", "geändert", "geandert", "aenderung",
              "änderung", "changed", "lastchange", "updated", "mutation", "edit")
TIMESTAMP_TYPES = {"timestamp", "modtime", "datetime"}
# Fill-rate rule per ADS type family: (rule label, SQL template for "carries a value").
# X2 and other ADS applications store an empty date as 1899-12-30, hence "after 1900".
FILL_RULES = {
    "text": ("not blank", "SUM(CASE WHEN TRIM({c}) <> '' THEN 1 ELSE 0 END)"),
    "number": ("non-zero", "SUM(CASE WHEN {c} <> 0 THEN 1 ELSE 0 END)"),
    "date": ("after 1900", "SUM(CASE WHEN YEAR({c}) > 1900 THEN 1 ELSE 0 END)"),
    "logical": ("true", "SUM(CASE WHEN {c} = TRUE THEN 1 ELSE 0 END)"),
}
TYPE_FAMILY = {
    **dict.fromkeys(("char", "cichar", "varchar", "nchar", "ncichar", "nvarchar",
                     "varcharfox"), "text"),
    **dict.fromkeys(("integer", "short", "double", "curdouble", "money", "numeric",
                     "autoinc", "rowversion", "longlong", "float"), "number"),
    **dict.fromkeys(("date", "timestamp", "modtime"), "date"),
    "logical": "logical",
}
FILL_CHUNK = 40  # columns per aggregate query; wide tables take several scans
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

    The ADS driver names its result columns its own way (`columnname`, `typename`,
    `precision`, `scale`, ODBC 2 style) instead of ODBC 3's `column_name`/`type_name`,
    so rows are read by description name with both spellings accepted. Falls back to
    an empty SELECT's description.
    """
    def pick(r: dict[str, Any], *keys: str) -> Any:
        return next((r[k] for k in keys if r.get(k) is not None), None)

    out: list[dict[str, Any]] = []
    try:
        cur.columns(table=table)
        names = [d[0].lower() for d in cur.description]
        rows = [dict(zip(names, r)) for r in cur.fetchall()]
    except Exception:  # noqa: BLE001
        rows = []
    for pos, r in enumerate(rows, 1):
        col = str(pick(r, "column_name", "columnname") or "").strip()
        if not col:
            out = []
            break
        sc = sys_cols.get(col.lower(), {})
        out.append({
            "name": col,
            "type": str(pick(r, "type_name", "typename") or "").strip(),
            "size": pick(r, "column_size", "precision"),
            "decimals": pick(r, "decimal_digits", "scale"),
            "nullable": bool(pick(r, "nullable")),
            "position": pick(r, "ordinal_position") or pos,
            "comment": _clean(sc.get("Comment")) or _clean(pick(r, "remarks")) or None,
        })
    if out:
        return out
    cur.execute(f"SELECT * FROM {ads.quote_ident(table)} WHERE 1 = 0")
    return [{"name": d[0], "type": getattr(d[1], "__name__", str(d[1])), "size": d[3],
             "decimals": d[5], "nullable": bool(d[6]), "position": pos, "comment": None}
            for pos, d in enumerate(cur.description, 1)]


def _parse_tables(spec: str) -> list[str]:
    """Comma-separated names, or @file with one name per line (# comments allowed)."""
    if spec.startswith("@"):
        text = Path(spec[1:]).read_text(encoding="utf-8-sig")
        items = [ln.split("#", 1)[0] for ln in text.splitlines()]
    else:
        items = spec.split(",")
    seen: dict[str, str] = {}
    for item in (i.strip() for i in items):
        if item and item.lower() not in seen:
            seen[item.lower()] = item
    return list(seen.values())


def _fill_exprs(col: dict[str, Any]) -> tuple[str, str | None, str]:
    """(non-null expression, value expression or None, rule label) for one column."""
    q = ads.quote_ident(col["name"])
    family = TYPE_FAMILY.get((col.get("type") or "").lower())
    if family is None:  # memo, blob, binary, time, ...: NULL test only
        return f"COUNT({q})", None, "not null"
    rule, tmpl = FILL_RULES[family]
    return f"COUNT({q})", tmpl.format(c=q), rule


def _fill_rates(cur, table: str, cols: list[dict[str, Any]], problems: list[str]) -> list[dict[str, Any]]:
    """Per-column non-NULL and value counts, computed by the server as aggregates.

    One query per FILL_CHUNK columns. A chunk the server rejects is retried column by
    column, and a column whose value test fails falls back to the NULL test alone.
    """
    def run(exprs: list[str]) -> list[Any]:
        cur.execute(f"SELECT {', '.join(exprs)} FROM {ads.quote_ident(table)}")
        return list(cur.fetchone())

    def as_int(v: Any) -> int:
        return int(v or 0)

    results: list[dict[str, Any]] = []
    for start in range(0, len(cols), FILL_CHUNK):
        chunk = cols[start:start + FILL_CHUNK]
        specs = [_fill_exprs(c) for c in chunk]
        exprs: list[str] = []
        for nn, val, _ in specs:
            exprs += [nn] + ([val] if val else [])
        try:
            vals = iter(run(exprs))
            per_col = [(as_int(next(vals)), as_int(next(vals)) if val else None, rule)
                       for _, val, rule in specs]
        except Exception as e:  # noqa: BLE001
            problems.append(f"fill({table}) chunk at column {start + 1}: {str(e)[:160]}; "
                            "retrying per column")
            per_col = []
            for c, (nn, val, rule) in zip(chunk, specs):
                try:
                    got = run([nn] + ([val] if val else []))
                    per_col.append((as_int(got[0]), as_int(got[1]) if val else None, rule))
                except Exception:  # noqa: BLE001
                    try:
                        per_col.append((as_int(run([nn])[0]), None, "not null"))
                    except Exception as e2:  # noqa: BLE001
                        problems.append(f"fill({table}.{c['name']}): {str(e2)[:160]}")
                        per_col.append((None, None, "failed"))
        for c, (non_null, filled, rule) in zip(chunk, per_col):
            results.append({"column": c["name"], "type": c.get("type"),
                            "non_null": non_null,
                            "filled": filled if filled is not None else non_null,
                            "rule": rule})
    return results


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
    missing: list[str] = []
    if args.tables:
        wanted = _parse_tables(args.tables)
        by_lower = {n.lower(): n for n in names}
        missing = [w for w in wanted if w.lower() not in by_lower]
        names = [by_lower[w.lower()] for w in wanted if w.lower() in by_lower]
        print(f"  {len(names)} of {len(wanted)} requested tables found"
              + (f"; not in this dictionary: {', '.join(missing)}" if missing else ""), flush=True)

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
        started = datetime.now()
        if not args.no_counts or args.fill:
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
        if args.fill and cols:
            obj["fill"] = (_fill_rates(cur, name, cols, problems) if row_count else
                           [{"column": c["name"], "type": c.get("type"), "non_null": 0,
                             "filled": 0, "rule": "empty table"} for c in cols])
            obj["probe_seconds"] = round((datetime.now() - started).total_seconds(), 1)
            print(f"  {name}: {row_count if row_count is not None else '?'} rows, "
                  f"{len(cols)} columns, {obj['probe_seconds']} s", flush=True)
        if args.sample and obj["delta_candidates"] and row_count:
            col = ads.quote_ident(obj["delta_candidates"][0])
            try:
                cur.execute(f"SELECT MIN({col}), MAX({col}) FROM {ads.quote_ident(name)}")
                lo, hi = cur.fetchone()
                obj["delta_sample"] = {"min": str(lo), "max": str(hi)}
            except Exception as e:  # noqa: BLE001
                problems.append(f"sample({name}): {e}")
        objects.append(obj)
        if i % 50 == 0 and not args.fill:
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
        "tables_missing": missing,
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
    if data.get("tables_missing"):
        print(f"\nNot in this dictionary ({len(data['tables_missing'])}): "
              + ", ".join(data["tables_missing"]))
    filled = [o for o in objs if o.get("fill")]
    if filled:
        secs = sum(o.get("probe_seconds") or 0 for o in filled)
        used = sum(1 for o in filled for fc in o["fill"] if fc["filled"])
        cols = sum(len(o["fill"]) for o in filled)
        print(f"Fill:      {used} of {cols} columns carry values across {len(filled)} tables "
              f"({secs:.0f} s of queries)")


def write_fill_csv(data: dict[str, Any], path: Path) -> None:
    """One row per column: counts and percentages only, never values."""
    import csv
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["table", "rows", "column", "position", "type", "non_null", "filled",
                     "filled_pct", "rule"])
        for o in data["objects"]:
            rows = o.get("row_count")
            for pos, fc in enumerate(o.get("fill") or [], 1):
                pct = (round(100 * fc["filled"] / rows, 1)
                       if rows and fc["filled"] is not None else "")
                w.writerow([o["name"], rows, fc["column"], pos, fc["type"], fc["non_null"],
                            fc["filled"], pct, fc["rule"]])
        for name in data.get("tables_missing") or []:
            w.writerow([name, "", "", "", "", "", "", "", "table not in this dictionary"])


def upload_results(paths: list[Path], label: str, container: str) -> None:
    """Copy the probe files to <container>/_probe/<label>/<UTC time>/ with the .env SAS."""
    from datetime import timezone
    from common import azure

    url = os.getenv("AZURE_STORAGE_URL")
    sas = os.getenv("AZURE_STORAGE_SAS_TOKEN")
    if not url or not sas:
        print("[WARN] --upload: AZURE_STORAGE_URL / AZURE_STORAGE_SAS_TOKEN not set; skipped")
        return
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    safe = re.sub(r"[^A-Za-z0-9._-]+", "-", label).strip("-") or "ads"
    base = f"{url.rstrip('/')}/{container}/_probe/{safe}/{stamp}"
    for path in paths:
        azure.upload_file(path, f"{base}/{path.name}", sas)
    print(f"Uploaded {len(paths)} file(s) to {container}/_probe/{safe}/{stamp}/")


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
        "  # Binary columns: text = decode text-like values, leave images/compressed data empty;",
        "  # base64 = keep all as base64 strings; keep = raw bytes.",
        "  blob_handling: text",
        "  blob_encoding: cp1252",
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
    p.add_argument("--tables", help="Only these tables: comma-separated, or @file with one per line")
    p.add_argument("--fill", metavar="CSV", help="Write per-column fill rates (counts only) here")
    p.add_argument("--upload", action="store_true",
                   help="Copy --out/--fill files to <container>/_probe/<source>/<time>/ "
                        "using AZURE_STORAGE_URL and AZURE_STORAGE_SAS_TOKEN from .env")
    p.add_argument("--container", default="warehouse", help="Container for --upload")
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
    if args.sample and args.upload:
        p.error("--sample reads MIN/MAX values; not combined with --upload")

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
    written: list[Path] = []
    if args.out:
        Path(args.out).write_text(json.dumps(data, indent=2, default=str, ensure_ascii=False),
                                  encoding="utf-8")
        print(f"Full schema written to {args.out}")
        written.append(Path(args.out))
    if args.fill:
        write_fill_csv(data, Path(args.fill))
        print(f"Fill rates written to {args.fill}")
        written.append(Path(args.fill))
    if args.upload:
        if not written:
            p.error("--upload needs --out and/or --fill")
        label = args.source_name or Path(str(conn_cfg["data_directory"])).stem
        upload_results(written, label, args.container)
    return 0


if __name__ == "__main__":
    sys.exit(main())
