"""Dump a SQL Server schema to JSON so a source's config.yaml can be written offline.

Read-only. Row counts come from sys.partitions metadata (no COUNT(*) scans), so it
is safe to run against a production instance. Only objects the login can actually
see are listed — which makes this a permission check as well as a schema dump.

Run it on the customer machine, hand back the JSON, and the objects: block for
config.yaml can be generated without further access.

Usage:
  # Which database engines does this machine even have drivers for? (no connection)
  poetry run python tools/probe_mssql.py --drivers

  # Schema dump
  poetry run python tools/probe_mssql.py --host abasbi1 --database abas_bi \
      --user vw_readonly --out abasbi1-schema.json

  # ...plus the actual date range of each delta candidate column (runs MIN/MAX queries)
  poetry run python tools/probe_mssql.py --host abasbi1 --database abas_bi \
      --user vw_readonly --sample --out abasbi1-schema.json

Password comes from --password, else the MSSQL_PASSWORD env var, else a prompt.
"""
from __future__ import annotations

import argparse
import getpass
import json
import os
import sys
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from common import mssql  # noqa: E402

# Types usable as `timestamp_field` (the collector filters with `col > ? AND col <= ?`).
DELTA_TYPES = {"datetime", "datetime2", "smalldatetime", "datetimeoffset", "date"}
# Changes on every row update but cannot be compared to a timestamp — needs custom code.
ROWVERSION_TYPES = {"timestamp", "rowversion"}
# Column-name fragments that suggest a last-changed column, best first.
NAME_HINTS = ("lastchange", "last_change", "lastmodif", "modified", "changed", "updated",
              "aenderung", "geaendert", "letzteaenderung", "mutation", "changedate",
              "updatedat", "modifiedat", "timestamp", "chgdate")


def list_drivers() -> int:
    """Print installed ODBC drivers — tells you what a mystery product runs on."""
    try:
        import pyodbc
    except ImportError:
        print("pyodbc is not installed. Run: poetry install --with mssql")
        return 2
    drivers = pyodbc.drivers()
    if not drivers:
        print("No ODBC drivers installed.")
        return 1
    print(f"{len(drivers)} ODBC driver(s) installed on this machine:\n")
    for d in drivers:
        low = d.lower()
        tag = ""
        if "sql server" in low:
            tag = "  <- Microsoft SQL Server: use library pymssql or pyodbc"
        elif any(k in low for k in ("pervasive", "btrieve", "advantage", "actian")):
            tag = "  <- Pervasive/Btrieve/Advantage: needs a generic-ODBC source, not the MSSQL template"
        elif "mysql" in low or "mariadb" in low:
            tag = "  <- MySQL/MariaDB: needs a generic-ODBC source"
        elif "oracle" in low:
            tag = "  <- Oracle: needs a generic-ODBC source"
        print(f"  - {d}{tag}")
    return 0


def connect(args: argparse.Namespace, password: str):
    """Delegates to common.mssql so this probe tests the exact code path a source uses."""
    return mssql.connect({
        "library": args.library, "host": args.host, "port": args.port,
        "database": args.database, "username": args.user, "password": password,
        "driver": args.driver, "encrypt": args.encrypt,
        "trust_server_certificate": args.trust_server_certificate,
        "login_timeout": args.login_timeout,
    })[0]


def _rows(conn, sql: str, params: tuple = ()) -> list[tuple]:
    cur = conn.cursor()
    try:
        cur.execute(sql, params) if params else cur.execute(sql)
        return cur.fetchall()
    finally:
        cur.close()


def rank_delta_candidates(columns: list[dict]) -> list[str]:
    """Usable delta columns, most likely first (name hint beats declaration order)."""
    usable = [c for c in columns if c["data_type"].lower() in DELTA_TYPES]

    def score(col: dict) -> tuple[int, int]:
        low = col["name"].lower().replace("_", "")
        for i, hint in enumerate(NAME_HINTS):
            if hint.replace("_", "") in low:
                return (i, col["position"])
        return (len(NAME_HINTS), col["position"])

    return [c["name"] for c in sorted(usable, key=score)]


def clock_domain(conn) -> dict:
    """Compare the server clock with the collector host clock.

    Delta refresh builds its window from the collector's local wall clock and compares
    it against database-native timestamp values. If the two clocks sit in different
    time zones, the window is offset and rows changed inside the offset are skipped
    permanently, because the watermark still advances. So capture it at onboarding.
    """
    server_now, server_utc = _rows(conn, "SELECT SYSDATETIME(), GETUTCDATE()")[0]
    local_now = datetime.now()
    try:
        tz = _rows(conn, "SELECT CURRENT_TIMEZONE_ID()")[0][0]  # SQL Server 2019+
    except Exception:  # noqa: BLE001
        tz = None
    return {
        "server_local": str(server_now),
        "server_utc": str(server_utc),
        "server_timezone": tz,
        "collector_local": local_now.isoformat(),
        "skew_seconds": round((local_now - server_now).total_seconds()),
        "server_is_utc": abs((server_now - server_utc).total_seconds()) < 60,
    }



def view_sources(conn) -> tuple[dict, dict]:
    """Return (definitions, dependencies) for every view.

    Views are the customer's to change, so a pipeline built on them is fragile. To
    rebuild their logic on our side we need the SQL text; to know what to collect we
    need the base tables. sys.sql_expression_dependencies gives the latter reliably
    even when the view renames every column, which defeats name-matching.

    Both need VIEW DEFINITION permission — db_datareader alone is not enough, so this
    can come back empty against a least-privilege login. That is reported, not fatal.
    """
    definitions: dict[str, str] = {}
    dependencies: dict[str, list[str]] = {}
    try:
        for schema, name, sql in _rows(conn, """
            SELECT OBJECT_SCHEMA_NAME(m.object_id), OBJECT_NAME(m.object_id), m.definition
            FROM sys.sql_modules m JOIN sys.views v ON v.object_id = m.object_id
        """):
            if sql:
                definitions[name] = sql if isinstance(sql, str) else sql.decode("utf-8", "replace")
    except Exception as e:  # noqa: BLE001
        print(f"  [WARN] could not read view definitions: {type(e).__name__}: {e}")
    try:
        for name, ref_schema, ref_name in _rows(conn, """
            SELECT OBJECT_NAME(d.referencing_id), d.referenced_schema_name, d.referenced_entity_name
            FROM sys.sql_expression_dependencies d
            JOIN sys.views v ON v.object_id = d.referencing_id
        """):
            if name and ref_name:
                dependencies.setdefault(name, [])
                ref = f"{ref_schema or 'dbo'}.{ref_name}"
                if ref not in dependencies[name]:
                    dependencies[name].append(ref)
    except Exception as e:  # noqa: BLE001
        print(f"  [WARN] could not read view dependencies: {type(e).__name__}: {e}")
    return definitions, dependencies


def probe(conn, args: argparse.Namespace) -> dict:
    server_info = _rows(conn, "SELECT @@VERSION, DB_NAME(), SUSER_NAME(), "
                              "CAST(SERVERPROPERTY('Collation') AS nvarchar(128))")[0]
    result: dict = {
        "server_version": str(server_info[0]).split("\n")[0].strip(),
        "database": server_info[1],
        "login": server_info[2],
        "collation": server_info[3],
        "clock": clock_domain(conn),
        "objects": [],
    }

    objects: dict[tuple[str, str], dict] = {}
    for schema, name, rowcount in _rows(conn, """
        SELECT s.name, t.name, SUM(p.rows)
        FROM sys.tables t
        JOIN sys.schemas s ON s.schema_id = t.schema_id
        JOIN sys.partitions p ON p.object_id = t.object_id AND p.index_id IN (0, 1)
        GROUP BY s.name, t.name
    """):
        objects[(schema, name)] = {"schema": schema, "name": name, "type": "TABLE",
                                   "row_count": int(rowcount or 0), "columns": []}
    for schema, name in _rows(conn, """
        SELECT s.name, v.name FROM sys.views v
        JOIN sys.schemas s ON s.schema_id = v.schema_id
    """):
        objects[(schema, name)] = {"schema": schema, "name": name, "type": "VIEW",
                                   "row_count": None, "columns": []}

    for schema, table, col, dtype, nullable, maxlen, pos in _rows(conn, """
        SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE, IS_NULLABLE,
               CHARACTER_MAXIMUM_LENGTH, ORDINAL_POSITION
        FROM INFORMATION_SCHEMA.COLUMNS
        ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION
    """):
        obj = objects.get((schema, table))
        if obj is not None:
            obj["columns"].append({"name": col, "data_type": dtype, "nullable": nullable == "YES",
                                   "max_length": maxlen, "position": pos})

    definitions, dependencies = view_sources(conn)
    for o in objects.values():
        if o["type"] == "VIEW":
            o["definition"] = definitions.get(o["name"])
            o["depends_on"] = dependencies.get(o["name"], [])

    selected = [
        o for o in objects.values()
        if not args.include or args.include.lower() in f"{o['schema']}.{o['name']}".lower()
    ]
    for obj in selected:
        obj["delta_candidates"] = rank_delta_candidates(obj["columns"])
        obj["rowversion_columns"] = [c["name"] for c in obj["columns"]
                                     if c["data_type"].lower() in ROWVERSION_TYPES]
        if args.sample and obj["type"] == "TABLE" and obj["delta_candidates"] and obj["row_count"]:
            col = obj["delta_candidates"][0]
            try:
                lo, hi = _rows(conn, f"SELECT MIN([{col}]), MAX([{col}]) "
                                     f"FROM [{obj['schema']}].[{obj['name']}]")[0]
                obj["delta_sample"] = {"column": col, "min": str(lo), "max": str(hi)}
            except Exception as e:  # noqa: BLE001
                obj["delta_sample"] = {"column": col, "error": f"{type(e).__name__}: {e}"}

    result["objects"] = sorted(selected, key=lambda o: (o["schema"], o["name"]))
    return result


def print_summary(data: dict) -> None:
    objects = data["objects"]
    tables = [o for o in objects if o["type"] == "TABLE"]
    print(f"\nServer:    {data['server_version']}")
    print(f"Database:  {data['database']}  (login: {data['login']})")
    print(f"Visible:   {len(tables)} tables, {len(objects) - len(tables)} views")

    clock = data.get("clock") or {}
    skew = clock.get("skew_seconds")
    print(f"Clock:     server {clock.get('server_local')} "
          f"(tz {clock.get('server_timezone') or 'unknown'}"
          f"{', UTC' if clock.get('server_is_utc') else ''}), "
          f"collector offset {skew:+}s" if skew is not None else "Clock:     unknown")
    if skew is not None and abs(skew) > 60:
        print(f"  [WARN] the collector host clock is {abs(skew)}s ({abs(skew) / 3600:.1f}h) "
              f"{'ahead of' if skew > 0 else 'behind'} the database clock.")
        print("         Delta refresh compares a window built from the collector's local")
        print("         clock against database timestamps, so rows changed inside that")
        print("         offset would be skipped permanently. Report this before the")
        print("         source config is written.")
    print()

    views = [o for o in objects if o["type"] == "VIEW"]
    if views:
        with_def = sum(1 for v in views if v.get("definition"))
        with_dep = sum(1 for v in views if v.get("depends_on"))
        print(f"Views:     {len(views)} — {with_def} with SQL text, {with_dep} with base-table info")
        if not with_def and not with_dep:
            print("  [WARN] no view internals readable. To collect base tables instead of views,")
            print("         the login needs: GRANT VIEW DEFINITION ON SCHEMA::dbo TO <login>;")
        else:
            for v in sorted(views, key=lambda x: -len(x.get("depends_on") or []))[:10]:
                dep = ", ".join(v.get("depends_on") or []) or "(none reported)"
                print(f"    {v['name']:<40}-> {dep[:90]}")
        print()

    print(f"{'OBJECT':<50} {'ROWS':>12}  DELTA CANDIDATE")
    print("-" * 90)
    for o in sorted(objects, key=lambda x: -(x["row_count"] or 0)):
        rows = f"{o['row_count']:,}" if o["row_count"] is not None else "(view)"
        candidate = o["delta_candidates"][0] if o["delta_candidates"] else "-- none, full refresh"
        sample = o.get("delta_sample", {})
        if sample.get("max"):
            candidate += f"  (max {sample['max'][:19]})"
        print(f"{o['schema'] + '.' + o['name']:<50} {rows:>12}  {candidate}")
        if o["rowversion_columns"] and not o["delta_candidates"]:
            print(f"{'':<50} {'':>12}  has rowversion {o['rowversion_columns']} — custom delta possible")


def build_config(data: dict, min_rows: int, source_name: str, enable_delta: bool = False) -> str:
    """Render a complete, ready-to-use config.yaml for this database.

    Emitting the whole file (not just the objects: block) means the customer can
    assemble a working source locally and does not have to hand over a full schema
    dump. Env var names follow the <SOURCE>_MSSQL_<KEY> convention.
    """
    prefix = source_name.upper().replace("-", "_")
    lines = [
        f"# Generated by tools/probe_mssql.py from {data['database']} on"
        f" {data['server_version'].split('(')[0].strip()}.",
        "# Review before use: check the table selection.",
        "#",
        "# Every object starts on full refresh (timestamp_field: null). Delta is opt-in per",
        "# object: set timestamp_field to the candidate noted in its comment, but only once",
        "# the volumes are known to need it AND the database clock domain has been checked",
        "# (see the Clock line this tool prints). Full refresh cannot be tripped up by a",
        "# clock offset; delta silently can be."
        if not enable_delta else
        "# Review before use: check the table selection and each timestamp_field.",
        f"name: {source_name}",
        f"output_dir: C:\\output\\{source_name}",
        "max_workers: 1",
        "",
        "azure:",
        "  storage_url: ${env:AZURE_STORAGE_URL}",
        "  sas_token:   ${env:AZURE_STORAGE_SAS_TOKEN}",
        "  container:   warehouse",
        f"  # Shadow prefix first; switch to raw/{source_name} once the data is confirmed.",
        f"  prefix:      raw/{source_name}-shadow",
        "",
        "partition_field: null",
        "partitions: []",
        "",
        "connection:",
        "  # pymssql needs no system packages; switch to pyodbc only if it cannot connect",
        "  # (pyodbc additionally requires the msodbcsql18 driver on the host).",
        "  library: pymssql",
        f"  host:     ${{env:{prefix}_MSSQL_HOST}}",
        f"  port:     ${{env:{prefix}_MSSQL_PORT}}",
        f"  database: ${{env:{prefix}_MSSQL_DATABASE}}",
        f"  username: ${{env:{prefix}_MSSQL_USERNAME}}",
        f"  password: ${{env:{prefix}_MSSQL_PASSWORD}}",
        "",
        "objects:",
    ]
    skipped = 0
    for o in data["objects"]:
        if o["type"] == "TABLE" and (o["row_count"] or 0) < min_rows:
            skipped += 1
            continue
        ts = o["delta_candidates"][0] if o["delta_candidates"] else None
        rows = f"{o['row_count']:,} rows" if o["row_count"] is not None else "view"
        lines.append(f"  # {rows}")
        lines.append(f"  - name: {o['name']}")
        lines.append(f"    endpoint: {o['schema']}.{o['name']}")
        if ts and enable_delta:
            lines.append(f"    timestamp_field: {ts}")
        elif ts:
            lines.append(f"    timestamp_field: null   # delta candidate: {ts}")
        else:
            lines.append("    timestamp_field: null   # no datetime column available")
        lines.append("    partition_scoped: false")
    if skipped:
        lines.insert(2, f"# {skipped} table(s) below {min_rows} row(s) were omitted.")
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Dump a SQL Server schema for config.yaml authoring")
    parser.add_argument("--drivers", action="store_true",
                        help="List installed ODBC drivers and exit (no connection needed)")
    parser.add_argument("--host")
    parser.add_argument("--port", default=1433)
    parser.add_argument("--database")
    parser.add_argument("--user")
    parser.add_argument("--password", help="Omit to read MSSQL_PASSWORD or prompt")
    parser.add_argument("--library", choices=["pymssql", "pyodbc"], default="pymssql")
    parser.add_argument("--driver", default="ODBC Driver 18 for SQL Server", help="pyodbc only")
    parser.add_argument("--encrypt", action="store_true", default=True, help="pyodbc only")
    parser.add_argument("--trust-server-certificate", action="store_true", help="pyodbc only")
    parser.add_argument("--login-timeout", type=int, default=30)
    parser.add_argument("--include", help="Only objects whose schema.name contains this substring")
    parser.add_argument("--sample", action="store_true",
                        help="Also query MIN/MAX of each table's top delta candidate")
    parser.add_argument("--emit-config", metavar="PATH",
                        help="Write a complete ready-to-use config.yaml here")
    parser.add_argument("--source-name", default=None,
                        help="Source name used in the generated config (default: database name, lowercased)")
    parser.add_argument("--enable-delta", action="store_true",
                        help="Set timestamp_field to the detected candidate instead of starting "
                             "every object on full refresh (default: full refresh)")
    parser.add_argument("--min-rows", type=int, default=1,
                        help="With --emit-config: omit tables below this row count (default 1)")
    parser.add_argument("--out", help="Write the full schema JSON here")
    args = parser.parse_args()

    if args.drivers:
        return list_drivers()

    for required in ("host", "database", "user"):
        if not getattr(args, required):
            parser.error(f"--{required} is required (or use --drivers)")
    password = args.password or os.getenv("MSSQL_PASSWORD") or getpass.getpass("Password: ")

    print(f"Connecting to {args.host}:{args.port}/{args.database} as {args.user} via {args.library}...")
    try:
        conn = connect(args, password)
    except Exception as e:  # noqa: BLE001
        print(f"\nCONNECTION FAILED: {type(e).__name__}: {e}")
        print("Check: host/port reachable, SQL Server authentication enabled (not Windows-only),")
        print("       login exists and has db_datareader on the database, TCP/IP protocol enabled.")
        return 1

    try:
        data = probe(conn, args)
    finally:
        conn.close()

    print_summary(data)
    if args.emit_config:
        name = args.source_name or str(data["database"]).lower()
        Path(args.emit_config).write_text(
            build_config(data, args.min_rows, name, args.enable_delta), encoding="utf-8")
        print(f"Config written to {args.emit_config} — review the table selection before use.")
    if args.out:
        Path(args.out).write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")
        print(f"\nFull schema written to {args.out} — send this back to ValueWorks.")
    else:
        print("\nTip: add --out schema.json to save the full dump.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
