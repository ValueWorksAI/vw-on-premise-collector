"""Shared Advantage Database Server (ADS) connection handling.

ADS keeps every table as its own file (.adt/.adi/.adm, or .dbf/.cdx) next to a data
dictionary (.add) that lists the tables, users, rights, indexes and relations. A
connection names the dictionary file; the ADS *server* opens the table files on
our behalf.

Always connect through the server (`ServerTypes=2`, remote). The Advantage Local
Server — a client-side engine that opens the table files directly — must never touch
files a running ADS server is serving: the two use different locking, and mixing them
risks inconsistent reads and index corruption on the customer's live database.

Lives in common/ so a fix reaches every source copied from `sources/_example_ads/`.
"""
from __future__ import annotations

from typing import Any

DEFAULT_DRIVER = "Advantage StreamlineSQL ODBC"
REMOTE_SERVER = 2  # ServerTypes bitmask: 1 local, 2 remote, 4 internet
DEFAULT_LOGIN_TIMEOUT = 30


def _odbc_value(value: Any) -> str:
    """Brace-quote only when needed.

    ODBC allows `{...}` around any value, but a value that does not contain `;` or start
    with `{` is passed through untouched — the safest choice for driver-specific keys
    such as DataDirectory, whose parsing we cannot test on every driver build.
    """
    s = str(value)
    if ";" in s or s.startswith("{") or s != s.strip():
        return "{" + s.replace("}", "}}") + "}"
    return s


def connection_string(c: dict[str, Any]) -> str:
    """Build a DSN-less ADS connection string from a source's `connection` block.

    Keys:
      data_directory  path to the .add dictionary as the ADS server sees it, e.g.
                      \\\\SERVER\\X2\\Daten\\datenbank.add, or a local path when the
                      collector runs on the database server itself
      username / password
      driver          default "Advantage StreamlineSQL ODBC"
      server_types    default 2 (remote only); never include 1 against a live database
      char_type       optional, e.g. ANSI or OEM, if umlauts come out wrong
      odbc_extra      optional raw "Key=Value;Key=Value" appended verbatim
    """
    server_types = int(c.get("server_types", REMOTE_SERVER))
    if server_types & 1:
        raise ValueError("server_types includes 1 (Advantage Local Server); refusing — "
                         "it would open the live table files directly. Use 2.")
    parts = [
        f"Driver={{{c.get('driver', DEFAULT_DRIVER)}}}",
        f"DataDirectory={_odbc_value(c['data_directory'])}",
        f"ServerTypes={server_types}",
        f"UID={_odbc_value(c['username'])}",
        f"PWD={_odbc_value(c['password'])}",
        "TrimTrailingSpaces=TRUE",
    ]
    if c.get("char_type"):
        parts.append(f"CharType={c['char_type']}")
    extra = c.get("odbc_extra")
    if extra:
        parts.append(str(extra).strip(";"))
    return ";".join(parts) + ";"


def connect(c: dict[str, Any]):
    """Open a pyodbc connection. Placeholder style is `?`."""
    import pyodbc

    timeout = int(c.get("login_timeout", DEFAULT_LOGIN_TIMEOUT))
    conn = pyodbc.connect(connection_string(c), timeout=timeout, autocommit=True)
    return conn


def quote_ident(name: str) -> str:
    """Quote a table or column name for ADS SQL.

    Names in X2 contain spaces and umlauts in places, so everything is quoted. ADS
    accepts double quotes; an embedded quote is doubled.
    """
    return '"' + str(name).replace('"', '""') + '"'
