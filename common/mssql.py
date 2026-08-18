"""Shared SQL Server connection handling.

Lives in common/ rather than in the source template so that a fix here reaches every
source folder copied from `sources/_example_mssql/`, instead of having to be applied
to each one.
"""
from __future__ import annotations

from typing import Any

DEFAULT_DRIVER = "ODBC Driver 18 for SQL Server"
DEFAULT_PORT = 1433
DEFAULT_LOGIN_TIMEOUT = 30


def quote_odbc(value: Any) -> str:
    """Brace-quote a value for an ODBC connection string.

    Required for anything that may contain `;`, which otherwise terminates the field:
    a password of `p@ss;word` would be sent as `p@ss`, and the remainder parsed as a
    bogus keyword. Closing braces are doubled, per the ODBC connection-string grammar.
    """
    return "{" + str(value).replace("}", "}}") + "}"


def odbc_connection_string(c: dict[str, Any]) -> str:
    """Build a DSN-less connection string from a source's `connection` block."""
    parts = [
        f"DRIVER={quote_odbc(c.get('driver', DEFAULT_DRIVER))}",
        f"SERVER={c['host']},{c.get('port', DEFAULT_PORT)}",
        f"DATABASE={quote_odbc(c['database'])}",
        f"UID={quote_odbc(c['username'])}",
        f"PWD={quote_odbc(c['password'])}",
        f"Encrypt={'yes' if c.get('encrypt', True) else 'no'}",
    ]
    if c.get("trust_server_certificate", False):
        parts.append("TrustServerCertificate=yes")
    return ";".join(parts)


def connect(c: dict[str, Any]) -> tuple[Any, str]:
    """Open a connection per `connection.library`.

    Returns (connection, paramstyle placeholder) — pymssql uses %s, pyodbc uses ?.
    Imports are deferred so the mssql dependency group is only needed when an MSSQL
    source is actually configured.
    """
    library = c.get("library", "pymssql")
    timeout = int(c.get("login_timeout", DEFAULT_LOGIN_TIMEOUT))

    if library == "pymssql":
        import pymssql

        return pymssql.connect(
            server=c["host"],
            port=int(c.get("port", DEFAULT_PORT)),
            database=c["database"],
            user=c["username"],
            password=c["password"],
            login_timeout=timeout,
        ), "%s"

    if library == "pyodbc":
        import pyodbc

        return pyodbc.connect(odbc_connection_string(c), timeout=timeout), "?"

    raise ValueError(f"Unknown connection.library {library!r} (use 'pymssql' or 'pyodbc')")
