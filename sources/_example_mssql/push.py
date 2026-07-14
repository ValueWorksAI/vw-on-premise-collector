"""Microsoft SQL Server source template. Copy to sources/<your-source>/ and edit config.yaml.

Requires the `mssql` dependency group:
  poetry install --with mssql

Two client libraries are supported via `connection.library`:
  - pymssql (default): self-contained wheel bundling FreeTDS — no system packages
    required. Recommended, especially on distros where msodbcsql18 is unavailable.
  - pyodbc: uses the Microsoft ODBC driver (msodbcsql18), which must be installed
    on the host.

`endpoint` in config.yaml is the schema-qualified table/view name; the optional
`extra.columns` list restricts the SELECT to specific columns. `timestamp_field`
must be a datetime column that is updated on every row change (enables delta
refresh); leave it null for tables without one — they full-refresh every run.
"""
from __future__ import annotations

from typing import Any, Iterable

from common.config import ObjectSpec
from common.source import Source

FETCH_BATCH_SIZE = 10_000


class MSSQLSource(Source):
    def authenticate(self) -> None:
        c = self.config.connection
        library = c.get("library", "pymssql")
        if library == "pymssql":
            import pymssql  # deferred import: only needed when an MSSQL source is configured

            self.placeholder = "%s"
            self.conn = pymssql.connect(
                server=c["host"],
                port=int(c.get("port", 1433)),
                database=c["database"],
                user=c["username"],
                password=c["password"],
                login_timeout=int(c.get("login_timeout", 30)),
            )
        elif library == "pyodbc":
            import pyodbc

            self.placeholder = "?"
            parts = [
                f"DRIVER={{{c.get('driver', 'ODBC Driver 18 for SQL Server')}}}",
                f"SERVER={c['host']},{c.get('port', 1433)}",
                f"DATABASE={c['database']}",
                f"UID={c['username']}",
                f"PWD={c['password']}",
                f"Encrypt={'yes' if c.get('encrypt', True) else 'no'}",
            ]
            if c.get("trust_server_certificate", False):
                parts.append("TrustServerCertificate=yes")
            self.conn = pyodbc.connect(";".join(parts), timeout=int(c.get("login_timeout", 30)))
        else:
            raise ValueError(f"Unknown connection.library {library!r} (use 'pymssql' or 'pyodbc')")

    def build_filter(self, obj: ObjectSpec, partition: Any | None,
                     lower: str | None, upper: str | None) -> tuple[str, list[Any]]:
        """Return (where_sql, params) — always parameterized, never inlined values."""
        ph = self.placeholder
        clauses: list[str] = []
        params: list[Any] = []
        if obj.timestamp_field and lower:
            clauses.append(f"[{obj.timestamp_field}] > {ph} AND [{obj.timestamp_field}] <= {ph}")
            params += [lower, upper]
        if partition is not None and self.config.partition_field:
            clauses.append(f"[{self.config.partition_field}] = {ph}")
            params.append(partition)
        return " AND ".join(clauses), params

    def fetch(self, obj: ObjectSpec, partition: Any | None,
              filter_: tuple[str, list[Any]]) -> Iterable[dict]:
        where, params = filter_
        columns = obj.extra.get("columns")
        select = ", ".join(f"[{c}]" for c in columns) if columns else "*"
        sql = f"SELECT {select} FROM {obj.endpoint}"
        if where:
            sql += f" WHERE {where}"

        cursor = self.conn.cursor()
        try:
            cursor.execute(sql, tuple(params))
            col_names = [d[0] for d in cursor.description]
            while True:
                rows = cursor.fetchmany(FETCH_BATCH_SIZE)
                if not rows:
                    break
                for row in rows:
                    yield dict(zip(col_names, row))
        finally:
            cursor.close()


SOURCE_CLASS = MSSQLSource
