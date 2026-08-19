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

from common import mssql
from common.config import ObjectSpec
from common.source import Source

FETCH_BATCH_SIZE = 10_000


class MSSQLSource(Source):
    def authenticate(self) -> None:
        self.conn, self.placeholder = mssql.connect(self.config.connection)

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
        # Static per-object predicate from config, e.g. a history cutoff
        # ("[budat] >= '2024-01-01'"). Authored by us, not user input, so it is
        # inlined as-is — never build it from anything a caller supplies.
        static = obj.extra.get("where")
        if static:
            clauses.append(f"({static})")
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
