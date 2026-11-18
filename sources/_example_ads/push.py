"""Advantage Database Server (ADS) source template. Copy to sources/<your-source>/ and
edit config.yaml.

Requires the `mssql` dependency group (it carries pyodbc) and the 64-bit
"Advantage StreamlineSQL ODBC" driver on the host:
  poetry install --with mssql

`endpoint` in config.yaml is the table name as listed in the data dictionary; the
optional `extra.columns` list restricts the SELECT. `timestamp_field` must be a
timestamp column updated on every row change; leave it null for a full refresh.
ADS stores local server time without a zone, so delta bounds are passed as naive
local datetimes — the collector and the database server must share a clock and
time zone before delta is switched on.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Iterable

from common import ads
from common.config import ObjectSpec
from common.source import Source

FETCH_BATCH_SIZE = 10_000


def _as_local_naive(iso: str) -> datetime:
    dt = datetime.fromisoformat(iso)
    return dt.astimezone().replace(tzinfo=None) if dt.tzinfo else dt


class ADSSource(Source):
    def authenticate(self) -> None:
        self.conn = ads.connect(self.config.connection)

    def build_filter(self, obj: ObjectSpec, partition: Any | None,
                     lower: str | None, upper: str | None) -> tuple[str, list[Any]]:
        """Return (where_sql, params) — always parameterized, never inlined values."""
        clauses: list[str] = []
        params: list[Any] = []
        if obj.timestamp_field and lower:
            col = ads.quote_ident(obj.timestamp_field)
            clauses.append(f"{col} > ? AND {col} <= ?")
            params += [_as_local_naive(lower), _as_local_naive(upper)]
        if partition is not None and self.config.partition_field:
            clauses.append(f"{ads.quote_ident(self.config.partition_field)} = ?")
            params.append(partition)
        # Static per-object predicate from config (authored by us, inlined as-is).
        static = obj.extra.get("where")
        if static:
            clauses.append(f"({static})")
        return " AND ".join(clauses), params

    def fetch(self, obj: ObjectSpec, partition: Any | None,
              filter_: tuple[str, list[Any]]) -> Iterable[dict]:
        where, params = filter_
        columns = obj.extra.get("columns")
        select = ", ".join(ads.quote_ident(c) for c in columns) if columns else "*"
        sql = f"SELECT {select} FROM {ads.quote_ident(obj.endpoint)}"
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


SOURCE_CLASS = ADSSource
