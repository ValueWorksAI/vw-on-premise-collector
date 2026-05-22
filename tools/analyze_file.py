"""Inspect a single parquet file: schema, row count, column samples.

Usage: poetry run python tools/analyze_file.py <path_to_parquet>
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd


def main() -> int:
    if len(sys.argv) < 2:
        print("Usage: analyze_file.py <parquet_path>")
        return 2
    fp = Path(sys.argv[1])
    if not fp.exists():
        print(f"File not found: {fp}")
        return 1

    df = pd.read_parquet(fp)
    print(f"records={len(df):,} columns={len(df.columns)}\n")
    print("Columns:")
    for c in df.columns:
        print(f"  - {c}")
    print("\nFirst 3 rows:")
    print(df.head(3).to_string())
    return 0


if __name__ == "__main__":
    sys.exit(main())
