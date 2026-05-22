"""Scan a local output directory for parquet/jsonl files and report row counts,
columns, and unique company/firma/mandant values per file.

Usage: poetry run python tools/check_partitions.py C:\\output\\diamant
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd


def main() -> int:
    if len(sys.argv) < 2:
        print("Usage: check_partitions.py <output_dir>")
        return 2
    out = Path(sys.argv[1])
    if not out.exists():
        print(f"Directory does not exist: {out}")
        return 1

    files = list(out.glob("**/*.parquet")) + list(out.glob("**/*.jsonl"))
    if not files:
        print("No parquet/jsonl files found")
        return 1

    for f in files:
        print(f"\n{'=' * 80}\n{f.relative_to(out)}\n{'=' * 80}")
        try:
            df = pd.read_parquet(f) if f.suffix == ".parquet" else pd.read_json(f, lines=True)
        except Exception as e:  # noqa: BLE001
            print(f"  ERROR: {e}")
            continue
        print(f"records={len(df):,} columns={len(df.columns)}")
        partition_cols = [c for c in df.columns if any(k in c.lower() for k in ("company", "firma", "mandant"))]
        for col in partition_cols:
            vc = df[col].value_counts().sort_index()
            print(f"  {col}: {len(vc)} unique")
            for val, cnt in vc.head(20).items():
                print(f"    {val}: {cnt:,}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
