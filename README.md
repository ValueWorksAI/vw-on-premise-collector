# vw-on-prem-collector

Generic on-premise data collection framework. Each data source is a self-contained "push" under `sources/`. The orchestrator runs all sources, uploads outputs to Azure Blob Storage.

## Layout

```
common/             shared utilities (config, azure upload, parquet, delta, secrets, logging, retry)
sources/
  _example/         template for new sources
  diamant/          Diamant ERP OData source
tools/              maintenance & validation scripts (compare_runs, etc.)
orchestrator.py     entry point: discovers sources, runs them, aggregates results
run.ps1             thin PowerShell launcher for Task Scheduler
```

## Setup (customer PC)

1. Install Python 3.10–3.12 (pyarrow has no 3.13/3.14 wheels yet).
2. Install Poetry: `pip install poetry`
3. Install AzCopy v10 — required for uploads.
   - Download: <https://aka.ms/downloadazcopy-v10-windows>
   - Extract `azcopy.exe` somewhere (default expected path: `C:\AzCopy\azcopy.exe`).
   - Override with `AZCOPY_PATH` in `.env` if installed elsewhere.
4. `cd C:\vw-on-prem-collector && poetry install`
5. Copy `.env.example` to `.env` and fill in real secrets.
6. Schedule `run.ps1` in Task Scheduler.

## Running

```
poetry run python orchestrator.py --mode delta
poetry run python orchestrator.py --mode full --sources diamant
poetry run python orchestrator.py --mode delta --parallel --max-workers 3
```

## Adding a new source

1. Copy `sources/_example/` to `sources/<name>/`.
2. Edit `config.yaml` (azure target, partitions, objects, delta settings).
3. Implement the 3 required methods in `push.py`: `authenticate`, `build_filter`, `fetch`.
4. Optionally override `transform_record` and `list_partitions`.
5. Drop secrets into `.env` using the convention `<NAME>_<KEY>`.

The orchestrator auto-discovers any folder under `sources/` containing both `config.yaml` and `push.py`.
