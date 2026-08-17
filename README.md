# vw-on-prem-collector

Generic on-premise data collection framework. Each data source is a self-contained "push" under `sources/`. The orchestrator runs all sources, uploads outputs to Azure Blob Storage.

All traffic is **outbound only** (HTTPS, port 443, via AzCopy) — no inbound access to the customer network is required. The only firewall destination is `https://<account>.blob.core.windows.net`.

## Layout

```text
common/             shared utilities (config, azure upload, parquet, delta, secrets, logging, retry)
sources/
  _example/         template for new HTTP/API sources
  _example_mssql/   template for Microsoft SQL Server sources (pyodbc)
tools/              maintenance & validation scripts
orchestrator.py     entry point: discovers sources, runs them, aggregates results
run.ps1             thin PowerShell launcher for Windows Task Scheduler
run.sh              thin shell launcher for cron / systemd
```

Customer-specific source folders are never committed to this repo — only the framework and the `_example*/` templates. Real sources live on the customer machine.

## Setup — Windows

1. Install Python 3.10–3.14.
2. Install Poetry: `pip install poetry`
3. Install AzCopy v10 — required for uploads.
   - Download: <https://aka.ms/downloadazcopy-v10-windows>
   - Extract `azcopy.exe` somewhere (default expected path: `C:\AzCopy\azcopy.exe`).
   - Override with `AZCOPY_PATH` in `.env` if installed elsewhere.
4. `cd C:\vw-on-prem-collector && poetry install`
5. Copy `.env.example` to `.env` and fill in real secrets.
6. Schedule `run.ps1` in Task Scheduler (daily).

## Setup — Ubuntu / Linux

1. Install Python 3.10–3.14 and Poetry (via pipx — recent Ubuntu blocks pip installs into the system Python):

   ```bash
   sudo apt-get install -y python3 python3-venv pipx
   pipx install poetry && pipx ensurepath
   ```

2. Install AzCopy v10:

   ```bash
   curl -sSL https://aka.ms/downloadazcopy-v10-linux | sudo tar -xz -C /usr/local/bin --strip-components=1 --wildcards '*/azcopy'
   ```

   (`azcopy` on PATH is picked up automatically; otherwise set `AZCOPY_PATH` in `.env`.)

3. **MSSQL sources with `library: pyodbc` only** — install the Microsoft ODBC driver
   (not needed for the default `library: pymssql`, whose wheel bundles FreeTDS —
   use pymssql on distros where `msodbcsql18` is not yet available, e.g. brand-new
   Ubuntu releases):

   ```bash
   curl -sSL -O https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/packages-microsoft-prod.deb
   sudo dpkg -i packages-microsoft-prod.deb && rm packages-microsoft-prod.deb
   sudo apt-get update
   sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18 unixodbc-dev
   ```

4. Install dependencies:

   ```bash
   cd /opt/vw-on-prem-collector
   poetry install              # plain HTTP/API sources
   poetry install --with mssql # additionally pulls pyodbc for MSSQL sources
   ```

5. Copy `.env.example` to `.env` and fill in real secrets (`chmod 600 .env`).
6. Schedule a daily run, either via cron:

   ```text
   0 5 * * * /opt/vw-on-prem-collector/run.sh delta >> /opt/vw-on-prem-collector/logs/cron.log 2>&1
   ```

   or via a systemd timer:

   ```ini
   # /etc/systemd/system/vw-collector.service
   [Unit]
   Description=ValueWorks on-prem collector

   [Service]
   Type=oneshot
   WorkingDirectory=/opt/vw-on-prem-collector
   ExecStart=/opt/vw-on-prem-collector/run.sh delta

   # /etc/systemd/system/vw-collector.timer
   [Unit]
   Description=Daily ValueWorks collector run

   [Timer]
   OnCalendar=*-*-* 05:00:00
   Persistent=true

   [Install]
   WantedBy=timers.target
   ```

   ```bash
   sudo systemctl enable --now vw-collector.timer
   ```

Logs go to `C:\logs\vw-on-prem-collector` on Windows and `<repo>/logs` on Linux (override with `VW_LOG_DIR` in `.env`).

## Running

```bash
poetry run python orchestrator.py --mode delta
poetry run python orchestrator.py --mode full --sources mysource
poetry run python orchestrator.py --mode delta --parallel --max-workers 3
```

## Onboarding a new customer machine

Send the IT preparation guide to the customer's IT ahead of time — it lists the host
requirements, the read-only SQL login script, and the firewall/proxy questions.
Available in [German](docs/onboarding-it-admin.de.md) and
[English](docs/onboarding-it-admin.en.md); keep both in sync when editing.
Then, on the machine, in this order:

```bash
# 1. Is the SAS token right? (scope, permissions, expiry, List/Write/Read, real AzCopy upload)
poetry run python tools/check_azure.py --container warehouse --prefix raw/<name>

# 2. What does the source database look like? Writes a JSON dump to send back.
poetry run python tools/probe_mssql.py --host <server> --database <db> \
    --user vw_readonly --sample --emit-config --out <name>-schema.json

# 2b. If the product's database engine is unknown, this needs no connection at all:
poetry run python tools/probe_mssql.py --drivers
```

Run `check_azure.py` **first** — it surfaces proxy and TLS-interception problems, which are
the most common cause of a failing install, before any database work. The schema dump from
step 2 is enough to author `config.yaml` offline, so source selection does not need another
session with the customer.

## Adding a new source

1. Copy `sources/_example/` (HTTP/API) or `sources/_example_mssql/` (SQL Server) to `sources/<name>/`.
2. Edit `config.yaml` (azure target, connection, partitions, objects, delta settings).
3. For HTTP/API sources, implement the 3 required methods in `push.py`: `authenticate`, `build_filter`, `fetch`. The MSSQL template's `push.py` works as-is — configuration happens entirely in `config.yaml`.
4. Optionally override `transform_record` and `list_partitions`.
5. Drop secrets into `.env` using the convention `<NAME>_<KEY>`.

The orchestrator auto-discovers any folder under `sources/` containing both `config.yaml` and `push.py` (folders starting with `_` are skipped).

## Delta refresh

Objects with a `timestamp_field` run incrementally: each run reads its own latest META file from Azure and only fetches rows changed since the previous upper bound. Objects without a `timestamp_field` full-refresh every run. Because META files are read back from Azure, the SAS token needs **Read + List** in addition to **Create + Write** (container-scoped; no Delete required).
