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

The collector runs as a daily scheduled task. Installation needs local admin rights;
the task itself should run under a dedicated service account.

### 1. Python 3.10–3.14

Install from <https://www.python.org/downloads/> and tick **"Add python.exe to PATH"**.

Do **not** use the Microsoft Store build. It installs into a per-user sandboxed
`WindowsApps` path that a scheduled task running as a different account cannot execute,
and it fails at run time rather than at install time — which makes it look like a
collector bug weeks later.

```powershell
python --version
```

### 2. Poetry

```powershell
pip install poetry
poetry --version
```

### 3. AzCopy v10

Download <https://aka.ms/downloadazcopy-v10-windows>, extract the ZIP, and copy
`azcopy.exe` to `C:\AzCopy\` (the default path the collector looks for; override with
`AZCOPY_PATH` in `.env`).

```powershell
C:\AzCopy\azcopy.exe --version
```

### 4. The collector

```powershell
cd C:\vw-on-prem-collector
poetry install                  # HTTP/API sources
poetry install --with mssql     # additionally for SQL Server sources
```

### 5. MSSQL sources with `library: pyodbc` only

Not needed for the default `library: pymssql`, whose wheel bundles FreeTDS. For pyodbc,
install the Microsoft ODBC driver from <https://aka.ms/odbc18> and confirm the name
matches `connection.driver` in `config.yaml`:

```powershell
Get-OdbcDriver | Where-Object Name -like "*SQL Server*" | Select-Object Name
```

To verify the pyodbc path end to end, run the probe against the same database with both
libraries and compare — `probe_mssql.py` connects through `common/mssql.py`, the exact
code path a source uses, so agreement means the source will connect too:

```powershell
poetry run python tools\probe_mssql.py --library pymssql --host <server> `
    --database <db> --user vw_readonly --out via-pymssql.json
poetry run python tools\probe_mssql.py --library pyodbc  --host <server> `
    --database <db> --user vw_readonly --driver "ODBC Driver 18 for SQL Server" `
    --out via-pyodbc.json
```

If pyodbc fails while pymssql succeeds, the driver name or the `encrypt` /
`trust_server_certificate` settings are wrong — not the network.

### 6. Configuration

Copy `.env.example` to `.env` and fill in real secrets, then restrict who can read it —
it holds the SAS token and database passwords:

```powershell
Copy-Item .env.example .env
icacls .env /inheritance:r /grant:r "Administrators:(R,W)" /grant:r "SYSTEM:(R)" `
            /grant:r "DOMAIN\svc_vwcollector:(R)"
```

Behind an outbound proxy, add it to `.env` rather than to the machine environment:

```text
HTTPS_PROXY=http://proxy.internal:8080
```

`.env` is loaded into the process environment before anything runs, so this reaches both
the REST calls and the AzCopy subprocess.

### 7. Verify before scheduling

Run these in order — each isolates one failure domain, so you never debug two at once:

```powershell
poetry run python tools\check_azure.py --container warehouse --prefix raw/<name>
poetry run python tools\probe_mssql.py --drivers
poetry run python orchestrator.py --mode delta --sources <name>
```

The third command only works once a source folder exists under `sources/`. On a fresh
install there is none, and the orchestrator exits `2` with "No sources to run" — that is
expected, not a failure. The first two commands are the ones that validate the install.

### 8. Scheduled task

The GUI works, but three settings are easy to miss and each causes a task that silently
never runs. This sets them correctly:

```powershell
$repo = "C:\vw-on-prem-collector"
$action = New-ScheduledTaskAction -Execute "powershell.exe" `
    -Argument "-NoProfile -ExecutionPolicy Bypass -File $repo\run.ps1 -Mode delta" `
    -WorkingDirectory $repo
$trigger = New-ScheduledTaskTrigger -Daily -At 5:00am
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable `
    -DontStopIfGoingOnBatteries -AllowStartIfOnBatteries `
    -ExecutionTimeLimit (New-TimeSpan -Hours 6)
Register-ScheduledTask -TaskName "ValueWorks Collector" -Action $action -Trigger $trigger `
    -Settings $settings -RunLevel Limited `
    -User "DOMAIN\svc_vwcollector" -Password (Read-Host "Service account password")
```

The three that matter:

- **`-User` / `-Password`** — equivalent to "Run whether user is logged on or not".
  Without it the task only runs while that user is interactively logged in.
- **`-StartWhenAvailable`** — runs a missed schedule after the machine comes back up.
  Without it, a reboot at 05:00 silently skips that day entirely.
- **`-ExecutionPolicy Bypass`** — `run.ps1` is otherwise blocked by the default policy.

Then trigger it once manually and confirm it actually ran as the service account:

```powershell
Start-ScheduledTask -TaskName "ValueWorks Collector"
Get-ScheduledTaskInfo -TaskName "ValueWorks Collector"   # LastTaskResult 0 = success
```

Also confirm the machine will be awake: a host that sleeps or hibernates overnight will
not run the task at all.

### Logs and exit codes

Logs go to `C:\logs\vw-on-prem-collector` (override with `VW_LOG_DIR` in `.env`).
Orchestrator exit codes: `0` all sources succeeded, `1` at least one source failed,
`2` configuration problem (unknown source, or AzCopy not found).

| Symptom | Cause |
|---|---|
| AzCopy fails with 403 | SAS expired, missing permission, or IP restriction — run `check_azure.py` |
| AzCopy fails with a TLS/certificate error | Proxy doing SSL inspection — see step 6 |
| Works interactively, `LastTaskResult` non-zero | Service account cannot reach Python (Store build) or `.env` (ACL) |
| Every run reports full-refresh | SAS lacks Read+List, so META cannot be read back — see [Delta refresh](#delta-refresh) |

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

Logs go to `<repo>/logs` (override with `VW_LOG_DIR` in `.env`).

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
