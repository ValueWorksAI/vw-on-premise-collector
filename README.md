# vw-on-prem-collector

Generic on-premise data collection framework. Each data source is a self-contained "push" under `sources/`. The orchestrator runs all sources, uploads outputs to Azure Blob Storage.

All traffic is **outbound only** (HTTPS, port 443, via AzCopy) — no inbound access to the customer network is required. The only firewall destination is `https://<account>.blob.core.windows.net`.

## Layout

```text
common/             shared utilities (config, azure upload, parquet, delta, secrets, logging, retry)
sources/
  _example/         template for new HTTP/API sources
  _example_mssql/   template for Microsoft SQL Server sources (pyodbc)
  _example_ads/     template for Advantage Database Server sources (pyodbc + Advantage ODBC)
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

## Advantage Database Server (ADS) sources

ADS keeps each table as its own file next to a data dictionary (`.add`). The collector
connects to the dictionary through the ADS **server** with the 64-bit "Advantage
StreamlineSQL ODBC" driver (`adsodbc_x86_64.exe`, from the application vendor; SAP only
offers it behind an S-user login). `common/ads.py` refuses `server_types` that include
the Advantage Local Server, which would open the live table files directly.

`tools/setup_windows.ps1 -AdsDriverInstaller <path>` installs the driver silently. The
InstallShield wrapper needs `/s /b"<folder>" /v"/qn"`; without `/b` it returned 1619.

```powershell
poetry run python tools\probe_ads.py --drivers
poetry run python tools\probe_ads.py --data-directory "\\SERVER\share\x.add" --user <user> `
    --source-name <name> --env-prefix <PREFIX> --emit-config sources\<name>\config.yaml --out schema.json
copy sources\_example_ads\push.py sources\<name>\
```

Notes:

- The driver reports its catalog with its own result names (`columnname`, `typename`),
  so `probe_ads.py` reads SQLColumns rows by name and falls back to an empty SELECT.
- `data_directory` is the dictionary path as the ADS server sees it: a UNC path that
  names the server, or a local path when the collector runs on the database server.
- Some ADS applications store a change stamp as separate date and time columns. The
  delta filter compares one column, so such objects stay on full refresh until a
  combined predicate exists.

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

# 2. What does the source database look like? Prints a summary and writes a
#    complete, ready-to-use config.yaml.
poetry run python tools/probe_mssql.py --host <server> --database <db> \
    --user vw_readonly --sample --source-name <name> \
    --emit-config sources/<name>/config.yaml

# 2b. If the product's database engine is unknown, this needs no connection at all:
poetry run python tools/probe_mssql.py --drivers
```

Run `check_azure.py` **first** — it surfaces proxy and TLS-interception problems, which are
the most common cause of a failing install, before any database work.

Step 2 generates the source config on the spot, so nothing has to be sent back and forth to
get a working collector. Only the printed summary — object names, row counts, delta
candidates — is needed to review the table selection; add `--out schema.json` for the full
column-level dump, but note that customers often treat a complete schema as confidential and
it is not required (objects default to `SELECT *` when `extra.columns` is omitted).

Generated configs start **every object on full refresh** (`timestamp_field: null`), with the
detected delta candidate recorded in a comment. Enable delta per object later, with
`--enable-delta` or by hand, once volumes justify it and the database clock domain has been
checked — see [Delta refresh](#delta-refresh).

## Adding a new source

1. Copy `sources/_example/` (HTTP/API) or `sources/_example_mssql/` (SQL Server) to `sources/<name>/`.
2. Edit `config.yaml` (azure target, connection, partitions, objects, delta settings).
3. For HTTP/API sources, implement the 3 required methods in `push.py`: `authenticate`, `build_filter`, `fetch`. The MSSQL template's `push.py` works as-is — configuration happens entirely in `config.yaml`.
4. Optionally override `transform_record` and `list_partitions`.
5. Drop secrets into `.env` using the convention `<NAME>_<KEY>`.

The orchestrator auto-discovers any folder under `sources/` containing both `config.yaml` and `push.py` (folders starting with `_` are skipped).

## Delta refresh

Objects with a `timestamp_field` run incrementally: each run reads its own latest META file from Azure and only fetches rows changed since the previous upper bound. Objects without a `timestamp_field` full-refresh every run. Because META files are read back from Azure, the SAS token needs **Read + List** in addition to **Create + Write** (container-scoped; no Delete required).

## File attachments

Some tables only point at files, e.g. a document table whose rows name scanned invoices in a folder on the server. Add `extra.files` to such an object and the collector also copies the files its rows name:

```yaml
  - name: DOCUMENTS
    endpoint: DOCUMENTS
    timestamp_field: null
    partition_scoped: false
    extra:
      columns: [ID, FOLDER, FILE_NAME]
      where: "\"INVOICE_ID\" IS NOT NULL"        # the rows decide which files go
      files:
        base_dir: 'D:\App\Documents'             # the folder the row paths are relative to
        path_columns: [FOLDER, FILE_NAME]        # joined in this order; the last is the file name
        prefix: files/my-source                  # optional; default <azure.prefix>/_files/<object>
        max_file_mb: 100                         # optional; larger files are skipped and counted
```

- **Opt-in and bounded.** Nothing outside `base_dir` is read: absolute paths (`C:\…`, `\\server\…`) and `..` in a row are skipped and counted. The `where` and `columns` of the object decide which rows, and so which files, are in scope. With `columns` set, the path columns must be among them.
- **New files only.** Each run lists the target prefix once and uploads only the files not there yet: the first run copies everything, later runs only what is new. A blob is never overwritten (`If-None-Match: *`) and never deleted, so a file changed or removed on the host keeps its first copy in storage. Each upload carries Content-MD5, so a damaged body is rejected by the service.
- **After the tables.** The paths are read back from the object's parquet before the local copy is dropped, but the files themselves are copied at the end of the source's run, so a large first copy cannot delay the tables behind it. Run the first copy by hand during the day if it is large.
- **In the run report.** The object's entry in `_runs/` gets a `files` block: `named`, `already_in_storage`, `uploaded`, `uploaded_mb`, `missing_on_host`, `too_large`, `failed`, `not_attempted` and `skipped_*`. A file the rows name but the host does not have is counted, not an error. A failed upload (after 3 attempts) marks the object failed, so the run exits `1`, and the next run sends the file again. After 20 failed files the rest is left for the next run.
- **Permissions.** Needs the SAS's List permission in addition to Create + Write. Without List the copy refuses to start rather than sending everything again.
- **Choose a prefix outside the table folders** (e.g. `files/<source>`) when the files are many. The default `_files/` sits under the source's prefix: a reader that picks parquet by extension ignores the files there, but still has to page through them when it lists the prefix.

## Local disk usage

Each object's parquet is uploaded and then deleted as soon as it lands, so peak local disk is **one object**, not one dataset. Size the machine for the largest single object plus headroom, not for the total volume.

"One object" is the high-water mark *during* a run, not something retained after it. Once a run finishes **no parquet remains at all** — only the META JSONs, a few hundred bytes each, which are themselves wiped at the start of the next run. The steady state between runs is empty.

Set `keep_local_copy: true` in a source's `config.yaml` to keep the newest parquet per object instead. Nothing in the run path reads it back — the delta watermark comes from the META blob in Azure, and local META is wiped at the start of every run — so it is purely for inspecting output on the machine. It costs a second full copy of the dataset: the kept copy from the previous run coexists with the one being written, so peak becomes **2x the dataset**. Leave it off on anything volume-constrained.

The `_parts_*` scratch directories are removed per object after the combine, and an end-of-run sweep clears anything an upload failure or a crashed run left behind.

Each source needs its **own** `output_dir`. Two sources sharing one directory collide on the `_parts_<object>` scratch dirs and on each other's sweep, so `--parallel` refuses to start when it detects it (exit `2`). Sequential runs are unaffected — nothing interleaves.
