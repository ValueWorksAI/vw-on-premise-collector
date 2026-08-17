# ValueWorks On-Premise Collector — IT Preparation Guide

This page describes what should be in place before the setup session. If the items
under "Before the session" are done, the setup itself takes about 30 minutes.

## What the collector does

A small Python program runs once a day on a machine inside your network. It reads
**read-only** from the agreed databases/interfaces, writes the result as Parquet files
to a local output directory, and uploads those files over HTTPS to ValueWorks' Azure
Blob Storage. The local files are cleaned up afterwards.

**What is explicitly *not* required:**

- No inbound access to your network. All traffic is outbound only.
- No VPN, no port forwarding, no static IP on your side.
- No write access to the source databases — read-only is sufficient.
- No domain admin rights. Local admin rights are needed once, to install
  Python and AzCopy.

The only firewall destination is `https://<account>.blob.core.windows.net` (TCP 443).
ValueWorks will provide the account name.

## Before the session

### 1. A machine for the collector

Please **not** a workstation PC that gets switched off or goes to sleep in the evening —
the scheduled task would not run. A small, always-on Windows VM or a server is ideal.

- Windows Server or Windows 10/11, powered on continuously
- **16 GB RAM minimum.** The collector holds a table fully in memory while processing it
  before writing it out, so the requirement scales with the largest single table rather
  than with the total data volume.
- At least 20 GB free disk space for intermediate files (they are removed again after
  each run)
- Network access to the source database servers
- A service account for the scheduled task whose password **does not expire**

### 2. A read-only database user

The collector uses **SQL Server authentication** (username + password), not Windows
authentication. If the instance is set to Windows-authentication-only mode, please let
us know in advance — we will need a different approach.

```sql
-- Run on the SQL instance (replace the password):
USE [master];
CREATE LOGIN [vw_readonly] WITH PASSWORD = 'REPLACE-ME', CHECK_POLICY = ON;

USE [<DATABASE_NAME>];
CREATE USER [vw_readonly] FOR LOGIN [vw_readonly];
ALTER ROLE [db_datareader] ADD MEMBER [vw_readonly];
```

Please run this once per source database. Additionally, please confirm:

- The TCP/IP protocol is enabled for the instance (SQL Server Configuration Manager)
- The port (1433 by default, different for named instances) is reachable from the
  collector machine

What we need from you: **server name\instance, port, database name, username, password.**
Please send the password over a secure channel, not in a plaintext email.

### 3. Outbound internet access

- Allow TCP 443 from the collector machine to `*.blob.core.windows.net`
- If an **HTTP(S) proxy** is in use: address, port, and whether authentication is required
- If the proxy performs **TLS interception** (SSL inspection): please tell us in advance.
  This is the single most common cause of failing uploads and needs specific
  configuration on our side.

### 4. Software (local admin rights required)

| Software | Source | Notes |
|---|---|---|
| Python 3.10–3.14 | <https://www.python.org/downloads/> | **Not** from the Microsoft Store — the Store build does not work reliably under service accounts. Tick "Add python.exe to PATH" during installation. |
| Poetry | `pip install poetry` | |
| AzCopy v10 | <https://aka.ms/downloadazcopy-v10-windows> | Extract the ZIP, place `azcopy.exe` in `C:\AzCopy\` |

### 5. If a database backend is unclear

If it is not known which database one of the applications runs on, please list the
installed ODBC drivers on the relevant server. Without Python installed, this also works
via *ODBC Data Sources (64-bit)* → *Drivers* tab, or in PowerShell:

```powershell
Get-OdbcDriver | Select-Object Name, Platform
```

A screenshot or the text output is enough. This lets us determine before the session
whether we connect via SQL Server, via generic ODBC, or via a file export.

## Agenda for the session (approx. 30–45 min)

1. Connectivity test against Azure Storage (`tools/check_azure.py`) — first, because
   proxy and firewall issues surface here
2. Database connectivity test and schema dump (`tools/probe_mssql.py`)
3. Install the collector, fill in `.env` with the credentials
4. Create the scheduled task (daily, overnight)

Selecting the specific tables and fields happens **after** the session, at ValueWorks,
based on the schema dump. That requires no further session with you — you will simply
receive a small configuration file to drop in place.

## Security and auditability

- Credentials live only in the local `.env` file on the collector machine — not in
  source control and not at ValueWorks.
- Access to Azure Storage uses a time-limited, revocable SAS token, scoped to a single
  container, with no delete permission.
- Every run writes a log file to `C:\logs\vw-on-prem-collector`.
- The collector can be stopped at any time by disabling the scheduled task. No services
  or background processes are left behind.
