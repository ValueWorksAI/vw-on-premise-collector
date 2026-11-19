# Windows host setup for the on-prem collector. Run in an ELEVATED Windows PowerShell
# console (not ISE) from a checkout or a copy of this file:
#
#   powershell -ExecutionPolicy Bypass -File tools\setup_windows.ps1 `
#       -StorageUrl https://<account>.blob.core.windows.net -Source <name>
#
# Optional: -AdsDriverInstaller C:\path\adsodbc_x86_64.exe installs the 64-bit Advantage
# ODBC driver silently (Advantage Database Server sources).
#
# Re-runnable: every step skips what is already in place. Prerequisites installed by
# hand beforehand: Python 3.10-3.14 64-bit from python.org ("Install for all users",
# "Add to PATH") and Git for Windows. Nothing here reads from a source database.
param(
    [Parameter(Mandatory = $true)][string]$StorageUrl,
    [Parameter(Mandatory = $true)][string]$Source,
    [string]$Repo = 'C:\vw-on-prem-collector',
    [string]$AdsDriverInstaller,
    [string]$RepoUrl = 'https://github.com/ValueWorksAI/vw-on-premise-collector.git'
)
$ErrorActionPreference = 'Continue'
$ProgressPreference = 'SilentlyContinue'
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

function Step($m) { Write-Host "`n=== $m" -ForegroundColor Cyan }
function Ok($m)   { Write-Host "ok: $m" -ForegroundColor Green }
function Warn($m) { Write-Host "warning: $m" -ForegroundColor Yellow }
function Fail($m) { Write-Host "STOP: $m" -ForegroundColor Red; exit 1 }
function Native($what) { if ($LASTEXITCODE -ne 0) { Fail "$what failed (exit $LASTEXITCODE)" } }

$me = [Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()
if (-not $me.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) { Fail 'run this in an elevated PowerShell (Run as administrator)' }
$env:Path = [Environment]::GetEnvironmentVariable('Path', 'Machine') + ';' + [Environment]::GetEnvironmentVariable('Path', 'User')

Step '1/9 Host'
$os = Get-CimInstance Win32_OperatingSystem
$ramGb = [math]::Round($os.TotalVisibleMemorySize / 1MB, 1)
$freeGb = [math]::Round((Get-PSDrive C).Free / 1GB, 1)
Write-Host "$($os.Caption), RAM $ramGb GB, C: free $freeGb GB"
if ($ramGb -lt 8) { Warn 'less than 8 GB RAM (one batch of the widest table is held in memory)' }
if ($freeGb -lt 20) { Warn 'less than 20 GB free on C:' }

Step '2/9 Python'
$py = Get-ChildItem "$env:ProgramFiles\Python3*\python.exe" -ErrorAction SilentlyContinue |
    Sort-Object { [int]($_.Directory.Name -replace '\D', '') } -Descending | Select-Object -First 1 -ExpandProperty FullName
if (-not $py) { try { $py = (& py -3 -c "import sys; print(sys.executable)" 2>$null) } catch { } }
if (-not $py -or -not (Test-Path $py)) { Fail 'Python not found under C:\Program Files\Python3* (install from python.org for all users)' }
if ($py -match 'WindowsApps|\\Users\\') { Fail "Python lives in a user profile ($py). Reinstall with 'Install for all users'; a service account cannot run it." }
$bits = & $py -c "import struct; print(struct.calcsize('P') * 8)"
if ("$bits".Trim() -ne '64') { Fail "Python is $bits-bit; install the 64-bit build" }
Ok "$(& $py --version) at $py"

Step '3/9 Network (outbound 443)'
foreach ($h in @(([Uri]$StorageUrl).Host, 'github.com', 'pypi.org', 'files.pythonhosted.org', 'aka.ms')) {
    $t = Test-NetConnection $h -Port 443 -WarningAction SilentlyContinue
    if ($t.TcpTestSucceeded) { Ok "${h}:443" } else { Warn "${h}:443 not reachable directly (firewall, or a proxy is required)" }
}

Step '4/9 AzCopy'
$az = 'C:\AzCopy\azcopy.exe'
if (-not (Test-Path $az)) {
    try {
        $zip = Join-Path $env:TEMP 'azcopy.zip'; $tmp = Join-Path $env:TEMP 'azcopy'
        Invoke-WebRequest 'https://aka.ms/downloadazcopy-v10-windows' -OutFile $zip -UseBasicParsing -ErrorAction Stop
        Expand-Archive $zip $tmp -Force -ErrorAction Stop
        New-Item -ItemType Directory 'C:\AzCopy' -Force | Out-Null
        Get-ChildItem $tmp -Recurse -Filter azcopy.exe | Select-Object -First 1 | Copy-Item -Destination $az -ErrorAction Stop
    } catch { Fail "AzCopy download/extract: $($_.Exception.Message)" }
}
Ok (& $az --version)

Step '5/9 Collector code'
if (Test-Path (Join-Path $Repo '.git')) {
    git -C $Repo pull --ff-only; Native 'git pull'
} elseif (Test-Path $Repo) {
    Warn "$Repo exists but is not a git checkout (ZIP copy?) - using it as is; updates need a fresh clone"
} elseif (Get-Command git -ErrorAction SilentlyContinue) {
    git clone $RepoUrl $Repo; Native 'git clone'
} else {
    Fail 'git not on PATH: install Git for Windows, open a new elevated PowerShell, rerun'
}
if (Test-Path (Join-Path $Repo '.git')) { Ok "$(git -C $Repo log -1 --format='%h %ad %s' --date=short)" }

Step '6/9 Poetry + dependencies'
& $py -m pip install --upgrade --quiet --disable-pip-version-check pip poetry; Native 'pip install poetry'
Push-Location $Repo
& $py -m poetry config virtualenvs.in-project true --local; Native 'poetry config'
& $py -m poetry env use $py; Native 'poetry env use'
& $py -m poetry install --with mssql --no-interaction --no-ansi; Native 'poetry install'
$vpy = Join-Path $Repo '.venv\Scripts\python.exe'
& $vpy -c "import pandas, pyarrow, pyodbc, yaml, dotenv; print('imports ok, pyodbc', pyodbc.version, 'pyarrow', pyarrow.__version__)"; Native 'import check'
if (-not (Get-Command poetry -ErrorAction SilentlyContinue)) { Warn 'poetry.exe not on the machine PATH; run.ps1 (scheduled task) needs it' }
Pop-Location

Step '7/9 ADS ODBC driver'
if (Get-OdbcDriver | Where-Object { $_.Name -match 'Advantage' -and $_.Platform -eq '64-bit' }) {
    Ok 'Advantage ODBC driver (64-bit) already installed'
} elseif ($AdsDriverInstaller) {
    $sig = Get-AuthenticodeSignature $AdsDriverInstaller
    Write-Host "installer signature: $($sig.Status) $($sig.SignerCertificate.Subject)"
    if ($sig.Status -ne 'Valid') { Fail 'installer signature is not valid; get the file from the vendor again' }
    # InstallShield wrapper: /s silent, /b caches the MSI to a fixed folder (without it the
    # wrapper returned 1619 "package could not be opened"), /v passes /qn to msiexec.
    $cache = 'C:\vw-setup\adsmsi'; New-Item -ItemType Directory $cache -Force | Out-Null
    $p = Start-Process cmd.exe -ArgumentList "/c `"`"$AdsDriverInstaller`" /s /b`"$cache`" /v`"/qn /norestart /l*v C:\vw-setup\adsodbc-msi.log`"`"" -PassThru -WindowStyle Hidden
    if (-not $p.WaitForExit(300000)) { $p | Stop-Process -Force; Fail 'ADS driver installer timed out (see C:\vw-setup\adsodbc-msi.log)' }
    if (Get-OdbcDriver | Where-Object { $_.Name -match 'Advantage' -and $_.Platform -eq '64-bit' }) { Ok 'Advantage ODBC driver installed' }
    else { Fail "ADS driver installer exit $($p.ExitCode); see C:\vw-setup\adsodbc-msi.log" }
} else {
    Write-Host 'skipped (no -AdsDriverInstaller; only needed for Advantage Database Server sources)'
}

Step '8/9 .env + Azure check'
$envFile = Join-Path $Repo '.env'
if (-not (Test-Path $envFile)) {
    $sec = Read-Host 'Paste the SAS token (from ValueWorks)' -AsSecureString
    $sas = [Runtime.InteropServices.Marshal]::PtrToStringAuto([Runtime.InteropServices.Marshal]::SecureStringToBSTR($sec)).Trim()
    if ($sas -notmatch 'sig=') { Fail 'that does not look like a SAS token (no sig=)' }
    $content = "AZURE_STORAGE_URL=$StorageUrl`r`nAZURE_STORAGE_SAS_TOKEN=$sas`r`nAZCOPY_PATH=$az`r`nVW_LOG_DIR=C:\logs\vw-on-prem-collector`r`n"
    [IO.File]::WriteAllText($envFile, $content, (New-Object Text.UTF8Encoding($false)))
    icacls $envFile /inheritance:r /grant:r '*S-1-5-32-544:(R,W)' '*S-1-5-18:(R)' | Out-Null; Native 'icacls'
    Ok '.env written (Administrators + SYSTEM only; grant the service account read access later)'
} else { Ok '.env exists, left unchanged' }
New-Item -ItemType Directory 'C:\logs\vw-on-prem-collector' -Force | Out-Null
Push-Location $Repo
& $vpy tools\check_azure.py --container warehouse --prefix "raw/$Source"; Native 'check_azure'
Pop-Location

Step '9/9 Database discovery (no connection)'
Write-Host '-- ODBC drivers visible to 64-bit Python'
& $vpy -c "import pyodbc; [print('  ' + d) for d in pyodbc.drivers()]"
Write-Host '-- Database services and processes on this machine'
Get-Service | Where-Object { $_.DisplayName -match 'Advantage|SQL|Anywhere|Sybase' } | Format-Table Name, DisplayName, Status -AutoSize | Out-String | Write-Host
Get-CimInstance Win32_Process | Where-Object { $_.Name -match '^(ads|dbsrv|dbeng|sqlservr)' } | Select-Object Name, CommandLine | Format-List | Out-String | Write-Host
Write-Host '-- Listening database ports (1433 SQL Server, 2638 SQL Anywhere, 6262 ADS)'
Get-NetTCPConnection -State Listen -ErrorAction SilentlyContinue | Where-Object LocalPort -in 1433, 2638, 6262 |
    Format-Table LocalAddress, LocalPort, OwningProcess -AutoSize | Out-String | Write-Host
Write-Host "`nNext: probe the database (tools\probe_mssql.py or tools\probe_ads.py), then create sources\$Source." -ForegroundColor Cyan
