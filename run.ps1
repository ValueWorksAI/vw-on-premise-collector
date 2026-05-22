param(
    [ValidateSet("full", "delta")]
    [string]$Mode = "delta",
    [string]$Sources = "",
    [switch]$Parallel
)

$ErrorActionPreference = "Stop"
Set-Location -Path $PSScriptRoot

$cliArgs = @("run", "python", "orchestrator.py", "--mode", $Mode)
if ($Sources)  { $cliArgs += @("--sources", $Sources) }
if ($Parallel) { $cliArgs += "--parallel" }

& poetry @cliArgs
exit $LASTEXITCODE
