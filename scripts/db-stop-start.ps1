<#
.SYNOPSIS
  Stop or start the database for the DB-outage failure demo (AGENTS.md §10 demo 6).
  Usage:
    .\db-stop-start.ps1 stop
    .\db-stop-start.ps1 start
#>
param(
    [Parameter(Mandatory=$true)]
    [ValidateSet("stop","start")]
    [string]$Action
)
$ErrorActionPreference = "Stop"

$repo    = Split-Path $MyInvocation.MyCommand.Path -Parent | Split-Path -Parent
$envFile = Join-Path $repo ".env"
$deployMode = "docker"
if (Test-Path $envFile) {
    Get-Content $envFile | ForEach-Object {
        if ($_ -match '^\s*DEPLOY_MODE=(.*)$') { $deployMode = $Matches[1].Trim() }
    }
}

if ($deployMode -eq "docker") {
    Write-Host "$Action timescaledb container (docker compose) ..."
    if ($Action -eq "stop") {
        docker compose -f (Join-Path $repo "docker-compose.yml") pause timescaledb
    } else {
        docker compose -f (Join-Path $repo "docker-compose.yml") unpause timescaledb
    }
} else {
    # Native mode: stop/start the PostgreSQL service
    $svcName = "postgresql*"
    $svc = Get-Service -Name $svcName -ErrorAction SilentlyContinue | Select-Object -First 1
    if (-not $svc) { Write-Error "Could not find a PostgreSQL service. Adjust svcName in this script."; exit 1 }
    if ($Action -eq "stop") {
        Write-Host "Stopping service $($svc.Name) ..."
        Stop-Service -Name $svc.Name -Force
    } else {
        Write-Host "Starting service $($svc.Name) ..."
        Start-Service -Name $svc.Name
    }
}
Write-Host "Done."
