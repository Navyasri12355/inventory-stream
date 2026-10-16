<#
.SYNOPSIS
  Hard-kill a single broker (for the broker-failure demo).
  Usage: .\kafka-kill.ps1 -BrokerNum 2
#>
param(
    [Parameter(Mandatory=$true)]
    [ValidateSet("1","2","3")]
    [string]$BrokerNum
)

$repo    = Split-Path $MyInvocation.MyCommand.Path -Parent | Split-Path -Parent
$pidFile = Join-Path $repo "broker-$BrokerNum.pid"

if (-not (Test-Path $pidFile)) {
    Write-Error "No PID file for broker $BrokerNum. Is it running?"
    exit 1
}

$pid = (Get-Content $pidFile).Trim()
Write-Host "Kill -9 broker $BrokerNum (pid $pid) ..."
Stop-Process -Id $pid -Force -ErrorAction Stop
Remove-Item $pidFile -ErrorAction SilentlyContinue
Write-Host "Broker $BrokerNum killed. Use kafka-start.ps1 to restart it."
