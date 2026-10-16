<#
.SYNOPSIS
  Gracefully stop all three Kafka brokers using their PID files.
#>
$ErrorActionPreference = "Continue"

$repo = Split-Path $MyInvocation.MyCommand.Path -Parent | Split-Path -Parent

foreach ($i in 1..3) {
    $pidFile = Join-Path $repo "broker-$i.pid"
    if (-not (Test-Path $pidFile)) { Write-Host "No PID file for broker $i, skipping."; continue }

    $pid = (Get-Content $pidFile).Trim()
    $proc = Get-Process -Id $pid -ErrorAction SilentlyContinue
    if ($proc) {
        Write-Host "Stopping broker $i (pid $pid) ..."
        Stop-Process -Id $pid -Force
        $proc.WaitForExit(15000) | Out-Null
        Write-Host "  Stopped."
    } else {
        Write-Host "Broker $i (pid $pid) not running."
    }
    Remove-Item $pidFile -ErrorAction SilentlyContinue
}
Write-Host "All brokers stopped."
