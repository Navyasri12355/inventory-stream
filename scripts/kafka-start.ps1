<#
.SYNOPSIS
  Start Kafka brokers in KRaft mode.
  By default starts all three. Use -BrokerId to restart a single broker.
  Waits until the cluster is healthy before returning.

.PARAMETER BrokerId
  Optional. Start only this broker ID (1, 2, or 3). Useful after a chaos-kill.
#>
param(
    [ValidateSet(1, 2, 3)]
    [int]$BrokerId = 0   # 0 = start all
)
$ErrorActionPreference = "Stop"

$kafkaHome = if ($env:KAFKA_HOME) { $env:KAFKA_HOME } else { "C:\kafka" }
$repo = Split-Path $MyInvocation.MyCommand.Path -Parent | Split-Path -Parent
$configDir = Join-Path $repo "config\kafka"
$pidDir    = $repo   # PID files land at repo root

$brokers = if ($BrokerId -gt 0) { @($BrokerId) } else { 1..3 }

# Start each broker as a background window-less process
foreach ($i in $brokers) {
    $cfg    = Join-Path $configDir "server-$i.properties"
    $pidFile = Join-Path $pidDir   "broker-$i.pid"
    $logFile = Join-Path $repo     "kafka-data\broker-$i\broker.log"

    if (Test-Path $pidFile) {
        $old = Get-Content $pidFile
        if (Get-Process -Id $old -ErrorAction SilentlyContinue) {
            Write-Host "Broker $i already running (pid $old), skipping."
            continue
        }
    }

    Write-Host "Starting broker $i ..."
    $proc = Start-Process `
        -FilePath "$kafkaHome\bin\windows\kafka-server-start.bat" `
        -ArgumentList $cfg `
        -RedirectStandardOutput $logFile `
        -RedirectStandardError  "$logFile.err" `
        -PassThru `
        -WindowStyle Hidden
    $proc.Id | Set-Content $pidFile
    Write-Host "  pid=$($proc.Id)  log=$logFile"
}

# Poll until all three brokers respond
Write-Host "Waiting for cluster to become healthy ..."
$bootstrap = "localhost:9092"
$deadline  = (Get-Date).AddSeconds(60)
while ((Get-Date) -lt $deadline) {
    $result = & "$kafkaHome\bin\windows\kafka-broker-api-versions.bat" `
        --bootstrap-server $bootstrap 2>&1
    if ($LASTEXITCODE -eq 0) {
        Write-Host "Cluster is ready."
        exit 0
    }
    Start-Sleep -Seconds 2
}

Write-Error "Cluster did not become healthy within 60 s. Check kafka-data\broker-*\broker.log."
exit 1
