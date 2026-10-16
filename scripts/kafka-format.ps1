<#
.SYNOPSIS
  One-time: generate a KRaft cluster ID and format all three broker log dirs.
  Run this ONCE before the very first kafka-start.ps1.
  Safe to re-run only if you delete kafka-data/ first.
#>
$ErrorActionPreference = "Stop"

# Resolve KAFKA_HOME: env var takes priority, else default to C:\kafka
$kafkaHome = if ($env:KAFKA_HOME) { $env:KAFKA_HOME } else { "C:\kafka" }
if (-not (Test-Path $kafkaHome)) {
    Write-Error "Kafka not found at '$kafkaHome'. Set KAFKA_HOME or install Kafka to C:\kafka."
    exit 1
}

$repo = Split-Path $MyInvocation.MyCommand.Path -Parent | Split-Path -Parent
$configDir = Join-Path $repo "config\kafka"

# Create log directories
foreach ($i in 1..3) {
    $logDir = Join-Path $repo "kafka-data\broker-$i"
    New-Item -ItemType Directory -Force -Path $logDir | Out-Null
}

# Generate a single cluster ID shared by all three nodes
$clusterId = & "$kafkaHome\bin\windows\kafka-storage.bat" random-uuid
$clusterId = $clusterId.Trim()
Write-Host "Cluster ID: $clusterId"

# Format each broker's log directory
foreach ($i in 1..3) {
    $cfg = Join-Path $configDir "server-$i.properties"
    Write-Host "Formatting broker $i ..."
    & "$kafkaHome\bin\windows\kafka-storage.bat" format `
        --config $cfg `
        --cluster-id $clusterId `
        --ignore-formatted   # safe if called again; errors if not formatted
    if ($LASTEXITCODE -ne 0) { Write-Error "Format failed for broker $i"; exit 1 }
}

Write-Host ""
Write-Host "Cluster formatted successfully. Run scripts\kafka-start.ps1 to start."
