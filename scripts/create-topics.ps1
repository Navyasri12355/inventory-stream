<#
.SYNOPSIS
  Idempotent topic creation: inventory-events, inventory-alerts, inventory-dlq.
  Safe to run multiple times (--if-not-exists).
#>
$ErrorActionPreference = "Stop"

$kafkaHome  = if ($env:KAFKA_HOME) { $env:KAFKA_HOME } else { "C:\kafka" }
$bootstrap  = if ($env:KAFKA_BOOTSTRAP) { $env:KAFKA_BOOTSTRAP } else { "localhost:9092,localhost:9094,localhost:9096" }
$topics = @(
    @{ name="inventory-events"; partitions=6; replication=3; minIsr=2 },
    @{ name="inventory-alerts"; partitions=3; replication=3; minIsr=2 },
    @{ name="inventory-dlq";    partitions=1; replication=3; minIsr=2 }
)

foreach ($t in $topics) {
    Write-Host "Creating topic '$($t.name)' (partitions=$($t.partitions), RF=$($t.replication)) ..."
    & "$kafkaHome\bin\windows\kafka-topics.bat" `
        --bootstrap-server $bootstrap `
        --create `
        --if-not-exists `
        --topic $t.name `
        --partitions $t.partitions `
        --replication-factor $t.replication `
        --config min.insync.replicas=$($t.minIsr)
    if ($LASTEXITCODE -ne 0) { Write-Error "Failed to create topic $($t.name)"; exit 1 }
}

Write-Host ""
Write-Host "Verifying topics ..."
& "$kafkaHome\bin\windows\kafka-topics.bat" --bootstrap-server $bootstrap --describe `
    --topic inventory-events,inventory-alerts,inventory-dlq
