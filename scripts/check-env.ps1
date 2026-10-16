<#
.SYNOPSIS
  Phase 1 environment check - reports versions and confirms all prerequisites.
  Run this before anything else.
#>
$ErrorActionPreference = "Continue"

Write-Host "=== Environment Check ===" -ForegroundColor Cyan

# OS
Write-Host ""
Write-Host "[OS]" -ForegroundColor Yellow
[System.Environment]::OSVersion.VersionString
Write-Host "NOTE: Native Windows Kafka can crash on log-segment deletion (file locking)."
Write-Host "      WSL2 is strongly recommended for the broker-failure demo."

# Java
Write-Host ""
Write-Host "[Java]" -ForegroundColor Yellow
java -version 2>&1

# Kafka
Write-Host ""
Write-Host "[Kafka]" -ForegroundColor Yellow
$kafkaHome = if ($env:KAFKA_HOME) { $env:KAFKA_HOME } else { "C:\kafka" }
Write-Host "KAFKA_HOME=$kafkaHome"
if (Test-Path $kafkaHome) {
    $ver = & "$kafkaHome\bin\windows\kafka-topics.bat" --version 2>&1
    Write-Host "kafka-topics version: $ver"
    if ($ver -notmatch "3\.9") {
        Write-Warning "Expected Kafka 3.9.x"
    }
} else {
    Write-Warning "Kafka not found at $kafkaHome"
}

# Python
Write-Host ""
Write-Host "[Python]" -ForegroundColor Yellow
python --version 2>&1
pip --version 2>&1

# Docker
Write-Host ""
Write-Host "[Docker]" -ForegroundColor Yellow
docker --version 2>&1
docker compose version 2>&1

# psql
Write-Host ""
Write-Host "[PostgreSQL / psql]" -ForegroundColor Yellow
$psqlCmd = Get-Command psql -ErrorAction SilentlyContinue
if ($psqlCmd) {
    psql --version
} else {
    Write-Host "psql not found - docker mode required for DB."
}

# Grafana
Write-Host ""
Write-Host "[Grafana]" -ForegroundColor Yellow
$gfCmd = Get-Command grafana-server -ErrorAction SilentlyContinue
if ($gfCmd) {
    grafana-server --version
} else {
    Write-Host "grafana-server not found - docker mode required for dashboard."
}

# Dataset
Write-Host ""
Write-Host "[Dataset]" -ForegroundColor Yellow
$repo = Split-Path $MyInvocation.MyCommand.Path -Parent | Split-Path -Parent
$csv  = Join-Path $repo "data\raw\online_retail_ii.csv"
$xlsx = Join-Path $repo "data\raw\online_retail_ii.xlsx"
if (Test-Path $csv) {
    $lineCount = (Get-Content $csv | Measure-Object -Line).Lines
    Write-Host "CSV found: $csv  ($lineCount lines)"
} elseif (Test-Path $xlsx) {
    Write-Host "XLSX found but CSV missing. Run: python scripts\convert_to_csv.py"
} else {
    Write-Warning "Dataset not found. Download Online Retail II (UCI id 502) and place in data\raw\"
}

# Summary
Write-Host ""
Write-Host "=== Recommendation ===" -ForegroundColor Cyan
if (-not $psqlCmd) {
    Write-Host "DEPLOY_MODE=docker  (psql/Grafana not available natively)"
} else {
    Write-Host "DEPLOY_MODE=native  (psql found)"
}
Write-Host ""
Write-Host "Next steps:"
Write-Host "  1. Copy .env.example to .env and review settings"
Write-Host "  2. scripts\kafka-format.ps1  (one-time)"
Write-Host "  3. scripts\kafka-start.ps1"
Write-Host "  4. scripts\create-topics.ps1"
Write-Host "  5. docker compose up -d  (if DEPLOY_MODE=docker)"
