<#
.SYNOPSIS
  Native-mode: create the PostgreSQL role, database, enable timescaledb extension,
  and apply sql/init.sql.  Idempotent.
  Requires psql on PATH and a superuser connection.
.PARAMETER SuperUser
  PostgreSQL superuser name (default: postgres)
#>
param(
    [string]$SuperUser = "postgres"
)
$ErrorActionPreference = "Stop"

# Load .env if present
$repo   = Split-Path $MyInvocation.MyCommand.Path -Parent | Split-Path -Parent
$envFile = Join-Path $repo ".env"
if (Test-Path $envFile) {
    Get-Content $envFile | ForEach-Object {
        if ($_ -match '^\s*([^#=]+)=(.*)$') {
            [System.Environment]::SetEnvironmentVariable($Matches[1].Trim(), $Matches[2].Trim(), "Process")
        }
    }
}

$dbName = if ($env:DB_NAME) { $env:DB_NAME } else { "inventory" }
$dbUser = if ($env:DB_USER) { $env:DB_USER } else { "inventory" }
$dbPass = if ($env:DB_PASSWORD) { $env:DB_PASSWORD } else { "inventorydev" }
$dbHost = if ($env:DB_HOST) { $env:DB_HOST } else { "localhost" }
$dbPort = if ($env:DB_PORT) { $env:DB_PORT } else { "5432" }

function Invoke-Psql($sql, [switch]$AsSuper) {
    $u = if ($AsSuper) { $SuperUser } else { $dbUser }
    $env:PGPASSWORD = if ($AsSuper) { "" } else { $dbPass }
    psql -h $dbHost -p $dbPort -U $u -d postgres -c $sql 2>&1
}

Write-Host "Creating role '$dbUser' if not exists ..."
Invoke-Psql "DO `$`$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='$dbUser') THEN CREATE ROLE $dbUser LOGIN PASSWORD '$dbPass'; END IF; END `$`$;" -AsSuper

Write-Host "Creating database '$dbName' if not exists ..."
$exists = (psql -h $dbHost -p $dbPort -U $SuperUser -tAc "SELECT 1 FROM pg_database WHERE datname='$dbName'" 2>&1).Trim()
if ($exists -ne "1") {
    psql -h $dbHost -p $dbPort -U $SuperUser -c "CREATE DATABASE $dbName OWNER $dbUser;"
}

Write-Host "Enabling timescaledb extension ..."
$env:PGPASSWORD = ""
psql -h $dbHost -p $dbPort -U $SuperUser -d $dbName -c "CREATE EXTENSION IF NOT EXISTS timescaledb;"

Write-Host "Applying sql/init.sql ..."
$env:PGPASSWORD = $dbPass
$initSql = Join-Path $repo "sql\init.sql"
psql -h $dbHost -p $dbPort -U $dbUser -d $dbName -f $initSql

Write-Host "Native DB setup complete."
