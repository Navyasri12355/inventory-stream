<#
.SYNOPSIS
  Apply sql/init.sql via the DATABASE_URL env var.
  Works in both native and docker mode (DB must already be running).
#>
$ErrorActionPreference = "Stop"

$repo   = Split-Path $MyInvocation.MyCommand.Path -Parent | Split-Path -Parent
$envFile = Join-Path $repo ".env"
if (Test-Path $envFile) {
    Get-Content $envFile | ForEach-Object {
        if ($_ -match '^\s*([^#=]+)=(.*)$') {
            [System.Environment]::SetEnvironmentVariable($Matches[1].Trim(), $Matches[2].Trim(), "Process")
        }
    }
}

$h  = if ($env:DB_HOST)     { $env:DB_HOST }     else { "localhost" }
$p  = if ($env:DB_PORT)     { $env:DB_PORT }     else { "5432" }
$db = if ($env:DB_NAME)     { $env:DB_NAME }     else { "inventory" }
$u  = if ($env:DB_USER)     { $env:DB_USER }     else { "inventory" }
$pw = if ($env:DB_PASSWORD) { $env:DB_PASSWORD } else { "inventorydev" }

$env:PGPASSWORD = $pw
$initSql = Join-Path $repo "sql\init.sql"
Write-Host "Applying $initSql to $db@$h:$p ..."
psql -h $h -p $p -U $u -d $db -f $initSql
if ($LASTEXITCODE -ne 0) { Write-Error "init.sql failed"; exit 1 }
Write-Host "Done."
