param(
    [Parameter(Mandatory=$true)][string]$Project,
    [Parameter(Mandatory=$true)][string]$DataDir,
    [Parameter(Mandatory=$true)][string]$Output
)
$ErrorActionPreference = 'Stop'
$rayRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
& (Join-Path $rayRoot '.venv\Scripts\python.exe') -m ray_de --project $Project --data-dir $DataDir backup $Output
if ($LASTEXITCODE -ne 0) { throw 'Backup failed' }
