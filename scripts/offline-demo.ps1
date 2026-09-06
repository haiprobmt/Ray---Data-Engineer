param([Parameter(Mandatory=$true)][string]$Output)
$ErrorActionPreference = 'Stop'
$rayRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
& (Join-Path $rayRoot '.venv\Scripts\python.exe') -m ray_de.demo --output $Output
if ($LASTEXITCODE -ne 0) { throw 'Offline demonstration failed' }
