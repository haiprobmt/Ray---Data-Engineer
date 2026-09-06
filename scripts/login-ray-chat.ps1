param([string]$Root = (Join-Path $env:LOCALAPPDATA 'Ray'))
$ErrorActionPreference = 'Stop'
$rayRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$rayPython = Join-Path $rayRoot '.venv\Scripts\python.exe'
$rayProject = Join-Path $Root 'lobby\config.yaml'
$rayData = Join-Path $Root 'state'
Write-Host 'Sign in to enable conversations with Ray. Follow the device sign-in instructions below.'
& $rayPython -m ray_de.cli --project $rayProject --data-dir $rayData login codex
if ($LASTEXITCODE -ne 0) { throw 'Ray sign-in did not finish.' }
Write-Host 'Ray sign-in completed. You can close this sign-in window.'
