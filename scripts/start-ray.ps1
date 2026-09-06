param(
    [Parameter(Mandatory=$true)][string]$Config,
    [Parameter(Mandatory=$true)][string]$DataDir
)
$ErrorActionPreference = 'Stop'
$rayRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$rayPython = Join-Path $rayRoot '.venv\Scripts\python.exe'
$rayConfig = (Resolve-Path -LiteralPath $Config).Path
$rayData = [IO.Path]::GetFullPath($DataDir)
if (-not (Test-Path -LiteralPath $rayPython)) { throw 'Run scripts\setup.ps1 first' }
Push-Location $rayRoot
try {
    & $rayPython -m ray_de.telegram --config $rayConfig --data-dir $rayData
    exit $LASTEXITCODE
} finally { Pop-Location }
