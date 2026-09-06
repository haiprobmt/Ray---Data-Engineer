param([string]$Root = (Join-Path $env:LOCALAPPDATA 'Ray'))
$ErrorActionPreference = 'Stop'
$rayRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$rayPython = Join-Path $rayRoot '.venv\Scripts\python.exe'
$rayHome = [IO.Path]::GetFullPath($Root)
$rayConfig = Join-Path $rayHome 'gateway.yaml'
$rayData = Join-Path $rayHome 'state'
if (-not (Test-Path -LiteralPath $rayPython)) { throw 'Run scripts\setup.ps1 first' }
Push-Location $rayRoot
try {
    if (-not (Test-Path -LiteralPath $rayConfig)) {
        & $rayPython -m ray_de.telegram_setup --root $rayHome
        if ($LASTEXITCODE -ne 0) { throw 'Telegram setup did not finish. You can run this script again.' }
    }
    Write-Host 'Starting Ray. Keep this window open; Ctrl+C stops the bot.'
    & $rayPython -m ray_de.telegram --config $rayConfig --data-dir $rayData
    if ($LASTEXITCODE -ne 0) { throw 'Ray stopped with an error.' }
} finally { Pop-Location }
