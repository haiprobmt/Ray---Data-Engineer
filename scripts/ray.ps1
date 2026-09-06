# Convenience launcher. All remaining arguments are passed unchanged to Ray.
$ErrorActionPreference = "Stop"
$rayRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."))
$rayPython = Join-Path $rayRoot ".venv\Scripts\python.exe"
if (-not (Test-Path -LiteralPath $rayPython)) { throw "Run scripts\setup.ps1 first" }
Push-Location $rayRoot
try {
    & $rayPython -m ray_de @args
    exit $LASTEXITCODE
} finally { Pop-Location }
