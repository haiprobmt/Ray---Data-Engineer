param([string]$Python = "python")
$ErrorActionPreference = "Stop"
$rayRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."))
Push-Location $rayRoot
try {
    if (-not (Test-Path -LiteralPath ".venv\Scripts\python.exe")) {
        & $Python -m venv .venv
        if ($LASTEXITCODE -ne 0) { throw "Virtual environment creation failed" }
    }
    & .\.venv\Scripts\python.exe -m pip install -r requirements-windows-py312.lock.txt
    if ($LASTEXITCODE -ne 0) { throw "Locked dependency installation failed" }
    & .\.venv\Scripts\python.exe -m pip install --no-deps --no-build-isolation -e .
    if ($LASTEXITCODE -ne 0) { throw "Dependency installation failed" }
    & .\.venv\Scripts\python.exe -m pytest tests -q
    if ($LASTEXITCODE -ne 0) { throw "Ray tests failed" }
    Write-Output "Ray installed. Follow README.md to configure and sign in to your project."
} finally { Pop-Location }
