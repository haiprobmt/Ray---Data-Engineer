# Preview by default. Registration occurs only with -Install.
param(
    [Parameter(Mandatory=$true)][string]$Config,
    [Parameter(Mandatory=$true)][string]$DataDir,
    [string]$TaskName = 'Ray Telegram Gateway',
    [switch]$Install
)
$ErrorActionPreference = 'Stop'
$rayStart = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot 'start-ray.ps1')).Path
$rayConfig = (Resolve-Path -LiteralPath $Config).Path
$rayData = [IO.Path]::GetFullPath($DataDir)
if (@($rayStart,$rayConfig,$rayData) | Where-Object { $_.Contains('"') }) { throw 'Double quotes are not supported in paths' }
$rayShell = Join-Path $env:SystemRoot 'System32\WindowsPowerShell\v1.0\powershell.exe'
$rayArguments = '-NoProfile -NonInteractive -WindowStyle Hidden -File "{0}" -Config "{1}" -DataDir "{2}"' -f $rayStart,$rayConfig,$rayData
$rayUser = [Security.Principal.WindowsIdentity]::GetCurrent().Name
$rayPlan = [ordered]@{TaskName=$TaskName;Executable=$rayShell;Arguments=$rayArguments;User=$rayUser;Trigger='At user logon';RunLevel='Limited';InstallRequested=[bool]$Install}
$rayPlan | ConvertTo-Json
if (-not $Install) { return }
if (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue) { throw 'Task already exists. Inspect it or choose a different name.' }
$rayAction = New-ScheduledTaskAction -Execute $rayShell -Argument $rayArguments
$rayTrigger = New-ScheduledTaskTrigger -AtLogOn -User $rayUser
$raySettings = New-ScheduledTaskSettingsSet -StartWhenAvailable -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1) -ExecutionTimeLimit ([TimeSpan]::Zero) -MultipleInstances IgnoreNew
$rayPrincipal = New-ScheduledTaskPrincipal -UserId $rayUser -LogonType Interactive -RunLevel Limited
Register-ScheduledTask -TaskName $TaskName -Action $rayAction -Trigger $rayTrigger -Settings $raySettings -Principal $rayPrincipal | Select-Object TaskName,State
