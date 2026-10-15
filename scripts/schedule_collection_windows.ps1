<#
.SYNOPSIS
  Registers (or removes) a Windows Task Scheduler task that runs the CTA collector every hour.

.DESCRIPTION
  Each run is a 59-minute session (python -m backend.jobs.scheduled_collection) that polls the
  configured stations, uploads to Databricks every 15 minutes, and writes a run summary.
  The task runs only while you are logged on (no password is stored). Missed runs (e.g. the PC
  was asleep) start as soon as possible, and overlapping instances are ignored.

.EXAMPLE
  .\scripts\schedule_collection_windows.ps1              # register / update
  .\scripts\schedule_collection_windows.ps1 -Unregister  # remove
  Get-ScheduledTaskInfo -TaskName "CTA Collector"        # last run time and result
#>
param(
    [string]$TaskName = "CTA Collector",
    [int]$DurationMinutes = 59,
    [switch]$Unregister
)
$ErrorActionPreference = "Stop"

if ($Unregister) {
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
    Write-Host "Removed scheduled task '$TaskName'."
    return
}

$repo = Split-Path -Parent $PSScriptRoot
$python = Join-Path $repo ".venv\Scripts\python.exe"
if (-not (Test-Path $python)) { throw "Virtual environment not found at $python - complete the README setup first." }
if (-not (Test-Path (Join-Path $repo ".env"))) { throw ".env not found in $repo - add your CTA_API_KEY first." }

$action = New-ScheduledTaskAction -Execute $python `
    -Argument "-m backend.jobs.scheduled_collection --duration-minutes $DurationMinutes" `
    -WorkingDirectory $repo

# Start at the next full hour, then repeat hourly (no end date).
$start = (Get-Date).Date.AddHours((Get-Date).Hour + 1)
$trigger = New-ScheduledTaskTrigger -Once -At $start -RepetitionInterval (New-TimeSpan -Hours 1)

$settings = New-ScheduledTaskSettingsSet `
    -MultipleInstances IgnoreNew `
    -StartWhenAvailable `
    -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 75)

Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Settings $settings `
    -Description "CTA Transit Intelligence: hourly Train Tracker collection + upload to Databricks" -Force | Out-Null

Write-Host "Registered '$TaskName': hourly from $start, $DurationMinutes-minute sessions."
Write-Host "Logs: $repo\data\logs\collector.log"
