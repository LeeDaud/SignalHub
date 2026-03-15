param(
    [string]$HostName = "127.0.0.1",
    [int]$Port = 8000,
    [switch]$OpenDashboard
)

$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$stopScript = Join-Path $projectRoot "stop_service.ps1"
$startScript = Join-Path $projectRoot "start_service.ps1"

& $stopScript -Port $Port
Start-Sleep -Seconds 1

if ($OpenDashboard.IsPresent) {
    & $startScript -HostName $HostName -Port $Port -OpenDashboard
} else {
    & $startScript -HostName $HostName -Port $Port
}
