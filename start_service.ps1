param(
    [string]$HostName = "127.0.0.1",
    [int]$Port = 8000,
    [switch]$OpenDashboard
)

$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$runtimeDir = Join-Path $projectRoot ".signalhub-runtime"
$logDir = Join-Path $projectRoot "logs"
$pidFile = Join-Path $runtimeDir ("signalhub-{0}.pid" -f $Port)
$stdoutLog = Join-Path $logDir ("signalhub-{0}.stdout.log" -f $Port)
$stderrLog = Join-Path $logDir ("signalhub-{0}.stderr.log" -f $Port)

New-Item -ItemType Directory -Path $runtimeDir -Force | Out-Null
New-Item -ItemType Directory -Path $logDir -Force | Out-Null

function Get-ListeningProcessId {
    param([int]$TargetPort)

    $connection = Get-NetTCPConnection -LocalPort $TargetPort -State Listen -ErrorAction SilentlyContinue |
        Select-Object -First 1
    if ($null -eq $connection) {
        return $null
    }
    return [int]$connection.OwningProcess
}

function Get-AliveProcess {
    param([int]$ProcessId)

    try {
        return Get-Process -Id $ProcessId -ErrorAction Stop
    } catch {
        return $null
    }
}

if (Test-Path $pidFile) {
    $existingPid = (Get-Content -Path $pidFile -Raw).Trim()
    if ($existingPid) {
        $existingProcess = Get-AliveProcess -ProcessId ([int]$existingPid)
        if ($null -ne $existingProcess) {
            Write-Output ("SignalHub is already running on port {0} (PID {1})." -f $Port, $existingProcess.Id)
            exit 0
        }
    }
    Remove-Item -Path $pidFile -Force -ErrorAction SilentlyContinue
}

$listeningPid = Get-ListeningProcessId -TargetPort $Port
if ($null -ne $listeningPid) {
    throw ("Port {0} is already in use by PID {1}. Stop that process or choose a different port." -f $Port, $listeningPid)
}

$pythonCommand = Get-Command python -ErrorAction SilentlyContinue
if ($null -eq $pythonCommand) {
    $pythonCommand = Get-Command py -ErrorAction SilentlyContinue
}
if ($null -eq $pythonCommand) {
    throw "Python executable not found in PATH."
}

$pythonPath = $pythonCommand.Source
$arguments = @("run_local.py")
if ([System.IO.Path]::GetFileName($pythonPath).ToLowerInvariant() -eq "py.exe") {
    $arguments = @("-3", "run_local.py")
}

$env:HOST = $HostName
$env:PORT = [string]$Port
$env:RELOAD = "false"
$env:AUTO_OPEN_DASHBOARD = $(if ($OpenDashboard.IsPresent) { "true" } else { "false" })

$process = Start-Process `
    -FilePath $pythonPath `
    -ArgumentList $arguments `
    -WorkingDirectory $projectRoot `
    -RedirectStandardOutput $stdoutLog `
    -RedirectStandardError $stderrLog `
    -PassThru

Set-Content -Path $pidFile -Value $process.Id -Encoding ascii

Start-Sleep -Seconds 1
$aliveProcess = Get-AliveProcess -ProcessId $process.Id
if ($null -eq $aliveProcess) {
    Remove-Item -Path $pidFile -Force -ErrorAction SilentlyContinue
    $stderrTail = ""
    if (Test-Path $stderrLog) {
        $stderrTail = (Get-Content -Path $stderrLog -Tail 20) -join [Environment]::NewLine
    }
    throw ("SignalHub failed to stay running. Review stderr log:{0}{1}" -f [Environment]::NewLine, $stderrTail)
}

Write-Output ("SignalHub started on http://{0}:{1} (PID {2})." -f $HostName, $Port, $process.Id)
Write-Output ("PID file: {0}" -f $pidFile)
Write-Output ("Stdout log: {0}" -f $stdoutLog)
Write-Output ("Stderr log: {0}" -f $stderrLog)
