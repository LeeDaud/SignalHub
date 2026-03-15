param(
    [int]$Port = 8000
)

$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$runtimeDir = Join-Path $projectRoot ".signalhub-runtime"
$pidFile = Join-Path $runtimeDir ("signalhub-{0}.pid" -f $Port)

function Get-ListeningProcessId {
    param([int]$TargetPort)

    $connection = Get-NetTCPConnection -LocalPort $TargetPort -State Listen -ErrorAction SilentlyContinue |
        Select-Object -First 1
    if ($null -eq $connection) {
        return $null
    }
    return [int]$connection.OwningProcess
}

function Stop-ProcessSafe {
    param([int]$ProcessId)

    try {
        $process = Get-Process -Id $ProcessId -ErrorAction Stop
    } catch {
        return $false
    }

    Stop-Process -Id $ProcessId -Force -ErrorAction Stop
    $process.WaitForExit(10000) | Out-Null
    return $true
}

$stopped = $false

if (Test-Path $pidFile) {
    $rawPid = (Get-Content -Path $pidFile -Raw).Trim()
    if ($rawPid) {
        $stopped = Stop-ProcessSafe -ProcessId ([int]$rawPid)
        if ($stopped) {
            Write-Output ("Stopped SignalHub PID {0} from PID file." -f $rawPid)
        }
    }
    Remove-Item -Path $pidFile -Force -ErrorAction SilentlyContinue
}

if (-not $stopped) {
    $listeningPid = Get-ListeningProcessId -TargetPort $Port
    if ($null -ne $listeningPid) {
        $stopped = Stop-ProcessSafe -ProcessId $listeningPid
        if ($stopped) {
            Write-Output ("Stopped SignalHub PID {0} listening on port {1}." -f $listeningPid, $Port)
        }
    }
}

if (-not $stopped) {
    Write-Output ("No SignalHub process found for port {0}." -f $Port)
    exit 0
}

Remove-Item -Path $pidFile -Force -ErrorAction SilentlyContinue
