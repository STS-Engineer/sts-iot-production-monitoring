param(
    [ValidateSet("once", "scheduler")]
    [string]$Mode = "scheduler",
    [int]$Interval = 60,
    [switch]$Align
)

$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$workspaceDir = Split-Path -Parent $scriptDir
$pythonExe = Join-Path $workspaceDir ".venv\Scripts\python.exe"

if (-not (Test-Path $pythonExe)) {
    throw "Python virtual environment not found at $pythonExe"
}

Push-Location $scriptDir
try {
    if ($Mode -eq "once") {
        & $pythonExe "main.py" --once
    }
    else {
        if ($Align) {
            & $pythonExe "main.py" --interval $Interval --align
        }
        else {
            & $pythonExe "main.py" --interval $Interval
        }
    }
}
finally {
    Pop-Location
}
