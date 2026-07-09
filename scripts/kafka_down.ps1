$ErrorActionPreference = "Stop"

$ScriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Split-Path -Parent $ScriptRoot

Push-Location $RepoRoot
try {
    $oldErrorActionPreference = $ErrorActionPreference
    $ErrorActionPreference = "SilentlyContinue"
    docker info *> $null
    $exitCode = $LASTEXITCODE
    $ErrorActionPreference = $oldErrorActionPreference

    if ($exitCode -ne 0) {
        Write-Host "Docker daemon is not running; skip docker compose down."
        return
    }

    docker compose down -v
} finally {
    Pop-Location
}
