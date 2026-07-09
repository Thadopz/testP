param(
    [int]$TimeoutSeconds = 60
)

$ErrorActionPreference = "Stop"

function Assert-DockerReady {
    $oldErrorActionPreference = $ErrorActionPreference
    $ErrorActionPreference = "SilentlyContinue"
    docker info *> $null
    $exitCode = $LASTEXITCODE
    $ErrorActionPreference = $oldErrorActionPreference

    if ($exitCode -ne 0) {
        throw "Docker daemon is not running. Start Docker Desktop and retry."
    }
}

function Wait-KafkaReady {
    param([int]$TimeoutSeconds)

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        $oldErrorActionPreference = $ErrorActionPreference
        $ErrorActionPreference = "SilentlyContinue"
        docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list *> $null
        $exitCode = $LASTEXITCODE
        $ErrorActionPreference = $oldErrorActionPreference

        if ($exitCode -eq 0) {
            return
        }
        Start-Sleep -Seconds 1
    }

    throw "Timed out waiting for Kafka to become ready"
}

$ScriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Split-Path -Parent $ScriptRoot

Push-Location $RepoRoot
try {
    Assert-DockerReady
    docker compose up -d kafka
    if ($LASTEXITCODE -ne 0) {
        throw "docker compose up failed"
    }
    Wait-KafkaReady -TimeoutSeconds $TimeoutSeconds
    Write-Host "Kafka is ready at localhost:9092"
} finally {
    Pop-Location
}
