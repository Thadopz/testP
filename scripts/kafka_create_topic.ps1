param(
    [string]$Topic = "order-events",
    [int]$Partitions = 64,
    [int]$ReplicationFactor = 1,
    [int]$ReadyTimeoutSeconds = 60
)

$ErrorActionPreference = "Stop"

$ScriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Split-Path -Parent $ScriptRoot

function Wait-KafkaReady {
    $deadline = (Get-Date).AddSeconds($ReadyTimeoutSeconds)

    Write-Host "Waiting for Kafka to become ready..."
    while ((Get-Date) -lt $deadline) {
        $previousErrorAction = $ErrorActionPreference
        $ErrorActionPreference = "Continue"
        docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh `
            --bootstrap-server localhost:9092 `
            --list *> $null
        $kafkaExitCode = $LASTEXITCODE
        $ErrorActionPreference = $previousErrorAction

        if ($kafkaExitCode -eq 0) {
            Write-Host "Kafka is ready."
            return
        }

        Start-Sleep -Seconds 1
    }

    throw "Kafka did not become ready within $ReadyTimeoutSeconds seconds. Check: docker compose logs kafka"
}

Push-Location $RepoRoot
try {
    Wait-KafkaReady

    docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh `
        --bootstrap-server localhost:9092 `
        --create `
        --if-not-exists `
        --topic $Topic `
        --partitions $Partitions `
        --replication-factor $ReplicationFactor

    docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh `
        --bootstrap-server localhost:9092 `
        --describe `
        --topic $Topic
} finally {
    Pop-Location
}
