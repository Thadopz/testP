param(
    [string]$Topic = "order-events",
    [int]$Partitions = 64,
    [int]$ReplicationFactor = 1
)

$ErrorActionPreference = "Stop"

$ScriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Split-Path -Parent $ScriptRoot

Push-Location $RepoRoot
try {
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
