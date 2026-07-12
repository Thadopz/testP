param(
    [int]$Partitions = 64,
    [int]$ReplicationFactor = 1,
    [int]$ReadyTimeoutSeconds = 60
)

$ErrorActionPreference = "Stop"
$ScriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path

foreach ($Topic in @("order-events", "rider-events", "match-requests")) {
    & "$ScriptRoot\kafka_create_topic.ps1" `
        -Topic $Topic `
        -Partitions $Partitions `
        -ReplicationFactor $ReplicationFactor `
        -ReadyTimeoutSeconds $ReadyTimeoutSeconds
}
