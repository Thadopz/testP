param(
    [string]$DataDir = "",
    [string]$EtcdPrefix = "",
    [string]$Brokers = "127.0.0.1:9092",
    [int]$ShardCount = 64,
    [int]$TimeoutSeconds = 25,
    [switch]$KeepKafka
)

$ErrorActionPreference = "Stop"

function Write-Step {
    param([string]$Message)
    Write-Host "[e2e] $Message"
}

function Wait-Until {
    param(
        [scriptblock]$Condition,
        [string]$Description,
        [int]$TimeoutSeconds
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        if (& $Condition) {
            return
        }
        Start-Sleep -Milliseconds 100
    }

    throw "Timed out waiting for $Description"
}

function Stop-ProcessIfRunning {
    param($Process)

    if ($null -eq $Process) {
        return
    }
    if ($Process.HasExited) {
        return
    }

    Stop-Process -Id $Process.Id -Force -ErrorAction SilentlyContinue
    $Process.WaitForExit(3000) | Out-Null
}

function Invoke-CheckedScript {
    param([string[]]$ArgumentList)

    & powershell @ArgumentList
    if ($LASTEXITCODE -ne 0) {
        throw "script failed: powershell $($ArgumentList -join ' ')"
    }
}

function Start-E2EProcess {
    param(
        [string]$FilePath,
        [string[]]$ArgumentList,
        [string]$Name,
        [string]$LogDir
    )

    $stdout = Join-Path $LogDir "$Name.out.log"
    $stderr = Join-Path $LogDir "$Name.err.log"
    return Start-Process `
        -FilePath $FilePath `
        -ArgumentList $ArgumentList `
        -WorkingDirectory $RepoRoot `
        -RedirectStandardOutput $stdout `
        -RedirectStandardError $stderr `
        -PassThru `
        -WindowStyle Hidden
}

function Invoke-EtcdCtl {
    param([string[]]$Arguments)

    $allArgs = @(
        "exec",
        "testp-etcd",
        "/usr/local/bin/etcdctl",
        "--endpoints=http://127.0.0.1:2379"
    ) + $Arguments

    return docker @allArgs
}

function Clear-EtcdPrefix {
    param([string]$Prefix)

    Invoke-EtcdCtl -Arguments @("del", $Prefix, "--prefix") | Out-Null
}

function Get-EtcdOwnershipValues {
    param([string]$Prefix)

    $jsonText = Invoke-EtcdCtl -Arguments @("get", "$Prefix/ownership/shards/", "--prefix", "-w", "json")
    if ([string]::IsNullOrWhiteSpace($jsonText)) {
        return @()
    }

    $parsed = $jsonText | ConvertFrom-Json
    if ($null -eq $parsed.kvs) {
        return @()
    }

    $values = @()
    foreach ($kv in $parsed.kvs) {
        $bytes = [Convert]::FromBase64String($kv.value)
        $valueText = [System.Text.Encoding]::UTF8.GetString($bytes)
        $values += ($valueText | ConvertFrom-Json)
    }
    return $values
}

function Get-EtcdCheckpointValues {
    param([string]$Prefix)

    $jsonText = Invoke-EtcdCtl -Arguments @("get", "$Prefix/checkpoints/shards/", "--prefix", "-w", "json")
    if ([string]::IsNullOrWhiteSpace($jsonText)) {
        return @()
    }

    $parsed = $jsonText | ConvertFrom-Json
    if ($null -eq $parsed.kvs) {
        return @()
    }

    $values = @()
    foreach ($kv in $parsed.kvs) {
        $bytes = [Convert]::FromBase64String($kv.value)
        $valueText = [System.Text.Encoding]::UTF8.GetString($bytes)
        $values += ($valueText | ConvertFrom-Json)
    }
    return $values
}

function Get-AnyCheckpointOffset {
    param([string]$Prefix)

    $checkpoints = @(Get-EtcdCheckpointValues -Prefix $Prefix)
    foreach ($checkpoint in $checkpoints) {
        $offset = [int64]$checkpoint.Offset
        if ($offset -gt 0) {
            return $offset
        }
    }

    return $null
}

$ScriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Split-Path -Parent $ScriptRoot

if ([string]::IsNullOrWhiteSpace($DataDir)) {
    $DataDir = Join-Path ([System.IO.Path]::GetTempPath()) ("testP-e2e-" + [Guid]::NewGuid().ToString("N"))
}
if ([string]::IsNullOrWhiteSpace($EtcdPrefix)) {
    $EtcdPrefix = "/testp-e2e-" + [Guid]::NewGuid().ToString("N")
}

$DataDir = [System.IO.Path]::GetFullPath($DataDir)
$BinDir = Join-Path $DataDir "bin"
$LogDir = Join-Path $DataDir "logs"
$Topic = "order-events-" + [Guid]::NewGuid().ToString("N")

$controller = $null
$node1 = $null
$node2 = $null

try {
    Write-Step "repo: $RepoRoot"
    Write-Step "data dir: $DataDir"
    Write-Step "etcd prefix: $EtcdPrefix"
    Write-Step "topic: $Topic"

    New-Item -ItemType Directory -Force -Path $BinDir, $LogDir | Out-Null

    Write-Step "starting Kafka"
    Invoke-CheckedScript -ArgumentList @(
        "-ExecutionPolicy", "Bypass",
        "-File", (Join-Path $ScriptRoot "kafka_up.ps1"),
        "-TimeoutSeconds", "$TimeoutSeconds"
    )

    Push-Location $RepoRoot
    try {
        Write-Step "starting etcd"
        docker compose up -d etcd | Out-Null

        Write-Step "waiting for etcd"
        Wait-Until -TimeoutSeconds $TimeoutSeconds -Description "etcd health" -Condition {
            Invoke-EtcdCtl -Arguments @("endpoint", "health") | Out-Null
            return ($LASTEXITCODE -eq 0)
        }

        Clear-EtcdPrefix -Prefix $EtcdPrefix

        Write-Step "creating Kafka topic"
        Invoke-CheckedScript -ArgumentList @(
            "-ExecutionPolicy", "Bypass",
            "-File", (Join-Path $ScriptRoot "kafka_create_topic.ps1"),
            "-Topic", $Topic,
            "-Partitions", "$ShardCount",
            "-ReplicationFactor", "1"
        )

        Write-Step "building controller/node/producer"
        go build -o (Join-Path $BinDir "controller.exe") ./cmd/controller
        go build -o (Join-Path $BinDir "node.exe") ./cmd/node
        go build -o (Join-Path $BinDir "producer.exe") ./cmd/producer
    } finally {
        Pop-Location
    }

    Write-Step "starting node 1"
    $node1 = Start-E2EProcess `
        -FilePath (Join-Path $BinDir "node.exe") `
        -ArgumentList @(
            "-node-id", "1",
            "-data-dir", $DataDir,
            "-heartbeat-interval", "200ms",
            "-membership-ttl", "1500ms",
            "-etcd-endpoints", "127.0.0.1:2379",
            "-etcd-prefix", $EtcdPrefix,
            "-kafka-brokers", $Brokers,
            "-kafka-topic", $Topic,
            "-riders", "20",
            "-workers", "1"
        ) `
        -Name "node1" `
        -LogDir $LogDir

    Write-Step "starting node 2"
    $node2 = Start-E2EProcess `
        -FilePath (Join-Path $BinDir "node.exe") `
        -ArgumentList @(
            "-node-id", "2",
            "-data-dir", $DataDir,
            "-heartbeat-interval", "200ms",
            "-membership-ttl", "1500ms",
            "-etcd-endpoints", "127.0.0.1:2379",
            "-etcd-prefix", $EtcdPrefix,
            "-kafka-brokers", $Brokers,
            "-kafka-topic", $Topic,
            "-riders", "20",
            "-workers", "1"
        ) `
        -Name "node2" `
        -LogDir $LogDir

    Write-Step "starting controller"
    $controller = Start-E2EProcess `
        -FilePath (Join-Path $BinDir "controller.exe") `
        -ArgumentList @(
            "-etcd-endpoints", "127.0.0.1:2379",
            "-etcd-prefix", $EtcdPrefix,
            "-membership-ttl", "1500ms",
            "-election-ttl", "2s",
            "-sweep-interval", "200ms",
            "-shards", "$ShardCount"
        ) `
        -Name "controller" `
        -LogDir $LogDir

    Start-Sleep -Milliseconds 800
    if ($node1.HasExited -or $node2.HasExited -or $controller.HasExited) {
        throw "node or controller exited early. See $LogDir"
    }

    Write-Step "waiting for initial ownership"
    Wait-Until -TimeoutSeconds $TimeoutSeconds -Description "initial ownership layout" -Condition {
        $values = @(Get-EtcdOwnershipValues -Prefix $EtcdPrefix)
        if ($values.Count -ne $ShardCount) {
            return $false
        }
        foreach ($ownership in $values) {
            if ([int]$ownership.NodeID -eq 2) {
                return $true
            }
        }
        return $false
    }

    Write-Step "simulating node 2 failure"
    Stop-ProcessIfRunning $node2
    $node2 = $null

    Write-Step "waiting for controller failover: node 2 shards -> node 1"
    Wait-Until -TimeoutSeconds $TimeoutSeconds -Description "ownership failover to node 1" -Condition {
        $values = @(Get-EtcdOwnershipValues -Prefix $EtcdPrefix)
        if ($values.Count -lt $ShardCount) {
            return $false
        }

        foreach ($ownership in $values) {
            if ([int]$ownership.NodeID -ne 1) {
                return $false
            }
        }
        return $true
    }

    Write-Step "writing one order event"
    $producer = Start-Process `
        -FilePath (Join-Path $BinDir "producer.exe") `
        -ArgumentList @(
            "-kafka-brokers", $Brokers,
            "-kafka-topic", $Topic,
            "-data-dir", $DataDir,
            "-orders", "1",
            "-seed", "7",
            "-start-id", "1001"
        ) `
        -WorkingDirectory $RepoRoot `
        -Wait `
        -PassThru `
        -RedirectStandardOutput (Join-Path $LogDir "producer.out.log") `
        -RedirectStandardError (Join-Path $LogDir "producer.err.log") `
        -WindowStyle Hidden

    if ($producer.ExitCode -ne 0) {
        throw "producer failed with exit code $($producer.ExitCode). See $LogDir"
    }

    Write-Step "waiting for node 1 checkpoint to advance"
    Wait-Until -TimeoutSeconds $TimeoutSeconds -Description "node 1 checkpoint offset" -Condition {
        $offset = Get-AnyCheckpointOffset -Prefix $EtcdPrefix
        return ($null -ne $offset -and $offset -ge 1)
    }

    Write-Step "PASS"
    Write-Host "data_dir: $DataDir"
    Write-Host "logs_dir: $LogDir"
    Write-Host "etcd_prefix: $EtcdPrefix"
    Write-Host "checkpoint_prefix: $EtcdPrefix/checkpoints/shards/"
} finally {
    Stop-ProcessIfRunning $node2
    Stop-ProcessIfRunning $node1
    Stop-ProcessIfRunning $controller
    if (-not $KeepKafka) {
        & powershell -ExecutionPolicy Bypass -File (Join-Path $ScriptRoot "kafka_down.ps1")
    }
}
