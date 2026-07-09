param(
    [string]$DataDir = "",
    [string]$Brokers = "127.0.0.1:9092",
    [int]$ShardCount = 64,
    [int]$TimeoutSeconds = 45,
    [switch]$KeepKafka
)

$ErrorActionPreference = "Stop"

function Write-Step {
    param([string]$Message)
    Write-Host "[kafka-e2e] $Message"
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
        Start-Sleep -Milliseconds 200
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

    return Start-Process `
        -FilePath $FilePath `
        -ArgumentList $ArgumentList `
        -WorkingDirectory $RepoRoot `
        -RedirectStandardOutput (Join-Path $LogDir "$Name.out.log") `
        -RedirectStandardError (Join-Path $LogDir "$Name.err.log") `
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

function Get-OrderStatus {
    param(
        [string]$OrderStatePath,
        [int64]$OrderID
    )

    if (!(Test-Path $OrderStatePath)) {
        return $null
    }

    $orders = Get-Content -Path $OrderStatePath -Raw | ConvertFrom-Json
    $property = $orders.PSObject.Properties["$OrderID"]
    if ($null -eq $property) {
        return $null
    }

    return [string]$property.Value.Status
}

$ScriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Split-Path -Parent $ScriptRoot

if ([string]::IsNullOrWhiteSpace($DataDir)) {
    $DataDir = Join-Path ([System.IO.Path]::GetTempPath()) ("testP-kafka-e2e-" + [Guid]::NewGuid().ToString("N"))
}

$DataDir = [System.IO.Path]::GetFullPath($DataDir)
$BinDir = Join-Path $DataDir "bin"
$LogDir = Join-Path $DataDir "logs"
$OrderStatePath = Join-Path (Join-Path $DataDir "orders") "orders.json"
$Topic = "order-events-" + [Guid]::NewGuid().ToString("N")
$EtcdPrefix = "/testp-kafka-e2e-" + [Guid]::NewGuid().ToString("N")
$OrderID = 1001
$node = $null
$controller = $null

try {
    Write-Step "repo: $RepoRoot"
    Write-Step "data dir: $DataDir"
    Write-Step "topic: $Topic"

    New-Item -ItemType Directory -Force -Path $BinDir, $LogDir | Out-Null

    Write-Step "starting Kafka"
    Invoke-CheckedScript -ArgumentList @(
        "-ExecutionPolicy", "Bypass",
        "-File", (Join-Path $ScriptRoot "kafka_up.ps1"),
        "-TimeoutSeconds", "$TimeoutSeconds"
    )

    Write-Step "starting etcd"
    Push-Location $RepoRoot
    try {
        docker compose up -d etcd | Out-Null
    } finally {
        Pop-Location
    }

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

    Push-Location $RepoRoot
    try {
        Write-Step "building controller/node/producer"
        go build -o (Join-Path $BinDir "controller.exe") ./cmd/controller
        go build -o (Join-Path $BinDir "node.exe") ./cmd/node
        go build -o (Join-Path $BinDir "producer.exe") ./cmd/producer
    } finally {
        Pop-Location
    }

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

    Write-Step "starting node with Kafka eventlog"
    $node = Start-E2EProcess `
        -FilePath (Join-Path $BinDir "node.exe") `
        -ArgumentList @(
            "-node-id", "1",
            "-data-dir", $DataDir,
            "-etcd-endpoints", "127.0.0.1:2379",
            "-etcd-prefix", $EtcdPrefix,
            "-heartbeat-interval", "200ms",
            "-membership-ttl", "1500ms",
            "-kafka-brokers", $Brokers,
            "-kafka-topic", $Topic,
            "-riders", "20",
            "-workers", "1"
        ) `
        -Name "node" `
        -LogDir $LogDir

    Start-Sleep -Seconds 2
    if ($node.HasExited -or $controller.HasExited) {
        throw "node or controller exited early. See $LogDir"
    }

    Write-Step "producing one order to Kafka"
    $producer = Start-Process `
        -FilePath (Join-Path $BinDir "producer.exe") `
        -ArgumentList @(
            "-eventlog", "kafka",
            "-kafka-brokers", $Brokers,
            "-kafka-topic", $Topic,
            "-data-dir", $DataDir,
            "-orders", "1",
            "-seed", "7",
            "-start-id", "$OrderID"
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

    Write-Step "waiting for order state to become matched or missed"
    Wait-Until -TimeoutSeconds $TimeoutSeconds -Description "order final state" -Condition {
        $status = Get-OrderStatus -OrderStatePath $OrderStatePath -OrderID $OrderID
        return ($status -eq "matched" -or $status -eq "missed")
    }

    Write-Step "PASS"
    Write-Host "data_dir: $DataDir"
    Write-Host "logs_dir: $LogDir"
    Write-Host "topic: $Topic"
    Write-Host "order_state_file: $OrderStatePath"
} finally {
    Stop-ProcessIfRunning $node
    Stop-ProcessIfRunning $controller
    if (-not $KeepKafka) {
        & powershell -ExecutionPolicy Bypass -File (Join-Path $ScriptRoot "kafka_down.ps1")
    }
}
