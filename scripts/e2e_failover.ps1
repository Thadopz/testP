param(
    [string]$DataDir = "",
    [string]$ControllerAddr = "127.0.0.1:19000",
    [int]$ShardCount = 64,
    [int]$TimeoutSeconds = 20
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

function Initialize-AllShardsToNode {
    param(
        [string]$OwnershipDir,
        [int]$ShardCount,
        [int]$NodeID
    )

    New-Item -ItemType Directory -Force -Path $OwnershipDir | Out-Null

    $ownership = [ordered]@{}
    for ($shardID = 0; $shardID -lt $ShardCount; $shardID++) {
        $ownership["$shardID"] = [ordered]@{
            ShardID = $shardID
            NodeID  = $NodeID
            Epoch   = 1
        }
    }

    $ownershipPath = Join-Path $OwnershipDir "ownership.json"
    $json = $ownership | ConvertTo-Json -Depth 5
    $utf8NoBom = New-Object System.Text.UTF8Encoding($false)
    [System.IO.File]::WriteAllText($ownershipPath, $json, $utf8NoBom)
}

function Get-OwnershipValues {
    param([string]$OwnershipPath)

    if (!(Test-Path $OwnershipPath)) {
        return @()
    }

    $json = Get-Content -Path $OwnershipPath -Raw | ConvertFrom-Json
    $values = @()
    foreach ($property in $json.PSObject.Properties) {
        $values += $property.Value
    }
    return $values
}

function Get-ProducedShardID {
    param([string]$EventDir)

    $eventFile = Get-ChildItem -Path $EventDir -Filter "shard-*.log" | Select-Object -First 1
    if ($null -eq $eventFile) {
        return $null
    }

    if ($eventFile.Name -match "^shard-(\d+)\.log$") {
        return [int]$Matches[1]
    }

    return $null
}

function Get-CheckpointOffset {
    param(
        [string]$CheckpointPath,
        [int]$ShardID
    )

    if (!(Test-Path $CheckpointPath)) {
        return $null
    }

    $checkpoint = Get-Content -Path $CheckpointPath -Raw | ConvertFrom-Json
    $property = $checkpoint.Offset.PSObject.Properties["$ShardID"]
    if ($null -eq $property) {
        return $null
    }

    return [int64]$property.Value
}

$ScriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Split-Path -Parent $ScriptRoot

if ([string]::IsNullOrWhiteSpace($DataDir)) {
    $DataDir = Join-Path ([System.IO.Path]::GetTempPath()) ("testP-e2e-" + [Guid]::NewGuid().ToString("N"))
}

$DataDir = [System.IO.Path]::GetFullPath($DataDir)
$BinDir = Join-Path $DataDir "bin"
$LogDir = Join-Path $DataDir "logs"
$OwnershipDir = Join-Path $DataDir "ownership"
$OwnershipPath = Join-Path $OwnershipDir "ownership.json"
$EventDir = Join-Path $DataDir "events"
$CheckpointPath = Join-Path (Join-Path $DataDir "checkpoints") "node-1.json"

$controller = $null
$node1 = $null
$node2 = $null

try {
    Write-Step "repo: $RepoRoot"
    Write-Step "data dir: $DataDir"

    New-Item -ItemType Directory -Force -Path $BinDir, $LogDir | Out-Null

    Push-Location $RepoRoot
    try {
        Write-Step "building controller/node/producer"
        go build -o (Join-Path $BinDir "controller.exe") ./cmd/controller
        go build -o (Join-Path $BinDir "node.exe") ./cmd/node
        go build -o (Join-Path $BinDir "producer.exe") ./cmd/producer
    } finally {
        Pop-Location
    }

    Write-Step "initializing ownership: all shards -> node 2"
    Initialize-AllShardsToNode -OwnershipDir $OwnershipDir -ShardCount $ShardCount -NodeID 2

    Write-Step "starting controller"
    $controller = Start-E2EProcess `
        -FilePath (Join-Path $BinDir "controller.exe") `
        -ArgumentList @(
            "-addr", $ControllerAddr,
            "-data-dir", $DataDir,
            "-heartbeat-timeout", "1500ms",
            "-sweep-interval", "200ms"
        ) `
        -Name "controller" `
        -LogDir $LogDir

    Start-Sleep -Milliseconds 500
    if ($controller.HasExited) {
        throw "controller exited early. See $LogDir"
    }

    Write-Step "starting node 1 dynamic runner"
    $node1 = Start-E2EProcess `
        -FilePath (Join-Path $BinDir "node.exe") `
        -ArgumentList @(
            "-node-id", "1",
            "-dynamic",
            "-tail",
            "-data-dir", $DataDir,
            "-heartbeat-addr", $ControllerAddr,
            "-heartbeat-interval", "200ms",
            "-riders", "20",
            "-workers", "1"
        ) `
        -Name "node1" `
        -LogDir $LogDir

    Start-Sleep -Milliseconds 800
    if ($node1.HasExited) {
        throw "node 1 exited early. See $LogDir"
    }

    Write-Step "starting node 2 heartbeat owner"
    $node2 = Start-E2EProcess `
        -FilePath (Join-Path $BinDir "node.exe") `
        -ArgumentList @(
            "-node-id", "2",
            "-dynamic",
            "-tail",
            "-data-dir", $DataDir,
            "-heartbeat-addr", $ControllerAddr,
            "-heartbeat-interval", "200ms",
            "-riders", "20",
            "-workers", "1"
        ) `
        -Name "node2" `
        -LogDir $LogDir

    Start-Sleep -Milliseconds 800
    if ($node2.HasExited) {
        throw "node 2 exited before simulated failure. See $LogDir"
    }

    Write-Step "simulating node 2 failure"
    Stop-ProcessIfRunning $node2
    $node2 = $null

    Write-Step "waiting for controller failover: node 2 shards -> node 1"
    Wait-Until -TimeoutSeconds $TimeoutSeconds -Description "ownership failover to node 1" -Condition {
        $values = @(Get-OwnershipValues -OwnershipPath $OwnershipPath)
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
        -ArgumentList @("-data-dir", $DataDir, "-orders", "1", "-seed", "7", "-start-id", "1001") `
        -WorkingDirectory $RepoRoot `
        -Wait `
        -PassThru `
        -RedirectStandardOutput (Join-Path $LogDir "producer.out.log") `
        -RedirectStandardError (Join-Path $LogDir "producer.err.log") `
        -WindowStyle Hidden

    if ($producer.ExitCode -ne 0) {
        throw "producer failed with exit code $($producer.ExitCode). See $LogDir"
    }

    $producedShardID = Get-ProducedShardID -EventDir $EventDir
    if ($null -eq $producedShardID) {
        throw "could not find produced shard log in $EventDir"
    }
    Write-Step "produced shard: $producedShardID"

    Write-Step "waiting for node 1 checkpoint on produced shard"
    Wait-Until -TimeoutSeconds $TimeoutSeconds -Description "node 1 checkpoint offset for shard $producedShardID" -Condition {
        $offset = Get-CheckpointOffset -CheckpointPath $CheckpointPath -ShardID $producedShardID
        return ($null -ne $offset -and $offset -ge 1)
    }

    Write-Step "PASS"
    Write-Host "data_dir: $DataDir"
    Write-Host "logs_dir: $LogDir"
    Write-Host "ownership_file: $OwnershipPath"
    Write-Host "checkpoint_file: $CheckpointPath"
} finally {
    Stop-ProcessIfRunning $node2
    Stop-ProcessIfRunning $node1
    Stop-ProcessIfRunning $controller
}
