param(
    [int]$Orders = 1000,
    [int]$TimeoutSeconds = 180,
    [int]$ProducerBatchSize = 100,
    [int]$PublisherBatchSize = 100,
    [int]$PollIntervalMs = 100,
    [int]$WarmupSeconds = 5,
    [int]$PublisherCount = 1,
    [int]$RetryRounds = 0,
    [int]$MatcherMaxRiderOrders = 3,
    [int]$Riders = 100,
    [int64]$OrderStartID = 0,
    [string]$ResultCsv = ""
)

$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
if ($OrderStartID -gt 0) {
    $StartID = $OrderStartID
} else {
    $StartID = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
}
$LastID = $StartID + $Orders - 1
$TempDir = Join-Path $env:TEMP "testp-bench-outbox-$StartID"
$Processes = @()
$Samples = New-Object System.Collections.Generic.List[object]

function Invoke-PostgresScalar {
    param([string]$Sql)

    $value = docker exec testp-postgres psql -U testp -d testp -Atc $Sql
    if ($LASTEXITCODE -ne 0) {
        throw "postgres query failed: $Sql"
    }
    return [string]$value
}

function Start-BenchProcess {
    param(
        [string]$Name,
        [string]$Path,
        [string[]]$ProcessArguments
    )

    $executable = Join-Path $TempDir "$Name.exe"
    go build -o $executable $Path
    if ($LASTEXITCODE -ne 0) {
        throw "go build failed: $Path"
    }

    return Start-Process `
        -FilePath $executable `
        -ArgumentList $ProcessArguments `
        -PassThru `
        -WindowStyle Hidden `
        -RedirectStandardOutput (Join-Path $TempDir "$Name.out") `
        -RedirectStandardError (Join-Path $TempDir "$Name.err")
}

function Get-CompletionPercentile {
    param(
        [System.Collections.Generic.List[object]]$Values,
        [int]$TotalOrders,
        [double]$Percentile
    )

    if ($Values.Count -eq 0 -or $TotalOrders -le 0) {
        return 0
    }

    $target = [int][Math]::Ceiling(($Percentile / 100.0) * $TotalOrders)
    foreach ($sample in $Values) {
        if ($sample.retry_round -eq 0 -and $sample.terminal -ge $target) {
            return [double]$sample.elapsed_ms
        }
    }
    return [double]$Values[$Values.Count - 1].elapsed_ms
}

function Assert-DockerReady {
    docker info | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "Docker engine is not available. Start Docker Desktop and retry."
    }
}

New-Item -ItemType Directory -Path $TempDir -Force | Out-Null

Push-Location $RepoRoot
try {
    Assert-DockerReady

    docker compose up -d postgres etcd kafka
    if ($LASTEXITCODE -ne 0) {
        throw "docker compose up failed"
    }

    & .\scripts\kafka_create_topics.ps1 | Out-Null

    $existingPending = Invoke-PostgresScalar "SELECT count(*) FROM outbox_events WHERE published_at IS NULL;"
    Write-Host "bench_temp_dir: $TempDir"
    Write-Host "orders: $Orders"
    Write-Host "riders: $Riders"
    Write-Host "order_id_range: $StartID..$LastID"
    Write-Host "existing_outbox_pending: $existingPending"

    $Processes += Start-BenchProcess `
        -Name "controller" `
        -Path ".\cmd\controller" `
        -ProcessArguments @("-metrics-addr=")

    $Processes += Start-BenchProcess `
        -Name "node" `
        -Path ".\cmd\node" `
        -ProcessArguments @("-node-id", "1", "-metrics-addr=")

    $Processes += Start-BenchProcess `
        -Name "matcher" `
        -Path ".\cmd\matcher-worker" `
        -ProcessArguments @(
            "-node-id", "1",
            "-riders", "$Riders",
            "-max-rider-orders", "$MatcherMaxRiderOrders"
        )

    for ($index = 1; $index -le $PublisherCount; $index++) {
        $Processes += Start-BenchProcess `
            -Name "publisher-$index" `
            -Path ".\cmd\outbox-publisher" `
            -ProcessArguments @(
                "-worker-id", "bench-publisher-$index",
                "-batch-size", "$PublisherBatchSize",
                "-poll-interval", "$PollIntervalMs`ms"
            )
    }

    Start-Sleep -Seconds $WarmupSeconds

    $producerExe = Join-Path $TempDir "producer.exe"
    go build -o $producerExe .\cmd\producer
    if ($LASTEXITCODE -ne 0) {
        throw "go build failed: .\cmd\producer"
    }

    $retryProducerExe = Join-Path $TempDir "retry-producer.exe"
    if ($RetryRounds -gt 0) {
        go build -o $retryProducerExe .\cmd\retry-producer
        if ($LASTEXITCODE -ne 0) {
            throw "go build failed: .\cmd\retry-producer"
        }
    }

    $stopwatch = [System.Diagnostics.Stopwatch]::StartNew()
    $producer = Start-Process `
        -FilePath $producerExe `
        -ArgumentList @(
            "-orders", "$Orders",
            "-start-id", "$StartID",
            "-batch-size", "$ProducerBatchSize",
            "-metrics-addr="
        ) `
        -PassThru `
        -Wait `
        -WindowStyle Hidden `
        -RedirectStandardOutput (Join-Path $TempDir "producer.out") `
        -RedirectStandardError (Join-Path $TempDir "producer.err")

    if ($producer.ExitCode -ne 0) {
        throw "producer failed; logs: $TempDir"
    }

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    do {
        Start-Sleep -Milliseconds 500

        $terminal = [int](Invoke-PostgresScalar "SELECT count(*) FROM orders WHERE order_id BETWEEN $StartID AND $LastID AND status IN ('matched','missed');")
        $elapsedMs = $stopwatch.Elapsed.TotalMilliseconds
        $Samples.Add([pscustomobject]@{
            elapsed_ms = [Math]::Round($elapsedMs, 0)
            retry_round = 0
            terminal = $terminal
        }) | Out-Null

    } while ($terminal -lt $Orders -and (Get-Date) -lt $deadline)

    if ($terminal -lt $Orders) {
        throw "timed out waiting for initial matching round"
    }

    $retryEventsSent = 0
    for ($attempt = 1; $attempt -le $RetryRounds; $attempt++) {
        $retryOutput = & $retryProducerExe `
            -start-id $StartID `
            -end-id $LastID `
            -attempt $attempt `
            -batch-size $ProducerBatchSize
        if ($LASTEXITCODE -ne 0) {
            throw "retry producer failed at attempt $attempt"
        }

        $sentLine = $retryOutput | Where-Object { $_ -like "retry_events_sent:*" }
        $sent = [int](($sentLine -split ":", 2)[1].Trim())
        $retryEventsSent += $sent
        Write-Host "retry_round: $attempt, events_sent: $sent"

        if ($sent -eq 0) {
            continue
        }

        $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
        do {
            Start-Sleep -Milliseconds 500
            $terminal = [int](Invoke-PostgresScalar "SELECT count(*) FROM orders WHERE order_id BETWEEN $StartID AND $LastID AND (status = 'matched' OR (status = 'missed' AND attempt >= $attempt));")
            $Samples.Add([pscustomobject]@{
                elapsed_ms = [Math]::Round($stopwatch.Elapsed.TotalMilliseconds, 0)
                retry_round = $attempt
                terminal = $terminal
            }) | Out-Null
        } while ($terminal -lt $Orders -and (Get-Date) -lt $deadline)

        if ($terminal -lt $Orders) {
            throw "timed out waiting for retry attempt $attempt"
        }
    }

    $stopwatch.Stop()

    $matched = [int](Invoke-PostgresScalar "SELECT count(*) FROM orders WHERE order_id BETWEEN $StartID AND $LastID AND status = 'matched';")
    $missed = [int](Invoke-PostgresScalar "SELECT count(*) FROM orders WHERE order_id BETWEEN $StartID AND $LastID AND status = 'missed';")
    $created = [int](Invoke-PostgresScalar "SELECT count(*) FROM orders WHERE order_id BETWEEN $StartID AND $LastID;")
    $maxAttempt = [int](Invoke-PostgresScalar "SELECT COALESCE(max(attempt), 0) FROM orders WHERE order_id BETWEEN $StartID AND $LastID;")
    $pendingOutbox = [int](Invoke-PostgresScalar "SELECT count(*) FROM outbox_events WHERE published_at IS NULL AND aggregate_id ~ '^[0-9]+$' AND aggregate_id::bigint BETWEEN $StartID AND $LastID;")

    $elapsedSeconds = $stopwatch.Elapsed.TotalSeconds
    if ($elapsedSeconds -le 0) {
        $throughput = 0
    } else {
        $throughput = $terminal / $elapsedSeconds
    }

    Write-Host ""
    Write-Host "summary"
    Write-Host "-------"
    Write-Host ("created_orders:         {0}" -f $created)
    Write-Host ("terminal_orders:        {0}" -f $terminal)
    Write-Host ("matched_orders:         {0}" -f $matched)
    Write-Host ("missed_orders:          {0}" -f $missed)
    Write-Host ("retry_rounds:           {0}" -f $RetryRounds)
    Write-Host ("retry_events_sent:      {0}" -f $retryEventsSent)
    Write-Host ("max_attempt:            {0}" -f $maxAttempt)
    Write-Host ("scoped_outbox_pending:  {0}" -f $pendingOutbox)
    Write-Host ("elapsed_seconds:        {0:N2}" -f $elapsedSeconds)
    Write-Host ("throughput_orders_sec:  {0:N2}" -f $throughput)
    Write-Host ("estimated_p50_ms:       {0:N0}" -f (Get-CompletionPercentile $Samples $Orders 50))
    Write-Host ("estimated_p95_ms:       {0:N0}" -f (Get-CompletionPercentile $Samples $Orders 95))
    Write-Host ("estimated_p99_ms:       {0:N0}" -f (Get-CompletionPercentile $Samples $Orders 99))
    Write-Host "logs_dir:               $TempDir"

    $samplePath = Join-Path $TempDir "samples.csv"
    $Samples | Export-Csv -NoTypeInformation -Path $samplePath
    Write-Host "samples_csv:            $samplePath"

    if (-not [string]::IsNullOrWhiteSpace($ResultCsv)) {
        $resultPath = $ExecutionContext.SessionState.Path.GetUnresolvedProviderPathFromPSPath($ResultCsv)
        [pscustomobject]@{
            riders = $Riders
            orders = $Orders
            order_start_id = $StartID
            order_end_id = $LastID
            max_rider_orders = $MatcherMaxRiderOrders
            created_orders = $created
            terminal_orders = $terminal
            matched_orders = $matched
            missed_orders = $missed
            match_rate = [Math]::Round($matched / [Math]::Max($created, 1), 6)
            retry_rounds = $RetryRounds
            retry_events_sent = $retryEventsSent
            max_attempt = $maxAttempt
            scoped_outbox_pending = $pendingOutbox
            elapsed_seconds = [Math]::Round($elapsedSeconds, 3)
            throughput_orders_sec = [Math]::Round($throughput, 3)
            estimated_p50_ms = [Math]::Round((Get-CompletionPercentile $Samples $Orders 50), 0)
            estimated_p95_ms = [Math]::Round((Get-CompletionPercentile $Samples $Orders 95), 0)
            estimated_p99_ms = [Math]::Round((Get-CompletionPercentile $Samples $Orders 99), 0)
            logs_dir = $TempDir
            samples_csv = $samplePath
        } | Export-Csv -NoTypeInformation -Path $resultPath
        Write-Host "result_csv:             $resultPath"
    }

} finally {
    foreach ($process in $Processes) {
        if ($process -and -not $process.HasExited) {
            Stop-Process -Id $process.Id -Force -ErrorAction SilentlyContinue
        }
    }
    Pop-Location
}
