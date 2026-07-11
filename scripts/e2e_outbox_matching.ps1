param(
    [int]$Orders = 3,
    [int]$TimeoutSeconds = 60
)

$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$StartID = [DateTimeOffset]::UtcNow.ToUnixTimeSeconds()
$TempDir = Join-Path $env:TEMP "testp-outbox-e2e-$StartID"
$Processes = @()

New-Item -ItemType Directory -Path $TempDir -Force | Out-Null
Push-Location $RepoRoot
try {
    docker compose up -d postgres etcd kafka
    if ($LASTEXITCODE -ne 0) {
        throw "docker compose up failed"
    }
    & .\scripts\kafka_create_topics.ps1

    $Commands = @(
        @{ Name = "controller"; Path = ".\cmd\controller"; Args = @("-metrics-addr=") },
        @{ Name = "node"; Path = ".\cmd\node"; Args = @("-node-id", "1", "-metrics-addr=") },
        @{ Name = "matcher"; Path = ".\cmd\matcher-worker"; Args = @("-node-id", "1") },
        @{ Name = "publisher"; Path = ".\cmd\outbox-publisher"; Args = @("-worker-id", "e2e-publisher", "-poll-interval", "100ms") }
    )

    foreach ($Command in $Commands) {
        $Executable = Join-Path $TempDir "$($Command.Name).exe"
        go build -o $Executable $Command.Path
        $Processes += Start-Process `
            -FilePath $Executable `
            -ArgumentList $Command.Args `
            -PassThru `
            -WindowStyle Hidden `
            -RedirectStandardOutput (Join-Path $TempDir "$($Command.Name).out") `
            -RedirectStandardError (Join-Path $TempDir "$($Command.Name).err")
    }

    Start-Sleep -Seconds 5
    go run ./cmd/producer -orders $Orders -start-id $StartID -metrics-addr=

    $LastID = $StartID + $Orders - 1
    $Deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    do {
        Start-Sleep -Seconds 1
        $Terminal = docker exec testp-postgres psql -U testp -d testp -Atc `
            "SELECT count(*) FROM orders WHERE order_id BETWEEN $StartID AND $LastID AND status IN ('matched','missed');"
    } while ([int]$Terminal -lt $Orders -and (Get-Date) -lt $Deadline)

    docker exec testp-postgres psql -U testp -d testp -c `
        "SELECT order_id,status,rider_id FROM orders WHERE order_id BETWEEN $StartID AND $LastID ORDER BY order_id;"
    if ([int]$Terminal -lt $Orders) {
        throw "timed out waiting for orders; logs: $TempDir"
    }
} finally {
    foreach ($Process in $Processes) {
        if ($Process -and -not $Process.HasExited) {
            Stop-Process -Id $Process.Id -Force -ErrorAction SilentlyContinue
        }
    }
    Pop-Location
}
