param(
    [string]$OutputCsv = ".\benchmark-results\capacity-scenarios.csv",
    [int]$TimeoutSeconds = 43200,
    [int]$ProducerBatchSize = 1000,
    [int]$PublisherBatchSize = 1000,
    [int]$PublisherCount = 2,
    [int]$MatcherMaxRiderOrders = 3
)

$ErrorActionPreference = "Stop"
$ScriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Split-Path -Parent $ScriptRoot
$OutputPath = $ExecutionContext.SessionState.Path.GetUnresolvedProviderPathFromPSPath($OutputCsv)
$OutputDirectory = Split-Path -Parent $OutputPath
$BaseOrderID = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds() * 1000000

$Scenarios = @(
    [pscustomobject]@{ Riders = 100; Orders = 10000 },
    [pscustomobject]@{ Riders = 1000; Orders = 100000 },
    [pscustomobject]@{ Riders = 10000; Orders = 1000000 }
)

New-Item -ItemType Directory -Path $OutputDirectory -Force | Out-Null
$Results = New-Object System.Collections.Generic.List[object]
$NextOrderID = $BaseOrderID

Push-Location $RepoRoot
try {
    foreach ($Scenario in $Scenarios) {
        $scenarioCsv = Join-Path $OutputDirectory "capacity-$($Scenario.Riders)-riders-$($Scenario.Orders)-orders.csv"
        Write-Host ""
        Write-Host "Starting scenario: riders=$($Scenario.Riders), orders=$($Scenario.Orders)"

        & "$ScriptRoot\bench_outbox_matching.ps1" `
            -Riders $Scenario.Riders `
            -Orders $Scenario.Orders `
            -OrderStartID $NextOrderID `
            -MatcherMaxRiderOrders $MatcherMaxRiderOrders `
            -TimeoutSeconds $TimeoutSeconds `
            -ProducerBatchSize $ProducerBatchSize `
            -PublisherBatchSize $PublisherBatchSize `
            -PublisherCount $PublisherCount `
            -ResultCsv $scenarioCsv

        if ($LASTEXITCODE -ne 0) {
            throw "scenario failed: riders=$($Scenario.Riders), orders=$($Scenario.Orders)"
        }

        $Results.Add((Import-Csv $scenarioCsv)) | Out-Null
        $Results | Export-Csv -NoTypeInformation -Path $OutputPath
        $NextOrderID += [int64]$Scenario.Orders + 1000000
    }
} finally {
    Pop-Location
}

Write-Host ""
Write-Host "All scenarios completed."
Write-Host "result_csv: $OutputPath"
