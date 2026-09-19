# Offline results and live evidence deliberately have separate authorities.
[CmdletBinding()]
param(
    [switch]$Live,
    [string]$ConfigFile = '',
    [string]$BaselineFile = '',
    [string]$OutputDirectory = ''
)

$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $false
$root = Split-Path -Parent $PSScriptRoot
$config = Get-Content -LiteralPath (Join-Path $PSScriptRoot 'qualification-matrix.json') -Raw | ConvertFrom-Json
$legacy = Get-Content -LiteralPath (Join-Path $PSScriptRoot 'test-matrix.json') -Raw | ConvertFrom-Json

if (-not $OutputDirectory) {
    $OutputDirectory = Join-Path $root ('target/qualification/' + [guid]::NewGuid().ToString('N'))
}
$out = [IO.Path]::GetFullPath($OutputDirectory)
New-Item -ItemType Directory -Path $out -Force | Out-Null

$typePath = Join-Path $out 'types.json'
$recoveryPath = Join-Path $out 'recovery.json'
$sourcePath = Join-Path $out 'live-source.json'
$sinkPath = Join-Path $out 'live-sink.json'
$transactionRecoveryPath = Join-Path $out 'transaction-recovery.json'
$routePath = Join-Path $out 'route-smoke.json'
$summaryPath = Join-Path $out 'summary.json'
foreach ($path in @($typePath, $recoveryPath, $sourcePath, $sinkPath, $transactionRecoveryPath, $routePath, $summaryPath)) {
    if (Test-Path -LiteralPath $path) { Remove-Item -LiteralPath $path }
}

$results = [Collections.Generic.List[object]]::new()
$previous = @{}
$secrets = @()

function Set-RunEnvironment([string]$Name, [string]$Value) {
    if (-not $previous.ContainsKey($Name)) {
        $previous[$Name] = [Environment]::GetEnvironmentVariable($Name, 'Process')
    }
    [Environment]::SetEnvironmentVariable($Name, $Value, 'Process')
}

function Add-DatabaseEnvironment([object]$Suite) {
    $required = @($Suite.required_env | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
    $id = [string]$Suite.id
    if ($id -match '^mysql_(5_7|8_0|8_4)\.') {
        $port = switch ($Matches[1]) {
            '5_7' { 'CDC_MYSQL57_PORT' }
            '8_0' { 'CDC_MYSQL80_PORT' }
            '8_4' { 'CDC_MYSQL84_PORT' }
        }
        $required += @('CDC_MYSQL_HOST', 'CDC_MYSQL_WRITER_USER', $port)
        if ([string]$Suite.category -eq 'source') {
            $required += 'CDC_MYSQL_READER_USER'
        }
    } elseif ([string]$Suite.mode -eq 'Live') {
        $required += @('PG_CDC_HOST', 'PG_CDC_PORT', 'PG_CDC_ADMIN_USER', 'PG_CDC_READER_USER', 'PG_CDC_WRITER_USER')
    }
    return @($required | Select-Object -Unique)
}

function Run-Suite([object]$Suite, [string[]]$Required, [bool]$Execute) {
    $missing = @($Required | Where-Object {
        -not [string]::IsNullOrWhiteSpace($_) -and
        [string]::IsNullOrWhiteSpace([Environment]::GetEnvironmentVariable($_, 'Process'))
    })
    $status = 'REQUIRES_LIVE'
    $exitCode = $null
    $lines = [Collections.Generic.List[string]]::new()

    if (-not $Execute) {
        $missing = @('LIVE_MODE_NOT_REQUESTED')
    } elseif ($missing.Count -eq 0) {
        $saved = $ErrorActionPreference
        $ErrorActionPreference = 'Continue'
        try {
            & cargo @($Suite.args) 2>&1 | ForEach-Object {
                $line = $_.ToString()
                foreach ($secret in $secrets) {
                    $line = $line.Replace($secret, '[REDACTED]')
                }
                $lines.Add($line)
            }
            $exitCode = $LASTEXITCODE
        } finally {
            $ErrorActionPreference = $saved
        }
        $status = if ($exitCode -eq 0 -and ($lines -match 'test result: ok\. [1-9][0-9]* passed;')) {
            'PASS'
        } else {
            'FAIL'
        }
    }

    $logName = "$($Suite.id).log"
    $lines | Set-Content -LiteralPath (Join-Path $out $logName) -Encoding UTF8
    $record = [pscustomobject]@{
        id                          = [string]$Suite.id
        mode                        = [string]$Suite.mode
        category                    = [string]$Suite.category
        status                      = $status
        exit_code                   = $exitCode
        missing_environment         = $missing
        required_for_live_qualified = [bool]$Suite.required_for_live_qualified
        databases                   = @($Suite.databases)
        source_fixtures             = @($Suite.source_fixtures)
        log                         = $logName
    }
    $results.Add($record) | Out-Null
}

function Get-SuiteResult([string]$Id) {
    return $results | Where-Object { $_.id -eq $Id } | Select-Object -First 1
}

# Validate the semantic registry before running tests. This prevents a future
# configuration from silently turning a 4+4 qualification into 16 live links.
$roster = @($config.live_qualification.source_fixture_roster)
if ($roster.Count -ne 4 -or @($roster | Select-Object -Unique).Count -ne 4) {
    throw 'live_qualification.source_fixture_roster must contain four unique source fixtures.'
}
$sourceSpecs = @($config.live_qualification.sources)
$sinkSpecs = @($config.live_qualification.sinks)
if ($sourceSpecs.Count -ne 4 -or $sinkSpecs.Count -ne 4) {
    throw 'live_qualification must declare exactly four source and four sink components.'
}
if ((@($sourceSpecs | ForEach-Object { $_.database }) -join ',') -ne ($roster -join ',') -or
    (@($sinkSpecs | ForEach-Object { $_.database }) -join ',') -ne ($roster -join ',')) {
    throw 'live source and sink component rosters must match the four implemented databases.'
}

$suiteDefinitions = @($legacy.suites)
foreach ($sinkSpec in $sinkSpecs) {
    $sinkSuite = $suiteDefinitions | Where-Object { $_.id -eq $sinkSpec.suite } | Select-Object -First 1
    if ($null -eq $sinkSuite -or [string]$sinkSuite.category -ne 'sink' -or
        ((@($sinkSuite.source_fixtures) -join ',') -ne ($roster -join ','))) {
        throw "Sink suite $($sinkSpec.suite) must declare all four source fixtures."
    }
}

Push-Location $root
try {
    if ($Live -and $ConfigFile) {
        $allowed = @($legacy.suites.required_env | Select-Object -Unique) + @(
            'CDC_MYSQL_READER_USER', 'CDC_MYSQL_WRITER_USER', 'PG_CDC_ADMIN_USER',
            'PG_CDC_READER_USER', 'PG_CDC_WRITER_USER'
        )
        $seen = @{}
        foreach ($line in [IO.File]::ReadAllLines([IO.Path]::GetFullPath($ConfigFile))) {
            if ([string]::IsNullOrWhiteSpace($line) -or $line.TrimStart().StartsWith('#')) { continue }
        $parts = $line.Split([char[]]@('='), 2, [System.StringSplitOptions]::None)
            if ($parts.Count -ne 2 -or $parts[0].Trim() -notin $allowed -or [string]::IsNullOrEmpty($parts[1])) {
                throw 'Invalid live configuration entry (values withheld).'
            }
            $name = $parts[0].Trim()
            if ($seen.ContainsKey($name)) { throw "Duplicate configuration key: $name" }
            $seen[$name] = $true
            if (-not [Environment]::GetEnvironmentVariable($name, 'Process')) {
                Set-RunEnvironment $name $parts[1]
            }
        }
    }
    $secrets = @(Get-ChildItem Env: |
        Where-Object { $_.Name -match '^(CDC_MYSQL|PG_CDC)_.*PASSWORD$' -and $_.Value } |
        ForEach-Object { $_.Value })
    Set-RunEnvironment 'CDC_QUALIFICATION_REPORT' $typePath
    Set-RunEnvironment 'CDC_QUALIFICATION_RECOVERY_REPORT' $recoveryPath
    Set-RunEnvironment 'CDC_TEST_ARTIFACT_DIR' $out

    # The workspace run provides the complete offline matrix and regression
    # evidence. It never supplies live source/sink evidence.
    $offlineSuite = $suiteDefinitions | Where-Object { $_.id -eq 'offline-workspace' } | Select-Object -First 1
    if ($null -eq $offlineSuite) {
        $offlineSuite = [pscustomobject]@{
            id = 'offline-workspace'; mode = 'Offline'; category = 'offline'; args = @('test', '--workspace', '--locked')
            required_env = @(); required_for_live_qualified = $false; databases = @(); source_fixtures = @()
        }
    }
    Run-Suite $offlineSuite @() $true

    foreach ($suite in $suiteDefinitions | Where-Object { $_.mode -eq 'Live' -or $_.mode -eq 'Recovery' }) {
        $execute = ([string]$suite.mode -eq 'Recovery') -or $Live
        Run-Suite $suite (Add-DatabaseEnvironment $suite) $execute
    }

    $offlineResult = Get-SuiteResult 'offline-workspace'
    $offlinePassed = $null -ne $offlineResult -and $offlineResult.status -eq 'PASS'
    $types = if ($offlinePassed -and (Test-Path -LiteralPath $typePath)) {
        @((Get-Content -LiteralPath $typePath -Raw | ConvertFrom-Json).directions)
    } else { @() }
    $recoveryEvidence = if ($offlinePassed -and (Test-Path -LiteralPath $recoveryPath)) {
        @(Get-Content -LiteralPath $recoveryPath -Raw | ConvertFrom-Json)
    } else { @() }

    $missing = [Collections.Generic.List[string]]::new()
    $directions = @(foreach ($source in $config.databases) {
        foreach ($sink in $config.databases) {
            $key = "$source->$sink"
            $type = @($types | Where-Object { $_.source -eq $source -and $_.sink -eq $sink })
            $recover = @($recoveryEvidence | Where-Object { $_.source -eq $source -and $_.sink -eq $sink })
            $unsupported = $source -in $config.unsupported -or $sink -in $config.unsupported
            if ($type.Count -ne 1 -or $recover.Count -ne 1) {
                $missing.Add($key)
                $offlineStatus = 'MISSING_TEST'
            } elseif ($unsupported -and $type[0].offline -eq 'UNSUPPORTED' -and $recover[0].offline -eq 'UNSUPPORTED') {
                $offlineStatus = 'UNSUPPORTED'
            } elseif (-not $unsupported -and $type[0].offline -eq 'PASS' -and $recover[0].offline -eq 'PASS') {
                $offlineStatus = 'PASS'
            } else {
                $missing.Add($key)
                $offlineStatus = 'MISSING_TEST'
            }

            $sourceSpec = $sourceSpecs | Where-Object { $_.database -eq $source } | Select-Object -First 1
            $sinkSpec = $sinkSpecs | Where-Object { $_.database -eq $sink } | Select-Object -First 1
            $sourceEvidence = if ($null -ne $sourceSpec) { Get-SuiteResult $sourceSpec.suite } else { $null }
            $sinkEvidence = if ($null -ne $sinkSpec) { Get-SuiteResult $sinkSpec.suite } else { $null }
            if ($unsupported) {
                $liveStatus = 'UNSUPPORTED'
                $liveReason = 'connector.not_implemented'
            } elseif ($null -eq $sourceEvidence -or $null -eq $sinkEvidence) {
                $liveStatus = 'REQUIRES_LIVE'
                $liveReason = 'source_or_sink_component_not_registered'
            } elseif ($sourceEvidence.status -eq 'FAIL' -or $sinkEvidence.status -eq 'FAIL') {
                $liveStatus = 'FAIL'
                $liveReason = 'source_or_sink_component_failed'
            } elseif ($sourceEvidence.status -eq 'PASS' -and $sinkEvidence.status -eq 'PASS') {
                $liveStatus = 'PASS'
                $liveReason = 'source_and_sink_component_evidence'
            } else {
                $liveStatus = 'REQUIRES_LIVE'
                $liveReason = 'source_and_sink_component_evidence_incomplete'
            }
            $counts = [ordered]@{}
            $typeCases = if ($type.Count -eq 1) { @($type[0].cases) } else { @() }
            foreach ($level in @('EXACT', 'RANGE_CHECKED', 'EXPLICIT_CONVERSION', 'UNSUPPORTED/BLOCKED')) {
                $counts[$level] = @($typeCases | Where-Object qualification -eq $level).Count
            }
            [pscustomobject]@{
                source = $source
                sink = $sink
                offline = $offlineStatus
                live = $liveStatus
                live_reason = $liveReason
                live_evidence = if ($unsupported) { @() } else {
                    @([ordered]@{ source_suite = $sourceSpec.suite; sink_suite = $sinkSpec.suite })
                }
                qualification_counts = $counts
                cases = $typeCases
                recovery = @($recover)
            }
        }
    })

    $sourceQualification = @($sourceSpecs | ForEach-Object {
        $result = Get-SuiteResult $_.suite
        [ordered]@{
            database = $_.database
            suite = $_.suite
            status = if ($null -ne $result) { $result.status } else { 'REQUIRES_LIVE' }
            log = if ($null -ne $result) { $result.log } else { $null }
            evidence_scope = 'live_source_adapter'
        }
    })
    $sinkQualification = @($sinkSpecs | ForEach-Object {
        $spec = $_
        $result = Get-SuiteResult $spec.suite
        $suite = $suiteDefinitions | Where-Object { $_.id -eq $spec.suite } | Select-Object -First 1
        [ordered]@{
            database = $spec.database
            suite = $spec.suite
            status = if ($null -ne $result) { $result.status } else { 'REQUIRES_LIVE' }
            source_fixtures = @($suite.source_fixtures)
            log = if ($null -ne $result) { $result.log } else { $null }
            evidence_scope = 'live_sink_adapter'
        }
    })
    $transactionRecovery = @($config.live_qualification.transaction_recovery | ForEach-Object {
        $result = Get-SuiteResult $_
        [ordered]@{
            suite = $_
            status = if ($null -ne $result) { $result.status } else { 'REQUIRES_LIVE' }
            log = if ($null -ne $result) { $result.log } else { $null }
            evidence_scope = 'common_transaction_recovery'
        }
    })
    $routeSmoke = @($config.live_qualification.route_smoke | ForEach-Object {
        $result = Get-SuiteResult $_
        [ordered]@{
            suite = $_
            status = if ($null -ne $result) { $result.status } else { 'REQUIRES_LIVE' }
            log = if ($null -ne $result) { $result.log } else { $null }
            evidence_scope = 'representative_end_to_end_route'
        }
    })

    $sourceQualified = $sourceQualification.Count -eq 4 -and @($sourceQualification | Where-Object status -ne 'PASS').Count -eq 0
    $sinkQualified = $sinkQualification.Count -eq 4 -and @($sinkQualification | Where-Object status -ne 'PASS').Count -eq 0
    $transactionRecoveryQualified = $transactionRecovery.Count -gt 0 -and @($transactionRecovery | Where-Object status -ne 'PASS').Count -eq 0
    $routeSmokeQualified = $routeSmoke.Count -gt 0 -and @($routeSmoke | Where-Object status -ne 'PASS').Count -eq 0
    $liveQualified = $sourceQualified -and $sinkQualified -and $transactionRecoveryQualified

    ([ordered]@{
        schema = 'cdc.qualification-live-source.v1'
        qualified = $sourceQualified
        components = $sourceQualification
    } | ConvertTo-Json -Depth 20) | Set-Content -LiteralPath $sourcePath -Encoding UTF8
    ([ordered]@{
        schema = 'cdc.qualification-live-sink.v1'
        qualified = $sinkQualified
        source_fixture_roster = $roster
        components = $sinkQualification
    } | ConvertTo-Json -Depth 20) | Set-Content -LiteralPath $sinkPath -Encoding UTF8
    ([ordered]@{
        schema = 'cdc.qualification-transaction-recovery.v1'
        qualified = $transactionRecoveryQualified
        components = $transactionRecovery
    } | ConvertTo-Json -Depth 20) | Set-Content -LiteralPath $transactionRecoveryPath -Encoding UTF8
    ([ordered]@{
        schema = 'cdc.qualification-route-smoke.v1'
        qualified = $routeSmokeQualified
        components = $routeSmoke
    } | ConvertTo-Json -Depth 20) | Set-Content -LiteralPath $routePath -Encoding UTF8

    $added = @()
    $expectedAdditional = 0
    $baselineFailed = $false
    if ($BaselineFile) {
        $baseline = Get-Content -LiteralPath $BaselineFile -Raw | ConvertFrom-Json
        $oldIds = @($baseline.databases)
        if (-not $oldIds.Count -or @($oldIds | Select-Object -Unique).Count -ne $oldIds.Count) {
            throw 'Baseline must declare unique database identities.'
        }
        foreach ($oldSource in $oldIds) {
            foreach ($oldSink in $oldIds) {
                if ($oldSource -notin $config.databases -or $oldSink -notin $config.databases) {
                    $missing.Add("$oldSource->$oldSink")
                }
            }
        }
        $newIds = @($config.databases | Where-Object { $_ -notin $oldIds })
        $expectedAdditional = 2 * $oldIds.Count * $newIds.Count + $newIds.Count * $newIds.Count
        $added = @($directions | Where-Object { $_.source -in $newIds -or $_.sink -in $newIds } | ForEach-Object {
            "$($_.source)->$($_.sink)"
        })
        if ($added.Count -ne $expectedAdditional) { $baselineFailed = $true }
    }

    $offlineSuccess = $offlinePassed -and $missing.Count -eq 0
    $suiteFailures = @($results | Where-Object status -eq 'FAIL')
    $failed = $baselineFailed -or $missing.Count -gt 0 -or $suiteFailures.Count -gt 0
    if ($Live -and (-not $liveQualified -or -not $routeSmokeQualified)) { $failed = $true }
    $success = -not $failed
    $report = [ordered]@{
        schema = 'cdc.qualification.v2'
        matrix_semantics = [ordered]@{
            offline_direction_matrix = 'six_by_six_planning_and_type_qualification'
            live_qualification = 'four_source_adapters_plus_four_sink_adapters_plus_common_transaction_recovery'
            route_smoke = 'representative_end_to_end_runtime_routes'
            live_database_to_database_links = $false
        }
        databases = @($config.databases)
        implemented = @($config.implemented)
        unsupported = @($config.unsupported)
        success = $success
        offline_success = $offlineSuccess
        live_qualified = $liveQualified
        source_qualified = $sourceQualified
        sink_qualified = $sinkQualified
        transaction_recovery_qualified = $transactionRecoveryQualified
        route_smoke_qualified = $routeSmokeQualified
        live_evidence_files = [ordered]@{
            source = 'live-source.json'
            sink = 'live-sink.json'
            transaction_recovery = 'transaction-recovery.json'
            route_smoke = 'route-smoke.json'
        }
        source_qualification = $sourceQualification
        sink_qualification = $sinkQualification
        transaction_recovery = $transactionRecovery
        route_smoke = $routeSmoke
        directions = $directions
        missing_directions = @($missing.ToArray())
        added_directions = $added
        expected_additional_directions = $expectedAdditional
        suites = @($results.ToArray())
    }
    $report | ConvertTo-Json -Depth 40 | Set-Content -LiteralPath $summaryPath -Encoding UTF8

    Write-Output 'Source -> Sink | Offline | Live | EXACT/RANGE_CHECKED/EXPLICIT_CONVERSION/UNSUPPORTED_BLOCKED'
    foreach ($row in $directions) {
        Write-Output ("{0} -> {1} | {2} | {3} | {4}/{5}/{6}/{7}" -f `
            $row.source, $row.sink, $row.offline, $row.live,
            $row.qualification_counts.EXACT,
            $row.qualification_counts.RANGE_CHECKED,
            $row.qualification_counts.EXPLICIT_CONVERSION,
            $row.qualification_counts.'UNSUPPORTED/BLOCKED')
    }
    Write-Output "Live source qualified: $sourceQualified"
    Write-Output "Live sink qualified: $sinkQualified"
    Write-Output "Transaction recovery qualified: $transactionRecoveryQualified"
    Write-Output "Route smoke qualified: $routeSmokeQualified"
    Write-Output "Live qualified: $liveQualified"
    Write-Output "Missing directions: $($missing.Count)"
    Write-Output "Report: $summaryPath"
    if ($failed) { exit 1 }
} finally {
    Pop-Location
    foreach ($name in $previous.Keys) {
        [Environment]::SetEnvironmentVariable($name, $previous[$name], 'Process')
    }
}
