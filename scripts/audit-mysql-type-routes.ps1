[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string[]]$EvidenceDirectory,
    [string[]]$PassedSuiteId = @(),
    [string[]]$OfflineDirection = @(),
    [string]$MatrixFile = (Join-Path $PSScriptRoot 'test-matrix.json'),
    [string]$QualificationMatrixFile = (Join-Path $PSScriptRoot 'qualification-matrix.json'),
    [string]$InventoryFile = (Join-Path $PSScriptRoot 'type-inventory.json'),
    [string]$OutputFile = '',
    [switch]$RequireComplete
)

$ErrorActionPreference = 'Stop'

function Add-TypeToSetMap([System.Collections.IDictionary]$Map, [string]$Key, [string]$TypeId) {
    if (-not $Map.Contains($Key)) {
        $Map[$Key] = [System.Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
    }
    $null = $Map[$Key].Add($TypeId)
}

function Has-TypeInSetMap([System.Collections.IDictionary]$Map, [string]$Key, [string]$TypeId) {
    return $Map.Contains($Key) -and $Map[$Key].Contains($TypeId)
}

function Get-ParentSummaryPath([string]$Path) {
    $item = Get-Item -LiteralPath $Path
    $directory = if ($item.PSIsContainer) { $item.FullName } else { Split-Path -Parent $item.FullName }
    while ($directory) {
        $summary = Join-Path $directory 'summary.json'
        if (Test-Path -LiteralPath $summary -PathType Leaf) { return $summary }
        $parent = Split-Path -Parent $directory
        if (-not $parent -or $parent -eq $directory) { break }
        $directory = $parent
    }
    return $null
}

$matrix = Get-Content -LiteralPath $MatrixFile -Raw | ConvertFrom-Json
$qualificationMatrix = Get-Content -LiteralPath $QualificationMatrixFile -Raw | ConvertFrom-Json
$inventory = Get-Content -LiteralPath $InventoryFile -Raw | ConvertFrom-Json
$sameVersionPolicy = $qualificationMatrix.same_version_mysql_route_policy
if ($sameVersionPolicy.verification_mode -ne 'COMPONENTS_COMPOSED_MYSQL_SELF_ROUTE') {
    throw 'The qualification matrix must declare the explicit same-version MySQL component-composition policy.'
}
$connectors = @($inventory.connectors | ForEach-Object { [string]$_.id })
$mysqlSources = @($connectors | Where-Object { $_ -match '^mysql_(5_7|8_0|8_4)$' })
if ($mysqlSources.Count -ne 3) {
    throw 'The connector inventory must contain MySQL 5.7, 8.0, and 8.4 exactly once.'
}

$passedSuites = [System.Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
$offlineDirections = [System.Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
foreach ($suiteId in $PassedSuiteId) { $null = $passedSuites.Add([string]$suiteId) }
foreach ($direction in $OfflineDirection) {
    $parts = $direction.Split('>')
    if ($parts.Count -ne 2 -or $parts[0] -notin $connectors -or $parts[1] -notin $connectors) {
        throw "Offline direction is not a registered source>sink route: $direction"
    }
    $null = $offlineDirections.Add("$($parts[0])>$($parts[1])")
}
$resolvedDirectories = [System.Collections.Generic.List[string]]::new()
foreach ($path in $EvidenceDirectory) {
    $resolved = [IO.Path]::GetFullPath($path)
    if (-not (Test-Path -LiteralPath $resolved -PathType Container)) {
        throw "Evidence directory does not exist: $path"
    }
    $resolvedDirectories.Add($resolved)
    $summaryPath = Get-ParentSummaryPath $resolved
    if ($summaryPath) {
        $summary = Get-Content -LiteralPath $summaryPath -Raw | ConvertFrom-Json
        foreach ($direction in @($summary.directions)) {
            if ($direction.offline -eq 'PASS') {
                $null = $offlineDirections.Add("$($direction.source)>$($direction.sink)")
            }
        }
        foreach ($result in @($summary.results) + @($summary.suites)) {
            $id = if ($result.id) { [string]$result.id } elseif ($result.suite) { [string]$result.suite } else { '' }
            if ($id -and $result.status -eq 'PASS') { $null = $passedSuites.Add($id) }
        }
    }
}

$artifactFiles = [System.Collections.Generic.SortedSet[string]]::new([StringComparer]::OrdinalIgnoreCase)
foreach ($directory in $resolvedDirectories) {
    foreach ($file in Get-ChildItem -LiteralPath $directory -File -Filter '*.json' -Recurse) {
        $null = $artifactFiles.Add($file.FullName)
    }
}

$sourceTypeSets = @{}
$axisSets = @{}
foreach ($file in $artifactFiles) {
    try {
        $artifact = Get-Content -LiteralPath $file -Raw | ConvertFrom-Json
    } catch {
        continue
    }
    if ($artifact.schema -ne 'cdc.type_qualification_evidence_report.v1') { continue }

    foreach ($record in @($artifact.evidence)) {
        $source = [string]$record.source_connector_id
        $sink = [string]$record.sink_connector_id
        $axis = [string]$record.axis
        $typeId = [string]$record.type_id
        if (-not $typeId -or $record.status -ne 'PASS') { continue }
        $suiteId = [string]$artifact.suite_id
        if ($suiteId -eq "$source.all_types_capture" -and
            $source -match '^mysql_(5_7|8_0|8_4)$' -and $axis -eq 'source.live') {
            Add-TypeToSetMap $sourceTypeSets $source $typeId
        }
        $key = "$suiteId|$source|$sink|$axis"
        Add-TypeToSetMap $axisSets $key $typeId
    }
}

$configuredSuites = @($matrix.suites | ForEach-Object { [string]$_.id })
$routes = [System.Collections.Generic.List[object]]::new()
foreach ($source in $mysqlSources) {
    $sourceTypeIds = if ($sourceTypeSets.ContainsKey($source)) { $sourceTypeSets[$source] } else { [System.Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal) }
    foreach ($sink in $connectors) {
        $suiteId = if ($source -eq $sink) {
            "${source}.web_same_version_all_types"
        } else {
            "${source}.web_all_native_types_to_${sink}"
        }
        $suiteRegistered = $configuredSuites -contains $suiteId
        $suitePassed = $passedSuites.Contains($suiteId)
        $webSuite = "web_ui.${source}_all_native_types_to_${sink}_carrier_live"
        $sinkSuffix = if ($sink -match '^mysql_(5_7|8_0|8_4)$') { $Matches[1] } else { $sink }
        $sinkSuite = "${source}.all_types_to_${sinkSuffix}"
        $webTypes = [System.Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
        $sinkTypes = [System.Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
        $representationTypes = [System.Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
        foreach ($typeId in $sourceTypeIds) {
            if (Has-TypeInSetMap $axisSets "$webSuite|$source|$sink|web.plan" $typeId) {
                $null = $webTypes.Add($typeId)
            }
            foreach ($axis in @('sink.offline', 'sink.live')) {
                if (Has-TypeInSetMap $axisSets "$sinkSuite|$source|$sink|$axis" $typeId) {
                    $null = $sinkTypes.Add($typeId)
                }
            }
            foreach ($axis in @('sink.representation_carrier', 'sink.representation_preserved')) {
                if (Has-TypeInSetMap $axisSets "$sinkSuite|$source|$sink|$axis" $typeId) {
                    $null = $representationTypes.Add($typeId)
                }
            }
        }
        $missingSource = @($sourceTypeIds | Where-Object {
            -not (Has-TypeInSetMap $axisSets "$source.all_types_capture|$source||source.protocol_capture" $_) -or
            -not (Has-TypeInSetMap $axisSets "$source.all_types_capture|$source||source.change_event" $_) -or
            -not (Has-TypeInSetMap $axisSets "$source.all_types_capture|$source||source.semantic_codec" $_) -or
            -not (Has-TypeInSetMap $axisSets "$source.all_types_capture|$source||source.live" $_)
        })
        $missingSink = @($sourceTypeIds | Where-Object {
            -not (Has-TypeInSetMap $axisSets "$sinkSuite|$source|$sink|sink.offline" $_) -or
            -not (Has-TypeInSetMap $axisSets "$sinkSuite|$source|$sink|sink.live" $_)
        })
        $missingWeb = @($sourceTypeIds | Where-Object { -not $webTypes.Contains($_) })
        $liveWebE2eQualified = $suiteRegistered -and $suitePassed -and $sourceTypeIds.Count -gt 0 -and
            $missingSource.Count -eq 0 -and $missingSink.Count -eq 0 -and $missingWeb.Count -eq 0
        $offlineDirectionPassed = $offlineDirections.Contains("$source>$sink")
        $componentCompositionQualified = $source -eq $sink -and
            $sameVersionPolicy.verification_mode -eq 'COMPONENTS_COMPOSED_MYSQL_SELF_ROUTE' -and
            $suiteRegistered -and $sourceTypeIds.Count -gt 0 -and $missingSource.Count -eq 0 -and
            $missingSink.Count -eq 0 -and $offlineDirectionPassed
        $isQualified = $liveWebE2eQualified -or $componentCompositionQualified
        $verificationMode = if ($liveWebE2eQualified) {
            'LIVE_WEB_E2E'
        } elseif ($componentCompositionQualified) {
            'COMPONENTS_COMPOSED_MYSQL_SELF_ROUTE'
        } else {
            'REQUIRES_EVIDENCE'
        }
        $routes.Add([ordered]@{
            source = $source
            sink = $sink
            suite = $suiteId
            source_suite = "$source.all_types_capture"
            verification_mode = $verificationMode
            suite_registered = $suiteRegistered
            suite_passed = $suitePassed
            live_web_e2e_qualified = $liveWebE2eQualified
            offline_direction_passed = $offlineDirectionPassed
            component_composition_qualified = $componentCompositionQualified
            source_type_count = $sourceTypeIds.Count
            sink_type_count = $sinkTypes.Count
            web_plan_type_count = $webTypes.Count
            representation_type_count = $representationTypes.Count
            qualified = $isQualified
            missing_source_receipts = $missingSource
            missing_sink_receipts = $missingSink
            missing_web_plans = $missingWeb
        })
    }
}

$unregisteredRouteSuites = @($mysqlSources | ForEach-Object {
    $source = $_
    foreach ($sink in $connectors) {
        $expected = if ($source -eq $sink) { "${source}.web_same_version_all_types" } else { "${source}.web_all_native_types_to_${sink}" }
        if ($expected -notin $configuredSuites) { $expected }
    }
})
$qualified = $routes.Count -eq 18 -and @($routes | Where-Object { -not $_.qualified }).Count -eq 0 -and
    $unregisteredRouteSuites.Count -eq 0
$liveWebE2eRouteCount = @($routes | Where-Object { $_.live_web_e2e_qualified }).Count
$componentComposedRouteCount = @($routes | Where-Object { $_.component_composition_qualified }).Count
$missingLiveWebE2eRouteCount = $routes.Count - $liveWebE2eRouteCount
$report = [ordered]@{
    schema = 'cdc.mysql_native_type_route_coverage.v1'
    qualified = $qualified
    live_web_e2e_route_count = $liveWebE2eRouteCount
    component_composed_route_count = $componentComposedRouteCount
    missing_live_web_e2e_route_count = $missingLiveWebE2eRouteCount
    qualification_policy = 'Every route must have per-type Source, ChangeEvent, and Sink receipts plus an offline direction result. Actual Web E2E is reported separately; only identical-version MySQL routes may use the declared component-composition mode.'
    expected_route_count = 18
    route_count = $routes.Count
    sources = @($mysqlSources)
    sinks = @($connectors)
    unregistered_route_suites = $unregisteredRouteSuites
    routes = @($routes.ToArray())
}

$json = ConvertTo-Json -InputObject $report -Depth 40
if ($OutputFile) {
    $outputPath = [IO.Path]::GetFullPath($OutputFile)
    $parent = Split-Path -Parent $outputPath
    if ($parent) { New-Item -ItemType Directory -Path $parent -Force | Out-Null }
    [IO.File]::WriteAllText($outputPath, $json, [Text.UTF8Encoding]::new($false))
}
$json
if ($RequireComplete -and -not $qualified) { exit 1 }
