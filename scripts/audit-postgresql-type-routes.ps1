[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string[]]$EvidenceDirectory,
    [string[]]$PassedSuiteId = @(),
    [string]$MatrixFile = (Join-Path $PSScriptRoot 'test-matrix.json'),
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
$inventory = Get-Content -LiteralPath $InventoryFile -Raw | ConvertFrom-Json
$connectors = @($inventory.connectors | ForEach-Object { [string]$_.id })
$postgresSources = @($connectors | Where-Object { $_ -match '^postgresql_(15|16|17)$' })
$requiredCatalogClasses = @($inventory.dynamic_type_classes |
    ForEach-Object { [string]$_.id } | Where-Object { $_ -like 'postgresql.*' } | Select-Object -Unique)
if ($postgresSources.Count -ne 3) {
    throw 'The connector inventory must contain PostgreSQL 15, 16, and 17 exactly once.'
}

$passedSuites = [System.Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
foreach ($suiteId in $PassedSuiteId) { $null = $passedSuites.Add([string]$suiteId) }
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

$latestRosterDigests = @{}
foreach ($filePath in $artifactFiles) {
    if ([IO.Path]::GetFileName($filePath) -notlike '*-catalog-roster-*.json') { continue }
    try {
        $artifact = Get-Content -LiteralPath $filePath -Raw | ConvertFrom-Json
    } catch {
        continue
    }
    if ($artifact.schema -ne 'cdc.type_qualification_evidence_report.v1' -or
        $artifact.suite_id -notmatch '^web_ui\.(postgresql_(15|16|17))_all_builtin_types_to_(mysql_(5_7|8_0|8_4)|postgresql_(15|16|17))_catalog_type_roster$' -or
        -not $artifact.catalog_type_roster) { continue }
    $source = [string]$Matches[1]
    $sink = [string]$Matches[3]
    $routeKey = "$source|$sink"
    $timestamp = if ([string]$artifact.run_id -match '(\d+)$') { [long]$Matches[1] } else { 0L }
    foreach ($definition in $artifact.catalog_type_roster) {
        $nominalKey = "$routeKey|$($definition.schema).$($definition.name)"
        $prior = $latestRosterDigests[$nominalKey]
        if ($null -eq $prior -or $timestamp -ge [long]$prior.timestamp) {
            $latestRosterDigests[$nominalKey] = @{
                timestamp = $timestamp
                digest = [string]$definition.definition_digest
            }
        }
    }
}

$rosters = @{}
$catalogMappingSets = @{}
$axisSets = @{}
$rosterConflicts = [System.Collections.Generic.List[object]]::new()
foreach ($file in $artifactFiles) {
    try {
        $artifact = Get-Content -LiteralPath $file -Raw | ConvertFrom-Json
    } catch {
        continue
    }
    if ($artifact.schema -ne 'cdc.type_qualification_evidence_report.v1') { continue }

    if ($artifact.suite_id -match '^web_ui\.(postgresql_(15|16|17))_all_builtin_types_to_(mysql_(5_7|8_0|8_4)|postgresql_(15|16|17))_catalog_type_roster$' -and
        $artifact.catalog_type_roster) {
        $source = [string]$Matches[1]
        $sink = [string]$Matches[3]
        $routeKey = "$source|$sink"
        if (-not $rosters.ContainsKey($routeKey)) { $rosters[$routeKey] = @{} }
        foreach ($definition in $artifact.catalog_type_roster) {
            $typeId = [string]$definition.type_id
            if (-not $typeId.StartsWith('dynamic:postgresql.instance.', [StringComparison]::Ordinal)) {
                $rosterConflicts.Add([pscustomobject]@{ source=$source; sink=$sink; type_id=$typeId; error='catalog roster contains a non-catalog identity' })
                continue
            }
            $expectedTypeId = "dynamic:postgresql.instance.$($definition.schema).$($definition.name):$($definition.definition_digest)"
            if ($typeId -cne $expectedTypeId) {
                $rosterConflicts.Add([pscustomobject]@{ source=$source; sink=$sink; type_id=$typeId; error='catalog type identity does not match its schema, name, and definition digest' })
                continue
            }
            $nominalKey = "$routeKey|$($definition.schema).$($definition.name)"
            if ($latestRosterDigests.ContainsKey($nominalKey) -and
                [string]$definition.definition_digest -cne [string]$latestRosterDigests[$nominalKey].digest) {
                continue
            }
            if ($rosters[$routeKey].ContainsKey($typeId)) {
                $prior = $rosters[$routeKey][$typeId]
                if ($prior.definition_digest -ne $definition.definition_digest -or
                    $prior.mapping_id -ne $definition.mapping_id -or
                    $prior.logical_type_digest -ne $definition.logical_type_digest -or
                    $prior.representation_mode -ne $definition.representation_mode) {
                    $rosterConflicts.Add([pscustomobject]@{ source=$source; sink=$sink; type_id=$typeId; error='catalog mapping identity changed within the route evidence' })
                }
            } else {
                $rosters[$routeKey][$typeId] = $definition
            }
        }
    }

    foreach ($record in @($artifact.evidence)) {
        $source = [string]$record.source_connector_id
        $sink = [string]$record.sink_connector_id
        $axis = [string]$record.axis
        $typeId = [string]$record.type_id
        if (-not $typeId -or $record.status -ne 'PASS') { continue }
        $suiteId = [string]$artifact.suite_id
        if ($axis -eq 'source.catalog_type_mapping' -and
            $suiteId -match '^web_ui\.(postgresql_(15|16|17))_all_builtin_types_to_(mysql_(5_7|8_0|8_4)|postgresql_(15|16|17))_catalog_type_roster$') {
            Add-TypeToSetMap $catalogMappingSets "$($Matches[1])|$($Matches[3])" $typeId
        }
        $key = "$suiteId|$source|$sink|$axis"
        Add-TypeToSetMap $axisSets $key $typeId
    }
}

$configuredSuites = @($matrix.suites | ForEach-Object { [string]$_.id })
$routes = [System.Collections.Generic.List[object]]::new()
foreach ($source in $postgresSources) {
    foreach ($sink in $connectors) {
        $suiteId = if ($source -eq $sink) {
            "${source}.web_all_builtin_types_self_sink"
        } else {
            "${source}.web_all_builtin_types_to_${sink}"
        }
        $suiteRegistered = $configuredSuites -contains $suiteId
        $suitePassed = $passedSuites.Contains($suiteId)
        $sinkEvidencePrefix = "web_ui.${source}_dynamic_types_to_${sink}_"
        # The route roster includes exact catalog definitions discovered by
        # both the complete built-in fixture and the dedicated dynamic-type
        # fixture. Their Web plans are emitted under different suite IDs, so
        # accept either exact per-type receipt instead of silently dropping
        # the dynamic suite's plan evidence.
        $webEvidencePrefixes = @(
            "web_ui.${source}_all_builtin_types_to_${sink}_",
            "web_ui.${source}_dynamic_types_to_${sink}_"
        )
        $sourceSuite = "web_ui.${source}_dynamic_types_to_${sink}_live"
        $nullOnlySourceSuite = "web_ui.${source}_all_builtin_types_to_${sink}_null_only_live"
        $routeKey = "$source|$sink"
        $roster = if ($rosters.ContainsKey($routeKey)) { $rosters[$routeKey] } else { @{} }
        $catalogClassCounts = @{}
        $unknownCatalogClasses = [System.Collections.Generic.List[string]]::new()
        foreach ($definition in $roster.Values) {
            $classId = [string]$definition.catalog_class_id
            if ($classId -notin $requiredCatalogClasses) {
                $unknownCatalogClasses.Add([string]$definition.type_id)
                continue
            }
            if (-not $catalogClassCounts.ContainsKey($classId)) { $catalogClassCounts[$classId] = 0 }
            $catalogClassCounts[$classId]++
        }
        $missingCatalogClasses = @($requiredCatalogClasses | Where-Object {
            -not $catalogClassCounts.ContainsKey($_) -or $catalogClassCounts[$_] -eq 0
        })
        $missingSource = [System.Collections.Generic.List[object]]::new()
        $missingSink = [System.Collections.Generic.List[object]]::new()
        $missingWeb = [System.Collections.Generic.List[object]]::new()
        $missingMapping = [System.Collections.Generic.List[string]]::new()
        $nullOnlyTypes = [System.Collections.Generic.List[string]]::new()
        $representationTypes = [System.Collections.Generic.List[string]]::new()

        $sourceRouteAxes = @{}
        foreach ($axis in @(
            'source.protocol_capture', 'source.change_event', 'source.live',
            'source.semantic_codec', 'source.source_representation_capture',
            'source.protocol_framing', 'source.null_only'
        )) {
            $sourceRouteAxes[$axis] = [System.Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
            foreach ($sourceEvidenceSuite in @($sourceSuite, $nullOnlySourceSuite)) {
                $key = "$sourceEvidenceSuite|$source||$axis"
                if ($axisSets.ContainsKey($key)) {
                    $null = $sourceRouteAxes[$axis].UnionWith($axisSets[$key])
                }
            }
        }

        $routeAxes = @{}
        foreach ($axis in @('sink.offline', 'sink.live', 'sink.representation_carrier', 'sink.representation_preserved', 'web.plan')) {
            $routeAxes[$axis] = [System.Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
            $suitePrefixes = if ($axis -eq 'web.plan') { $webEvidencePrefixes } else { @($sinkEvidencePrefix) }
            foreach ($key in @($axisSets.Keys | Where-Object {
                $candidate = $_
                $prefixMatches = @($suitePrefixes | Where-Object {
                    $candidate.StartsWith($_, [StringComparison]::Ordinal)
                }).Count -gt 0
                $prefixMatches -and $candidate.EndsWith("|$source|$sink|$axis", [StringComparison]::Ordinal)
            })) {
                $null = $routeAxes[$axis].UnionWith($axisSets[$key])
            }
        }

        foreach ($typeId in @($roster.Keys | Sort-Object)) {
            $definition = $roster[$typeId]
            $knownNullOnlyType = $typeId -match '^dynamic:postgresql\.instance\.pg_catalog\.gtsvector:'
            if (-not (Has-TypeInSetMap $catalogMappingSets $routeKey $typeId)) {
                $missingMapping.Add($typeId)
            }

            $sourceAxes = @('source.protocol_capture', 'source.change_event', 'source.live')
            foreach ($axis in $sourceAxes) {
                if (-not $sourceRouteAxes[$axis].Contains($typeId)) {
                    $missingSource.Add([pscustomobject]@{ type_id=$typeId; axis=$axis })
                }
            }
            switch ([string]$definition.representation_mode) {
                'SEMANTIC_CODEC' {
                    if (-not $sourceRouteAxes['source.semantic_codec'].Contains($typeId)) {
                        if ($knownNullOnlyType -and $sourceRouteAxes['source.null_only'].Contains($typeId)) {
                            $nullOnlyTypes.Add($typeId)
                        } else {
                            $missingSource.Add([pscustomobject]@{ type_id=$typeId; axis='source.semantic_codec' })
                        }
                    }
                }
                'SOURCE_REPRESENTATION' {
                    $hasRepresentationReceipt =
                        $sourceRouteAxes['source.source_representation_capture'].Contains($typeId) -and
                        $sourceRouteAxes['source.protocol_framing'].Contains($typeId)
                    if ($hasRepresentationReceipt) {
                        $representationTypes.Add($typeId)
                    } else {
                        if ($knownNullOnlyType -and $sourceRouteAxes['source.null_only'].Contains($typeId)) {
                            $nullOnlyTypes.Add($typeId)
                        } else {
                            foreach ($axis in @('source.source_representation_capture', 'source.protocol_framing')) {
                                if (-not $sourceRouteAxes[$axis].Contains($typeId)) {
                                    $missingSource.Add([pscustomobject]@{ type_id=$typeId; axis=$axis })
                                }
                            }
                        }
                    }
                }
                default {
                    if ($knownNullOnlyType -and $sourceRouteAxes['source.null_only'].Contains($typeId)) {
                        $nullOnlyTypes.Add($typeId)
                    } else {
                        $missingSource.Add([pscustomobject]@{ type_id=$typeId; axis='source.semantic_codec|source_representation_capture|source.null_only' })
                    }
                }
            }

            foreach ($axis in @('sink.offline', 'sink.live')) {
                if (-not $routeAxes[$axis].Contains($typeId)) {
                    $missingSink.Add([pscustomobject]@{ type_id=$typeId; axis=$axis })
                }
            }
            if ($routeAxes['sink.live'].Contains($typeId) -and $typeId -in $representationTypes -and
                (-not $routeAxes['sink.representation_carrier'].Contains($typeId) -or
                 -not $routeAxes['sink.representation_preserved'].Contains($typeId))) {
                $missingSink.Add([pscustomobject]@{ type_id=$typeId; axis='sink.representation_carrier|sink.representation_preserved' })
            }
            if (-not $routeAxes['web.plan'].Contains($typeId)) {
                $missingWeb.Add($typeId)
            }
        }

        $rosterCount = $roster.Count
        $isQualified = $suiteRegistered -and $suitePassed -and $rosterCount -gt 0 -and
            $missingMapping.Count -eq 0 -and $missingSource.Count -eq 0 -and
            $missingSink.Count -eq 0 -and $missingWeb.Count -eq 0 -and
            $missingCatalogClasses.Count -eq 0 -and $unknownCatalogClasses.Count -eq 0
        $routes.Add([ordered]@{
            source = $source
            sink = $sink
            suite = $suiteId
            suite_registered = $suiteRegistered
            suite_passed = $suitePassed
            catalog_type_count = $rosterCount
            catalog_class_type_counts = $catalogClassCounts
            missing_catalog_classes = $missingCatalogClasses
            unknown_catalog_class_types = @($unknownCatalogClasses.ToArray())
            source_representation_type_count = $representationTypes.Count
            null_only_type_count = $nullOnlyTypes.Count
            qualified = $isQualified
            missing_catalog_mapping = @($missingMapping.ToArray())
            missing_source_receipts = @($missingSource.ToArray())
            missing_sink_receipts = @($missingSink.ToArray())
            missing_web_plans = @($missingWeb.ToArray())
        })
    }
}

$unregisteredRouteSuites = @($postgresSources | ForEach-Object {
    $source = $_
    foreach ($sink in $connectors) {
        $expected = if ($source -eq $sink) { "${source}.web_all_builtin_types_self_sink" } else { "${source}.web_all_builtin_types_to_${sink}" }
        if ($expected -notin $configuredSuites) { $expected }
    }
})
$qualified = $routes.Count -eq 18 -and @($routes | Where-Object { -not $_.qualified }).Count -eq 0 -and
    $unregisteredRouteSuites.Count -eq 0 -and $rosterConflicts.Count -eq 0
$report = [ordered]@{
    schema = 'cdc.postgresql_catalog_type_route_coverage.v1'
    qualified = $qualified
    expected_route_count = 18
    route_count = $routes.Count
    sources = @($postgresSources)
    sinks = @($connectors)
    unregistered_route_suites = $unregisteredRouteSuites
    catalog_roster_conflicts = @($rosterConflicts.ToArray())
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
