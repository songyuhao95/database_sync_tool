[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)][string]$OriginalSummary,
    [Parameter(Mandatory = $true)][string]$TypeReport,
    [Parameter(Mandatory = $true)][string]$PostgresqlRouteAudit,
    [Parameter(Mandatory = $true)][string]$MysqlRouteAudit,
    [Parameter(Mandatory = $true)][string]$OutputFile
)

$ErrorActionPreference = 'Stop'
function Read-Evidence([string]$Path) {
    $resolved = (Resolve-Path -LiteralPath $Path -ErrorAction Stop).Path
    return [pscustomobject]@{
        path = $resolved
        sha256 = (Get-FileHash -LiteralPath $resolved -Algorithm SHA256).Hash.ToLowerInvariant()
        document = Get-Content -LiteralPath $resolved -Raw | ConvertFrom-Json
    }
}

$original = Read-Evidence $OriginalSummary
$type = Read-Evidence $TypeReport
$postgresql = Read-Evidence $PostgresqlRouteAudit
$mysql = Read-Evidence $MysqlRouteAudit
$summary = $original.document
$inventory = $type.document.type_inventory
$pgAudit = $postgresql.document
$mysqlAudit = $mysql.document

$suiteStatuses = @{}
foreach ($suite in @($summary.suites)) {
    if ($suiteStatuses.ContainsKey([string]$suite.id)) { throw "Duplicate suite: $($suite.id)" }
    $suiteStatuses[[string]$suite.id] = [string]$suite.status
}
$requiredSuites = @($summary.required_live_suite_results)
$requiredSuitePass = $requiredSuites.Count -gt 0 -and
    @($requiredSuites | Where-Object {
        $_.status -ne 'PASS' -or -not $suiteStatuses.ContainsKey([string]$_.id) -or
        $suiteStatuses[[string]$_.id] -ne 'PASS'
    }).Count -eq 0
$directions = @($summary.directions)
$source = @($summary.source_qualification)
$sink = @($summary.sink_qualification)
$dynamic = @($inventory.dynamic_type_classes)
$routeRecords = @($pgAudit.routes) + @($mysqlAudit.routes)
$selfRoutes = @($mysqlAudit.routes | Where-Object {
    $_.verification_mode -eq 'COMPONENTS_COMPOSED_MYSQL_SELF_ROUTE'
})
$selfRouteTypeCount = [int](($selfRoutes | Measure-Object -Property source_type_count -Sum).Sum)
$directionProof = @(foreach ($direction in $directions) {
    $sourceComponent = @($source | Where-Object database -eq $direction.source)
    $sinkComponent = @($sink | Where-Object database -eq $direction.sink)
    $route = @($routeRecords | Where-Object {
        $_.source -eq $direction.source -and $_.sink -eq $direction.sink
    })
    [ordered]@{
        source = $direction.source
        sink = $direction.sink
        offline = $direction.offline
        source_component = if ($sourceComponent.Count -eq 1) { $sourceComponent[0].status } else { 'MISSING' }
        sink_component = if ($sinkComponent.Count -eq 1) { $sinkComponent[0].status } else { 'MISSING' }
        route_qualified = $route.Count -eq 1 -and $route[0].qualified -eq $true
        route_mode = if ($route.Count -eq 1 -and $route[0].verification_mode) {
            $route[0].verification_mode
        } elseif ($route.Count -eq 1) { 'LIVE_WEB_E2E' } else { 'MISSING' }
    }
})
$checks = [ordered]@{
    original_schema = $summary.schema -eq 'cdc.qualification.v2'
    original_offline = $summary.offline_success -eq $true
    directions = $directionProof.Count -eq 36 -and
        @($directionProof | ForEach-Object { "$($_.source)>$($_.sink)" } | Select-Object -Unique).Count -eq 36 -and
        @($directionProof | Where-Object {
            $_.offline -ne 'PASS' -or $_.source_component -ne 'PASS' -or
            $_.sink_component -ne 'PASS' -or -not $_.route_qualified
        }).Count -eq 0
    required_live_suites = $requiredSuitePass -and $summary.required_live_suites_qualified -eq $true
    source = $source.Count -eq 6 -and @($source | Where-Object status -ne 'PASS').Count -eq 0
    sink = $sink.Count -eq 6 -and @($sink | Where-Object status -ne 'PASS').Count -eq 0
    recovery = $summary.transaction_recovery_qualified -eq $true
    capability_invalidation = $summary.capability_invalidation_qualified -eq $true
    route_smoke = $summary.route_smoke_qualified -eq $true
    no_suite_failure = @($summary.suites | Where-Object status -eq 'FAIL').Count -eq 0
    no_password_in_report = $summary.password_scan.status -eq 'PASS' -and
        $summary.password_scan.matches_redacted -eq 0
    type_report_schema = $type.document.schema -eq 'cdc.qualification.v2'
    type_inventory = $inventory.status -eq 'PASS' -and
        $inventory.source_evidence_profiles.native_type_baseline.semantic_codec.qualified_declaration_count +
        $inventory.source_evidence_profiles.native_type_baseline.source_representation_capture.qualified_declaration_count -eq
            $inventory.source_declaration_count -and
        $inventory.source_mapping_gaps -eq 0 -and
        $inventory.types_without_qualification_fixture -eq 0 -and
        $inventory.per_type_live_source_gaps -eq 0 -and
        $inventory.per_type_live_sink_gaps -eq 0 -and
        $inventory.per_type_route_qualification_gaps -eq 0 -and
        $inventory.per_type_web_plan_gaps -eq $selfRouteTypeCount -and
        $inventory.per_type_web_sink_plan_gaps -eq $selfRouteTypeCount -and
        $dynamic.Count -eq 27 -and @($dynamic | Where-Object status -ne 'PASS').Count -eq 0
    postgresql_catalog_routes = $pgAudit.schema -eq 'cdc.postgresql_catalog_type_route_coverage.v1' -and
        $pgAudit.qualified -eq $true -and $pgAudit.route_count -eq 18 -and
        @($pgAudit.routes | Where-Object { -not $_.qualified }).Count -eq 0
    mysql_native_routes = $mysqlAudit.schema -eq 'cdc.mysql_native_type_route_coverage.v1' -and
        $mysqlAudit.qualified -eq $true -and $mysqlAudit.route_count -eq 18 -and
        $mysqlAudit.live_web_e2e_route_count -ge 15 -and
        $mysqlAudit.component_composed_route_count -eq 3 -and
        @($mysqlAudit.routes | Where-Object { -not $_.qualified }).Count -eq 0
    user_configurable_target_paths = $inventory.per_type_web_plan_gaps -eq 0 -and
        $inventory.per_type_web_sink_plan_gaps -eq 0 -and
        @($mysqlAudit.routes | Where-Object {
            $_.web_plan_type_count -ne $_.source_type_count -or
            @($_.missing_web_plans).Count -ne 0
        }).Count -eq 0
}
$passed = @($checks.Values | Where-Object { $_ -ne $true }).Count -eq 0
$report = [ordered]@{
    schema = 'cdc.qualification.evidence_reassessment.v1'
    qualified = $passed
    evidence_semantics = 'Existing live suites are reused. Type inventory and route audits are recomputed from saved artifact-bound receipts. Component-composed routes prove adapter compatibility only; map completion additionally requires a Web plan receipt for every source type and target route.'
    checks = $checks
    directions = $directionProof
    counts = [ordered]@{
        native_type_families = $inventory.native_type_count
        native_source_declarations = $inventory.source_declaration_count
        source_semantic_codec_declarations = $inventory.source_evidence_profiles.native_type_baseline.semantic_codec.qualified_declaration_count
        source_representation_declarations = $inventory.source_evidence_profiles.native_type_baseline.source_representation_capture.qualified_declaration_count
        postgresql_catalog_routes = $pgAudit.route_count
        mysql_web_end_to_end_routes = $mysqlAudit.live_web_e2e_route_count
        mysql_component_composed_self_routes = $mysqlAudit.component_composed_route_count
        mysql_self_route_web_plan_gaps = $inventory.per_type_web_plan_gaps
    }
    inputs = [ordered]@{
        original_live_summary = [ordered]@{ path = $original.path; sha256 = $original.sha256 }
        rebuilt_type_report = [ordered]@{ path = $type.path; sha256 = $type.sha256 }
        postgresql_catalog_route_audit = [ordered]@{ path = $postgresql.path; sha256 = $postgresql.sha256 }
        mysql_native_route_audit = [ordered]@{ path = $mysql.path; sha256 = $mysql.sha256 }
    }
}
$target = [IO.Path]::GetFullPath($OutputFile)
$parent = Split-Path -Parent $target
if ($parent) { New-Item -ItemType Directory -Path $parent -Force | Out-Null }
[IO.File]::WriteAllText($target, (ConvertTo-Json -InputObject $report -Depth 12), [Text.UTF8Encoding]::new($false))
$report | ConvertTo-Json -Depth 4
if (-not $passed) { exit 1 }
