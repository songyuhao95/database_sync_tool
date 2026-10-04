[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$workspaceRoot = Split-Path -Parent $PSScriptRoot
$allowedRoot = [IO.Path]::GetFullPath((Join-Path $workspaceRoot 'target/qualification-audit-tests'))
$runDirectory = Join-Path $allowedRoot ([guid]::NewGuid().ToString('N'))
$evidenceDirectory = Join-Path $runDirectory 'type-evidence'
$audit = Join-Path $PSScriptRoot 'audit-mysql-type-routes.ps1'

function Assert-True([bool]$Condition, [string]$Message) {
    if (-not $Condition) { throw $Message }
}

function Write-Evidence([string]$Name, [object]$Report) {
    $path = Join-Path $evidenceDirectory $Name
    [IO.File]::WriteAllText($path, (ConvertTo-Json -InputObject $Report -Depth 8), [Text.UTF8Encoding]::new($false))
}

try {
    New-Item -ItemType Directory -Path $evidenceDirectory -Force | Out-Null
    foreach ($version in @('5_7', '8_0', '8_4')) {
        $connector = "mysql_$version"
        $sourceAxes = @('source.protocol_capture', 'source.change_event', 'source.semantic_codec', 'source.live')
        $sinkAxes = @('sink.offline', 'sink.live')
        Write-Evidence "$connector-source.json" ([ordered]@{
            schema = 'cdc.type_qualification_evidence_report.v1'
            suite_id = "$connector.all_types_capture"
            evidence = @($sourceAxes | ForEach-Object {
                [ordered]@{ source_connector_id = $connector; sink_connector_id = $null; type_id = 'mysql.bigint'; axis = $_; status = 'PASS' }
            })
        })
        Write-Evidence "$connector-sink.json" ([ordered]@{
            schema = 'cdc.type_qualification_evidence_report.v1'
            suite_id = "$connector.all_types_to_$version"
            evidence = @($sinkAxes | ForEach-Object {
                [ordered]@{ source_connector_id = $connector; sink_connector_id = $connector; type_id = 'mysql.bigint'; axis = $_; status = 'PASS' }
            })
        })
    }

    $withoutDirections = (& $audit -EvidenceDirectory $evidenceDirectory) | ConvertFrom-Json
    $selfRoutes = @($withoutDirections.routes | Where-Object { $_.source -eq $_.sink -and $_.source -match '^mysql_' })
    Assert-True ($selfRoutes.Count -eq 3) 'The fixture must expose three same-version MySQL routes.'
    Assert-True (@($selfRoutes | Where-Object component_composition_qualified).Count -eq 0) 'A self route cannot qualify without offline direction evidence.'

    $offlineDirections = @('mysql_5_7>mysql_5_7', 'mysql_8_0>mysql_8_0', 'mysql_8_4>mysql_8_4')
    $withDirections = (& $audit -EvidenceDirectory $evidenceDirectory -OfflineDirection $offlineDirections) | ConvertFrom-Json
    $selfRoutes = @($withDirections.routes | Where-Object { $_.source -eq $_.sink -and $_.source -match '^mysql_' })
    Assert-True ($selfRoutes.Count -eq 3) 'All three same-version MySQL routes must be reported.'
    Assert-True (@($selfRoutes | Where-Object { $_.verification_mode -eq 'COMPONENTS_COMPOSED_MYSQL_SELF_ROUTE' -and $_.component_composition_qualified -and $_.offline_direction_passed }).Count -eq 3) 'Offline route evidence must qualify all three self routes through component composition.'
    Assert-True ($withDirections.component_composed_route_count -eq 3) 'The report must count three component-composed routes.'
    Assert-True ($withDirections.live_web_e2e_route_count -eq 0) 'Component composition must never claim a live Web E2E route.'
    Assert-True (-not $withDirections.qualified) 'Missing cross-version route receipts must keep the overall audit incomplete.'
    Write-Output 'PASS: offline directions qualify three MySQL self routes through component evidence without claiming Web E2E.'
} finally {
    $resolvedRunDirectory = [IO.Path]::GetFullPath($runDirectory)
    if (-not $resolvedRunDirectory.StartsWith($allowedRoot + [IO.Path]::DirectorySeparatorChar, [StringComparison]::OrdinalIgnoreCase)) {
        throw "Refusing to remove test output outside $allowedRoot"
    }
    if (Test-Path -LiteralPath $resolvedRunDirectory) {
        Remove-Item -LiteralPath $resolvedRunDirectory -Recurse -Force
    }
}
