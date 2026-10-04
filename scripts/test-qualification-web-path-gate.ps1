[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'qualification-web-path-gate.ps1')

$inventory = [pscustomobject]@{
    per_type_web_plan_gaps = 111
    per_type_web_sink_plan_gaps = 111
}
$routes = @(
    for ($index = 0; $index -lt 18; $index++) {
        [pscustomobject]@{
            source_type_count = 37
            web_plan_type_count = if ($index -lt 3) { 0 } else { 37 }
            missing_web_plans = if ($index -lt 3) { @('37 missing type receipts') } else { @() }
        }
    }
)
$audit = [pscustomobject]@{ routes = $routes }
if (Test-UserConfigurableTargetPaths -TypeInventory $inventory -MysqlRouteCoverage $audit) {
    throw 'Component-only routes must not complete the all-type goal.'
}

$inventory.per_type_web_plan_gaps = 0
$inventory.per_type_web_sink_plan_gaps = 0
foreach ($route in $routes[0..2]) {
    $route.web_plan_type_count = 37
    $route.missing_web_plans = @()
}
if (-not (Test-UserConfigurableTargetPaths -TypeInventory $inventory -MysqlRouteCoverage $audit)) {
    throw 'A fully witnessed Web plan matrix must pass.'
}

$routes[0].web_plan_type_count = 36
if (Test-UserConfigurableTargetPaths -TypeInventory $inventory -MysqlRouteCoverage $audit) {
    throw 'One missing field plan must fail the whole goal.'
}
Write-Output 'PASS: component-only and missing-field paths fail; complete Web plan matrix passes.'
