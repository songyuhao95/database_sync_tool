function Test-UserConfigurableTargetPaths {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][object]$TypeInventory,
        [Parameter(Mandatory = $true)][object]$MysqlRouteCoverage
    )

    $routes = @($MysqlRouteCoverage.routes)
    if ($TypeInventory.per_type_web_plan_gaps -ne 0 -or
        $TypeInventory.per_type_web_sink_plan_gaps -ne 0 -or
        $routes.Count -ne 18) {
        return $false
    }
    foreach ($route in $routes) {
        if ($route.source_type_count -le 0 -or
            $route.web_plan_type_count -ne $route.source_type_count -or
            @($route.missing_web_plans).Count -ne 0) {
            return $false
        }
    }
    return $true
}
