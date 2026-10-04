function Get-QualificationSourceSuiteIds([object]$Spec) {
    if ($null -eq $Spec) { return }
    [string]$Spec.suite
    foreach ($additional in @($Spec.additional_suites)) {
        if (-not [string]::IsNullOrWhiteSpace([string]$additional)) {
            [string]$additional
        }
    }
}
