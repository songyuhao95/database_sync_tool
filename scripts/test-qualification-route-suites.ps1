$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'qualification-route-suites.ps1')

$spec = [pscustomobject]@{
    suite = 'mysql_5_7.capture'
    additional_suites = @('mysql_5_7.all_types_capture')
}
$ids = @(Get-QualificationSourceSuiteIds $spec)
if ($ids.Count -ne 2 -or $ids[0] -ne $spec.suite -or
    $ids[1] -ne $spec.additional_suites[0]) {
    throw 'Source suite IDs were concatenated or lost.'
}
if (@(Get-QualificationSourceSuiteIds $null).Count -ne 0) {
    throw 'An absent source must not produce a suite ID.'
}
'PASS: source suite IDs remain separate in qualification directions.'
