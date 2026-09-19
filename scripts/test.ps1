# Requires PowerShell 5.1+ and Cargo. No extra PowerShell modules.
[CmdletBinding()]
param(
    [switch]$Live,
    [ValidateSet('All', 'Read', 'ChangeEvent', 'Sql')]
    [string]$Stage = 'All',
    [string[]]$Database = @('all'),
    [string]$ConfigFile = '',
    [switch]$List
)
$ErrorActionPreference = 'Stop'
# Cargo emits build progress on stderr. A native nonzero exit is handled explicitly below.
$PSNativeCommandUseErrorActionPreference = $false
$root = Split-Path -Parent $PSScriptRoot
$matrix = Get-Content -LiteralPath (Join-Path $PSScriptRoot 'test-matrix.json') -Raw | ConvertFrom-Json
$allowedConfigNames = @(
    'CDC_MYSQL_HOST',
    'CDC_MYSQL57_PORT',
    'CDC_MYSQL80_PORT',
    'CDC_MYSQL84_PORT',
    'CDC_MYSQL_READER_USER',
    'CDC_MYSQL_READER_PASSWORD',
    'CDC_MYSQL_WRITER_USER',
    'CDC_MYSQL_WRITER_PASSWORD',
    'PG_CDC_HOST',
    'PG_CDC_PORT',
    'PG_CDC_ADMIN_USER',
    'PG_CDC_READER_USER',
    'PG_CDC_WRITER_USER',
    'PG_CDC_TEST_PASSWORD'
)
function Import-TestConfig {
    param([Parameter(Mandatory)][string]$Path)
    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) {
        throw "Live test configuration does not exist: $Path"
    }
    $values = @{}
    $lineNumber = 0
    foreach ($rawLine in [IO.File]::ReadAllLines($Path, [Text.Encoding]::UTF8)) {
        $lineNumber++
        if ([string]::IsNullOrWhiteSpace($rawLine) -or $rawLine.TrimStart().StartsWith('#')) {
            continue
        }
        $separator = $rawLine.IndexOf('=')
        if ($separator -le 0) {
            throw "Invalid test configuration at line ${lineNumber}: expected KEY=VALUE"
        }
        $name = $rawLine.Substring(0, $separator).Trim()
        $value = $rawLine.Substring($separator + 1)
        if ($name -notin $allowedConfigNames) {
            throw "Unknown test configuration key '$name' at line $lineNumber"
        }
        if ($values.ContainsKey($name)) {
            throw "Duplicate test configuration key '$name' at line $lineNumber"
        }
        if ([string]::IsNullOrEmpty($value)) {
            throw "Empty test configuration value for '$name' at line $lineNumber"
        }
        $values[$name] = $value
    }
    $imported = [Collections.Generic.List[string]]::new()
    foreach ($name in $values.Keys) {
        # Explicit process environment variables remain useful for CI and override the local file.
        if ([string]::IsNullOrEmpty([Environment]::GetEnvironmentVariable($name, 'Process'))) {
            [Environment]::SetEnvironmentVariable($name, $values[$name], 'Process')
            $imported.Add($name)
        }
    }
    return $imported.ToArray()
}
$databases = if ($Database -contains 'all') { @($matrix.databases) } else { @($Database | Select-Object -Unique) }
foreach ($db in $databases) {
    if ($db -notin $matrix.databases) { throw "Unknown database '$db'. Available: $($matrix.databases -join ', ')" }
}
$stages = if ($Stage -eq 'All') { @('Read', 'ChangeEvent', 'Sql') } else { @($Stage) }
$selected = @($matrix.suites | Where-Object {
    $suite = $_
    @($suite.databases | Where-Object { $_ -in $databases }).Count -gt 0 -and
    @($suite.stages | Where-Object { $_ -in $stages }).Count -gt 0 -and
    ($suite.mode -eq 'Local' -or $Live)
})
if ($List) {
    $selected | Select-Object id, mode, @{n='Stages';e={$_.stages -join ','}}, @{n='Credentials';e={$_.required_env -join ','}} | Format-Table -AutoSize
    return
}
Get-Command cargo -ErrorAction Stop | Out-Null
$configPath = if ([string]::IsNullOrWhiteSpace($ConfigFile)) {
    Join-Path $PSScriptRoot 'test.txt'
} elseif ([IO.Path]::IsPathRooted($ConfigFile)) {
    $ConfigFile
} else {
    Join-Path (Get-Location) $ConfigFile
}
$configPath = [IO.Path]::GetFullPath($configPath)
$runId = (Get-Date -Format 'yyyyMMdd-HHmmss') + '-' + [guid]::NewGuid().ToString('N').Substring(0,8)
$out = Join-Path $root "target\test-results\$runId"
New-Item -ItemType Directory -Path $out -Force | Out-Null
$oldArtifacts = [Environment]::GetEnvironmentVariable('CDC_TEST_ARTIFACT_DIR', 'Process')
$env:CDC_TEST_ARTIFACT_DIR = $out
$results = [Collections.Generic.List[object]]::new()
$importedConfigKeys = @()
Push-Location $root
try {
    if ($Live -and (Test-Path -LiteralPath $configPath)) {
        $importedConfigKeys = @(Import-TestConfig -Path $configPath)
    }
    $secrets = @(Get-ChildItem Env: | Where-Object {
        $_.Name -match '^(CDC_MYSQL|PG_CDC)_.*PASSWORD$' -and $_.Value
    } | ForEach-Object { $_.Value })
    foreach ($suite in $selected) {
        $timer = [Diagnostics.Stopwatch]::StartNew()
        $log = Join-Path $out "$($suite.id).log"
        $status = 'PASS'
        $detail = ''
        $missing = @($suite.required_env | Where-Object { -not [Environment]::GetEnvironmentVariable($_, 'Process') })
        $exitCode = -1
        $lines = [Collections.Generic.List[string]]::new()
        try {
            if ($missing.Count) {
                $status = 'REQUIRES_LIVE'
                $detail = "Missing environment: $($missing -join ', ')"
                $lines.Add($detail)
                continue
            }
            $arguments = @($suite.args)
            # Run each registered suite once even when it covers both Read and ChangeEvent.
            $savedPreference = $ErrorActionPreference
            $ErrorActionPreference = 'Continue'
            try {
                & cargo @arguments 2>&1 | ForEach-Object {
                    $line = $_.ToString()
                    foreach ($secret in $secrets) { $line = $line.Replace($secret, '[REDACTED]') }
                    $lines.Add($line)
                }
                $exitCode = $LASTEXITCODE
            } finally { $ErrorActionPreference = $savedPreference }
            if ($exitCode -ne 0) { throw "Cargo exited with code $exitCode" }
            if (-not ($lines -match 'test result: ok\. [1-9][0-9]* passed;')) {
                throw 'No passing test was executed; check the test filter in test-matrix.json.'
            }
        } catch {
            $status = 'FAIL'
            $detail = $_.Exception.Message
            foreach ($secret in $secrets) { $detail = $detail.Replace($secret, '[REDACTED]') }
            $lines.Add($detail)
        } finally {
            $timer.Stop()
            $lines | Set-Content -LiteralPath $log -Encoding UTF8
            $results.Add([pscustomobject]@{
            suite=$suite.id; mode=$suite.mode; databases=@($suite.databases | Where-Object { $_ -in $databases })
            stages=@($suite.stages | Where-Object { $_ -in $stages }); status=$status
            seconds=[math]::Round($timer.Elapsed.TotalSeconds,2); exit_code=$exitCode; detail=$detail; log=$log
            })
        }
    }
    $coverage = @(foreach ($db in $databases) {
        foreach ($part in $stages) {
            $unsupported = @($matrix.unsupported | Where-Object { $_.database -eq $db -and $_.stage -eq $part })
            $states = @{}
            foreach ($mode in @('Local','Live')) {
                $checks = @($results | Where-Object { $db -in $_.databases -and $part -in $_.stages -and $_.mode -eq $mode })
                $registered = @($matrix.suites | Where-Object { $db -in $_.databases -and $part -in $_.stages -and $_.mode -eq $mode })
                $states[$mode] = if ($unsupported.Count) { 'UNSUPPORTED' }
                    elseif (@($checks | Where-Object status -eq 'FAIL').Count) { 'FAIL' }
                    elseif (@($checks | Where-Object status -eq 'REQUIRES_LIVE').Count) { 'REQUIRES_LIVE' }
                    elseif ($checks.Count) { 'PASS' }
                    elseif ($registered.Count) { 'REQUIRES_LIVE' }
                    elseif ($mode -eq 'Local' -and $part -eq 'Read') { 'REQUIRES_LIVE' }
                    else { 'MISSING_TEST' }
            }
            [pscustomobject]@{ Database=$db; Stage=$part; Local=$states.Local; Live=$states.Live }
        }
    })
    $failed = @($results | Where-Object status -eq 'FAIL').Count -gt 0 -or
        @($coverage | Where-Object { $_.Local -eq 'MISSING_TEST' -or $_.Live -eq 'MISSING_TEST' }).Count -gt 0
    $report = [ordered]@{ run_id=$runId; live=[bool]$Live; success=(-not $failed); coverage=$coverage; results=@($results.ToArray()) }
    $report | ConvertTo-Json -Depth 10 | Set-Content -LiteralPath (Join-Path $out 'summary.json') -Encoding UTF8
    $coverage | Format-Table -AutoSize | Out-Host
    Write-Host "Report: $out\summary.json"
    if ($failed) { exit 1 }
} finally {
    Pop-Location
    [Environment]::SetEnvironmentVariable('CDC_TEST_ARTIFACT_DIR', $oldArtifacts, 'Process')
    foreach ($name in $importedConfigKeys) {
        Remove-Item -LiteralPath "Env:$name" -ErrorAction SilentlyContinue
    }
}
