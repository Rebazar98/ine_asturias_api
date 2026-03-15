param(
    [string]$EnvFile = "",
    [string]$ProjectName = "",
    [string]$BaseUrl = "http://127.0.0.1:8001",
    [string]$BackupPath = "backups/ine_asturias.dump",
    [string]$MunicipalityCode = "",
    [switch]$RunRestoreDrill
)

$ErrorActionPreference = 'Stop'
$composePrefix = @('compose')
if ($EnvFile) {
    $composePrefix += @('--env-file', $EnvFile)
}
if ($ProjectName) {
    $composePrefix += @('-p', $ProjectName)
}

function Invoke-Compose {
    param([string[]]$Subcommand)

    $display = @('docker') + $composePrefix + $Subcommand
    Write-Host "[release-candidate] running: $($display -join ' ')"
    & docker @composePrefix @Subcommand
    if ($LASTEXITCODE -ne 0) {
        throw "Command failed with exit code ${LASTEXITCODE}: $($display -join ' ')"
    }
}

function Wait-Http {
    param(
        [string]$Url,
        [hashtable]$Headers = @{}
    )

    $deadline = (Get-Date).AddMinutes(2)
    $lastError = $null

    while ((Get-Date) -lt $deadline) {
        try {
            $response = Invoke-WebRequest -Uri $Url -Headers $Headers -UseBasicParsing -TimeoutSec 5
            if ($response.StatusCode -eq 200) {
                Write-Host "[release-candidate] $Url OK"
                return
            }
            $lastError = "unexpected status $($response.StatusCode)"
        }
        catch {
            $lastError = $_.Exception.Message
        }

        Start-Sleep -Seconds 2
    }

    throw "Timeout waiting for $Url. Last error: $lastError"
}

function Resolve-ComposeEnvValue {
    param([string]$Name)

    $current = [Environment]::GetEnvironmentVariable($Name)
    if ($current) {
        return $current
    }

    $candidateFiles = @()
    if ($EnvFile) {
        $candidateFiles += $EnvFile
    }
    $candidateFiles += '.env'

    foreach ($candidate in $candidateFiles) {
        if (-not $candidate -or -not (Test-Path $candidate)) {
            continue
        }
        foreach ($line in Get-Content $candidate) {
            if ($line -match '^\s*#' -or $line -notmatch '=') {
                continue
            }
            $parts = $line -split '=', 2
            if ($parts[0].Trim() -eq $Name) {
                return $parts[1].Trim()
            }
        }
    }

    return ''
}

$base = $BaseUrl.TrimEnd('/')
$apiKey = Resolve-ComposeEnvValue -Name 'API_KEY'
$metricsHeaders = @{}
if ($apiKey) {
    $metricsHeaders['X-API-Key'] = $apiKey
}

Invoke-Compose -Subcommand @('up', '--build', '-d', 'db', 'redis', 'api', 'worker')
Wait-Http -Url "$base/health"
Wait-Http -Url "$base/health/ready"
Wait-Http -Url "$base/metrics" -Headers $metricsHeaders

Invoke-Compose -Subcommand @('run', '--rm', 'api', 'python', '-m', 'pip', 'check')
Invoke-Compose -Subcommand @('run', '--rm', 'api', 'ruff', 'check', '.')
Invoke-Compose -Subcommand @('run', '--rm', 'api', 'ruff', 'format', '--check', 'app/api', 'app/core', 'scripts', 'main.py', 'app/settings.py', 'app/worker.py')
Invoke-Compose -Subcommand @('run', '--rm', 'api', 'pytest')
Invoke-Compose -Subcommand @('run', '--rm', 'migrate')
$seedCommand = @('run', '--rm', 'api', 'python', 'scripts/seed_municipality_analytics.py')
if ($MunicipalityCode) {
    $seedCommand += @('--municipality-code', $MunicipalityCode)
    Invoke-Compose -Subcommand $seedCommand
}
$smokeCommand = @('run', '--rm', 'api', 'python', 'scripts/smoke_stack.py', '--base-url', 'http://api:8000')
if ($MunicipalityCode) {
    $smokeCommand += @('--municipality-code', $MunicipalityCode)
}
Invoke-Compose -Subcommand $smokeCommand
Invoke-Compose -Subcommand @('run', '--rm', 'api', 'python', 'scripts/verify_restore.py', '--base-url', 'http://api:8000', '--min-ingestion-rows', '1', '--min-normalized-rows', '1')

if ($RunRestoreDrill) {
    & $PSScriptRoot\restore_drill.ps1 -BackupPath $BackupPath -EnvFile $EnvFile -ProjectName $ProjectName -BaseUrl $BaseUrl -MunicipalityCode $MunicipalityCode
    if ($LASTEXITCODE -ne 0) {
        throw "Restore drill failed with exit code ${LASTEXITCODE}"
    }
}
else {
    Write-Host '[release-candidate] restore drill not executed in this run. Run scripts/restore_drill.ps1 or the manual Restore Drill workflow before closing the RC.'
}

Write-Host '[release-candidate] security scan is not executed locally by this script. Review the latest Security Scan workflow run or trigger it manually before approving the RC.'
Write-Host '[release-candidate] release candidate checks completed successfully'
