param(
    [string]$BackupPath = "backups/ine_asturias.dump",
    [string]$EnvFile = "",
    [string]$ProjectName = "",
    [string]$BaseUrl = "http://127.0.0.1:8001",
    [string]$DbService = "db",
    [string]$DbName = "ine_asturias",
    [string]$PostgresUser = "postgres",
    [int]$TimeoutSeconds = 120,
    [switch]$TeardownOnSuccess
)

$ErrorActionPreference = 'Stop'
$restoreSucceeded = $false

$resolvedBackup = Resolve-Path -Path $BackupPath -ErrorAction Stop
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
    Write-Host "[restore-drill] running: $($display -join ' ')"
    & docker @composePrefix @Subcommand
    if ($LASTEXITCODE -ne 0) {
        throw "Command failed with exit code ${LASTEXITCODE}: $($display -join ' ')"
    }
}

function Wait-Http {
    param([string]$Url)

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    $lastError = $null

    while ((Get-Date) -lt $deadline) {
        try {
            $response = Invoke-WebRequest -Uri $Url -UseBasicParsing -TimeoutSec 5
            if ($response.StatusCode -eq 200) {
                Write-Host "[restore-drill] $Url OK"
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

function Wait-DbReady {
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    $lastError = $null
    $successCount = 0
    $requiredSuccesses = 5

    while ((Get-Date) -lt $deadline) {
        try {
            & docker @composePrefix exec -T $DbService pg_isready -U $PostgresUser -d $DbName | Out-Null
            if ($LASTEXITCODE -eq 0) {
                $successCount += 1
                if ($successCount -ge $requiredSuccesses) {
                    Write-Host "[restore-drill] database service stable after $successCount checks"
                    return
                }
            }
            else {
                $successCount = 0
                $lastError = "pg_isready exit code $LASTEXITCODE"
            }
        }
        catch {
            $successCount = 0
            $lastError = $_.Exception.Message
        }

        Start-Sleep -Seconds 2
    }

    throw "Timeout waiting for database readiness. Last error: $lastError"
}

try {
    $backupName = Split-Path -Path $resolvedBackup -Leaf

    Invoke-Compose -Subcommand @('down', '-v')
    Invoke-Compose -Subcommand @('up', '-d', 'db', 'redis')
    Wait-DbReady
    Invoke-Compose -Subcommand @('cp', $resolvedBackup, "$DbService`:/tmp/$backupName")
    Invoke-Compose -Subcommand @('exec', '-T', $DbService, 'dropdb', '-U', $PostgresUser, '--if-exists', $DbName)
    Invoke-Compose -Subcommand @('exec', '-T', $DbService, 'createdb', '-U', $PostgresUser, '-T', 'template0', $DbName)
    Invoke-Compose -Subcommand @('exec', '-T', $DbService, 'pg_restore', '-U', $PostgresUser, '-d', $DbName, "/tmp/$backupName")
    Invoke-Compose -Subcommand @('run', '--rm', 'migrate')
    Invoke-Compose -Subcommand @('up', '--build', '-d', 'api', 'worker')

    Wait-Http -Url "$($BaseUrl.TrimEnd('/'))/health"
    Wait-Http -Url "$($BaseUrl.TrimEnd('/'))/health/ready"
    Invoke-Compose -Subcommand @('run', '--rm', 'api', 'python', 'scripts/smoke_stack.py', '--base-url', 'http://api:8000')

    $restoreSucceeded = $true
    Write-Host '[restore-drill] restore drill completed successfully'
}
finally {
    if ($TeardownOnSuccess -and $restoreSucceeded) {
        Invoke-Compose -Subcommand @('down', '-v')
    }
}
