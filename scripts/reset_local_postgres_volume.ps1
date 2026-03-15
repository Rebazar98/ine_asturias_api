param(
    [string]$EnvFile = ".env",
    [string]$ProjectName = "playground",
    [switch]$Force
)

$ErrorActionPreference = "Stop"

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$postgresVolume = "${ProjectName}_postgres_data"

Push-Location $repoRoot
try {
    if (-not $Force) {
        Write-Host "Este script elimina el volumen local de PostgreSQL y recrea la base desde cero."
        Write-Host "Vuelve a ejecutarlo con -Force si quieres continuar."
        exit 1
    }

    if (-not (Test-Path $EnvFile)) {
        throw "No se encontro el fichero de entorno: $EnvFile"
    }

    Write-Host "[reset-local-postgres] bajando stack local..."
    docker compose --env-file $EnvFile -p $ProjectName down --remove-orphans

    $volumeExists = docker volume ls --format "{{.Name}}" | Where-Object { $_ -eq $postgresVolume }
    if ($volumeExists) {
        Write-Host "[reset-local-postgres] eliminando volumen $postgresVolume ..."
        docker volume rm $postgresVolume | Out-Null
    } else {
        Write-Host "[reset-local-postgres] volumen $postgresVolume no existe; continuamos."
    }

    Write-Host "[reset-local-postgres] levantando db y redis..."
    docker compose --env-file $EnvFile -p $ProjectName up --build -d db redis

    Write-Host "[reset-local-postgres] aplicando migraciones..."
    docker compose --env-file $EnvFile -p $ProjectName run --rm migrate

    Write-Host "[reset-local-postgres] listo. Puedes continuar con:"
    Write-Host "  docker compose --env-file $EnvFile -p $ProjectName up --build -d api worker"
    Write-Host "  python scripts/smoke_stack.py"
}
finally {
    Pop-Location
}
