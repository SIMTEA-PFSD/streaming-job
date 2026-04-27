# ===============================================================
#  bajar-todo.ps1
#  Apaga todos los contenedores Docker del sistema.
#
#  Uso:
#    .\bajar-todo.ps1            -> apaga contenedores (preserva volumenes/datos)
#    .\bajar-todo.ps1 -Clean     -> apaga Y BORRA volumenes (reset total)
#                                   Usalo cuando Postgres este corrupto o con
#                                   usuarios/tablas de corridas anteriores.
# ===============================================================

param(
    [string]$Root = $PSScriptRoot,
    [switch]$Clean
)

$CheckinDir    = Join-Path $Root "Check-in"
$SecurityDir   = Join-Path $Root "Security"
$DispatcherDir = Join-Path $Root "Dispatcher"

function Down {
    param([string]$dir, [string]$name, [bool]$cleanMode)
    if (Test-Path $dir) {
        Write-Host ""
        if ($cleanMode) {
            Write-Host ">> Bajando contenedores + borrando volumenes de $name" -ForegroundColor Cyan
            Push-Location $dir
            docker compose down -v
            Pop-Location
        }
        else {
            Write-Host ">> Bajando contenedores de $name (volumenes preservados)" -ForegroundColor Cyan
            Push-Location $dir
            docker compose down
            Pop-Location
        }
    }
    else {
        Write-Host "  (no existe $dir, salto $name)" -ForegroundColor DarkGray
    }
}

Write-Host ""
Write-Host "============================================================" -ForegroundColor Magenta
if ($Clean) {
    Write-Host "  Apagando contenedores + BORRANDO volumenes (reset total)" -ForegroundColor Magenta
}
else {
    Write-Host "  Apagando contenedores del sistema (datos preservados)" -ForegroundColor Magenta
}
Write-Host "============================================================" -ForegroundColor Magenta

# Matar procesos sbt/java zombis si quedaron dando vueltas
Write-Host ""
Write-Host ">> Matando procesos java/sbt zombis si los hay" -ForegroundColor Cyan
Get-Process java, sbt -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
Write-Host "  OK" -ForegroundColor Green

Down $DispatcherDir "Dispatcher" $Clean
Down $SecurityDir   "Security"   $Clean
Down $CheckinDir    "Check-in"   $Clean

Write-Host ""
if ($Clean) {
    Write-Host "[OK] Todo abajo y volumenes borrados. La proxima vez que corras" -ForegroundColor Green
    Write-Host "     levantar-todo.ps1, Postgres se inicializa limpio con init.sql." -ForegroundColor Green
}
else {
    Write-Host "[OK] Todo abajo. Los datos en Postgres se preservaron." -ForegroundColor Green
    Write-Host "     Si necesitas un reset total (ej: role does not exist),"  -ForegroundColor Yellow
    Write-Host "     ejecuta:  .\bajar-todo.ps1 -Clean" -ForegroundColor Yellow
}
Write-Host ""