# ===============================================================
#  levantar-todo.ps1
#  Arranca todo el sistema distribuido de equipajes con un comando
# ===============================================================
#
#  Que hace:
#    1. Verifica que Docker Desktop este corriendo
#    2. Levanta infra compartida del Check-in (Kafka 9092 + Postgres 5432 + Kafka UI 8080)
#    3. Levanta el Postgres de Security (5434)
#    4. Levanta el Postgres de Dispatcher (5433) -- NO su Kafka
#    5. Espera a que Kafka responda
#    6. Abre 3 ventanas de PowerShell nuevas con:
#         - Check-in    (sbt run)        -> puerto 8081
#         - Security    (sbt run)        -> puerto 9000
#         - Dispatcher  (sbt run + env)  -> puerto 8084, apuntando a Kafka 9092
#    7. Hace "kickstart" de Security con un GET /health
#    8. Imprime un resumen de URLs
#
#  Ubicacion:
#    Guardar este script en la carpeta raiz que contiene las 4 carpetas:
#         Check-in/   Security/   Dispatcher/   streaming-job/
#
#  Requisitos:
#    - Docker Desktop abierto
#    - sbt instalado y en PATH
#    - Java 17+
#    - Puertos libres: 5432, 5433, 5434, 8080, 8081, 8084, 9000, 9092
#
#  Ejecucion:
#    .\levantar-todo.ps1
# ===============================================================

param(
    [string]$Root = $PSScriptRoot,
    [switch]$WithSpark
)

$ErrorActionPreference = "Stop"

$CheckinDir    = Join-Path $Root "Check-in"
$SecurityDir   = Join-Path $Root "Security"
$DispatcherDir = Join-Path $Root "Dispatcher"

# --- Higiene de entorno ----------------------------------------
# Limpiamos cualquier env var que haya quedado pegada en esta
# sesion de PowerShell (ej: de corridas anteriores o debugging manual).
# Esto evita que Check-in o Security hereden un DB_URL apuntando
# al Postgres equivocado (caso tipico: hereda el de Dispatcher :5433
# y explota con "role checkin does not exist").
$contaminantes = @(
    "DB_URL", "DB_USER", "DB_PASSWORD",
    "KAFKA_BOOTSTRAP"
)
foreach ($v in $contaminantes) {
    if (Test-Path "Env:$v") {
        Write-Host "  (limpiando env var heredada: $v)" -ForegroundColor DarkGray
        Remove-Item "Env:$v" -ErrorAction SilentlyContinue
    }
}

function Write-Step {
    param([string]$msg)
    Write-Host ""
    Write-Host ">> $msg" -ForegroundColor Cyan
}

function Test-PathOrDie {
    param([string]$path, [string]$name)
    if (-not (Test-Path $path)) {
        Write-Host "[X] No encontre la carpeta de $name en: $path" -ForegroundColor Red
        Write-Host "    Coloca este script en la carpeta que contiene Check-in/, Security/, Dispatcher/, streaming-job/" -ForegroundColor Yellow
        Write-Host "    O pasa la ruta como parametro:  .\levantar-todo.ps1 -Root C:\ruta\al\proyecto" -ForegroundColor Yellow
        exit 1
    }
}

Test-PathOrDie $CheckinDir    "Check-in"
Test-PathOrDie $SecurityDir   "Security"
Test-PathOrDie $DispatcherDir "Dispatcher"

Write-Host ""
Write-Host "============================================================" -ForegroundColor Magenta
Write-Host "  Levantando sistema distribuido de equipajes" -ForegroundColor Magenta
Write-Host "  Check-in | Security | Dispatcher | (streaming aparte)" -ForegroundColor Magenta
Write-Host "============================================================" -ForegroundColor Magenta

# --- 1. Verificar Docker ---------------------------------------
Write-Step "Paso 1/8 - Verificando Docker"
try {
    docker info --format "{{.ServerVersion}}" 2>$null | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "docker info fallo" }
    Write-Host "  [OK] Docker corriendo" -ForegroundColor Green
}
catch {
    Write-Host "  [X] Docker no esta corriendo. Abri Docker Desktop y reintenta." -ForegroundColor Red
    exit 1
}

# --- 2. Infra compartida (Kafka + Check-in Postgres + UI) ------
Write-Step "Paso 2/8 - Infra compartida (Kafka 9092, Postgres 5432, Kafka UI 8080)"
Push-Location $CheckinDir
docker compose up -d | Out-Host
Pop-Location

# --- 3. Postgres Security --------------------------------------
Write-Step "Paso 3/8 - Postgres de Security (5434)"
Push-Location $SecurityDir
docker compose up -d postgres | Out-Host
Pop-Location

# --- 4. Postgres Dispatcher ------------------------------------
Write-Step "Paso 4/8 - Postgres de Dispatcher (5433) - sin su Kafka"
Push-Location $DispatcherDir
docker compose up -d postgres | Out-Host
Pop-Location

# --- 5. Esperar a Kafka (admin, no solo TCP) -------------------
# Un simple TCP connect no es suficiente: el puerto 9092 se abre
# enseguida, pero el broker tarda varios segundos mas en registrarse
# con Zookeeper y aceptar comandos admin.
#
# IMPORTANTE: Kafka AdminClient escupe WARNs a stderr durante el
# bootstrap ("Connection to node -1 could not be established"), y con
# ErrorActionPreference = Stop PowerShell los trata como errores fatales.
# Por eso bajamos el nivel a Continue para todo el bloque de Kafka.
$prevEAP = $ErrorActionPreference
$ErrorActionPreference = "Continue"

Write-Step "Paso 5/8 - Esperando a que Kafka admin responda"
$kafkaReady = $false
for ($i = 1; $i -le 45; $i++) {
    # 2>&1 + Out-Null manda stdout y stderr al vacio para no llenar la consola
    # de WARNs mientras el broker aun no esta listo.
    docker exec checkin-kafka kafka-topics --list `
        --bootstrap-server localhost:9092 2>&1 | Out-Null
    if ($LASTEXITCODE -eq 0) {
        $kafkaReady = $true
        Write-Host "  [OK] Kafka admin responde" -ForegroundColor Green
        break
    }
    Start-Sleep -Seconds 2
}
if (-not $kafkaReady) {
    Write-Host "  [X] Kafka no respondio en 90s. Revisa 'docker ps' y 'docker logs checkin-kafka'." -ForegroundColor Red
    $ErrorActionPreference = $prevEAP
    exit 1
}

# --- 5b. Crear topics de Kafka ---------------------------------
# Los creamos idempotentemente (--if-not-exists) para que Spark pueda
# suscribirse sin fallar con UnknownTopicOrPartitionException.
Write-Step "Paso 5b/8 - Asegurando topics de Kafka"
$topics = @("registro.pasajero", "equipaje.bodega", "equipaje.despacho")
foreach ($topic in $topics) {
    docker exec checkin-kafka kafka-topics --create --if-not-exists `
        --topic $topic --bootstrap-server localhost:9092 `
        --partitions 1 --replication-factor 1 2>&1 | Out-Null
    if ($LASTEXITCODE -eq 0) {
        Write-Host "  [OK] $topic" -ForegroundColor Green
    }
    else {
        Write-Host "  [!] $topic (exit $LASTEXITCODE - posiblemente ya existia)" -ForegroundColor Yellow
    }
}

# Restauramos el ErrorActionPreference para el resto del script
$ErrorActionPreference = $prevEAP

# --- 6. Arrancar Check-in --------------------------------------
Write-Step "Paso 6/8 - Arrancando Check-in en ventana nueva (:8081)"
$checkinCmd = @"
Set-Location '$CheckinDir'
`$host.UI.RawUI.WindowTitle = 'CHECK-IN :: 8081'
# Higiene: limpiar env vars que podrian contaminar la conexion JDBC.
# Check-in usa su propio application.conf (localhost:5432/checkin_db).
Remove-Item Env:DB_URL      -ErrorAction SilentlyContinue
Remove-Item Env:DB_USER     -ErrorAction SilentlyContinue
Remove-Item Env:DB_PASSWORD -ErrorAction SilentlyContinue
Write-Host '>> Arrancando Check-in (sbt run)...' -ForegroundColor Cyan
sbt run
"@
Start-Process powershell -ArgumentList @("-NoExit", "-Command", $checkinCmd)

Write-Host "  Esperando a que Check-in responda /health (la primera compilacion tarda 1-2 min)..." -ForegroundColor Yellow
$checkinReady = $false
for ($i = 1; $i -le 150; $i++) {
    try {
        Invoke-RestMethod -Uri "http://localhost:8081/health" -TimeoutSec 2 -ErrorAction Stop | Out-Null
        $checkinReady = $true
        Write-Host "  [OK] Check-in responde en :8081" -ForegroundColor Green
        break
    }
    catch {
        Start-Sleep -Seconds 2
    }
}
if (-not $checkinReady) {
    Write-Host "  [!] Check-in aun no responde. Sigo adelante - mira la ventana de Check-in." -ForegroundColor Yellow
}

# --- 7. Arrancar Security --------------------------------------
Write-Step "Paso 7/8 - Arrancando Security en ventana nueva (:9000)"
$securityCmd = @"
Set-Location '$SecurityDir'
`$host.UI.RawUI.WindowTitle = 'SECURITY :: 9000'
# Higiene: Security tiene su propia config (localhost:5434/security_db).
Remove-Item Env:DB_URL      -ErrorAction SilentlyContinue
Remove-Item Env:DB_USER     -ErrorAction SilentlyContinue
Remove-Item Env:DB_PASSWORD -ErrorAction SilentlyContinue
Write-Host '>> Arrancando Security (sbt run)...' -ForegroundColor Cyan
sbt run
"@
Start-Process powershell -ArgumentList @("-NoExit", "-Command", $securityCmd)

Write-Host "  Esperando a Play Framework (tarda mas que Check-in)..." -ForegroundColor Yellow
$securityReady = $false
for ($i = 1; $i -le 180; $i++) {
    try {
        Invoke-RestMethod -Uri "http://localhost:9000/health" -TimeoutSec 2 -ErrorAction Stop | Out-Null
        $securityReady = $true
        Write-Host "  [OK] Security responde en :9000 (Play listo)" -ForegroundColor Green
        Write-Host "  [OK] Kickstart (GET /health) disparo la carga del consumer de Kafka" -ForegroundColor Green
        break
    }
    catch {
        Start-Sleep -Seconds 2
    }
}
if (-not $securityReady) {
    Write-Host "  [!] Security aun no responde. Cuando veas 'Listening on 9000' en su ventana, ejecuta:" -ForegroundColor Yellow
    Write-Host "      curl http://localhost:9000/health" -ForegroundColor Yellow
}

# --- 8. Arrancar Dispatcher ------------------------------------
Write-Step "Paso 8/8 - Arrancando Dispatcher en ventana nueva (:8084) apuntando a Kafka :9092"
$dispatcherCmd = @"
Set-Location '$DispatcherDir'
`$host.UI.RawUI.WindowTitle = 'DISPATCHER :: 8084'
# Higiene: arrancamos de cero y seteamos SOLO las vars que Dispatcher necesita.
# Estas vars viven unicamente dentro de esta ventana (proceso hijo),
# no contaminan la sesion padre ni a Check-in / Security.
Remove-Item Env:DB_URL      -ErrorAction SilentlyContinue
Remove-Item Env:DB_USER     -ErrorAction SilentlyContinue
Remove-Item Env:DB_PASSWORD -ErrorAction SilentlyContinue
Remove-Item Env:KAFKA_BOOTSTRAP -ErrorAction SilentlyContinue
# Solo seteamos lo imprescindible. El user/password del Postgres de
# Dispatcher viene de su application.conf (defaults correctos).
`$env:KAFKA_BOOTSTRAP = 'localhost:9092'
`$env:DB_URL          = 'jdbc:postgresql://localhost:5433/dispatcher_db'
Write-Host '>> Arrancando Dispatcher (sbt run) apuntando al Kafka compartido...' -ForegroundColor Cyan
Write-Host '   KAFKA_BOOTSTRAP = localhost:9092' -ForegroundColor DarkGray
Write-Host '   DB_URL          = jdbc:postgresql://localhost:5433/dispatcher_db' -ForegroundColor DarkGray
sbt run
"@
Start-Process powershell -ArgumentList @("-NoExit", "-Command", $dispatcherCmd)

Write-Host "  Esperando a Dispatcher..." -ForegroundColor Yellow
$dispatcherReady = $false
for ($i = 1; $i -le 150; $i++) {
    try {
        Invoke-RestMethod -Uri "http://localhost:8084/health" -TimeoutSec 2 -ErrorAction Stop | Out-Null
        $dispatcherReady = $true
        Write-Host "  [OK] Dispatcher responde en :8084" -ForegroundColor Green
        break
    }
    catch {
        Start-Sleep -Seconds 2
    }
}
if (-not $dispatcherReady) {
    Write-Host "  [!] Dispatcher aun no responde. Revisa su ventana." -ForegroundColor Yellow
}

# --- 9. (opcional) Spark Streaming -----------------------------
if ($WithSpark) {
    Write-Step "Paso 9/9 - Arrancando Spark Streaming (flag -WithSpark activo)"
    $sparkScript = Join-Path $Root "levantar-spark.ps1"
    if (Test-Path $sparkScript) {
        & $sparkScript -Root $Root
    }
    else {
        Write-Host "  [!] No encontre levantar-spark.ps1 en $Root." -ForegroundColor Yellow
        Write-Host "      Podes arrancar Spark manualmente despues con:" -ForegroundColor Yellow
        Write-Host "        cd streaming-job; .\run-streaming.ps1" -ForegroundColor Yellow
    }
}

# --- Resumen ---------------------------------------------------
Write-Host ""
Write-Host "============================================================" -ForegroundColor Green
Write-Host "                     SISTEMA ARRIBA" -ForegroundColor Green
Write-Host "============================================================" -ForegroundColor Green
Write-Host ""
Write-Host "  Servicios HTTP:" -ForegroundColor White
Write-Host "    Check-in     -> http://localhost:8081/health"
Write-Host "    Security     -> http://localhost:9000/health"
Write-Host "    Dispatcher   -> http://localhost:8084/health"
Write-Host ""
Write-Host "  Infra:" -ForegroundColor White
Write-Host "    Kafka UI     -> http://localhost:8080"
Write-Host "    Postgres     -> 5432 (Check-in) | 5433 (Dispatcher) | 5434 (Security)"
Write-Host ""
Write-Host "  Probar end-to-end:" -ForegroundColor White
Write-Host "    cd `"$CheckinDir\examples`"; .\probar-api.ps1"
Write-Host ""
Write-Host "  Ver eventos fluyendo:" -ForegroundColor White
Write-Host "    Abri http://localhost:8080 -> Topics -> mira:"
Write-Host "      registro.pasajero | equipaje.bodega | equipaje.despacho"
Write-Host ""
Write-Host "  Spark Streaming:" -ForegroundColor White
if ($WithSpark) {
    Write-Host "    Ya arrancado (flag -WithSpark) en ventana propia." -ForegroundColor DarkGray
}
else {
    Write-Host "    Arranca aparte cuando quieras con:  .\levantar-spark.ps1"
    Write-Host "    (o relanza este script con: .\levantar-todo.ps1 -WithSpark)"
}
Write-Host ""
Write-Host "  Apagar todo:" -ForegroundColor White
Write-Host "    .\bajar-todo.ps1 (y cerra con Ctrl+C las ventanas sbt)"
Write-Host ""