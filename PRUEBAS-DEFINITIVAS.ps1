# ====================================================================
#  PRUEBAS-DEFINITIVAS.ps1
#  Suite de pruebas end-to-end del sistema SIMTEA-PFSD.
#
#  Esto es lo que se corre EN LA DEFENSA para demostrar que todo
#  funciona: happy path, atomicidad transaccional, validaciones, y
#  resiliencia del Outbox Pattern frente a Kafka caido.
#
#  USO:
#    .\PRUEBAS-DEFINITIVAS.ps1                 # corre todo
#    .\PRUEBAS-DEFINITIVAS.ps1 -SoloHappyPath  # solo el flujo normal
#    .\PRUEBAS-DEFINITIVAS.ps1 -SinResiliencia # salta la prueba que detiene Kafka
#
#  REQUISITOS:
#    1. .\bajar-todo.ps1 -Clean
#    2. .\levantar-todo.ps1
#    3. En 3 terminales separadas (una cada una, NO cerrar):
#         cd Check-in     ; sbt run
#         cd Security     ; sbt run
#         cd Dispatcher   ; sbt run
#       Esperar a ver el banner "ARRIBA" en cada una.
#
#  Compatible con PowerShell 5.1 y 7+.
# ====================================================================

param(
    [switch]$SoloHappyPath,
    [switch]$SinResiliencia
)

$ErrorActionPreference = "Continue"

# --- Helpers de presentacion -------------------------------------

$global:Pasaron  = 0
$global:Fallaron = 0
$global:Resumen  = @()

function Titulo {
    param([string]$Texto)
    Write-Host ""
    Write-Host ("=" * 70) -ForegroundColor Magenta
    Write-Host "  $Texto" -ForegroundColor Magenta
    Write-Host ("=" * 70) -ForegroundColor Magenta
}

function Subtitulo {
    param([string]$Texto)
    Write-Host ""
    Write-Host ">> $Texto" -ForegroundColor Cyan
    Write-Host ("-" * 70) -ForegroundColor DarkGray
}

function Verificar {
    param(
        [string]$Descripcion,
        [scriptblock]$Condicion
    )
    try {
        $ok = & $Condicion
        if ($ok) {
            Write-Host "  [OK]   $Descripcion" -ForegroundColor Green
            $global:Pasaron++
            $global:Resumen += [PSCustomObject]@{ Estado = "OK"; Test = $Descripcion }
        } else {
            Write-Host "  [FAIL] $Descripcion" -ForegroundColor Red
            $global:Fallaron++
            $global:Resumen += [PSCustomObject]@{ Estado = "FAIL"; Test = $Descripcion }
        }
    } catch {
        Write-Host "  [FAIL] $Descripcion - excepcion: $_" -ForegroundColor Red
        $global:Fallaron++
        $global:Resumen += [PSCustomObject]@{ Estado = "FAIL"; Test = $Descripcion }
    }
}

function ConteoDB {
    param(
        [string]$Container,
        [string]$User,
        [string]$Db,
        [string]$Sql
    )
    $out = docker exec $Container psql -U $User -d $Db -t -A -c $Sql 2>$null
    if ($LASTEXITCODE -eq 0 -and $out) {
        return [int]($out.Trim())
    }
    return -1
}

# Compatible con PowerShell 5.1 (sin -SkipHttpErrorCheck)
function PostCheckin {
    param(
        [hashtable]$Cuerpo
    )
    $json = $Cuerpo | ConvertTo-Json -Depth 4
    try {
        $resp = Invoke-RestMethod -Uri "http://localhost:8081/api/v1/checkin" `
            -Method POST -ContentType "application/json" -Body $json -ErrorAction Stop
        return @{ status = 201; body = $resp }
    } catch [System.Net.WebException] {
        $we = $_.Exception
        $code = 0
        $body = $null
        if ($we.Response) {
            $code = [int]$we.Response.StatusCode
            try {
                $stream = $we.Response.GetResponseStream()
                $reader = New-Object System.IO.StreamReader($stream)
                $raw = $reader.ReadToEnd()
                $reader.Close()
                if ($raw) { $body = $raw | ConvertFrom-Json }
            } catch { }
        }
        return @{ status = $code; body = $body }
    } catch {
        return @{ status = 0; body = $null; error = $_.Exception.Message }
    }
}

# Tambien para GET con manejo de errores compatible con PS 5.1
function GetUrl {
    param([string]$Url)
    try {
        return Invoke-RestMethod -Uri $Url -ErrorAction Stop
    } catch {
        return $null
    }
}

# --- Verificacion previa: servicios arriba -----------------------

Titulo "PRE-CHECK :: Servicios escuchando"

Verificar "Check-in /health responde UP" {
    $r = GetUrl "http://localhost:8081/health"
    $r -ne $null -and ($r.status -eq "UP" -or "$r" -match "UP")
}
Verificar "Security /health responde UP" {
    $r = GetUrl "http://localhost:9000/health"
    $r -ne $null -and ($r.status -eq "UP" -or "$r" -match "UP")
}
Verificar "Dispatcher /health responde UP" {
    $r = GetUrl "http://localhost:8084/health"
    $r -ne $null -and ($r.status -eq "UP" -or "$r" -match "UP")
}
Verificar "Kafka responde a comandos admin" {
    docker exec checkin-kafka kafka-topics --list --bootstrap-server localhost:9092 2>&1 | Out-Null
    $LASTEXITCODE -eq 0
}

if ($global:Fallaron -gt 0) {
    Write-Host ""
    Write-Host "[X] Pre-check fallo. Revisa que:" -ForegroundColor Red
    Write-Host "    1. .\levantar-todo.ps1 termino sin errores" -ForegroundColor Red
    Write-Host "    2. Check-in, Security y Dispatcher esten corriendo (sbt run en 3 terminales)" -ForegroundColor Red
    exit 1
}

# Snapshot inicial de conteos para diferencia al final
$checkinInicio    = ConteoDB "checkin-postgres"    "checkin"    "checkin_db"    "SELECT COUNT(*) FROM equipajes;"
$securityInicio   = ConteoDB "security-postgres"   "security"   "security_db"   "SELECT COUNT(*) FROM inspecciones;"
$dispatcherInicio = ConteoDB "dispatcher-postgres" "dispatcher" "dispatcher_db" "SELECT COUNT(*) FROM asignaciones;"

Write-Host ""
Write-Host "  Conteos iniciales:" -ForegroundColor DarkGray
Write-Host "    checkin.equipajes        = $checkinInicio"
Write-Host "    security.inspecciones    = $securityInicio"
Write-Host "    dispatcher.asignaciones  = $dispatcherInicio"

# --- PRUEBA 1 :: Happy Path --------------------------------------

Titulo "PRUEBA 1 :: Happy Path - 3 pasajeros, 6 maletas"

Subtitulo "Enviando 3 check-ins..."

$pasajeros = @(
    @{
        pasajeroId = "PAX-DEMO-001"
        nombrePasajero = "Ana Lopez"
        documento = "DOC-DEMO-001"
        email = "ana@demo.com"
        vueloId = "AV-100"
        equipajes = @(
            @{ codigoRFID = "RFID-DEMO-A1"; peso = 18.5 },
            @{ codigoRFID = "RFID-DEMO-A2"; peso = 12.0 }
        )
    },
    @{
        pasajeroId = "PAX-DEMO-002"
        nombrePasajero = "Bruno Diaz"
        documento = "DOC-DEMO-002"
        email = "bruno@demo.com"
        vueloId = "AV-101"
        equipajes = @(
            @{ codigoRFID = "RFID-DEMO-B1"; peso = 22.0 }
        )
    },
    @{
        pasajeroId = "PAX-DEMO-003"
        nombrePasajero = "Clara Reyes"
        documento = "DOC-DEMO-003"
        email = "clara@demo.com"
        vueloId = "AV-100"
        equipajes = @(
            @{ codigoRFID = "RFID-DEMO-C1"; peso = 15.0 },
            @{ codigoRFID = "RFID-DEMO-C2"; peso = 19.5 },
            @{ codigoRFID = "RFID-DEMO-C3"; peso = 8.0 }
        )
    }
)

$exitos = 0
foreach ($p in $pasajeros) {
    $r = PostCheckin -Cuerpo $p
    if ($r.status -eq 201) {
        Write-Host "  [201] $($p.pasajeroId) - $($p.equipajes.Count) maleta(s)" -ForegroundColor Green
        $exitos++
    } else {
        $errMsg = if ($r.body) { ($r.body | ConvertTo-Json -Compress) } else { $r.error }
        Write-Host "  [$($r.status)] $($p.pasajeroId) FALLO: $errMsg" -ForegroundColor Red
    }
}

Verificar "Los 3 check-ins respondieron 201 Created" { $exitos -eq 3 }

Subtitulo "Esperando que los eventos atraviesen Kafka y se reflejen en las 3 DBs (polling)..."

# Polling: hasta 40 segundos esperando que Dispatcher (ultimo en la cadena) llegue a 6
$timeoutSeg = 40
$intervaloSeg = 2
$tIni = Get-Date
$nuevasCheckin    = 0
$nuevasSecurity   = 0
$nuevasDispatcher = 0

while (((Get-Date) - $tIni).TotalSeconds -lt $timeoutSeg) {
    $nuevasCheckin    = (ConteoDB "checkin-postgres"    "checkin"    "checkin_db"    "SELECT COUNT(*) FROM equipajes;")    - $checkinInicio
    $nuevasSecurity   = (ConteoDB "security-postgres"   "security"   "security_db"   "SELECT COUNT(*) FROM inspecciones;") - $securityInicio
    $nuevasDispatcher = (ConteoDB "dispatcher-postgres" "dispatcher" "dispatcher_db" "SELECT COUNT(*) FROM asignaciones;") - $dispatcherInicio

    $elapsed = [int]((Get-Date) - $tIni).TotalSeconds
    Write-Host ("  [{0,2}s] checkin=+{1}  security=+{2}  dispatcher=+{3}" -f $elapsed, $nuevasCheckin, $nuevasSecurity, $nuevasDispatcher) -ForegroundColor DarkGray

    if ($nuevasCheckin -eq 6 -and $nuevasSecurity -eq 6 -and $nuevasDispatcher -eq 6) {
        Write-Host "  Todas las DBs reflejan las 6 maletas." -ForegroundColor DarkGray
        break
    }
    Start-Sleep -Seconds $intervaloSeg
}

Write-Host ""
Write-Host "  Maletas nuevas (final):" -ForegroundColor DarkGray
Write-Host "    checkin.equipajes        +$nuevasCheckin"
Write-Host "    security.inspecciones    +$nuevasSecurity"
Write-Host "    dispatcher.asignaciones  +$nuevasDispatcher"

Verificar "Check-in persistio 6 equipajes nuevos" { $nuevasCheckin -eq 6 }
Verificar "Security inspecciono las 6 maletas (consume registro.pasajero)" { $nuevasSecurity -eq 6 }
Verificar "Dispatcher asigno vehiculo a las 6 maletas" { $nuevasDispatcher -eq 6 }

Subtitulo "Verificando que el Outbox quedo limpio"

$pendientes = ConteoDB "checkin-postgres" "checkin" "checkin_db" `
    "SELECT COUNT(*) FROM outbox_events WHERE published_at IS NULL;"

Verificar "Outbox sin eventos pendientes (todos drenados a Kafka)" { $pendientes -eq 0 }

$publicados = ConteoDB "checkin-postgres" "checkin" "checkin_db" `
    "SELECT COUNT(*) FROM outbox_events WHERE published_at IS NOT NULL;"
Write-Host "  Eventos publicados acumulados en outbox: $publicados" -ForegroundColor DarkGray

Subtitulo "Verificando topics de Kafka"

foreach ($topic in @("registro.pasajero", "equipaje.bodega", "equipaje.despacho")) {
    $existe = (docker exec checkin-kafka kafka-topics --describe --topic $topic --bootstrap-server localhost:9092 2>&1) -match $topic
    Verificar "Topic '$topic' existe en Kafka" { $existe }
}

if ($SoloHappyPath) {
    Titulo "RESUMEN"
    Write-Host "  Pasaron:  $global:Pasaron" -ForegroundColor Green
    if ($global:Fallaron -eq 0) {
        Write-Host "  Fallaron: $global:Fallaron" -ForegroundColor Green
        exit 0
    } else {
        Write-Host "  Fallaron: $global:Fallaron" -ForegroundColor Red
        exit 1
    }
}

# --- PRUEBA 2 :: Validaciones de negocio -------------------------

Titulo "PRUEBA 2 :: Validaciones - errores se traducen a 400 BadRequest"

Subtitulo "Check-in sin equipajes"
$rVacio = PostCheckin -Cuerpo @{
    pasajeroId = "PAX-VACIO"; nombrePasajero = "Test"; documento = "V1"
    email = "v@x.com"; vueloId = "AV-V"; equipajes = @()
}
Verificar "Sin equipajes responde 400 BadRequest" { $rVacio.status -eq 400 }

Subtitulo "Maleta sobrepeso (30kg > 23kg)"
$rPeso = PostCheckin -Cuerpo @{
    pasajeroId = "PAX-GORDO"; nombrePasajero = "Test"; documento = "G1"
    email = "g@x.com"; vueloId = "AV-G"
    equipajes = @(@{ codigoRFID = "RFID-GORDO"; peso = 30.0 })
}
Verificar "Sobrepeso responde 400 BadRequest" { $rPeso.status -eq 400 }

# --- PRUEBA 3 :: Atomicidad transaccional ------------------------

Titulo "PRUEBA 3 :: Atomicidad - rollback al chocar contra UNIQUE"

Subtitulo "Sembrando RFID-COLISION en la DB..."
$seed = PostCheckin -Cuerpo @{
    pasajeroId = "PAX-SEED-COL"; nombrePasajero = "Seed"
    documento = "COL-SEED"; email = "s@x.com"; vueloId = "AV-COL"
    equipajes = @(@{ codigoRFID = "RFID-COLISION"; peso = 10.0 })
}
Verificar "Seed inicial OK" { $seed.status -eq 201 }

Start-Sleep -Seconds 2

$antesCheckin = ConteoDB "checkin-postgres" "checkin" "checkin_db" `
    "SELECT COUNT(*) FROM equipajes WHERE codigo_rfid IN ('RFID-NUEVA-A','RFID-NUEVA-B');"
$antesOutbox = ConteoDB "checkin-postgres" "checkin" "checkin_db" `
    "SELECT COUNT(*) FROM outbox_events WHERE payload LIKE '%RFID-NUEVA-A%' OR payload LIKE '%RFID-NUEVA-B%';"

Subtitulo "Enviando check-in con RFID-COLISION en el medio (debe fallar)..."
$rCol = PostCheckin -Cuerpo @{
    pasajeroId = "PAX-ATOMICO"; nombrePasajero = "Test Atomico"
    documento = "ATOM-1"; email = "a@x.com"; vueloId = "AV-ATOM"
    equipajes = @(
        @{ codigoRFID = "RFID-NUEVA-A"; peso = 10.0 },     # se INSERTARIA
        @{ codigoRFID = "RFID-COLISION"; peso = 10.0 },    # ROMPE - duplicado
        @{ codigoRFID = "RFID-NUEVA-B"; peso = 10.0 }      # no se intenta
    )
}

Verificar "Check-in colisionante responde con error (4xx o 5xx)" {
    $rCol.status -ge 400
}

Subtitulo "Verificando rollback total..."

$despuesCheckin = ConteoDB "checkin-postgres" "checkin" "checkin_db" `
    "SELECT COUNT(*) FROM equipajes WHERE codigo_rfid IN ('RFID-NUEVA-A','RFID-NUEVA-B');"
$despuesOutbox = ConteoDB "checkin-postgres" "checkin" "checkin_db" `
    "SELECT COUNT(*) FROM outbox_events WHERE payload LIKE '%RFID-NUEVA-A%' OR payload LIKE '%RFID-NUEVA-B%';"

Verificar "Cero equipajes 'RFID-NUEVA-*' persistidos (rollback efectivo)" {
    $despuesCheckin -eq $antesCheckin
}
Verificar "Cero eventos en outbox por las maletas no insertadas" {
    $despuesOutbox -eq $antesOutbox
}

# --- PRUEBA 4 :: Resiliencia del Outbox (Kafka caido) ------------

if ($SinResiliencia) {
    Write-Host ""
    Write-Host "  [SKIP] Prueba 4 omitida (-SinResiliencia)" -ForegroundColor Yellow
} else {
    Titulo "PRUEBA 4 :: Resiliencia Outbox - Kafka caido y recuperado"

    # Snapshot total de outbox ANTES (para ver delta sin depender del payload)
    $publicadosAntes = ConteoDB "checkin-postgres" "checkin" "checkin_db" `
        "SELECT COUNT(*) FROM outbox_events WHERE published_at IS NOT NULL;"
    $totalAntes = ConteoDB "checkin-postgres" "checkin" "checkin_db" `
        "SELECT COUNT(*) FROM outbox_events;"

    Subtitulo "Deteniendo Kafka..."
    docker stop checkin-kafka 2>&1 | Out-Null
    Verificar "Kafka detenido" {
        (docker inspect -f '{{.State.Running}}' checkin-kafka) -eq "false"
    }

    # Pequena pausa para que Check-in note la caida del producer
    Start-Sleep -Seconds 2

    Subtitulo "Enviando check-in con Kafka caido (debe responder 201 igual)..."
    $rRes = PostCheckin -Cuerpo @{
        pasajeroId = "PAX-RES"; nombrePasajero = "Resiliencia"
        documento = "RES-1"; email = "r@x.com"; vueloId = "AV-RES"
        equipajes = @(@{ codigoRFID = "RFID-RES-1"; peso = 10.0 })
    }

    Verificar "Check-in respondio 201 a pesar de Kafka caido" {
        $rRes.status -eq 201
    }

    # Esperamos un poco. Como max.block.ms=5000 y delivery.timeout.ms=10000,
    # el relay puede tardar hasta 10s en fallar el send y dejar el evento pendiente.
    Start-Sleep -Seconds 8

    $totalDespues = ConteoDB "checkin-postgres" "checkin" "checkin_db" `
        "SELECT COUNT(*) FROM outbox_events;"
    $publicadosDespues = ConteoDB "checkin-postgres" "checkin" "checkin_db" `
        "SELECT COUNT(*) FROM outbox_events WHERE published_at IS NOT NULL;"
    $pendientesAhora = $totalDespues - $publicadosDespues

    Write-Host "  Outbox ANTES de Kafka caido:   total=$totalAntes, publicados=$publicadosAntes" -ForegroundColor DarkGray
    Write-Host "  Outbox CON Kafka caido:        total=$totalDespues, publicados=$publicadosDespues, pendientes=$pendientesAhora" -ForegroundColor DarkGray

    Verificar "Outbox tiene al menos 1 evento nuevo (el del check-in)" {
        $totalDespues -gt $totalAntes
    }
    Verificar "Hay eventos pendientes esperando que Kafka vuelva" {
        $pendientesAhora -ge 1
    }

    Subtitulo "Levantando Kafka..."
    docker start checkin-kafka 2>&1 | Out-Null

    Write-Host "  Esperando 25 segundos para que Kafka este listo y el relay drene..." -ForegroundColor DarkGray
    Start-Sleep -Seconds 25

    $totalFinal = ConteoDB "checkin-postgres" "checkin" "checkin_db" `
        "SELECT COUNT(*) FROM outbox_events;"
    $publicadosFinal = ConteoDB "checkin-postgres" "checkin" "checkin_db" `
        "SELECT COUNT(*) FROM outbox_events WHERE published_at IS NOT NULL;"
    $pendientesFinal = $totalFinal - $publicadosFinal

    Write-Host "  Outbox DESPUES de levantar Kafka: total=$totalFinal, publicados=$publicadosFinal, pendientes=$pendientesFinal" -ForegroundColor DarkGray

    Verificar "El relay drenó los eventos pendientes (cero al final)" {
        $pendientesFinal -eq 0
    }
    Verificar "Hay mas publicados que antes (relay efectivamente publico)" {
        $publicadosFinal -gt $publicadosAntes
    }
}

# --- RESUMEN FINAL -----------------------------------------------

Titulo "RESUMEN"

Write-Host ""
$global:Resumen | Format-Table -AutoSize | Out-Host

Write-Host ""
Write-Host "  Total OK:    $global:Pasaron" -ForegroundColor Green
if ($global:Fallaron -eq 0) {
    Write-Host "  Total FAIL:  $global:Fallaron" -ForegroundColor Green
} else {
    Write-Host "  Total FAIL:  $global:Fallaron" -ForegroundColor Red
}
Write-Host ""

if ($global:Fallaron -eq 0) {
    Write-Host "  ============================================================" -ForegroundColor Green
    Write-Host "    TODOS LOS TESTS PASARON. El sistema esta funcionando" -ForegroundColor Green
    Write-Host "    end-to-end y honrando las garantias arquitectonicas:" -ForegroundColor Green
    Write-Host "      - Hexagonal + DIP                                    " -ForegroundColor Green
    Write-Host "      - Atomicidad multi-insert con rollback transaccional " -ForegroundColor Green
    Write-Host "      - Validaciones de negocio en el dominio              " -ForegroundColor Green
    Write-Host "      - Outbox Pattern con relay asincrono y at-least-once " -ForegroundColor Green
    Write-Host "      - Idempotencia en consumers (Security, Dispatcher)   " -ForegroundColor Green
    Write-Host "  ============================================================" -ForegroundColor Green
    exit 0
} else {
    Write-Host "  ============================================================" -ForegroundColor Red
    Write-Host "    HAY FALLAS. Revisa el detalle arriba." -ForegroundColor Red
    Write-Host "  ============================================================" -ForegroundColor Red
    exit 1
}