package streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import streaming.config.BatchConfig
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/**
 * ═══════════════════════════════════════════════════════════
 *  JOB DE SPARK BATCH — ANÁLISIS HISTÓRICO
 * ═══════════════════════════════════════════════════════════
 *
 *  Lee los datos persistidos en PostgreSQL (Dispatcher DB)
 *  y genera un reporte consolidado del comportamiento del
 *  sistema de despacho de equipajes:
 *
 *    1. Resumen general de vehículos y asignaciones
 *    2. Vehículos más utilizados
 *    3. Disponibilidad de la flota
 *    4. Detección de anomalías (vehículos sin asignaciones)
 *    5. Línea de tiempo de despachos
 */
object SparkBatchJob {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    val config = BatchConfig()

    val spark = SparkSession.builder()
      .appName("equipaje-batch-job")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    val timestamp = LocalDateTime.now()
      .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm"))

    println(
      s"""
         |══════════════════════════════════════════════════
         |  Spark Batch Job · ANÁLISIS HISTÓRICO
         |══════════════════════════════════════════════════
         |  Fecha de ejecución : $timestamp
         |  Dispatcher DB      : ${config.dispatcherDbUrl}
         |  Output             : ${config.outputPath}
         |══════════════════════════════════════════════════
         |""".stripMargin
    )

    // ─── Helper para leer tablas de PostgreSQL ────────────────
    def leerTabla(tabla: String) =
      spark.read
        .format("jdbc")
        .option("url",      config.dispatcherDbUrl)
        .option("dbtable",  tabla)
        .option("user",     config.dispatcherUser)
        .option("password", config.dispatcherPass)
        .option("driver",   "org.postgresql.Driver")
        .load()

    // ─── Cargar tablas ────────────────────────────────────────
    val vehiculos    = leerTabla("vehiculos")
    val asignaciones = leerTabla("asignaciones")

    // ─── ANÁLISIS 1: Resumen general ─────────────────────────
    println("\n" + "═" * 52)
    println("  ANÁLISIS 1 — RESUMEN GENERAL DEL SISTEMA")
    println("═" * 52)

    val totalVehiculos    = vehiculos.count()
    val totalDisponibles  = vehiculos.filter(col("disponible") === true).count()
    val totalOcupados     = totalVehiculos - totalDisponibles
    val totalAsignaciones = asignaciones.count()
    val equipajesUnicos   = asignaciones.select("equipaje_id").distinct().count()

    println(s"  Total vehículos en flota     : $totalVehiculos")
    println(s"  Vehículos disponibles        : $totalDisponibles")
    println(s"  Vehículos en uso             : $totalOcupados")
    println(s"  Total despachos realizados   : $totalAsignaciones")
    println(s"  Equipajes únicos despachados : $equipajesUnicos")

    val utilizacion = if (totalVehiculos > 0)
      (totalOcupados.toDouble / totalVehiculos * 100).formatted("%.1f")
    else "0.0"
    println(s"  Tasa de utilización flota    : $utilizacion%")

    // ─── ANÁLISIS 2: Vehículos más utilizados ────────────────
    println("\n" + "═" * 52)
    println("  ANÁLISIS 2 — VEHÍCULOS MÁS UTILIZADOS")
    println("═" * 52)

    val usoVehiculos = asignaciones
      .join(vehiculos, asignaciones("vehiculo_id") === vehiculos("id"), "left")
      .groupBy(
        asignaciones("vehiculo_id"),
        vehiculos("placa"),
        vehiculos("capacidad"),
        vehiculos("disponible")
      )
      .agg(count("*").as("veces_despachado"))
      .orderBy(desc("veces_despachado"))

    usoVehiculos.show(truncate = false)

    // ─── ANÁLISIS 3: Disponibilidad de la flota ──────────────
    println("\n" + "═" * 52)
    println("  ANÁLISIS 3 — ESTADO ACTUAL DE LA FLOTA")
    println("═" * 52)

    val estadoFlota = vehiculos
      .select(
        col("placa"),
        col("capacidad"),
        when(col("disponible") === true, "DISPONIBLE")
          .otherwise("EN USO").as("estado")
      )
      .orderBy("estado", "placa")

    estadoFlota.show(truncate = false)

    // ─── ANÁLISIS 4: Línea de tiempo de despachos ─────────────
    println("\n" + "═" * 52)
    println("  ANÁLISIS 4 — LÍNEA DE TIEMPO DE DESPACHOS")
    println("═" * 52)

    val timeline = asignaciones
      .join(vehiculos, asignaciones("vehiculo_id") === vehiculos("id"), "left")
      .select(
        asignaciones("timestamp"),
        asignaciones("equipaje_id"),
        vehiculos("placa"),
        vehiculos("capacidad")
      )
      .orderBy(asc("timestamp"))

    timeline.show(20, truncate = false)

    // ─── ANÁLISIS 5: Anomalías — vehículos sin asignaciones ──
    println("\n" + "═" * 52)
    println("  ANÁLISIS 5 — ANOMALÍAS DETECTADAS")
    println("═" * 52)

    val vehiculosSinUso = vehiculos
      .join(asignaciones, vehiculos("id") === asignaciones("vehiculo_id"), "left_anti")
      .select(
        col("id").as("vehiculo_id"),
        col("placa"),
        col("capacidad"),
        col("disponible")
      )

    val totalSinUso = vehiculosSinUso.count()

    if (totalSinUso > 0) {
      println(s"  ⚠ Vehículos nunca utilizados: $totalSinUso")
      vehiculosSinUso.show(truncate = false)
    } else {
      println("  ✓ Todos los vehículos han sido utilizados al menos una vez.")
    }

    // ─── Guardar reportes en CSV ──────────────────────────────
    println("\n" + "═" * 52)
    println("  GUARDANDO REPORTES EN CSV...")
    println("═" * 52)

    val fechaArchivo = LocalDateTime.now()
      .format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmm"))

    usoVehiculos.coalesce(1)
      .write.mode("overwrite")
      .option("header", "true")
      .csv(s"${config.outputPath}/uso_vehiculos_$fechaArchivo")

    estadoFlota.coalesce(1)
      .write.mode("overwrite")
      .option("header", "true")
      .csv(s"${config.outputPath}/estado_flota_$fechaArchivo")

    timeline.coalesce(1)
      .write.mode("overwrite")
      .option("header", "true")
      .csv(s"${config.outputPath}/timeline_despachos_$fechaArchivo")

    if (totalSinUso > 0)
      vehiculosSinUso.coalesce(1)
        .write.mode("overwrite")
        .option("header", "true")
        .csv(s"${config.outputPath}/anomalias_$fechaArchivo")

    println(s"  ✓ Reportes guardados en: ${config.outputPath}")
    println("\n" + "═" * 52)
    println("  BATCH COMPLETADO EXITOSAMENTE")
    println("═" * 52 + "\n")

    spark.stop()
  }
}