package streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import streaming.config.StreamingConfig

/**
 * ═══════════════════════════════════════════════════════════
 *  JOB DE SPARK STRUCTURED STREAMING
 * ═══════════════════════════════════════════════════════════
 *
 *  Lee eventos de Kafka en tiempo real y calcula:
 *    1. Conteo de eventos por tipo (cada 30 seg)
 *    2. Conteo de equipajes por tópico
 *    3. Detección de anomalías: equipajes sin despachar
 *       después de haber pasado seguridad
 */
object SparkStreamingJob {

  def main(args: Array[String]): Unit = {

    val config = StreamingConfig()
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    System.setProperty("HADOOP_HOME", "C:\\hadoop")

    // ─── Sesión Spark ────────────────────────────────────────
    val spark = SparkSession.builder()
      .appName("equipaje-streaming-job")
      .master("local[*]")   // corre localmente con todos los cores
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")  // silencia logs innecesarios

    import spark.implicits._

    println("""
      |══════════════════════════════════════════════════
      |  Spark Streaming Job · ARRIBA
      |══════════════════════════════════════════════════
      |  Kafka  → """ + config.kafkaBootstrap + """
      |  Tópicos → """ + config.topicos.mkString(", ") + """
      |  Intervalo → """ + config.intervalSegundos + """ segundos
      |══════════════════════════════════════════════════
      |""".stripMargin)

    // ─── Leer de Kafka ────────────────────────────────────────
    val kafkaStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", config.kafkaBootstrap)
      .option("subscribe", config.topicos.mkString(","))
      .option("startingOffsets", "latest")
      .option("failOnDataLoss", "false")
      .load()

    // ─── Parsear el JSON de cada mensaje ─────────────────────
    // Kafka entrega: key (bytes), value (bytes), topic, timestamp, etc.
    // Convertimos value a String y extraemos campos del JSON
    val esquemaEvento = new org.apache.spark.sql.types.StructType()
      .add("eventId",    "string")
      .add("tipo",       "string")
      .add("equipajeId", "string")
      .add("timestamp",  "string")
      .add("topico",     "string")

    val eventos = kafkaStream
      .select(
        col("topic").as("topico_kafka"),
        col("timestamp").as("ts_kafka"),
        from_json(col("value").cast("string"), esquemaEvento).as("evento")
      )
      .select(
        col("topico_kafka"),
        col("ts_kafka"),
        col("evento.eventId").as("eventId"),
        col("evento.tipo").as("tipo"),
        col("evento.equipajeId").as("equipajeId"),
        col("evento.timestamp").as("timestamp_evento")
      )

    // ─── QUERY 1: Conteo de eventos por tipo ─────────────────
    // Ventana de 30 segundos — cuántos eventos de cada tipo llegan
    val conteoPorTipo = eventos
      .withWatermark("ts_kafka", "1 minute")
      .groupBy(
        window(col("ts_kafka"), s"${config.intervalSegundos} seconds"),
        col("tipo")
      )
      .count()
      .writeStream
      .outputMode(OutputMode.Append())
      .format("console")
      .option("truncate", "false")
      .option("numRows", "20")
      .trigger(Trigger.ProcessingTime(s"${config.intervalSegundos} seconds"))
      .queryName("conteo_por_tipo")
      .start()

    // ─── QUERY 2: Conteo por tópico Kafka ────────────────────
    val conteoPorTopico = eventos
      .withWatermark("ts_kafka", "1 minute")
      .groupBy(
        window(col("ts_kafka"), s"${config.intervalSegundos} seconds"),
        col("topico_kafka")
      )
      .count()
      .writeStream
      .outputMode(OutputMode.Append())
      .format("console")
      .option("truncate", "false")
      .trigger(Trigger.ProcessingTime(s"${config.intervalSegundos} seconds"))
      .queryName("conteo_por_topico")
      .start()

    // ─── QUERY 3: Detección de anomalías ─────────────────────
    // Equipajes que aparecen en bodega pero NO en despacho
    // (lleva más de 1 ventana sin ser despachado)
    val enBodega = eventos
      .filter(col("topico_kafka") === "equipaje.bodega")
      .select(col("equipajeId").as("id_bodega"), col("ts_kafka").as("ts_bodega"))

    val despachados = eventos
      .filter(col("topico_kafka") === "equipaje.despacho")
      .select(col("equipajeId").as("id_despacho"))

    // Reporte de equipajes que llegaron a bodega
    // (en producción haría join con despachados para detectar los sin despachar)
    val reporteBodega = enBodega
      .writeStream
      .outputMode(OutputMode.Append())
      .format("console")
      .option("truncate", "false")
      .trigger(Trigger.ProcessingTime(s"${config.intervalSegundos} seconds"))
      .queryName("equipajes_en_bodega")
      .start()

    // ─── Mantener el job vivo ─────────────────────────────────
    spark.streams.awaitAnyTermination()
  }
}