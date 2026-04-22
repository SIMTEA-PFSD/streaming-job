package streaming.config

case class BatchConfig(
  // Base de datos del Check-in
  checkinDbUrl:  String = "jdbc:postgresql://localhost:5432/checkin_db",
  checkinUser:   String = "checkin",
  checkinPass:   String = "checkin123",

  // Base de datos del Dispatcher
  dispatcherDbUrl:  String = "jdbc:postgresql://localhost:5433/dispatcher_db",
  dispatcherUser:   String = "dispatcher",
  dispatcherPass:   String = "dispatcher123",

  // Dónde guardar el reporte generado
  outputPath: String = "C:/tmp/spark-batch-output"
)