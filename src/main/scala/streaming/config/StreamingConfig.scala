package streaming.config

case class StreamingConfig(
  kafkaBootstrap: String  = sys.env.getOrElse("KAFKA_BOOTSTRAP", "localhost:9092"),
  topicos: List[String]   = List(
    "registro.pasajero",
    "equipaje.bodega",
    "equipaje.despacho"
  ),
  intervalSegundos: Int   = 30
)