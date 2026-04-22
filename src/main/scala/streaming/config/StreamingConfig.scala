package streaming.config

case class StreamingConfig(
  kafkaBootstrap: String  = "localhost:9093",
  topicos: List[String]   = List(
    "registro.pasajero",
    "equipaje.bodega",
    "equipaje.despacho"
  ),
  intervalSegundos: Int   = 30
)