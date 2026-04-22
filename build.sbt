name         := "streaming-job"
version      := "0.1.0"
scalaVersion := "2.12.18"  // OJO: Spark necesita Scala 2.12, no 2.13

val sparkVersion = "3.5.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core"           % sparkVersion,
  "org.apache.spark" %% "spark-sql"            % sparkVersion,
  "org.apache.spark" %% "spark-streaming"      % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion
)

// Spark trae su propio logger — evita conflictos
libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.7.36" % Runtime

fork := true

javaOptions ++= Seq(
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "-Djava.security.manager=allow"
)

fork := true