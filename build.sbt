name         := "streaming-job"
version      := "0.1.0"
scalaVersion := "2.12.18"

val sparkVersion = "3.5.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core"           % sparkVersion,
  "org.apache.spark" %% "spark-sql"            % sparkVersion,
  "org.apache.spark" %% "spark-streaming"      % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "org.postgresql"   %  "postgresql"           % "42.7.1"
)

libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.7.36" % Runtime

// Opciones de JVM necesarias para Java 17 + Spark
javaOptions ++= Seq(
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
  "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
  "-Djava.security.manager=allow"
)

fork := true

// Aliases para arrancar sin lidiar con el quoting de sbt en Windows.
// Con estos podemos hacer:
//    sbt runStreaming
//    sbt runBatch
// sin comillas ni ';' - son una sola palabra, PowerShell/cmd no los rompe.
addCommandAlias("runStreaming", "runMain streaming.SparkStreamingJob")
addCommandAlias("runBatch",     "runMain streaming.SparkBatchJob")