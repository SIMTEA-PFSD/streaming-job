$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-17.0.18.8-hotspot"
$env:PATH = "C:\Program Files\Eclipse Adoptium\jdk-17.0.18.8-hotspot\bin;" + $env:PATH
$env:HADOOP_HOME = "C:\hadoop"
$env:PATH = "C:\hadoop\bin;" + $env:PATH
sbt "runMain streaming.SparkBatchJob"