# streaming-job

Para ejecutar todos los microservicios es necesario dejar en la carpeta raíz los documentos asociados:

1. .\bajar-todo.ps1 -Clean

2. .\levantar-todo.ps1

En cada una de las raíces de los microservicios ejecutar
sbt run

Para probar spark (puerto 4040):

3. .\levantar-spark.ps1    

Aquí es necesario tener una carpeta en la raíz del equipo llamada tmp/spark-batch-otput

.\levantar-spark.ps1 -Mode batch   

En ambos pedirá:

runMain streaming.SparkStreamingJob
