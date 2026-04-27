# streaming-job
 Para ejecutar todos los microservicios:

.\bajar-todo.ps1 -Clean

.\levantar-todo.ps1

En cada una de las raíces de los microservicios ejecutar
sbt run

Para probar spark (puerto 4040):

.\levantar-spark.ps1    

Aquí es necesario tener una carpeta en la raíz del equipo llamada tmp/spark-batch-otput

.\levantar-spark.ps1 -Mode batch   

En ambos pedirá:

runMain streaming.SparkStreamingJob
