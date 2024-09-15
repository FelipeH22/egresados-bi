"""
ETL File

Extraction from the local MySQL database
Transformation of the data including tables' joins and aggregations
Load into BigQuery with a temporary GCS file.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType
from pyspark.sql.window import Window

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MySQL to BigQuery ETL") \
    .config("spark.driver.extraClassPath", "./mysql-connector-j-8.3.0.jar;./spark-3.5-bigquery-0.37.0.jar;./gcs-connector-hadoop3-latest.jar") \
    .config("credentialsFile", "./skilful-alpha-417701-27178c0388d2.json") \
    .config("spark.jars", "./gcs-connector-hadoop3-latest.jar") \
    .getOrCreate()

spark._jsc.hadoopConfiguration().set('fs.gs.auth.service.account.enable', 'true')
spark._jsc.hadoopConfiguration().set('google.cloud.auth.service.account.json.keyfile', "./skilful-alpha-417701-27178c0388d2.json")

mysql_url = os.environ.get("mysqlurl")

bigquery_project_id = os.environ.get("projectid")
bigquery_dataset_id = os.environ.get("dataset")

# dim_egresado
egresado = spark.read.jdbc(url=mysql_url, table="egresado").orderBy("id_egresado")
idioma_egresado = spark.read.jdbc(url=mysql_url, table="idioma_egresado")
idioma_egresado = idioma_egresado.groupBy("id_egresado").agg(F.count("id_idioma").alias("idiomas")).orderBy("id_egresado")
idioma_egresado = egresado.join(idioma_egresado, ["id_egresado"], how="left")
hijos = spark.read.jdbc(url=mysql_url, table="hijo").groupBy("id_egresado")\
                                                    .agg(F.count("id_hijo").alias("hijos"))\
                                                    .orderBy("id_egresado")
hijos = idioma_egresado.join(hijos, ["id_egresado"], how="left").fillna(0).orderBy("id_egresado")
lugar_nacimiento = spark.read.jdbc(url=mysql_url, table="lugar_nacimiento")\
                        .select("id_lugar_nacimiento", F.concat_ws(", ", "nombre_ciudad", "nombre_pais")
                                .alias("lugar_nacimiento"))
egresados = hijos.join(lugar_nacimiento, ["id_lugar_nacimiento"], how="left")
egresados = egresados.select(egresados.id_egresado.cast(LongType()), egresados.nombre_egresado.alias("nombre"),
                             egresados.apellido_egresado.alias("apellido"),
                             "genero", "lugar_nacimiento", egresados.hijos, egresados.idiomas,
                             "fecha_nacimiento").orderBy(egresados.id_egresado)
egresados.write\
    .format("bigquery")\
    .option("parentProject", bigquery_project_id)\
    .option("project", bigquery_project_id)\
    .option("dataset", bigquery_dataset_id)\
    .option("temporaryGcsBucket", "buckp")\
    .option("table", "egresado")\
    .option("enableModeCheckForSchemaFields", "false")\
    .mode("overwrite")\
    .save()

#dim_fechas
actividad_laboral = spark.read.jdbc(url=mysql_url, table="actividad_laboral")
grado = spark.read.jdbc(url=mysql_url, table="grado")
publicacion = spark.read.jdbc(url=mysql_url, table="publicacion")
profesor = spark.read.jdbc(url=mysql_url, table="profesor")
actividad_laboral_fechas = actividad_laboral.select(actividad_laboral.fecha_inicio.alias("fecha"))
grado_fechas = grado.select(grado.fecha)
publicacion_fechas = publicacion.select(publicacion.fecha_publicacion.alias("fecha"))
egresado_fechas = egresado.select(egresado.fecha_nacimiento.alias("fecha"))
profesor_fechas = profesor.select(profesor.fecha_nacimiento.alias("fecha"))
fechas = (actividad_laboral_fechas.union(grado_fechas).union(publicacion_fechas).union(grado_fechas)
          .union(publicacion_fechas).union(egresado_fechas).union(profesor_fechas)).distinct().orderBy("fecha")
w = Window().partitionBy(F.lit('A')).orderBy(F.lit('A'))
fechas = fechas.withColumn("id_fecha", F.row_number().over(w))
fechas = fechas.select("id_fecha", "fecha").orderBy("id_fecha")
fechas = fechas.withColumn("year", F.year("fecha")) \
               .withColumn("semester", F.expr("IF(MONTH(fecha) <= 6, 1, 2)")) \
               .withColumn("month", F.month("fecha")) \
               .withColumn("day_of_week", F.dayofweek("fecha"))
print(fechas.head(3))
fechas.write\
    .format("bigquery")\
    .option("parentProject", bigquery_project_id)\
    .option("project", bigquery_project_id)\
    .option("dataset", bigquery_dataset_id)\
    .option("temporaryGcsBucket", "buckp")\
    .option("table", "fecha")\
    .option("enableModeCheckForSchemaFields", "false")\
    .mode("overwrite")\
    .save()

# dim_linea_trabajo
linea_trabajo = spark.read.jdbc(url=mysql_url, table="linea_trabajo").orderBy("id_linea_trabajo")
print(linea_trabajo.head(3))
linea_trabajo.write\
    .format("bigquery")\
    .option("parentProject", bigquery_project_id)\
    .option("project", bigquery_project_id)\
    .option("dataset", bigquery_dataset_id)\
    .option("temporaryGcsBucket", "buckp")\
    .option("table", "linea_trabajo")\
    .option("enableModeCheckForSchemaFields", "false")\
    .mode("overwrite")\
    .save()

# dim_profesor
profesores = (profesor.join(linea_trabajo, ["id_linea_trabajo"], how="left")
              .select(profesor.id_profesor, F.concat_ws(" ", profesor.nombre_profesor,
                                                        profesor.apellido_profesor).alias("nombre"),
                      linea_trabajo.nombre_linea_trabajo.alias("linea_investigacion"))).orderBy("id_profesor")
print(profesores.head(3))
profesores.write\
    .format("bigquery")\
    .option("parentProject", bigquery_project_id)\
    .option("project", bigquery_project_id)\
    .option("dataset", bigquery_dataset_id)\
    .option("temporaryGcsBucket", "buckp")\
    .option("table", "profesor")\
    .option("enableModeCheckForSchemaFields", "false")\
    .mode("overwrite")\
    .save()

# dim_carrera
carrera = spark.read.jdbc(url=mysql_url, table="carrera")
departamento = spark.read.jdbc(url=mysql_url, table="departamento")
facultad = spark.read.jdbc(url=mysql_url, table="facultad")
sede = spark.read.jdbc(url=mysql_url, table="sede")
carreras = carrera.join(departamento, ["id_departamento"], "left")\
                  .join(facultad, ["id_facultad"], "left")\
                  .join(sede, ["id_sede"], "left")\
                  .select(carrera.id_carrera, carrera.nombre_carrera,
                          departamento.nombre_departamento.alias("departamento"),
                          facultad.nombre_facultad.alias("facultad"), sede.nombre_sede.alias("sede"))\
                  .orderBy("id_carrera")
print(carreras.head(3))
carreras.write\
    .format("bigquery")\
    .option("parentProject", bigquery_project_id)\
    .option("project", bigquery_project_id)\
    .option("dataset", bigquery_dataset_id)\
    .option("temporaryGcsBucket", "buckp")\
    .option("table", "carrera")\
    .option("enableModeCheckForSchemaFields", "false")\
    .mode("overwrite")\
    .save()

# dim_trabajo
trabajo = spark.read.jdbc(url=mysql_url, table="empresa")
print(trabajo.head(3))
trabajo.write\
    .format("bigquery")\
    .option("parentProject", bigquery_project_id)\
    .option("project", bigquery_project_id)\
    .option("dataset", bigquery_dataset_id)\
    .option("temporaryGcsBucket", "buckp")\
    .option("table", "trabajo")\
    .option("enableModeCheckForSchemaFields", "false")\
    .mode("overwrite")\
    .save()


# fact_graduacion
modalidad_grado = spark.read.jdbc(url=mysql_url, table="modalidad_grado")
graduacion = grado.join(carrera, ["id_carrera"], "left")\
                  .join(modalidad_grado, ["id_modalidad_grado"], "left")\
                  .join(linea_trabajo, ["id_linea_trabajo"], "left")
graduacion = graduacion.join(fechas.alias("fechas_lookup"), graduacion.fecha == F.col("fechas_lookup.fecha"), "left")\
                       .select(graduacion["*"], F.col("fechas_lookup.id_fecha").alias("id_fecha"))
graduacion = graduacion.select(graduacion.id_grado.alias("id_graduacion"), graduacion.id_egresado, graduacion.id_fecha,
                               graduacion.id_carrera, graduacion.id_profesor_tutor.alias("id_profesor"),
                               graduacion.id_linea_trabajo, graduacion.papa, graduacion.grado_honor,
                               graduacion.nombre_modalidad_grado.alias("modalidad_grado"), graduacion.cantidad_semestres,
                               graduacion.nombre_carrera, graduacion.nombre_linea_trabajo.alias("linea_trabajo"),
                               graduacion.fecha).orderBy("id_graduacion")
print(graduacion.head(5))
graduacion.write\
    .format("bigquery")\
    .option("parentProject", bigquery_project_id)\
    .option("project", bigquery_project_id)\
    .option("dataset", bigquery_dataset_id)\
    .option("temporaryGcsBucket", "buckp")\
    .option("table", "graduacion")\
    .option("enableModeCheckForSchemaFields", "false")\
    .mode("overwrite")\
    .save()

# fact_actividad
actividad_actual = actividad_laboral.join(hijos, ["id_egresado"], "left")
actividad_actual = actividad_actual.join(fechas.alias("fechas_lookup"), actividad_actual.fecha_inicio == F.col("fechas_lookup.fecha"), "left")\
                       .select(actividad_actual["*"], F.col("fechas_lookup.id_fecha").alias("id_fecha"))
actividad_actual = actividad_actual.select(actividad_actual.id_actividad_laboral.alias("id_actividad_actual"),
                                           actividad_actual.id_egresado, actividad_actual.id_fecha,
                                           actividad_actual.id_linea_trabajo,
                                           actividad_actual.id_empresa.alias("id_trabajo"),
                                           actividad_actual.salario_actual, actividad_actual.hijos,
                                           actividad_actual.cargo, actividad_actual.fecha_inicio).orderBy("id_actividad_actual")
print(actividad_actual.head(3))
actividad_actual.write\
    .format("bigquery")\
    .option("parentProject", bigquery_project_id)\
    .option("project", bigquery_project_id)\
    .option("dataset", bigquery_dataset_id)\
    .option("temporaryGcsBucket", "buckp")\
    .option("table", "actividad_actual")\
    .option("enableModeCheckForSchemaFields", "false")\
    .mode("overwrite")\
    .save()

# fact_publicacion
publicacion = publicacion.join(linea_trabajo, ["id_linea_trabajo"], "left")
publicacion = publicacion.join(fechas.alias("fechas_lookup"), publicacion.fecha_publicacion == F.col("fechas_lookup.fecha"), "left") \
                         .select(publicacion["*"], F.col("fechas_lookup.id_fecha").alias("id_fecha"))
publicacion = publicacion.select(publicacion.id_publicacion, publicacion.id_egresado, publicacion.id_fecha,
                                 publicacion.id_profesor.alias("id_profesor_principal"), publicacion.id_linea_trabajo,
                                 publicacion.titulo, publicacion.publicadora.alias("publicador"),
                                 publicacion.nombre_linea_trabajo.alias("linea_trabajo")).orderBy("id_publicacion")
publicacion.write\
    .format("bigquery")\
    .option("parentProject", bigquery_project_id)\
    .option("project", bigquery_project_id)\
    .option("dataset", bigquery_dataset_id)\
    .option("temporaryGcsBucket", "buckp")\
    .option("table", "publicacion")\
    .option("enableModeCheckForSchemaFields", "false")\ 
    .mode("overwrite")\
    .save()

spark.stop()