# encoding=utf-8

############################### INICIO LLAMADO FUNCIONES ###############################
from time import time
tiempo_inicial = time()
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import DoubleType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StringType
from pyspark.sql.functions import *
from pyspark.sql.functions import udf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import SQLContext
from pyspark import sql
import os
#sc.addPyFile("/data1/desarrollo/ejecutables/DESA_CTL/ingesta/CLT_RGL/Filtro.zip")
from Filtro import Filtro
############################### TERMINO LLAMADO FUNCIONES ###############################

############################### INICIO CREACION SPARK CONTEXT Y DAF ###############################
spark = SparkSession \
    .builder \
    .appName("Ejecucion Control De Reglas") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
DAF_CONS_RMS = spark.read.option("delimiter",";").csv("hdfs:///data1/desarrollo/hadoop/postcontrol/Consolidado_Resumen.csv", header=True)
DAF_CONT_RGL = spark.read.option("delimiter",";").csv("hdfs:///data1/desarrollo/hadoop/postcontrol/Control_de_Reglas.csv", header=True)
DAF_CONT_ITF = spark.read.option("delimiter",";").csv("hdfs:///data1/desarrollo/hadoop/postcontrol/Control_Interfaces_Activas.csv", header=True)
############################### TERMINO CREACION SPARK CONTEXT Y DAF ###############################

UDF_FILTRO = udf(Filtro,StringType())
UDF_FILTRO_2 = udf(Filtro_2,StringType())

DAF_CONS_RMS.createOrReplaceTempView("TBL_Resumen")
DAF_CONT_RGL.createOrReplaceTempView("TBL_Control")

DAF_CONT_RGL_TMP = spark.sql(""" \
select TBL_Control.Dominio, TBL_Resumen.Periodo, TBL_Resumen.Id_Regla, TBL_Control.EDC, TBL_Control.Tabla, TBL_Control.Regla, TBL_Control.Estado, TBL_Resumen.TotalRegistros, TBL_Resumen.TotalAprobados, \
(case when ltrim(rtrim(TBL_Resumen.Id_Regla)) is not null and TBL_Control.Estado = 'Ejecutada' then 'OK' else 'Error (1)' end) as RGL_con_RSM \
from TBL_Control \
left join TBL_Resumen \
on TBL_Resumen.Id_Regla = TBL_Control.Regla \
order by TBL_Control.EDC ASC \
""")
DAF_CONT_RGL = DAF_CONT_RGL_TMP

DAF_CONT_RGL_TMP= DAF_CONT_RGL.withColumn("Id_Regla", when(col("Id_Regla").isNull(),'').otherwise(col("Id_Regla")))

DAF_CONT_RGL = DAF_CONT_RGL_TMP

DAF_CONT_RGL_TMP = DAF_CONT_RGL.withColumn("RGL_con_RSM",UDF_FILTRO("Id_Regla","Estado","RGL_con_RSM"))

DAF_CONT_RGL = DAF_CONT_RGL_TMP

DAF_CONT_RGL.createOrReplaceTempView("TBL_Control")
DAF_CONT_ITF.createOrReplaceTempView("TBL_Control_ITF")

DAF_CONT_RGL_TMP = spark.sql(""" \
select TBL_Control.Dominio, TBL_Control.Periodo, TBL_Control.Id_Regla, TBL_Control.EDC, TBL_Control.Tabla, TBL_Control.Regla, TBL_Control.Estado, TBL_Control.TotalRegistros, TBL_Control.TotalAprobados, TBL_Control.RGL_con_RSM, TBL_Control_ITF.`Aprueba R0`\
from TBL_Control
left join TBL_Control_ITF \
on TBL_Control.Tabla = TBL_Control_ITF.Nombre \
order by TBL_Control.EDC ASC \
""")

DAF_CONT_RGL = DAF_CONT_RGL_TMP

DAF_CONT_RGL.createOrReplaceTempView("TBL_Control")

DAF_CONT_RGL_TMP = spark.sql(""" \
select *, \
(case when ltrim(rtrim(TBL_Control.RGL_con_RSM)) = 'Error (1)' and ltrim(rtrim(TBL_Control.`Aprueba R0`)) = 'NO' then 'OK' else TBL_Control.RGL_con_RSM end) as RGL_Tiene_RSM \
from TBL_Control
order by TBL_Control.EDC ASC \
""")

DAF_CONT_RGL = DAF_CONT_RGL_TMP

DAF_CONT_RGL_TMP = DAF_CONT_RGL.select("Dominio","EDC","Id_Regla","Periodo","Tabla","Regla","Estado","TotalRegistros","TotalAprobados","RGL_Tiene_RSM","Aprueba R0")

DAF_CONT_RGL = DAF_CONT_RGL_TMP

DAF_CONT_RGL.createOrReplaceTempView("TBL_Control")

DAF_CONT_RGL_TMP = spark.sql(""" \
select *, \
(case when ltrim(rtrim(TBL_Control.Estado)) = 'En prueba' then 'OK' else TBL_Control.RGL_Tiene_RSM end) as RGL_con_RSM \
from TBL_Control
order by TBL_Control.EDC ASC \
""")

DAF_CONT_RGL = DAF_CONT_RGL_TMP

DAF_CONT_RGL_TMP = DAF_CONT_RGL.select("Dominio","EDC","Id_Regla","Periodo","Tabla","Regla","Estado","TotalRegistros","TotalAprobados","RGL_con_RSM","Aprueba R0")

DAF_CONT_RGL = DAF_CONT_RGL_TMP

DAF_CONS_RMS.createOrReplaceTempView("TBL_Resumen")
DAF_CONT_RGL.createOrReplaceTempView("TBL_Control")

DAF_CONS_RMS_TMP = spark.sql(""" \
select TBL_Resumen.*, TBL_Control.Regla, TBL_Control.Estado, \
(case when ltrim(rtrim(TBL_Control.Regla)) is not null and  TBL_Control.Estado = 'Ejecutada' then 'OK' else 'Error (2)' end) as RSM_con_CTL \
from TBL_Resumen \
left join TBL_Control \
on TBL_Resumen.Id_Regla = TBL_Control.Regla \
order by TBL_Resumen.Cod_Edc DESC \
""")

DAF_CONS_RMS = DAF_CONS_RMS_TMP

DAF_CONS_RMS.createOrReplaceTempView("TBL_Resumen")

DAF_CONS_RMS_TMP = spark.sql(""" \
select *,
(case when ltrim(rtrim(TBL_Resumen.Cod_Edc)) in(substring(TBL_Resumen.Id_Regla,1,length(TBL_Resumen.Cod_Edc))) then 'OK' else 'Error (3)' end) as RGL_contiene_EDC \
from TBL_Resumen \
order by TBL_Resumen.Cod_Edc DESC \
""")

DAF_CONS_RMS = DAF_CONS_RMS_TMP

DAF_CONS_RMS.coalesce(1).write.save(path='/data1/OUT/Control_de_Resumen',format='csv', mode='overwrite', delimiter=";", header='true')
DAF_CONT_RGL.coalesce(1).write.save(path='/data1/OUT/Control_de_Reglas',format='csv', mode='overwrite', delimiter=";", header='true')

os.system('hadoop fs -get /data1/OUT/Control_de_Resumen /data1/desarrollo/edge/control')
os.system('hadoop fs -get /data1/OUT/Control_de_Reglas /data1/desarrollo/edge/control')

os.system('bzip2 -d /data1/desarrollo/edge/control/Control_de_Resumen/part-*.csv.bz2')
os.system('bzip2 -d /data1/desarrollo/edge/control/Control_de_Reglas/part-*.csv.bz2')

os.system('mv /data1/desarrollo/edge/control/Control_de_Resumen/part-*.csv /data1/desarrollo/edge/control/Control_de_Resumen_Final.csv')
os.system('mv /data1/desarrollo/edge/control/Control_de_Reglas/part-*.csv /data1/desarrollo/edge/control/Control_de_Reglas_final.csv')

os.system('rm -R /data1/desarrollo/edge/control/Control_de_Resumen/')
os.system('rm -R /data1/desarrollo/edge/control/Control_de_Reglas/')

tiempo_final = time()
tiempo_ejecucion = tiempo_final - tiempo_inicial
print 'El tiempo de ejecucion fue:', tiempo_ejecucion/60, 'Minutos'
print("CONTROLES EJECUTADOS CORRECTAMENTE")
quit()
