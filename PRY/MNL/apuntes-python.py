# --
# --

# [cloudera@quickstart ~]$ hadoop fs -ls /user/
# [cloudera@quickstart ~]$ hadoop fs -put /home/cloudera/T133OPC_limpio.txt /user/cloudera/
# [cloudera@quickstart ~]$ pyspark --packages com.databricks:spark-csv_2.10:1.4.0

# --
# --

from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType, StringType
from pyspark.sql.functions import *
from pyspark.sql.functions import udf # user defined functions
from pyspark.sql import functions as F
import unicodedata
import datetime

# HDFS A RDD
rdd = sc.textFile('hdfs:///user/spark/T133OPC_limpio.txt') \
    .map(lambda row: row.split('\t'))

sc.addPyFile("/data1/datalake-be/main/DIO_RGL.zip")

schema = StructType([
StructField("opc_num_ope", StringType(), True),
StructField("opc_cta_cpd", StringType(), True),
StructField("opc_cod_mon", StringType(), True),
StructField("opc_ofi_des", StringType(), True),
StructField("opc_fin_pro", StringType(), True),
StructField("opc_obj_pro", StringType(), True),
StructField("opc_aut_ini", StringType(), True),
StructField("opc_mto_ori", StringType(), True),
StructField("opc_mto_orp", StringType(), True),
StructField("opc_fec_ori", StringType(), True),
StructField("opc_fec_ctb", StringType(), True),
StructField("opc_fec_dev", StringType(), True),
StructField("opc_fec_pen", StringType(), True),
StructField("opc_trm",     StringType(), True),
StructField("opc_cod_rec", StringType(), True),
StructField("opc_tip_tas", StringType(), True),
StructField("opc_tas_pac", StringType(), True),
StructField("opc_bas_tas", StringType(), True),
StructField("opc_com_tas", StringType(), True),
StructField("opc_tbl_pa1", StringType(), True),
StructField("opc_tbl_pa2", StringType(), True),
StructField("opc_cod_pen", StringType(), True),
StructField("opc_tas_pen", StringType(), True),
StructField("opc_tbl_pen", StringType(), True),
StructField("opc_car_aut", StringType(), True),
StructField("opc_cta_cgo", StringType(), True),
StructField("opc_doc_leg", StringType(), True),
StructField("opc_seg_csa", StringType(), True),
StructField("opc_mar_ope", StringType(), True),
StructField("opc_num_pag", StringType(), True),
StructField("opc_cob_seg", StringType(), True),
StructField("opc_cod_sus", StringType(), True),
StructField("opc_fec_sur", StringType(), True),
StructField("opc_fec_sum", StringType(), True),
StructField("opc_est_ope", StringType(), True),
StructField("opc_cen_cos", StringType(), True),
StructField("opc_sal_cap", StringType(), True),
StructField("opc_pla_mes", StringType(), True),
StructField("opc_dia_pag", StringType(), True),
StructField("opc_vig_tit", StringType(), True),
StructField("opc_fec_mov", StringType(), True),
StructField("opc_fec_pro", StringType(), True),
StructField("opc_ana_fer", StringType(), True),
StructField("opc_num_gra", StringType(), True),
StructField("opc_fec_ext", StringType(), True),
StructField("opc_cuo_igu", StringType(), True),
StructField("opc_fre_cal", StringType(), True),
StructField("opc_mon_cur", StringType(), True),
StructField("opc_fec_ult_act", StringType(), True),
StructField("opc_fec_teo", StringType(), True),
StructField("opc_eje_oto", StringType(), True),
StructField("opc_uni_ges", StringType(), True),
StructField("opc_bol_gar", StringType(), True),
StructField("opc_rie_ori", StringType(), True),
StructField("opc_por_fdo", StringType(), True),
StructField("opc_cod_car", StringType(), True),
StructField("opc_fec_firm", StringType(), True),
StructField("opc_lin_cred", StringType(), True),
StructField("opc_mto_nom", StringType(), True),
StructField("opc_tas_tra", StringType(), True),
StructField("opc_mto_acv", StringType(), True),
StructField("opc_mto_aca", StringType(), True),
StructField("opc_tip_avs", StringType(), True),
StructField("opc_sdo_cpl_teo", StringType(), True),
StructField("opc_sdo_org", StringType(), True),
StructField("opc_sdo_org_pes", StringType(), True),
StructField("opc_sdo_vcd", StringType(), True),
StructField("opc_sdo_vcd_pes", StringType(), True),
StructField("opc_sdo_cbr_jud", StringType(), True),
StructField("opc_sdo_cst", StringType(), True),
StructField("opc_sdo_cst_pes", StringType(), True),
StructField("opc_tip_pmt", StringType(), True),
StructField("opc_sdo_cbr_jud_pe", StringType(), True)
])

#RDD A DATA FRAME
# creamos un dataframe con la estructura dada por schema y con los datos del rdd
df = sqlContext.createDataFrame(rdd, schema)

#primer registro
df.head()

#imprime estructura
df.printSchema()

#imprime primeros 2 registros en formato columnar
df.show(2,truncate= True)

#cuenta la cantidad de registros
df.count(),rdd.count()

#cuantas columnas tenemos y el largo de cada uno de ellas
len(df.columns), df.columns

#estadisticas de un dataframe
df.describe().show()

#estadisticas de una columna en particular
df.describe('columna').show()

#Agregamos una columna nueva con un valor fijo "0" de nombre rn001
df_new = df.withColumn("rn001", lit(0))

#Agregamos una columna nueva con un valor True o False (Boolean) si columna es nula
df_new = df.withColumn("rn001", df.columna.isNull())

#OPCION1: Crear una nueva columna rn001, la cual es 0 cuando la columna sea nula
df_new = df.withColumn('rn001',F.when(df.columna.isNull() == True,0).otherwise(1))

#OPCION2: Mediante una funcion definida por usuario, creamos una rutina reutilizable que realiza la misma operacion

def rn001_completitud_sin_nulos(x):
    if not x.encode('ascii','ignore').strip() or x.encode('ascii','ignore').strip() == "NULL":
        return 0 # si esta vacio "", blanco "    " o nulo u('NULL')
    else:
        return 1

udf_rn001_completitud_sin_nulos_o_vacios = udf(rn001_completitud_sin_nulos_o_vacios, StringType()) # if the function returns an int o string StringType()

df_final = df_new.withColumn("rn001", udf_rn001_completitud_sin_nulos("<nombre columna en la cual se debe aplicar la regla>"))  # generamos un nuevo data frame

#Funcion que define la posible fecha valida
def rn002_fecha_valida(x): #posible funcion

    date.isoformat(x.encode('ascii','ignore').strip()) #transforma fecha en formato Y-m-D
        
udf_rn002_fecha_valida = udf(rn002_fecha_valida, StringType())

df_new = df.withColumn("rn002", udf_rn002_fecha_valida("<nombre columna en la cual se debe aplicar la regla>"))  # generamos un nuevo data frame

#funcion rn003_mayor_o_igual_0 define so es mayor o igual a cero
def rn003_mayor_o_igual_0(x):
    #Evalua si el caracter trae alguna letra o simbolo si es asi pasara al else
    if x.isdigit():
        return 1 # en caso de ser mayor o igual a cer
    else:
        return 0 #en caso contrario

udf_rn003_mayor_o_igual_0 = udf(rn003_mayor_o_igual_0, StringType())

df_final = df_new.withColumn("rn003",udf_rn003_mayor_o_igual_0(<nombre columna en la cual se debe aplicar la regla>"))  # generamos un nuevo data frame

#Funcion que compara si la fecha es mayor a 1900/01/01
def rn004_distinto_a_1900_01_01(year,month,day):
    date = year + "/" + month + "/"+ day
    final_date = datetime.strptime(date,'%Y/%m/%d')
    if str(final_date)>"1900/01/01":
        return 1
    else:
        return 0
 
 udf_rn004_distinto_a_1900_01_01 = udf(rn004_distinto_a_1900_01_01,StringType())
 
 df_new = f.withColumn("rn004",udf_rn004_distinto_a_1900_01_01(<nombre columna en la cual se debe aplicar la regla>"))  # generamos un nuevo data frame
    

# DATA FRAME HACIA HDFS

# OPCION 1
df_new.map(lambda row: str(row[0]) + "\t" + str(row[1])).saveAsTextFile("hdfs:///user/spark/T113OPC_GOB") # directorio nuevo en el cual se exporta el dataframe

# DATA FRAME HACIA SISTEMA ARCHIVOS LINUX

# OPCION 1 (requiere iniciar pyspark con package databricks)
df_new.write.format("com.databricks.spark.csv").save("/user/cloudera/") # directorio nuevo en el cual se exporta el dataframe
# demora media hora con 4 gigabytes de ram, y archivo de 4 millones de registros (archivo de 2 gb)

# DATA FRAME HACIA RDD

rdd_tuple = df_new.rdd.map(tuple) # Lo guarda como tupla[]

rdd_list = df_new.rdd.map(list) # Lo guarda como lista()

# RDD hacia HDFS

# OPCION 1
rdd_tuple.saveAsTextFile("hdfs:///user/cloudera/rdd_tuple"); #Directorio nuevo donde rdd sera guardado en sistema hdfs
rdd_list.saveAsTextFile("hdfs:///user/cloudera/rdd_list");

# OPCION 2
rdd_tuple.saveAsObjectFile("hdfs:///user/cloudera/rdd_tuple"); #Directorio nuevo donde rdd sera guardado en sistema hdfs
rdd_list.saveAsObjectFile("hdfs:///user/cloudera/rdd_list"); #Directorio nuevo donde rdd sera guardado en sistema hdfs

# --
# FUERA DE CONSOLA SPARK DESDE HDFS HACIA SISTEMA DE ARCHIVOS LINUX
# ---

# hadoop fs -get /user/cloudera/T133OPC_muestra/ /home/cloudera/Downloads/
