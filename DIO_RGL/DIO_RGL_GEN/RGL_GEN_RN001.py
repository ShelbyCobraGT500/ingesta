# coding=utf-8
#CÓDIGO DE LA REGLA DE CALIDAD: RN001
#GLOSA: NO DEBE CONTENER NULOS YA SEA NULO ASCII "\x00" O NULO SQL "NULL"
#COMPLEJIDAD: BAJA (porque aplica a solo un EDC)
#PROBADO EN Pyspark 2.x
#FECHA MODIFICACIÓN: 18/07/2018
#AUTOR: FELIPE ÁVALOS

#INSTRUCCIONES DE COMO CREAR UDF PARA PYSPARK
#df_final = df_new.withColumn("RN001", UDF_RN001("<nombre columna en la cual se debe aplicar la regla>"))
#UDF_RN001 = udf(RN001,StringType())

import re

#definición de la función que evalua si la columna contiene NULOS
def RGL_GEN_RN001(Input):
  try:
    if re.search("\x00",Input):
      return 0 # si esta nulo ('NULL')"
    else:
      return 1 # en caso contrario
  except:
    return 0