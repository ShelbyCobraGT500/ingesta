# coding=utf-8
#CÓDIGO DE LA REGLA DE CALIDAD: CLI_1_004
#GLOSA: NO DEBE CONTENER NULOS YA SEA NULO ASCII "\x00" O NULO SQL "NULL"
#COMPLEJIDAD: BAJA (porque aplica a solo un EDC)
#PROBADO EN Pyspark 2.x
#FECHA MODIFICACIÓN: 30/05/2018
#AUTOR: FELIPE ÁVALOS

#INSTRUCCIONES DE COMO CREAR UDF PARA PYSPARK
#df_final = df_new.withColumn("RN001", UDF_RN001("<nombre columna en la cual se debe aplicar la regla>"))
#UDF_RN001 = udf(RN001,StringType())

import re

#definición de la función que evalua si la columna contiene NULOS
def CLI_1_004(Input,Input1):
  #condicional que evaluá si cumple con los parametros establecidos, transforma el dato de Unicode a Ascii
  if (re.search("\x00",Input) or re.search("NULL",Input.upper())) and (re.search("\x00",Input1) or re.search("NULL",Input1.upper())):
    return 0 # si esta nulo ('NULL')"
  else:
    return 1 # en caso contrario