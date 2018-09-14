# coding=utf-8
#CODIGO DE LA REGLA DE CALIDAD:RN003 (ID=3)
#GLOSA: NO CONTENGA ESPACIOS == NO PUEDE VENIR EN BLANCO
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#PROBADO EN Pyspark 2.x
#FECHA MODIFICACION: 30/05/2018
#AUTOR: FELIPE AVALOS
#EJEMPLO DE CREACION DE UDF
#df_final = df_new.withColumn("RN003",UDF_RN003(<nombre columna en la cual se debe aplicar la regla>"))
#UDF_RN003 = udf(RN003,StringType())

import re

#Definicion: funcion que evalua si el dato NO contiene espacios.
def RGL_GEN_RN003(Input):
  try:
    #condicional que evalua si cumple con los parametros establecidos, transforma el dato de Unicode a Ascii
    if re.search(" ",Input):
      return 0 # Si tiene espacio retorna 0
    else:
      return 1 # en caso contrario
  except:
    return 0