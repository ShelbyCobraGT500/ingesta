# coding=utf-8
#CODIGO DE LA REGLA DE CALIDAD:RN018
#GLOSA: VALIDA FECHA EN FORMATO AAAAMMDD
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#PROBADO EN Pyspark 2.x
#FECHA MODIFICACION: 13/07/2018
#AUTOR: MIGUEL CHOQUE HERRERA
#EJEMPLO DE CREACION DE UDF
#df_final = df_new.withColumn("RN003",UDF_RN003(<nombre columna en la cual se debe aplicar la regla>"))
#UDF_RN003 = udf(RN003,StringType())

from datetime import datetime

def RGL_ORN_RN018(input):
#ESTABLECE EL FORMATO DE FECHA QUE VALIDARÁ
  formato_fecha = '%Y%m%d'
  try:
      if datetime.strptime(input,formato_fecha):
          #SI EL FORMATO CORRESPONDE AL INDICADO, RETORNA 1
          return 1
      else:
          return 0
  except ValueError:
      return 0