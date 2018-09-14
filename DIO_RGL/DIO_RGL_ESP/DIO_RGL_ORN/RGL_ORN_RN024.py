# coding=utf-8
#GLOSA: Puede tener solo dos valores posibles 01 o 02 (interno o externo)
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#PROBADO EN Pyspark 2.x
#FECHA MODIFICACION: 18/07/2018
#AUTOR: MIGUEL CHOQUE HERRERA
#EJEMPLO DE CREACION DE UDF
#df_final = df_new.withColumn("RN003",UDF_RN003(<nombre columna en la cual se debe aplicar la regla>"))
#UDF_RN003 = udf(RN003,StringType())

def RGL_ORN_RN024(Input):
  try:
      if Input == '01' or Input == '02':
           return 1
      else:
           return 0
  except ValueError:
      return 0