# coding=utf-8
#GLOSA: Bajo 10 millones debe tener ceros a la izquierda
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#PROBADO EN Pyspark 2.x
#FECHA MODIFICACION: 18/07/2018
#AUTOR: MIGUEL CHOQUE HERRERA
#EJEMPLO DE CREACION DE UDF
#df_final = df_new.withColumn("RN003",UDF_RN003(<nombre columna en la cual se debe aplicar la regla>"))
#UDF_RN003 = udf(RN003,StringType())

def RGL_ORN_RN023(Input):
#ESTABLECE EL FORMATO DE FECHA QUE VALIDARÁ
  try:
      fill = Input
      if int(Input) < 10000000 and Input == fill.zfill(8):
           return 1
      else:
           return 0
  except ValueError:
      return 0