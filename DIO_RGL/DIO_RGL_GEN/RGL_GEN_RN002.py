# coding=utf-8
#CÓDIGO DE LA REGLA DE CALIDAD: RN002
#GLOSA: NO PUEDE VENIR EN BLANCO
#COMPLEJIDAD: BAJA (porque aplica a solo un EDC)
#PROBADO EN Pyspark 2.x
#FECHA MODIFICACIÓN: 17/07/2018
#AUTOR: MIGUEL CHOQUE

#INSTRUCCIONES DE COMO CREAR UDF PARA PYSPARK
#df_final = df_new.withColumn("RN001", UDF_RN001("<nombre columna en la cual se debe aplicar la regla>"))
#UDF_RN001 = udf(RN001,StringType())

def RGL_GEN_RN002(Input): 
  try:
    Input=Input.strip()
    if Input:
      return 1 # campo correcto
    else:
      return 0 # en caso contrario
  except:
    return 0