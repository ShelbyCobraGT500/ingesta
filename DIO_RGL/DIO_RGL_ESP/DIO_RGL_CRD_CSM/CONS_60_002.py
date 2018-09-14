# coding=utf-8
#CODIGO DE LA REGLA DE CALIDAD:CONS_60_002
#GLOSA: VALOR DISTINTO A 19000101
#COMPLEJIDAD: BAJA
#PROBADO EN Pyspark
#FECHA MODIFICACION: 01/08/2018
#AUTOR: MTS

#CREACIÓN UDF PARA LLAMAR A FUNCIÓN EN PYSAPRK
#df_final = df_new.withColumn("CONS_60_002",UDF_CONS_60_002("<nombre columna en la cual se debe aplicar la regla>"))
#UDF_CONS_60_002 = udf(CONS_60_002,StringType())
import datetime
from datetime import datetime
from datetime import date
def CONS_60_002(Input):
  formato_fecha = '%Y%m%d'
  try:
    if datetime.strptime(Input,formato_fecha) and datetime.strptime(Input,formato_fecha) == datetime.strptime("19000101",formato_fecha):
        return 0
    else:
        return 1
  except ValueError:
      return 0