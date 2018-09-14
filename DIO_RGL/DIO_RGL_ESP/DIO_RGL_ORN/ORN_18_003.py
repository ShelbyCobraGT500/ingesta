# coding=utf-8
#CODIGO DE LA REGLA DE CALIDAD:ORN_17_002
#GLOSA: STD_DT_END DEBE SER 01014000
#COMPLEJIDAD: BAJA
#PROBADO EN Pyspark
#FECHA MODIFICACION: 25/07/2018
#AUTOR: MIGUEL CHOQUE HERRERA

#CREACIÓN UDF PARA LLAMAR A FUNCIÓN EN PYSAPRK
#df_final = df_new.withColumn("ORN_RN007",UDF_ORN_RN007("<nombre columna en la cual se debe aplicar la regla>"))
#UDF__ORN_RN007 = udf(ORN_RN007,StringType())
import datetime
from datetime import datetime
from datetime import date
def ORN_18_003(Input):
  formato_fecha = '%d%m%Y'
  try:
    if datetime.strptime(Input,formato_fecha) and datetime.strptime(Input,formato_fecha) == datetime.strptime("01014000",formato_fecha):
        return 1
    else:
        return 0
  except ValueError:
      return 0