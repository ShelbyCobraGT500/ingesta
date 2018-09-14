# coding=utf-8
#CODIGO DE LA REGLA DE CALIDAD:CONS_06_005
#GLOSA: COMPARA DOS COLUMNAS DE FECHA DE LA MISMA TABLA
#COMPLEJIDAD: BAJA
#PROBADO EN Pyspark
#FECHA MODIFICACION: 10/08/2018
#AUTOR: MTS

#CREACIÓN UDF PARA LLAMAR A FUNCIÓN EN PYSAPRK
#df_final = df_new.withColumn("CONS_6_005",UDF_CONS_6_005("<nombre columna en la cual se debe aplicar la regla>"))
#UDF_CONS_6_005 = udf(CONS_6_005,StringType1(), StringType2())
import datetime
from datetime import datetime
from datetime import date
def CONS_6_005(Input1, Input2):
  formato_fecha = '%Y%m%d'
  try:
    if (datetime.strptime(Input1,formato_fecha) and datetime.strptime(Input2,formato_fecha)):
        if (Input1 > Input2):
            return 1
        else:
            return 0
    else:
        return 0
  except:
      return 0