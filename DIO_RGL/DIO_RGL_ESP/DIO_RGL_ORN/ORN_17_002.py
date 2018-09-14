# coding=utf-8
#CODIGO DE LA REGLA DE CALIDAD:ORN_17_002
#GLOSA: STD_DT_START ON DEBE SER MAYOR AL AÑO EN EJERCICIO (2018)
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
#DEFINICIÓN DE LA FUNCION QUE EVALUA SI EL TERMINO ES S (SI ES SI) O N (SI ES NO)
def ORN_17_002(Input,fec_eje):
  formato_fecha = '%d%m%Y'
  try:
    if datetime.strptime(Input,formato_fecha) and datetime.strptime(fec_eje,formato_fecha):
        entrada = datetime.strptime(Input,formato_fecha)
        fecha_ejercicio = datetime.strptime(fec_eje,formato_fecha)
        if entrada < fecha_ejercicio:
            return 1
        else: 
            return 0  
    else:
        return 0
  except ValueError:
      return 0