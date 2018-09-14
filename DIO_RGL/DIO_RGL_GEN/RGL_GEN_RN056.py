#coding=utf-8
#CODIGO DE LA REGLA DE CALIDAD:RN056 (ID=056)
#GLOSA: VALIDA FECHA EN FORMATO DDMMYYYY
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#FECHA MODIFICACION: 06/06/2018
#AUTOR: MIGUEL CHOQUE HERRERA

#CREACIÓN UDF PARA LLAMAR A FUNCIÓN EN PYSAPRK
#df_final = df_new.withColumn("RN056",UDF_RN048("<nombre columna en la cual se debe aplicar la regla>"))
#UDF_RN048 = udf(RN056,StringType())

import datetime
from datetime import datetime
from datetime import date

#definición de la funcion que evalua si la persona es mayor a 18 años a partir de la fecha de creacion del archivo evaluado
def RGL_GEN_RN056(Input):
#ESTABLECE EL FORMATO DE FECHA QUE VALIDARÁ
  formato_fecha = '%d%m%Y'
  try:
      if datetime.strptime(Input,formato_fecha):
          #SI EL FORMATO CORRESPONDE AL INDICADO, RETORNA 1
          return 1
      else:
          return 0
  except ValueError:
      return 0