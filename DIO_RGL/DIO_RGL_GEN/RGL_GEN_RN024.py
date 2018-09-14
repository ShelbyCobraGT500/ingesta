# encoding=utf-8
#CODIGO DE LA REGLA DE CALIDAD:RN024 (ID=24)
#GLOSA: DATO DEBE SER ALFANUMERICO
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#PROBADO EN Pyspark
#FECHA MODIFICACION: 16/06/2018
#AUTOR: MIGUEL CHOQUE HERRERA

#CREACIÓN UDF PARA LLAMAR A FUNCIÓN EN PYSAPRK
#df_final = df_new.withColumn("RN024",UDF_RN024("<nombre columna en la cual se debe aplicar la regla>"))
#UDF_RN024 = udf(RN024,StringType())

import re

#Definicion de la funcion que comprueba si el caracter es alfanumerico 
def RGL_GEN_RN024(Input):
    #validacion si el caracter es alfanumerico
    if re.match("^[a-zA-Z0-9]",Input):
        return 1
    else:
        return 0