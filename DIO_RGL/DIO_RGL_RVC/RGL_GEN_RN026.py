# coding=utf-8
#CODIGO DE LA REGLA DE CALIDAD:RN026 (ID=26)
#GLOSA: DATO DEBE SER ALFANUMERICO DE LARGO 5
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#PROBADO EN Pyspark
#FECHA MODIFICACION: 01/06/2018
#AUTOR: MARCO TORO

#CREACIÓN UDF PARA LLAMAR A FUNCIÓN EN PYSAPRK
#df_final = df_new.withColumn("RN026",UDF_RN026("<nombre columna en la cual se debe aplicar la regla>"))
#UDF_RN026 = udf(RN026,StringType())
import re
#Definicion de la funcion que comprueba si el caracter es alfanumerico de largo 5
def RGL_GEN_RN026(Input):
    #validacion si el caracter es alfanumerico y de largo 5, se actualiza la validación
    #con una expresión regular (04/06/2018)
    if re.match("^[a-zA-Z0-9]{5}",Input):
        return 1
    else:
        return 0