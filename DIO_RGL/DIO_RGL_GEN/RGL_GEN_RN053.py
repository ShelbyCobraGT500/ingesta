# coding=utf-8
#CODIGO DE LA REGLA DE CALIDAD:RN053 (ID=53)
#GLOSA: DATO VALIDO SI ES MENOR O IGUAL A 100
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#FECHA MODIFICACION: 07/06/2018
#AUTOR: FELIPE ÁVALOS DÍAZ

#CREACIÓN UDF PARA LLAMAR A FUNCIÓN EN PYSAPRK
#df_final = df_new.withColumn("RN053",UDF_RN053("<nombre columna en la cual se debe aplicar la regla>")
#UDF_RN053 = udf(RN053,StringType())

import re

def RGL_GEN_RN053(Input):
    if  re.match("^[0-9]{3}",Input):
        if float(Input) <= 100:
            return 1
        else:
            return 0
    else:
        return 0