# encoding=utf-8
#CODIGO DE LA REGLA DE CALIDAD:RN052 (ID=052)
#GLOSA: VALIDACION QUE EL DATO NO INCLUYA PUNTOS
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#FECHA MODIFICACION: 04/06/2018
#AUTOR: FELIPE ÁVALOS DÍAZ

#CREACIÓN UDF PARA LLAMAR A FUNCIÓN EN PYSAPRK
#df_final = df_new.withColumn("RN052",UDF_RN052("<nombre columna en la cual se debe aplicar la regla>"))
#UDF_RN052 = udf(RN052,StringType())

import re

#definición de la funcion
def RGL_GEN_RN052(Input):
    if re.search("\.",Input):
        return 0
    else:
        return 1

    
