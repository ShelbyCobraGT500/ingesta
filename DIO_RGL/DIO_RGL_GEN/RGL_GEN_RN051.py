# encoding=utf-8
#CODIGO DE LA REGLA DE CALIDAD:RN051 (ID=051)
#GLOSA: VALIDA SI EN EL DATO INGRESADO VIENE ALGÚN GUION
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#FECHA MODIFICACION: 04/06/2018
#AUTOR: FELIPE ÁVALOS DÍAZ

#CREACIÓN UDF PARA LLAMAR A FUNCIÓN EN PYSAPRK
#df_final = df_new.withColumn("RN051",UDF_RN051("<nombre columna en la cual se debe aplicar la regla>"))
#UDF_RN051 = udf(RN051,StringType())

import re

#definición de la funcion
def RGL_GEN_RN051(Input):
    if re.search("\-",Input):
        return 0
    else:
        return 1
