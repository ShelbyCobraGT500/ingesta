# coding=utf-8
#CODIGO DE LA REGLA DE CALIDAD:RN047 (ID=47)
#GLOSA: RUT (SINACOFI) ES VALIDO SI EL DATO ESTA ENTRE 20.050.000 Y 30 M (SE ASUME RUT SIN DV)
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#FECHA MODIFICACION: 07/06/2018
#AUTOR: FELIPE ÁVALOS DÍAZ

#CREACIÓN UDF PARA LLAMAR A FUNCIÓN EN PYSAPRK
#df_final = df_new.withColumn("RN047",UDF_RN047("<nombre columna en la cual se debe aplicar la regla>")
#UDF_RN047 = udf(RN047,StringType())

import re
#DEFINICION DE LA FUNCION QUE COMPRUEBA SI DATO ESTA ENTRE 20.050.000 Y 30 M (SE ASUME RUT SIN DV)
def RGL_GEN_RN047(Input):
    #validacion si rut es de largo 9 y el dv es entre 0 
    if re.match("^[0-9]{9}",Input):
        if int(Input) >= 20050000 and int(Input) <= 30000000:
            return 1
        else:
            return 0
    else:
        return 0