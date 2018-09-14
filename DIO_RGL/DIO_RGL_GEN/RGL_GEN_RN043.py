# coding=utf-8
#CODIGO DE LA REGLA DE CALIDAD:RN043 (ID=43)
#GLOSA: VALOR ENTRE [0-9]
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#FECHA MODIFICACION: 07/06/2018
#AUTOR: FELIPE ÁVALOS DÍAZ

#CREACIÓN UDF PARA LLAMAR A FUNCIÓN EN PYSAPRK
#df_final = df_new.withColumn("RN043",UDF_RN043("<nombre columna en la cual se debe aplicar la regla>"))
#UDF_RN043 = udf(RN043,StringType())

#Definicion de la funcion que comprueba si el valor esta entre 0 y 9
def RGL_GEN_RN043(Input):
    try:
        #validacion si el valor esta entre 0 o 9
        if Input == "0" or Input == "9":
            return 1
        else:
            return 0
    except ValueError:
        return 0
