# coding=utf-8
#CODIGO DE LA REGLA DE CALIDAD:RN033 (ID=33)
#GLOSA: DATO DEBE SER F O M
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#FECHA MODIFICACION: 04/06/2018
#AUTOR: FELIPE ÁVALOS DÍAZ

#CREACIÓN UDF PARA LLAMAR A FUNCIÓN EN PYSAPRK
#df_final = df_new.withColumn("RN033",UDF_RN033("<nombre columna en la cual se debe aplicar la regla>"))
#UDF_RN033 = udf(RN033,StringType())

#Definicion de la funcion que comprueba si el caracter es F o M
def RGL_GEN_RN033(Input):
    #validacion si el caracter es F o M
    if Input == "F" or Input == "M":
        return 1
    else:
        return 0