# coding=utf-8
#CODIGO DE LA REGLA DE CALIDAD:RN124 (ID=124)
#GLOSA: CODIGO DEBE SER 1 O 2
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#FECHA MODIFICACION: 04/06/2018
#AUTOR: FELIPE ÁVALOS DÍAZ

#CREACIÓN UDF PARA LLAMAR A FUNCIÓN EN PYSAPRK
#df_final = df_new.withColumn("RN124",UDF_RN124("<nombre columna en la cual se debe aplicar la regla>"))
#UDF_RN050 = udf(RN124,StringType())

#Definicion de la funcion que comprueba si el valor es 1 o 2
def RGL_GEN_RN124(Input):
    #validacion si el valor es 1 o 2
    if Input == "1" or Input == "2":
        return 1
    else:
        return 0