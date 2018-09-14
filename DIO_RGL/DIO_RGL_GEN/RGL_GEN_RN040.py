# coding=utf-8
#CODIGO DE LA REGLA DE CALIDAD:RN040 (ID=40)
#GLOSA: CODIGO DEBE SER 01, 03 O 08
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#FECHA MODIFICACION: 04/06/2018
#AUTOR: FELIPE ÁVALOS DÍAZ

#CREACIÓN UDF PARA LLAMAR A FUNCIÓN EN PYSAPRK
#df_final = df_new.withColumn("RN040",UDF_RN040("<nombre columna en la cual se debe aplicar la regla>"))
#UDF_RN040 = udf(RN040,StringType())

#Definicion de la funcion que comprueba si el codigo es 03, 01 o 08
def RGL_GEN_RN040(Input):
    #validacion si el valor esta 01, 03 o 08
    if Input == "03" or Input == "01" or Input == "08":
        return 1
    else:
        return 0