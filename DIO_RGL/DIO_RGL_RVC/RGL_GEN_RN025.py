# coding=utf-8
#CODIGO DE LA REGLA DE CALIDAD:RN025 (ID=25)
#GLOSA: DATO DEBE SER ALFANUMERICO DE LARGO 4
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#PROBADO EN Pyspark
#FECHA MODIFICACION: 01/06/2018
#AUTOR: FELIPE ÁVALOS DÍAZ

#CREACIÓN UDF PARA LLAMAR A FUNCIÓN EN PYSAPRK
#df_final = df_new.withColumn("RN025",UDF_RN025("<nombre columna en la cual se debe aplicar la regla>"))
#UDF_RN025 = udf(RN025,StringType())

#Definicion de la funcion que comprueba si el caracter es alfanumerico de largo 4
def RGL_GEN_RN025(Input):
    #validacion si el caracter es alfanumerico y de largo 5
    if Input.isalnum() and len(Input)==4:
        return 1
    else:
        return 0