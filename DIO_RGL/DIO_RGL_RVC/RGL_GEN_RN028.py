# coding=utf-8
#CODIGO DE LA REGLA DE CALIDAD:RN028 (ID=28)
#GLOSA: DATO DEBE SER EXPRESADO EN CARACTERES
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#PROBADO EN Pyspark
#FECHA MODIFICACION: 04/06/2018
#AUTOR: FELIPE ÁVALOS DÍAZ

#CREACIÓN UDF PARA LLAMAR A FUNCIÓN EN PYSAPRK
#df_final = df_new.withColumn("RN028",UDF_RN028("<nombre columna en la cual se debe aplicar la regla>"))
#UDF_RN028 = udf(RN028,StringType())

#Definicion de la funcion que comprueba si el caracter es alfanumerico de largo 3
def RGL_GEN_RN028(Input):
    #validacion si el caracter es alfanumerico y de largo 3
    if Input.isalnum():
        return 1
    else:
        return 0