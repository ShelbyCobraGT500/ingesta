# coding=utf-8
#CODIGO DE LA REGLA DE CALIDAD:RN132 (ID=132)
#Glosa: dv entre 0 y 9 o k
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#FECHA MODIFICACION: 04/06/2018
#AUTOR: FELIPE ÁVALOS DÍAZ

#CREACIÓN UDF PARA LLAMAR A FUNCIÓN EN PYSAPRK
#df_final = df_new.withColumn("RN132",UDF_RN132("<nombre columna en la cual se debe aplicar la regla>"))
#UDF_RN127 = udf(RN132,StringType())

#Definicion de la funcion que comprueba si el valor es 1, 3 o 4
def RGL_GEN_RN132(dv):
    #validacion Dv es entre 0 o 9 o k
    if '0' <= dv <= '9' or dv == 'k':
        return 1
    else:
        return 0