# coding=utf-8
#CODIGO DE LA REGLA DE CALIDAD:RN125 (ID=125)
#GLOSA: CODIGO DEBE SER 1, 3 O 4
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#FECHA MODIFICACION: 04/06/2018
#AUTOR: FELIPE ÁVALOS DÍAZ

#CREACIÓN UDF PARA LLAMAR A FUNCIÓN EN PYSAPRK
#df_final = df_new.withColumn("RN125",UDF_RN125("<nombre columna en la cual se debe aplicar la regla>"))
#UDF_RN125 = udf(RN125,StringType())

#Definicion de la funcion que comprueba si el valor es 1, 3 o 4
def RGL_GEN_RN125(Input):
    try:
        #validacion si el valor es 1, 3 o 4
        valor = int(Input)
        if valor  == 1 or valor == 3 or valor == 4:
            return 1
        else:
            return 0
    except:
        return 0