# coding=utf-8
#CODIGO DE LA REGLA DE CALIDAD:RN030 (ID=30)
#GLOSA: DATO DEBE SER DISTINTO DE 00
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#PROBADO EN Pyspark
#FECHA MODIFICACION: 04/06/2018
#AUTOR: FELIPE ÁVALOS DÍAZ

#CREACIÓN UDF PARA LLAMAR A FUNCIÓN EN PYSAPRK
#df_final = df_new.withColumn("RN030",UDF_RN030("<nombre columna en la cual se debe aplicar la regla>"))
#UDF_RN030 = udf(RN030,StringType())

#Definicion: funcion que evalua si el dato no contiene decimales
def RGL_GEN_RN030(Input):
    #Evalua si el dato es diferente a '00'
    try:
        if Input != '00' and len(Input) == 2:
            return 1 # si el dato es distinto a '00'
        else:
            return 0 #en caso contrario
    except ValueError:
        return 0