# coding=utf-8
#CODIGO DE LA REGLA DE CALIDAD:RN031 (ID=31)
#GLOSA: DATO DEBE SER DISTINTO DE 000
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#FECHA MODIFICACION: 04/06/2018
#AUTOR: FELIPE ÁVALOS DÍAZ

#CREACIÓN UDF PARA LLAMAR A FUNCIÓN EN PYSAPRK
#df_final = df_new.withColumn("RN031",UDF_RN031("<nombre columna en la cual se debe aplicar la regla>"))
#UDF_RN031 = udf(RN031,StringType())

#Definicion: funcion que evalua si el dato no contiene decimales
def RGL_GEN_RN031(Input):
    #Evalua si el dato es diferente a '000'
    try:
        if Input != '000':
            return 1 # si el dato es distinto de '000'
        else:
            return 0 #en caso contrario
    except ValueError:
        return 0